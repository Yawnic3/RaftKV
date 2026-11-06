package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"raftkv/internal/client"
	"raftkv/internal/cluster"
	"raftkv/internal/config"
	"raftkv/internal/state"
)

type testCluster struct {
	t       *testing.T
	cfg     map[string]config.Config
	nodes   map[string]*cluster.Server
	done    map[string]chan error
	mu      sync.RWMutex
	blocked map[string]bool
	client  *client.Client
}

func newCluster(t *testing.T) *testCluster {
	t.Helper()
	c := &testCluster{
		t: t, cfg: make(map[string]config.Config),
		nodes: make(map[string]*cluster.Server), done: make(map[string]chan error),
		blocked: make(map[string]bool),
	}
	listeners := make(map[string]net.Listener)
	peers := make(map[string]string)
	for _, id := range []string{"a", "b", "c"} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id] = l
		peers[id] = "http://" + l.Addr().String()
	}
	for id, l := range listeners {
		c.cfg[id] = config.Config{ID: id, Listen: l.Addr().String(), DataDir: filepath.Join(t.TempDir(), id), Peers: peers,
			ElectionMinMS: 450, ElectionMaxMS: 800, HeartbeatMS: 50,
			RPCTimeoutMS: 200, RequestTimeoutMS: 1500, SnapshotThreshold: 8,
		}
		c.start(id, l)
	}
	seeds := []string{peers["a"], peers["b"], peers["c"]}
	var err error
	c.client, err = client.New(seeds)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.client.Close()
		for id := range c.nodes {
			c.stop(id)
		}
	})
	return c
}

func (c *testCluster) start(id string, l net.Listener) {
	c.t.Helper()
	s, err := cluster.Open(c.cfg[id], slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		c.t.Fatal(err)
	}
	s.Transport.SetFilter(func(peer string) bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return !c.blocked[id+":"+peer]
	})
	c.nodes[id] = s
	done := make(chan error, 1)
	c.done[id] = done
	go func() { done <- s.Serve(l) }()
}

func (c *testCluster) stop(id string) {
	c.t.Helper()
	s := c.nodes[id]
	if s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := s.Close(ctx); err != nil {
		c.t.Error(err)
	}
	if err := <-c.done[id]; err != nil {
		c.t.Error(err)
	}
	delete(c.nodes, id)
}

func (c *testCluster) restart(id string) {
	c.t.Helper()
	l, err := net.Listen("tcp", c.cfg[id].Listen)
	if err != nil {
		c.t.Fatal(err)
	}
	c.start(id, l)
}

func eventually(t *testing.T, description string, fn func() bool) {
	t.Helper()
	timer := time.NewTimer(12 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if fn() {
			return
		}
		select {
		case <-timer.C:
			t.Fatalf("timed out: %s", description)
		case <-tick.C:
		}
	}
}

func (c *testCluster) leader(exclude string) string {
	c.t.Helper()
	var leader string
	eventually(c.t, "elect leader", func() bool {
		leader = ""
		for id, s := range c.nodes {
			if id != exclude && s.Node.Status().Role == "leader" {
				if leader != "" {
					return false
				}
				leader = id
			}
		}
		return leader != ""
	})
	return leader
}

func (c *testCluster) isolate(id string, blocked bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for peer := range c.cfg {
		if peer != id {
			c.blocked[id+":"+peer] = blocked
			c.blocked[peer+":"+id] = blocked
		}
	}
}

func (c *testCluster) put(key, value string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := c.client.Put(ctx, key, value); err != nil {
		c.t.Fatal(err)
	}
}

func (c *testCluster) get(key, want string) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	got, err := c.client.Get(ctx, key)
	if err != nil || got != want {
		c.t.Fatalf("get %q = %q, %v; want %q", key, got, err, want)
	}
}

func TestHealthyHTTPAndConcurrentClients(t *testing.T) {
	c := newCluster(t)
	leader := c.leader("")
	c.put("hello", "world")
	var follower string
	for id := range c.nodes {
		if id != leader {
			follower = id
			break
		}
	}
	resp, err := http.Get(c.cfg[follower].Peers[follower] + "/v1/kv/hello")
	if err != nil {
		t.Fatal(err)
	}
	var problem struct {
		LeaderURL string `json:"leader_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&problem); err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict || problem.LeaderURL == "" {
		t.Fatalf("missing follower hint: %+v", problem)
	}
	// Seed the client at another node; it discovers the leader and reads safely.
	other, err := client.New([]string{c.cfg[follower].Peers[follower], c.cfg[leader].Peers[leader]})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	value, err := other.Get(context.Background(), "hello")
	if err != nil || value != "world" {
		t.Fatalf("cross-node read: %q %v", value, err)
	}
	var wg sync.WaitGroup
	errorsCh := make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			errorsCh <- c.client.Put(ctx, fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
		}(i)
	}
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 24; i++ {
		c.get(fmt.Sprintf("key-%d", i), fmt.Sprintf("value-%d", i))
	}
	if err := c.client.Delete(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.client.Get(context.Background(), "hello"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("delete: %v", err)
	}
}

func TestLeaderFailureSnapshotCatchupAndFullRestart(t *testing.T) {
	c := newCluster(t)
	old := c.leader("")
	c.put("durable", "before")
	c.stop(old)
	c.leader("")
	for i := 0; i < 20; i++ {
		c.put(fmt.Sprintf("after-%d", i), "survives")
	}
	c.get("durable", "before")
	leader := c.leader("")
	target := c.nodes[leader].Node.Status().Commit
	c.restart(old)
	eventually(t, "lagging follower installs snapshot and catches up", func() bool {
		s := c.nodes[old].Node.Status()
		return s.Applied >= target && s.SnapshotIndex >= 8
	})
	// Exercise the recovered node in a two-member quorum before a full restart.
	var stopped []string
	for id := range c.nodes {
		if id != old {
			stopped = append(stopped, id)
		}
	}
	for _, id := range stopped {
		c.stop(id)
	}
	c.restart(stopped[0])
	c.leader("")
	c.get("after-19", "survives")
	c.restart(stopped[1])
	c.get("durable", "before")
	for id := range c.nodes {
		c.stop(id)
	}
	for id := range c.cfg {
		c.restart(id)
	}
	c.leader("")
	c.get("durable", "before")
	c.get("after-19", "survives")
}

func TestPartitionMinoritySafetyAndConflictingSuffixRepair(t *testing.T) {
	c := newCluster(t)
	old := c.leader("")
	c.put("shared", "initial")
	c.isolate(old, true)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Millisecond)
	_, _, err := c.nodes[old].Node.Execute(ctx, state.Command{Op: "put", Key: "shared", Value: "minority"})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("minority write: %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 180*time.Millisecond)
	_, _, err = c.nodes[old].Node.Execute(ctx, state.Command{Op: "get", Key: "shared"})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("isolated leader served read: %v", err)
	}
	newLeader := c.leader(old)
	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	_, _, err = c.nodes[newLeader].Node.Execute(ctx, state.Command{Op: "put", Key: "shared", Value: "majority"})
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	c.isolate(old, false)
	eventually(t, "old leader steps down", func() bool {
		s := c.nodes[old].Node.Status()
		return s.Role == "follower" && s.Term >= c.nodes[newLeader].Node.Status().Term
	})
	c.get("shared", "majority")
	// All links unavailable: no member may acknowledge a new write.
	for id := range c.nodes {
		c.isolate(id, true)
	}
	current := c.nodes[newLeader].Node.Status().Commit
	ctx, cancel = context.WithTimeout(context.Background(), 180*time.Millisecond)
	_, _, err = c.nodes[newLeader].Node.Execute(ctx, state.Command{Op: "put", Key: "never", Value: "committed"})
	cancel()
	if err == nil || c.nodes[newLeader].Node.Status().Commit != current {
		t.Fatal("write committed without majority")
	}
	for id := range c.nodes {
		c.isolate(id, false)
	}
	c.leader("")
	c.put("recovered", "yes")
	c.get("recovered", "yes")
}
