package tests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"raftkv/internal/client"
	"raftkv/internal/cluster"
	"raftkv/internal/config"
	"raftkv/internal/raft"
)

// TestNodeProcess is executed only in a child test binary. Process.Kill in the
// parent bypasses graceful shutdown, exercising real OS crash/recovery behavior.
func TestNodeProcess(t *testing.T) {
	path := os.Getenv("RAFTKV_TEST_NODE_CONFIG")
	if path == "" {
		return
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	s, err := cluster.Open(cfg, slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Serve(l); err != nil {
		t.Fatal(err)
	}
}

type childNode struct {
	cmd *exec.Cmd
	log *os.File
}

func TestProcessKillLeaderAndWholeClusterRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("process crash test")
	}
	dir := t.TempDir()
	peers := make(map[string]string)
	for _, id := range []string{"a", "b", "c"} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		peers[id] = "http://" + l.Addr().String()
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	children := make(map[string]childNode)
	start := func(id string) {
		t.Helper()
		cfg := config.Config{ID: id, Listen: peers[id][7:], DataDir: filepath.Join(dir, id), Peers: peers,
			ElectionMinMS: 600, ElectionMaxMS: 1000, HeartbeatMS: 75,
			RPCTimeoutMS: 250, RequestTimeoutMS: 1500, SnapshotThreshold: 8,
		}
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, id+".json")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		log, err := os.OpenFile(filepath.Join(dir, id+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(executable, "-test.run=^TestNodeProcess$", "-test.timeout=120s")
		cmd.Env = append(os.Environ(), "RAFTKV_TEST_NODE_CONFIG="+path)
		cmd.Stdout = log
		cmd.Stderr = log
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		children[id] = childNode{cmd: cmd, log: log}
	}
	kill := func(id string) {
		t.Helper()
		child, ok := children[id]
		if !ok {
			return
		}
		if err := child.cmd.Process.Kill(); err != nil {
			t.Error(err)
		}
		var exitErr *exec.ExitError
		if err := child.cmd.Wait(); err != nil && !errors.As(err, &exitErr) {
			t.Error(err)
		}
		if err := child.log.Close(); err != nil {
			t.Error(err)
		}
		delete(children, id)
	}
	t.Cleanup(func() {
		for id := range children {
			kill(id)
		}
	})
	for id := range peers {
		start(id)
	}
	httpClient := &http.Client{Timeout: 200 * time.Millisecond}
	defer httpClient.CloseIdleConnections()
	findLeader := func(exclude string) string {
		t.Helper()
		var leader string
		eventually(t, "process cluster leader", func() bool {
			for id := range children {
				if id == exclude {
					continue
				}
				resp, err := httpClient.Get(peers[id] + "/v1/status")
				if err != nil {
					continue
				}
				var status raft.Status
				err = json.NewDecoder(resp.Body).Decode(&status)
				resp.Body.Close()
				if err == nil && status.Role == "leader" {
					leader = id
					return true
				}
			}
			return false
		})
		return leader
	}
	c, err := client.New([]string{peers["a"], peers["b"], peers["c"]})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	put := func(key, value string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Put(ctx, key, value); err != nil {
			t.Fatal(err)
		}
	}
	get := func(key, want string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		value, err := c.Get(ctx, key)
		if err != nil || value != want {
			t.Fatalf("recovered %s = %q, %v; want %q", key, value, err, want)
		}
	}
	leader := findLeader("")
	put("crash-proof", "original")
	kill(leader)
	findLeader(leader)
	for i := 0; i < 12; i++ {
		put(fmt.Sprintf("after-kill-%d", i), "durable")
	}
	start(leader)
	get("crash-proof", "original")
	get("after-kill-11", "durable")
	// Abruptly kill every process, then reopen the same durable directories.
	for id := range children {
		kill(id)
	}
	for id := range peers {
		start(id)
	}
	findLeader("")
	get("crash-proof", "original")
	get("after-kill-11", "durable")
}
