package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"raftkv/internal/state"
	"raftkv/internal/storage"
)

type Node struct {
	mu        sync.Mutex
	cfg       Config
	store     Store
	transport Transport
	image     storage.Image
	machine   *state.Machine
	role      string
	leader    string
	applied   uint64
	next      map[string]uint64
	match     map[string]uint64
	triggers  map[string]chan struct{}
	deadline  time.Time
	changed   chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	stopped   bool
	fault     error
	elections uint64
	requests  uint64
}

func New(cfg Config, store Store, transport Transport, img storage.Image) (*Node, error) {
	if cfg.ID == "" || cfg.Peers[cfg.ID] == "" {
		return nil, errors.New("node must belong to configured peers")
	}
	if cfg.Heartbeat < 2*time.Nanosecond || cfg.RPCTimeout <= 0 ||
		cfg.ElectionMin <= 2*cfg.Heartbeat || cfg.ElectionMax <= cfg.ElectionMin {
		return nil, errors.New("require positive RPC timeout and election max > min > 2 * heartbeat")
	}
	if cfg.Logger == nil {
		return nil, errors.New("logger required")
	}
	if err := validateImage(img); err != nil {
		return nil, err
	}
	peers := make(map[string]string, len(cfg.Peers))
	for id, addr := range cfg.Peers {
		peers[id] = addr
	}
	cfg.Peers = peers
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg: cfg, store: store, transport: transport, image: img,
		machine: state.New(img.Snapshot.Values), role: "follower",
		applied: img.Snapshot.Index, next: make(map[string]uint64),
		match: make(map[string]uint64), triggers: make(map[string]chan struct{}),
		changed: make(chan struct{}), ctx: ctx, cancel: cancel,
	}
	for _, entry := range img.Entries {
		if entry.Index <= img.Commit {
			n.machine.Apply(entry.Command)
			n.applied = entry.Index
		}
	}
	n.resetDeadlineLocked()
	for id := range cfg.Peers {
		if id != cfg.ID {
			n.triggers[id] = make(chan struct{}, 1)
		}
	}
	return n, nil
}

func validateImage(img storage.Image) error {
	last := img.Snapshot.Index
	term := img.Snapshot.Term
	for _, e := range img.Entries {
		if e.Index != last+1 || e.Term < term || e.Term > img.Term {
			return errors.New("invalid durable log sequence")
		}
		if err := e.Command.Validate(); err != nil {
			return fmt.Errorf("durable command: %w", err)
		}
		last = e.Index
		term = e.Term
	}
	if img.Commit < img.Snapshot.Index || img.Commit > last || img.Snapshot.Term > img.Term {
		return errors.New("invalid durable commit/snapshot boundary")
	}
	return nil
}

// Start is called exactly once, after handlers are registered.
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.wg.Add(1)
	go n.run()
	for id, trigger := range n.triggers {
		n.wg.Add(1)
		go n.replicator(id, trigger)
	}
}

func (n *Node) Stop() error {
	n.mu.Lock()
	n.stopped = true
	n.role = "stopped"
	n.leader = ""
	n.cancel()
	n.notifyLocked()
	n.mu.Unlock()
	n.wg.Wait()
	return n.store.Close()
}

func (n *Node) run() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.Heartbeat / 2)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case now := <-ticker.C:
			n.mu.Lock()
			if n.availableLocked() == nil {
				if n.role == "leader" {
					n.triggerLocked()
				} else if !now.Before(n.deadline) {
					n.campaignLocked()
				}
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) resetDeadlineLocked() {
	jitter := time.Duration(rand.Int64N(int64(n.cfg.ElectionMax - n.cfg.ElectionMin)))
	n.deadline = time.Now().Add(n.cfg.ElectionMin + jitter)
}

func (n *Node) lastLocked() uint64 { return n.image.Snapshot.Index + uint64(len(n.image.Entries)) }

func (n *Node) termLocked(index uint64) (uint64, bool) {
	if index == n.image.Snapshot.Index {
		return n.image.Snapshot.Term, true
	}
	if index < n.image.Snapshot.Index || index > n.lastLocked() {
		return 0, false
	}
	return n.image.Entries[index-n.image.Snapshot.Index-1].Term, true
}

func (n *Node) notifyLocked() {
	close(n.changed)
	n.changed = make(chan struct{})
}

func (n *Node) availableLocked() error {
	if n.stopped {
		return ErrStopped
	}
	return n.fault
}

func (n *Node) failLocked(err error) error {
	n.fault = fmt.Errorf("durable storage failure: %w", err)
	n.role = "failed"
	n.leader = ""
	n.cfg.Logger.Error("node failed closed", "node", n.cfg.ID, "error", n.fault)
	n.notifyLocked()
	return n.fault
}

func (n *Node) saveLocked() error {
	if err := n.store.Save(n.image); err != nil {
		return n.failLocked(err)
	}
	return nil
}

func (n *Node) stepDownLocked(term uint64, leader string) error {
	changed := term > n.image.Term
	if changed {
		n.image.Term = term
		n.image.Vote = ""
	}
	n.role = "follower"
	n.leader = leader
	n.notifyLocked()
	if changed {
		return n.saveLocked()
	}
	return nil
}

func (n *Node) triggerLocked() {
	for _, trigger := range n.triggers {
		select {
		case trigger <- struct{}{}:
		default:
		}
	}
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	progress := make(map[string]uint64, len(n.match))
	if n.role == "leader" {
		for id, index := range n.match {
			progress[id] = index
		}
		progress[n.cfg.ID] = n.lastLocked()
	}
	s := Status{ID: n.cfg.ID, Role: n.role, Term: n.image.Term,
		Leader: n.leader, LeaderURL: n.cfg.Peers[n.leader], Commit: n.image.Commit,
		Applied: n.applied, LastIndex: n.lastLocked(), SnapshotIndex: n.image.Snapshot.Index,
		Match: progress, Elections: n.elections, Requests: n.requests}
	if n.fault != nil {
		s.Error = n.fault.Error()
	}
	return s
}
