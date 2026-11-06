package raft

import (
	"context"

	"raftkv/internal/state"
	"raftkv/internal/storage"
)

func noop() state.Command { return state.Command{Op: "noop"} }

func (n *Node) appendLocked(command state.Command) {
	entry := storage.Entry{
		Index:   n.lastLocked() + 1,
		Term:    n.image.Term,
		Command: command,
	}
	n.image.Entries = append(n.image.Entries, entry)
}

// Execute also handles reads: each get appends a log-backed read barrier.
// Committing this current-term entry confirms a fresh quorum after read arrival.
// This deliberately trades read throughput for a small, auditable safety path.
func (n *Node) Execute(ctx context.Context, command state.Command) (string, bool, error) {
	if err := command.Validate(); err != nil {
		return "", false, err
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	n.mu.Lock()
	n.requests++
	if err := n.availableLocked(); err != nil {
		n.mu.Unlock()
		return "", false, err
	}
	if n.role != "leader" {
		n.mu.Unlock()
		return "", false, ErrNotLeader
	}
	term := n.image.Term
	n.appendLocked(command)
	index := n.lastLocked()
	if err := n.saveLocked(); err != nil {
		n.mu.Unlock()
		return "", false, err
	}
	n.advanceLocked()
	n.triggerLocked()
	for {
		if err := n.availableLocked(); err != nil {
			n.mu.Unlock()
			return "", false, err
		}
		if n.role != "leader" || n.image.Term != term {
			n.mu.Unlock()
			return "", false, ErrNotLeader
		}
		if n.applied >= index {
			value, found := n.machine.Get(command.Key)
			n.mu.Unlock()
			return value, found, nil
		}
		changed := n.changed
		n.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", false, ctx.Err()
		case <-changed:
		}
		n.mu.Lock()
	}
}

func (n *Node) advanceLocked() {
	for index := n.lastLocked(); index > n.image.Commit; index-- {
		term, _ := n.termLocked(index)
		if term != n.image.Term {
			continue
		}
		votes := 1
		for _, match := range n.match {
			if match >= index {
				votes++
			}
		}
		if votes > len(n.cfg.Peers)/2 {
			n.image.Commit = index
			if n.saveLocked() != nil {
				return
			}
			n.applyLocked()
			n.triggerLocked()
			return
		}
	}
}

func (n *Node) applyLocked() {
	for n.applied < n.image.Commit {
		n.applied++
		n.machine.Apply(n.image.Entries[n.applied-n.image.Snapshot.Index-1].Command)
	}
	if n.cfg.SnapshotThreshold > 0 && n.applied-n.image.Snapshot.Index >= n.cfg.SnapshotThreshold {
		term, _ := n.termLocked(n.applied)
		suffix := append([]storage.Entry(nil), n.image.Entries[n.applied-n.image.Snapshot.Index:]...)
		n.image.Snapshot = storage.Snapshot{Index: n.applied, Term: term, Values: n.machine.Snapshot()}
		n.image.Entries = suffix
		if err := n.store.Compact(n.image); err != nil {
			n.failLocked(err)
			return
		}
	}
	n.notifyLocked()
}
