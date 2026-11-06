package raft

import (
	"context"
	"fmt"

	"raftkv/internal/state"
	"raftkv/internal/storage"
)

func (n *Node) replicator(peer string, trigger <-chan struct{}) {
	defer n.wg.Done()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-trigger:
			for n.replicateOnce(peer) {
			}
		}
	}
}

// Each peer has one replication worker, so delayed replies cannot regress its
// match index. No network operation runs under the Raft mutex.
func (n *Node) replicateOnce(peer string) bool {
	n.mu.Lock()
	if n.availableLocked() != nil || n.role != "leader" {
		n.mu.Unlock()
		return false
	}
	term := n.image.Term
	next := n.next[peer]
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	defer cancel()
	if next <= n.image.Snapshot.Index {
		snapshot := n.image.Snapshot // Snapshot maps are immutable after publication.
		n.mu.Unlock()
		resp, err := n.transport.InstallSnapshot(ctx, peer, SnapshotRequest{Term: term, Leader: n.cfg.ID, Snapshot: snapshot})
		n.mu.Lock()
		defer n.mu.Unlock()
		if err != nil || n.availableLocked() != nil {
			return false
		}
		if resp.Term > n.image.Term {
			if err := n.stepDownLocked(resp.Term, ""); err != nil {
				return false
			}
			return false
		}
		if n.role != "leader" || n.image.Term != term || resp.Term != term {
			return false
		}
		n.match[peer] = max(n.match[peer], snapshot.Index)
		n.next[peer] = n.match[peer] + 1
		n.advanceLocked()
		return n.next[peer] <= n.lastLocked()
	}
	prev := next - 1
	prevTerm, _ := n.termLocked(prev)
	end := min(n.lastLocked()+1, next+64)
	entries := append([]storage.Entry(nil), n.image.Entries[next-n.image.Snapshot.Index-1:end-n.image.Snapshot.Index-1]...)
	req := AppendRequest{
		Term: term, Leader: n.cfg.ID, PrevIndex: prev, PrevTerm: prevTerm,
		Entries: entries, Commit: n.image.Commit,
	}
	n.mu.Unlock()
	resp, err := n.transport.AppendEntries(ctx, peer, req)
	n.mu.Lock()
	defer n.mu.Unlock()
	if err != nil || n.availableLocked() != nil {
		return false
	}
	if resp.Term > n.image.Term {
		if err := n.stepDownLocked(resp.Term, ""); err != nil {
			return false
		}
		return false
	}
	if n.role != "leader" || n.image.Term != term || resp.Term != term {
		return false
	}
	if resp.Success {
		n.match[peer] = max(n.match[peer], prev+uint64(len(entries)))
		n.next[peer] = n.match[peer] + 1
		n.advanceLocked()
		return n.next[peer] <= n.lastLocked()
	}
	n.next[peer] = max(1, min(next-1, resp.Next))
	return true
}

func (n *Node) AppendEntries(req AppendRequest) (AppendResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := AppendResponse{Term: n.image.Term, Next: n.lastLocked() + 1}
	if err := n.availableLocked(); err != nil {
		return resp, err
	}
	if _, ok := n.cfg.Peers[req.Leader]; !ok || req.Term < n.image.Term {
		return resp, nil
	}
	if err := n.stepDownLocked(req.Term, req.Leader); err != nil {
		return resp, err
	}
	n.resetDeadlineLocked()
	resp.Term = n.image.Term
	prevTerm, exists := n.termLocked(req.PrevIndex)
	if !exists || prevTerm != req.PrevTerm {
		if req.PrevIndex <= n.lastLocked() {
			resp.Next = max(1, req.PrevIndex)
		}
		return resp, nil
	}
	for i, entry := range req.Entries {
		if entry.Index != req.PrevIndex+uint64(i)+1 || entry.Term > req.Term || entry.Term < req.PrevTerm {
			return resp, fmt.Errorf("invalid AppendEntries sequence")
		}
		if err := entry.Command.Validate(); err != nil {
			return resp, err
		}
		if i > 0 && entry.Term < req.Entries[i-1].Term {
			return resp, fmt.Errorf("decreasing entry term")
		}
	}
	changed := false
	for i, entry := range req.Entries {
		term, exists := n.termLocked(entry.Index)
		if exists && term == entry.Term {
			continue
		}
		if entry.Index <= n.image.Commit {
			return resp, fmt.Errorf("attempt to overwrite committed entry")
		}
		offset := entry.Index - n.image.Snapshot.Index - 1
		n.image.Entries = append(n.image.Entries[:offset], req.Entries[i:]...)
		changed = true
		break
	}
	// Bound commitment by the prefix verified by THIS request, not by an
	// unrelated local suffix, which may belong to a previous leader.
	matched := req.PrevIndex + uint64(len(req.Entries))
	commit := max(n.image.Commit, min(req.Commit, matched))
	if commit != n.image.Commit {
		n.image.Commit = commit
		changed = true
	}
	if changed {
		if err := n.saveLocked(); err != nil {
			return resp, err
		}
	}
	n.applyLocked()
	if err := n.availableLocked(); err != nil {
		return resp, err
	}
	resp.Success = true
	resp.Next = matched + 1
	return resp, nil
}

func (n *Node) InstallSnapshot(req SnapshotRequest) (SnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := SnapshotResponse{Term: n.image.Term}
	if err := n.availableLocked(); err != nil {
		return resp, err
	}
	if _, ok := n.cfg.Peers[req.Leader]; !ok || req.Term < n.image.Term {
		return resp, nil
	}
	if req.Snapshot.Term > req.Term {
		return resp, fmt.Errorf("snapshot term exceeds leader term")
	}
	if err := n.stepDownLocked(req.Term, req.Leader); err != nil {
		return resp, err
	}
	n.resetDeadlineLocked()
	resp.Term = n.image.Term
	if req.Snapshot.Index <= n.image.Commit {
		return resp, nil
	}
	var suffix []storage.Entry
	if term, ok := n.termLocked(req.Snapshot.Index); ok && term == req.Snapshot.Term {
		suffix = append(suffix, n.image.Entries[req.Snapshot.Index-n.image.Snapshot.Index:]...)
	}
	machine := state.New(req.Snapshot.Values)
	n.image.Snapshot = storage.Snapshot{Index: req.Snapshot.Index, Term: req.Snapshot.Term, Values: machine.Snapshot()}
	n.image.Entries = suffix
	n.image.Commit = req.Snapshot.Index
	if err := n.store.Compact(n.image); err != nil {
		return resp, n.failLocked(err)
	}
	n.machine = machine
	n.applied = req.Snapshot.Index
	n.notifyLocked()
	return resp, nil
}
