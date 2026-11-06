package raft

import "context"

func (n *Node) campaignLocked() {
	n.image.Term++
	n.image.Vote = n.cfg.ID
	n.role = "candidate"
	n.leader = ""
	n.elections++
	n.resetDeadlineLocked()
	if n.saveLocked() != nil {
		return
	}
	n.notifyLocked()
	term := n.image.Term
	lastTerm, _ := n.termLocked(n.lastLocked())
	req := VoteRequest{Term: term, Candidate: n.cfg.ID, LastIndex: n.lastLocked(), LastTerm: lastTerm}
	votes := 1 // Accessed only while holding n.mu, including response goroutines.
	if votes > len(n.cfg.Peers)/2 {
		n.becomeLeaderLocked()
		return
	}
	for id := range n.triggers {
		n.wg.Add(1)
		go func(peer string) {
			defer n.wg.Done()
			ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
			defer cancel()
			resp, err := n.transport.RequestVote(ctx, peer, req)
			if err != nil {
				n.cfg.Logger.Debug("vote RPC failed", "peer", peer, "error", err)
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if err != nil || n.availableLocked() != nil {
				return
			}
			if resp.Term > n.image.Term {
				if err := n.stepDownLocked(resp.Term, ""); err != nil {
					return
				}
				return
			}
			if n.role != "candidate" || n.image.Term != term || resp.Term != term || !resp.Granted {
				return
			}
			votes++
			if votes > len(n.cfg.Peers)/2 {
				n.becomeLeaderLocked()
			}
		}(id)
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = "leader"
	n.leader = n.cfg.ID
	for id := range n.triggers {
		n.next[id] = n.lastLocked() + 1
		n.match[id] = 0
	}
	// A current-term no-op allows commitment of inherited entries.
	n.appendLocked(noop())
	if n.saveLocked() != nil {
		return
	}
	n.cfg.Logger.Info("leader elected", "node", n.cfg.ID, "term", n.image.Term)
	n.advanceLocked()
	n.triggerLocked()
	n.notifyLocked()
}

func (n *Node) RequestVote(req VoteRequest) (VoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	resp := VoteResponse{Term: n.image.Term}
	if err := n.availableLocked(); err != nil {
		return resp, err
	}
	if _, ok := n.cfg.Peers[req.Candidate]; !ok {
		return resp, nil
	}
	if req.Term < n.image.Term {
		return resp, nil
	}
	if req.Term > n.image.Term {
		if err := n.stepDownLocked(req.Term, ""); err != nil {
			return resp, err
		}
	}
	resp.Term = n.image.Term
	lastTerm, _ := n.termLocked(n.lastLocked())
	upToDate := req.LastTerm > lastTerm || (req.LastTerm == lastTerm && req.LastIndex >= n.lastLocked())
	if upToDate && (n.image.Vote == "" || n.image.Vote == req.Candidate) {
		n.image.Vote = req.Candidate
		if err := n.saveLocked(); err != nil {
			return resp, err
		}
		n.resetDeadlineLocked()
		resp.Granted = true
	}
	return resp, nil
}
