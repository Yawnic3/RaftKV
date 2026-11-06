// Package transport implements bounded HTTP/JSON Raft RPCs.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"

	"raftkv/internal/raft"
)

const MaxRPCBytes = 256 << 20

// HTTP also offers a synchronized link filter for deterministic partition tests.
// The filter executes outside the transport mutex and must be concurrency-safe.
type HTTP struct {
	peers  map[string]string
	client *http.Client
	mu     sync.RWMutex
	filter func(string) bool
}

func New(peers map[string]string) *HTTP {
	copyPeers := make(map[string]string, len(peers))
	for id, addr := range peers {
		copyPeers[id] = addr
	}
	return &HTTP{peers: copyPeers, client: &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 4}}}
}

func (t *HTTP) SetFilter(filter func(string) bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.filter = filter
}

func (t *HTTP) Close() { t.client.CloseIdleConnections() }

func (t *HTTP) call(ctx context.Context, peer, path string, request, response any) error {
	t.mu.RLock()
	filter := t.filter
	t.mu.RUnlock()
	if filter != nil && !filter(peer) {
		return fmt.Errorf("link to %s unavailable", peer)
	}
	addr, ok := t.peers[peer]
	if !ok {
		return fmt.Errorf("unknown peer %q", peer)
	}
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("RPC %s: HTTP %d", path, resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, MaxRPCBytes)).Decode(response)
}

func (t *HTTP) RequestVote(ctx context.Context, peer string, req raft.VoteRequest) (raft.VoteResponse, error) {
	var resp raft.VoteResponse
	err := t.call(ctx, peer, "/internal/raft/vote", req, &resp)
	return resp, err
}

func (t *HTTP) AppendEntries(ctx context.Context, peer string, req raft.AppendRequest) (raft.AppendResponse, error) {
	var resp raft.AppendResponse
	err := t.call(ctx, peer, "/internal/raft/append", req, &resp)
	return resp, err
}

func (t *HTTP) InstallSnapshot(
	ctx context.Context, peer string, req raft.SnapshotRequest,
) (raft.SnapshotResponse, error) {
	var resp raft.SnapshotResponse
	err := t.call(ctx, peer, "/internal/raft/snapshot", req, &resp)
	return resp, err
}

func Register(mux *http.ServeMux, node *raft.Node, logger *slog.Logger) {
	mux.HandleFunc("POST /internal/raft/vote", handler(node.RequestVote, logger))
	mux.HandleFunc("POST /internal/raft/append", handler(node.AppendEntries, logger))
	mux.HandleFunc("POST /internal/raft/snapshot", handler(node.InstallSnapshot, logger))
}

func handler[Request, Response any](fn func(Request) (Response, error), logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req Request
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxRPCBytes))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil {
			http.Error(w, "invalid RPC body", http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "trailing RPC body", http.StatusBadRequest)
			return
		}
		resp, err := fn(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			logger.Debug("RPC response write failed", "error", err)
		}
	}
}
