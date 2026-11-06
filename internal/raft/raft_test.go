package raft

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"raftkv/internal/state"
	"raftkv/internal/storage"
)

// The in-memory store injects durability failures; integration tests use disk.
type testStore struct {
	fail  bool
	saved storage.Image
}

func (s *testStore) Save(img storage.Image) error {
	if s.fail {
		return errors.New("injected fsync failure")
	}
	s.saved = img
	return nil
}
func (s *testStore) Compact(img storage.Image) error { return s.Save(img) }
func (s *testStore) Close() error                    { return nil }

type unavailableTransport struct{}

func (unavailableTransport) RequestVote(context.Context, string, VoteRequest) (VoteResponse, error) {
	return VoteResponse{}, errors.New("offline")
}
func (unavailableTransport) AppendEntries(context.Context, string, AppendRequest) (AppendResponse, error) {
	return AppendResponse{}, errors.New("offline")
}
func (unavailableTransport) InstallSnapshot(context.Context, string, SnapshotRequest) (SnapshotResponse, error) {
	return SnapshotResponse{}, errors.New("offline")
}

func unitNode(t *testing.T, img storage.Image) (*Node, *testStore) {
	t.Helper()
	s := &testStore{}
	cfg := Config{ID: "a", Peers: map[string]string{"a": "http://a", "b": "http://b", "c": "http://c"},
		ElectionMin: time.Second, ElectionMax: 2 * time.Second, Heartbeat: 100 * time.Millisecond,
		RPCTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	n, err := New(cfg, s, unavailableTransport{}, img)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := n.Stop(); err != nil {
			t.Error(err)
		}
	})
	return n, s
}

func entry(index, term uint64) storage.Entry {
	return storage.Entry{Index: index, Term: term, Command: noop()}
}

func TestVotePersistenceAndLogFreshness(t *testing.T) {
	n, s := unitNode(t, storage.Image{Term: 2, Entries: []storage.Entry{entry(1, 2)}})
	resp, err := n.RequestVote(VoteRequest{Term: 3, Candidate: "b", LastIndex: 9, LastTerm: 1})
	if err != nil || resp.Granted || s.saved.Term != 3 {
		t.Fatalf("stale log vote: %+v %v", resp, err)
	}
	resp, err = n.RequestVote(VoteRequest{Term: 3, Candidate: "b", LastIndex: 1, LastTerm: 2})
	if err != nil || !resp.Granted || s.saved.Vote != "b" {
		t.Fatalf("vote not durable: %+v %v", resp, err)
	}
	resp, err = n.RequestVote(VoteRequest{Term: 3, Candidate: "c", LastIndex: 1, LastTerm: 2})
	if err != nil || resp.Granted {
		t.Fatalf("double vote: %+v %v", resp, err)
	}
}

func TestStorageFailureNeverGrantsVote(t *testing.T) {
	n, s := unitNode(t, storage.Image{Term: 2})
	s.fail = true
	resp, err := n.RequestVote(VoteRequest{Term: 3, Candidate: "b"})
	if err == nil || resp.Granted || n.Status().Role != "failed" {
		t.Fatalf("unsafe storage failure: %+v %v", resp, err)
	}
}

func TestAppendConsistencyConflictAndCommitBound(t *testing.T) {
	n, _ := unitNode(t, storage.Image{Term: 3, Commit: 1, Entries: []storage.Entry{entry(1, 1), entry(2, 2), entry(3, 2)}})
	resp, err := n.AppendEntries(AppendRequest{Term: 3, Leader: "b", PrevIndex: 2, PrevTerm: 1})
	if err != nil || resp.Success {
		t.Fatalf("accepted mismatching prefix: %+v %v", resp, err)
	}
	resp, err = n.AppendEntries(AppendRequest{Term: 3, Leader: "b", PrevIndex: 1, PrevTerm: 1, Commit: 3})
	if err != nil || !resp.Success || n.Status().Commit != 1 {
		t.Fatalf("committed unverified suffix: %+v %v", resp, err)
	}
	resp, err = n.AppendEntries(AppendRequest{
		Term: 3, Leader: "b", PrevIndex: 1, PrevTerm: 1,
		Entries: []storage.Entry{entry(2, 3)}, Commit: 2,
	})
	if err != nil || !resp.Success || n.Status().LastIndex != 2 || n.Status().Applied != 2 {
		t.Fatalf("conflict not replaced: %+v %v", resp, err)
	}
}

func TestOnlyCurrentTermMajorityAdvancesCommit(t *testing.T) {
	n, _ := unitNode(t, storage.Image{Term: 3, Entries: []storage.Entry{entry(1, 2)}})
	n.mu.Lock()
	defer n.mu.Unlock()
	n.role = "leader"
	n.match["b"] = 1
	n.advanceLocked()
	if n.image.Commit != 0 {
		t.Fatal("committed prior-term entry by counting replicas")
	}
	n.appendLocked(noop())
	n.match["b"] = 2
	n.advanceLocked()
	if n.image.Commit != 2 {
		t.Fatal("current-term majority did not commit prefix")
	}
}

func TestIsolatedLeaderCannotWriteOrRead(t *testing.T) {
	n, _ := unitNode(t, storage.Image{Term: 1})
	n.mu.Lock()
	n.role = "leader"
	n.mu.Unlock()
	for _, op := range []string{"put", "get"} {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		_, _, err := n.Execute(ctx, state.Command{Op: op, Key: "x", Value: "y"})
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || n.Status().Commit != 0 {
			t.Fatalf("minority %s succeeded: %v", op, err)
		}
	}
}

func TestSnapshotInstallationPreservesMatchingSuffix(t *testing.T) {
	n, s := unitNode(t, storage.Image{Term: 4, Commit: 1,
		Entries: []storage.Entry{entry(1, 1), entry(2, 3), entry(3, 4)}})
	resp, err := n.InstallSnapshot(SnapshotRequest{Term: 4, Leader: "b",
		Snapshot: storage.Snapshot{Index: 2, Term: 3, Values: map[string]string{"x": "restored"}}})
	if err != nil || resp.Term != 4 {
		t.Fatalf("install snapshot: %+v %v", resp, err)
	}
	if n.Status().Commit != 2 || n.Status().LastIndex != 3 || s.saved.Snapshot.Index != 2 {
		t.Fatalf("incorrect snapshot boundary: %+v", n.Status())
	}
	value, ok := n.machine.Get("x")
	if !ok || value != "restored" {
		t.Fatal("snapshot state not restored")
	}
	_, err = n.InstallSnapshot(SnapshotRequest{Term: 4, Leader: "b",
		Snapshot: storage.Snapshot{Index: 1, Term: 1, Values: map[string]string{"x": "stale"}}})
	if err != nil {
		t.Fatal(err)
	}
	value, _ = n.machine.Get("x")
	if value != "restored" {
		t.Fatal("stale snapshot rolled back state")
	}
}
