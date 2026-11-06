// Package raft implements fixed-membership Raft without a consensus library.
package raft

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"raftkv/internal/storage"
)

var (
	ErrNotLeader = errors.New("not leader")
	ErrStopped   = errors.New("node stopped")
)

type VoteRequest struct {
	Term      uint64 `json:"term"`
	Candidate string `json:"candidate"`
	LastIndex uint64 `json:"last_index"`
	LastTerm  uint64 `json:"last_term"`
}

type VoteResponse struct {
	Term    uint64 `json:"term"`
	Granted bool   `json:"granted"`
}

type AppendRequest struct {
	Term      uint64          `json:"term"`
	Leader    string          `json:"leader"`
	PrevIndex uint64          `json:"prev_index"`
	PrevTerm  uint64          `json:"prev_term"`
	Entries   []storage.Entry `json:"entries"`
	Commit    uint64          `json:"commit"`
}

type AppendResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	Next    uint64 `json:"next"`
}

type SnapshotRequest struct {
	Term     uint64           `json:"term"`
	Leader   string           `json:"leader"`
	Snapshot storage.Snapshot `json:"snapshot"`
}

type SnapshotResponse struct {
	Term uint64 `json:"term"`
}

type Transport interface {
	RequestVote(context.Context, string, VoteRequest) (VoteResponse, error)
	AppendEntries(context.Context, string, AppendRequest) (AppendResponse, error)
	InstallSnapshot(context.Context, string, SnapshotRequest) (SnapshotResponse, error)
}

type Store interface {
	Save(storage.Image) error
	Compact(storage.Image) error
	Close() error
}

type Config struct {
	ID                string
	Peers             map[string]string
	ElectionMin       time.Duration
	ElectionMax       time.Duration
	Heartbeat         time.Duration
	RPCTimeout        time.Duration
	SnapshotThreshold uint64
	Logger            *slog.Logger
}

type Status struct {
	ID            string            `json:"id"`
	Role          string            `json:"role"`
	Term          uint64            `json:"term"`
	Leader        string            `json:"leader"`
	LeaderURL     string            `json:"leader_url"`
	Commit        uint64            `json:"commit_index"`
	Applied       uint64            `json:"last_applied"`
	LastIndex     uint64            `json:"last_log_index"`
	SnapshotIndex uint64            `json:"snapshot_index"`
	Match         map[string]uint64 `json:"replication_progress"`
	Elections     uint64            `json:"elections"`
	Requests      uint64            `json:"requests"`
	Error         string            `json:"error,omitempty"`
}
