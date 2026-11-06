// Package storage provides a checksummed, fsynced append-only WAL.
package storage

import "raftkv/internal/state"

type Entry struct {
	Index   uint64        `json:"index"`
	Term    uint64        `json:"term"`
	Command state.Command `json:"command"`
}

type Snapshot struct {
	Index  uint64            `json:"index"`
	Term   uint64            `json:"term"`
	Values map[string]string `json:"values"`
}

// Image is a complete recovery image. Full-image records simplify atomicity
// between hard state, suffix replacement, commitment, and snapshot installation.
type Image struct {
	Cluster  string   `json:"cluster"`
	Node     string   `json:"node"`
	Term     uint64   `json:"term"`
	Vote     string   `json:"vote"`
	Commit   uint64   `json:"commit"`
	Snapshot Snapshot `json:"snapshot"`
	Entries  []Entry  `json:"entries"`
}
