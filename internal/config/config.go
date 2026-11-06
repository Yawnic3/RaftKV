// Package config validates fixed cluster membership and operational settings.
package config

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"time"

	"raftkv/internal/raft"
)

type Config struct {
	ID                string            `json:"id"`
	Listen            string            `json:"listen"`
	DataDir           string            `json:"data_dir"`
	Peers             map[string]string `json:"peers"`
	ElectionMinMS     int               `json:"election_min_ms"`
	ElectionMaxMS     int               `json:"election_max_ms"`
	HeartbeatMS       int               `json:"heartbeat_ms"`
	RPCTimeoutMS      int               `json:"rpc_timeout_ms"`
	RequestTimeoutMS  int               `json:"request_timeout_ms"`
	SnapshotThreshold uint64            `json:"snapshot_threshold"`
}

func Load(path string) (Config, error) {
	c := Config{ElectionMinMS: 600, ElectionMaxMS: 1000, HeartbeatMS: 100,
		RPCTimeoutMS: 300, RequestTimeoutMS: 3000, SnapshotThreshold: 256}
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return c, fmt.Errorf("trailing configuration data")
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.ID == "" || c.DataDir == "" || c.Peers[c.ID] == "" {
		return fmt.Errorf("id, data_dir, and self peer are required")
	}
	if len(c.Peers) < 3 {
		return fmt.Errorf("at least three fixed members required")
	}
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	addresses := make(map[string]bool)
	for id, addr := range c.Peers {
		u, err := url.Parse(addr)
		if err != nil || id == "" || u == nil {
			return fmt.Errorf("invalid peer %q", id)
		}
		if u.Scheme != "http" || u.Host == "" || u.Path != "" ||
			u.RawQuery != "" || u.Fragment != "" || u.User != nil {
			return fmt.Errorf("peer %q must have an http origin without path, credentials, or query", id)
		}
		if _, _, err := net.SplitHostPort(u.Host); err != nil {
			return fmt.Errorf("peer %q: %w", id, err)
		}
		if addresses[addr] {
			return fmt.Errorf("duplicate peer address %s", addr)
		}
		addresses[addr] = true
	}
	for name, value := range map[string]int{
		"heartbeat_ms":       c.HeartbeatMS,
		"election_min_ms":    c.ElectionMinMS,
		"election_max_ms":    c.ElectionMaxMS,
		"rpc_timeout_ms":     c.RPCTimeoutMS,
		"request_timeout_ms": c.RequestTimeoutMS,
	} {
		if value <= 0 || value > 3_600_000 {
			return fmt.Errorf("%s must be between 1 and 3600000", name)
		}
	}
	if c.ElectionMinMS <= 2*c.HeartbeatMS || c.ElectionMaxMS <= c.ElectionMinMS {
		return fmt.Errorf("require positive timeouts and election max > min > 2 * heartbeat")
	}
	return nil
}

func (c Config) Raft(logger *slog.Logger) raft.Config {
	return raft.Config{ID: c.ID, Peers: c.Peers,
		ElectionMin:       time.Duration(c.ElectionMinMS) * time.Millisecond,
		ElectionMax:       time.Duration(c.ElectionMaxMS) * time.Millisecond,
		Heartbeat:         time.Duration(c.HeartbeatMS) * time.Millisecond,
		RPCTimeout:        time.Duration(c.RPCTimeoutMS) * time.Millisecond,
		SnapshotThreshold: c.SnapshotThreshold, Logger: logger}
}
