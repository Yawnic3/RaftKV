// Package cluster wires configuration, persistence, Raft, and HTTP lifecycle.
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"raftkv/internal/api"
	"raftkv/internal/config"
	"raftkv/internal/raft"
	"raftkv/internal/storage"
	"raftkv/internal/transport"
)

type Server struct {
	Node      *raft.Node
	Transport *transport.HTTP
	http      *http.Server
}

func Open(cfg config.Config, logger *slog.Logger) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	w, image, err := storage.Open(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(cfg.Peers)
	if err != nil {
		return nil, errors.Join(err, w.Close())
	}
	hash := sha256.Sum256(encoded)
	fingerprint := hex.EncodeToString(hash[:])
	if image.Cluster != "" && (image.Cluster != fingerprint || image.Node != cfg.ID) {
		return nil, errors.Join(fmt.Errorf("data belongs to a different node or membership"), w.Close())
	}
	image.Cluster = fingerprint
	image.Node = cfg.ID
	if err := w.Save(image); err != nil {
		return nil, errors.Join(err, w.Close())
	}
	t := transport.New(cfg.Peers)
	n, err := raft.New(cfg.Raft(logger), w, t, image)
	if err != nil {
		t.Close()
		return nil, errors.Join(err, w.Close())
	}
	mux := http.NewServeMux()
	transport.Register(mux, n, logger)
	api.Register(mux, n, time.Duration(cfg.RequestTimeoutMS)*time.Millisecond, logger)
	server := &http.Server{Addr: cfg.Listen, Handler: mux,
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: time.Duration(cfg.RequestTimeoutMS)*time.Millisecond + 10*time.Second,
		IdleTimeout:  30 * time.Second, MaxHeaderBytes: 16 * 1024}
	return &Server{Node: n, Transport: t, http: server}, nil
}

// Serve blocks until shutdown. The caller owns the listener until Serve starts.
func (s *Server) Serve(listener net.Listener) error {
	s.Node.Start()
	err := s.http.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Close(ctx context.Context) error {
	// Stop consensus first to release in-flight proposal handlers immediately.
	err := s.Node.Stop()
	s.Transport.Close()
	if shutdownErr := s.http.Shutdown(ctx); shutdownErr != nil {
		return errors.Join(err, shutdownErr, s.http.Close())
	}
	return err
}
