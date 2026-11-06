package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"raftkv/internal/cluster"
	"raftkv/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("server exited", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	path := flag.String("config", "config/node1.json", "JSON configuration file")
	flag.Parse()
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	server, err := cluster.Open(cfg, logger)
	if err != nil {
		return errors.Join(err, listener.Close())
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	logger.Info("node listening", "id", cfg.ID, "address", cfg.Listen, "data_dir", cfg.DataDir)
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return errors.Join(server.Close(shutdownCtx), <-done)
	case err := <-done:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return errors.Join(err, server.Close(shutdownCtx))
	}
}
