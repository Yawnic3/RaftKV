// Package api serves the public REST API. Followers return leader hints.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"raftkv/internal/raft"
	"raftkv/internal/state"
)

type Error struct {
	Error     string `json:"error"`
	Leader    string `json:"leader,omitempty"`
	LeaderURL string `json:"leader_url,omitempty"`
}

func Register(mux *http.ServeMux, node *raft.Node, timeout time.Duration, logger *slog.Logger) {
	write := func(w http.ResponseWriter, status int, body any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if err := json.NewEncoder(w).Encode(body); err != nil {
			logger.Debug("response write failed", "error", err)
		}
	}
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) { write(w, 200, node.Status()) })
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		s := node.Status()
		if s.Error != "" || s.Role == "failed" || s.Role == "stopped" {
			write(w, 503, Error{Error: s.Error})
			return
		}
		write(w, 200, map[string]string{"status": "alive"})
	})
	for _, method := range []string{"PUT", "GET", "DELETE"} {
		mux.HandleFunc(method+" /v1/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
			command := state.Command{Key: r.PathValue("key")}
			switch r.Method {
			case "PUT":
				command.Op = "put"
				var body struct {
					Value *string `json:"value"`
				}
				d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 400*1024))
				d.DisallowUnknownFields()
				if err := d.Decode(&body); err != nil || body.Value == nil {
					write(w, 400, Error{Error: "expected JSON object with string value"})
					return
				}
				if err := d.Decode(new(any)); err != io.EOF {
					write(w, 400, Error{Error: "trailing request data"})
					return
				}
				command.Value = *body.Value
			case "GET":
				command.Op = "get"
			case "DELETE":
				command.Op = "delete"
			}
			if err := command.Validate(); err != nil {
				write(w, 400, Error{Error: err.Error()})
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel()
			value, found, err := node.Execute(ctx, command)
			if err != nil {
				s := node.Status()
				status := http.StatusServiceUnavailable
				if errors.Is(err, raft.ErrNotLeader) {
					status = http.StatusConflict
				}
				if errors.Is(err, context.DeadlineExceeded) {
					status = http.StatusGatewayTimeout
				}
				write(w, status, Error{Error: err.Error(), Leader: s.Leader, LeaderURL: s.LeaderURL})
				return
			}
			if r.Method == "GET" {
				if !found {
					write(w, 404, Error{Error: "key not found"})
					return
				}
				write(w, 200, map[string]string{"key": command.Key, "value": value})
				return
			}
			w.WriteHeader(http.StatusNoContent)
		})
	}
}
