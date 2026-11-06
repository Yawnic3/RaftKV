package tests

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestHTTPValidationAndEscapedKeys(t *testing.T) {
	c := newCluster(t)
	leader := c.leader("")
	base := c.cfg[leader].Peers[leader]
	httpClient := &http.Client{}
	defer httpClient.CloseIdleConnections()
	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   string
		status int
	}{
		{"missing value", "PUT", "/v1/kv/a", `{}`, 400},
		{"null value", "PUT", "/v1/kv/a", `{"value":null}`, 400},
		{"unknown field", "PUT", "/v1/kv/a", `{"value":"v","extra":true}`, 400},
		{"trailing JSON", "PUT", "/v1/kv/a", `{"value":"v"} {}`, 400},
		{"oversized value", "PUT", "/v1/kv/a", `{"value":"` + strings.Repeat("x", 65537) + `"}`, 400},
		{"oversized body", "PUT", "/v1/kv/a", strings.Repeat(" ", 401*1024), 400},
		{"method", "POST", "/v1/kv/a", "", 405},
		{"malformed RPC", "POST", "/internal/raft/vote", `{invalid`, 400},
		{"trailing RPC", "POST", "/internal/raft/vote", `{} {}`, 400},
		{"health", "GET", "/healthz", "", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, base+tc.path, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := httpClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.Copy(io.Discard, resp.Body); err != nil {
				t.Fatal(err)
			}
			if err := resp.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tc.status {
				t.Fatalf("HTTP %d; want %d", resp.StatusCode, tc.status)
			}
		})
	}
	c.put("folder/space key?", "escaped")
	c.get("folder/space key?", "escaped")
}
