// Package client discovers the leader through configured seed nodes and hints.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"raftkv/internal/api"
)

var ErrNotFound = errors.New("key not found")

type Client struct {
	seeds  []string
	http   *http.Client
	mu     sync.Mutex
	leader string
}

func New(seeds []string) (*Client, error) {
	if len(seeds) == 0 {
		return nil, errors.New("at least one seed required")
	}
	normalized := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		seed = strings.TrimRight(seed, "/")
		u, err := url.Parse(seed)
		if err != nil || u == nil {
			return nil, fmt.Errorf("invalid seed origin %q", seed)
		}
		if u.Scheme != "http" || u.Host == "" || u.Path != "" ||
			u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("invalid seed origin %q", seed)
		}
		normalized = append(normalized, seed)
	}
	return &Client{seeds: normalized, http: &http.Client{
		Timeout: time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 8},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func (c *Client) Close() { c.http.CloseIdleConnections() }

func (c *Client) Put(ctx context.Context, key, value string) error {
	body, err := json.Marshal(map[string]string{"value": value})
	if err != nil {
		return err
	}
	_, err = c.do(ctx, http.MethodPut, key, body)
	return err
}

func (c *Client) Delete(ctx context.Context, key string) error {
	_, err := c.do(ctx, http.MethodDelete, key, nil)
	return err
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	body, err := c.do(ctx, http.MethodGet, key, nil)
	if err != nil {
		return "", err
	}
	var result struct {
		Value string `json:"value"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", err
	}
	return result.Value, nil
}

func (c *Client) hint(addr string) {
	for _, seed := range c.seeds {
		if seed == addr {
			c.mu.Lock()
			c.leader = addr
			c.mu.Unlock()
			return
		}
	}
}

func (c *Client) do(ctx context.Context, method, key string, body []byte) ([]byte, error) {
	if key == "" {
		return nil, errors.New("key required")
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, lastErr)
		}
		c.mu.Lock()
		leader := c.leader
		c.mu.Unlock()
		addr := c.seeds[attempt%len(c.seeds)]
		if attempt%2 == 0 && leader != "" {
			addr = leader
		}
		req, err := http.NewRequestWithContext(ctx, method, addr+"/v1/kv/"+url.PathEscape(key), bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.http.Do(req)
		if err == nil {
			data, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			err = errors.Join(readErr, resp.Body.Close())
			if err == nil {
				switch resp.StatusCode {
				case http.StatusOK, http.StatusNoContent:
					c.hint(addr)
					return data, nil
				case http.StatusNotFound:
					return nil, ErrNotFound
				case http.StatusConflict, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
					var problem api.Error
					if decodeErr := json.Unmarshal(data, &problem); decodeErr == nil {
						c.hint(problem.LeaderURL)
					}
					err = fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
				default:
					return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
				}
			}
		}
		lastErr = err
		timer := time.NewTimer(30 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.Join(ctx.Err(), lastErr)
		case <-timer.C:
		}
	}
}
