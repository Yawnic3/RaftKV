package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadStrictConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
	}{
		{"unknown field", `{"surprise":true}`},
		{"trailing input", `{} {}`},
		{"missing identity", `{"listen":"127.0.0.1:8001"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "node.json")
			if err := os.WriteFile(path, []byte(tc.data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(path); err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
	for _, node := range []string{"node1", "node2", "node3"} {
		cfg, err := Load(filepath.Join("..", "..", "config", node+".json"))
		if err != nil {
			t.Fatal(err)
		}
		cfg.RequestTimeoutMS = int(^uint(0) >> 1)
		if err := cfg.Validate(); err == nil {
			t.Fatal("accepted overflowing timeout")
		}
	}
}
