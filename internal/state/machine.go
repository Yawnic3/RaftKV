// Package state implements the deterministic replicated key-value state machine.
package state

import "fmt"

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func (c Command) Validate() error {
	switch c.Op {
	case "noop":
		return nil
	case "put", "delete", "get":
		if c.Key == "" || len(c.Key) > 1024 {
			return fmt.Errorf("key must contain 1 to 1024 bytes")
		}
		if len(c.Value) > 64*1024 {
			return fmt.Errorf("value exceeds 64 KiB")
		}
		return nil
	default:
		return fmt.Errorf("unknown operation %q", c.Op)
	}
}

// Machine is owned by the Raft mutex; it has no independent synchronization.
type Machine struct{ values map[string]string }

func New(values map[string]string) *Machine {
	m := &Machine{values: make(map[string]string, len(values))}
	for k, v := range values {
		m.values[k] = v
	}
	return m
}

func (m *Machine) Apply(c Command) {
	switch c.Op {
	case "put":
		m.values[c.Key] = c.Value
	case "delete":
		delete(m.values, c.Key)
	}
}

func (m *Machine) Get(key string) (string, bool) {
	v, ok := m.values[key]
	return v, ok
}

func (m *Machine) Snapshot() map[string]string {
	values := make(map[string]string, len(m.values))
	for k, v := range m.values {
		values[k] = v
	}
	return values
}
