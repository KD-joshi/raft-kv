// Package kvstore provides a thread-safe, in-memory key-value store
// backed by a map[string]string with RWMutex concurrency control.
//
// This is the state machine that Raft will eventually drive —
// commands are applied here only after consensus is reached.
package kvstore

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// Command represents a single mutation operation on the KV store.
// Commands are serialized into the WAL and Raft log before being applied.
type Command struct {
	Op    string `json:"op"`    // "PUT" or "DELETE"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"` // Empty for DELETE
}

// EncodeCommand serializes a Command to bytes for storage in WAL/Raft log.
func EncodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}

// DecodeCommand deserializes bytes back into a Command.
func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}

// Store is the core in-memory key-value state machine.
// It wraps a map[string]string with a sync.RWMutex so multiple goroutines
// can read concurrently while writes are exclusive.
//
// In the Raft architecture, this is the Finite State Machine (FSM):
//   - Apply(cmd) mutates state (called only after Raft commit)
//   - Get(key) reads state (served locally after linearizability check)
//   - Snapshot()/Restore() enable log compaction
type Store struct {
	mu   sync.RWMutex
	data map[string]string

	// Metrics counters — use atomic operations because getCount is
	// incremented inside RLock (concurrent readers would race on a plain uint64)
	putCount    atomic.Uint64
	getCount    atomic.Uint64
	deleteCount atomic.Uint64
}

// New creates an empty KV store ready for use.
func New() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Apply executes a command against the state machine.
// This is the ONLY way to mutate state — never write to the map directly.
// Returns the result value (for PUT, the value stored; for DELETE, the old value).
func (s *Store) Apply(cmd Command) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case "PUT":
		s.data[cmd.Key] = cmd.Value
		s.putCount.Add(1)
		return cmd.Value, nil
	case "DELETE":
		old, existed := s.data[cmd.Key]
		delete(s.data, cmd.Key)
		s.deleteCount.Add(1)
		if !existed {
			return "", nil
		}
		return old, nil
	default:
		return "", fmt.Errorf("unknown operation: %s", cmd.Op)
	}
}

// Get reads a value from the store. Thread-safe for concurrent readers.
// Returns the value and whether the key exists.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	s.getCount.Add(1) // Atomic: safe under RLock with concurrent readers
	val, ok := s.data[key]
	return val, ok
}

// Snapshot serializes the entire store state to JSON bytes.
// Used for:
//   - Log compaction (Phase 4): discard old log entries and save a snapshot
//   - InstallSnapshot RPC: send full state to a far-behind follower
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Deep copy to avoid holding the lock during serialization
	cp := make(map[string]string, len(s.data))
	for k, v := range s.data {
		cp[k] = v
	}

	return json.Marshal(cp)
}

// Restore replaces the entire store state from a snapshot.
// The existing data is completely discarded.
func (s *Store) Restore(data []byte) error {
	var restored map[string]string
	if err := json.Unmarshal(data, &restored); err != nil {
		return fmt.Errorf("restore snapshot: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = restored
	return nil
}

// Len returns the number of keys in the store.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Keys returns all keys in the store (unordered). Useful for debugging.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	return keys
}

// Stats returns operation counters for Prometheus metrics.
func (s *Store) Stats() (puts, gets, deletes uint64) {
	return s.putCount.Load(), s.getCount.Load(), s.deleteCount.Load()
}
