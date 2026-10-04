// Package wal implements a Write-Ahead Log for crash recovery.
//
// Before any mutation is applied to the in-memory KV store, it is first
// written to this append-only log file on disk. On startup, the WAL is
// replayed to reconstruct the exact state before the crash.
//
// Format: Each entry is a line of JSON followed by a newline.
// This is simpler than binary encoding and allows easy debugging with
// cat/grep. In Phase 4, we'll upgrade to a binary format with CRC checksums.
//
// WAL entries are fsynced to disk on every write to guarantee durability.
// In Phase 4, we'll add group commit (batching) to amortize fsync cost
// across multiple writes — the same technique used by PostgreSQL and Kafka.
package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/kuldeep-joshi/raft-kv/internal/kvstore"
)

// Entry represents a single WAL record.
// It wraps a kvstore.Command with metadata for debugging and recovery.
type Entry struct {
	Timestamp time.Time       `json:"ts"`
	Index     uint64          `json:"idx"` // Monotonically increasing sequence number
	Command   kvstore.Command `json:"cmd"`
}

// WAL is an append-only Write-Ahead Log backed by a file on disk.
//
// Thread-safety: All methods are safe for concurrent use via a sync.Mutex.
// We use Mutex (not RWMutex) because the dominant operation is Append (write).
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	filePath string
	nextIdx  uint64

	// Metrics
	entryCount uint64
	fsyncCount uint64
	totalBytes uint64
}

// Open opens (or creates) a WAL file at the given path.
// The file is opened in append-only mode with synchronous writes.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal open: %w", err)
	}

	w := &WAL{
		file:     f,
		filePath: path,
		nextIdx:  1,
	}

	return w, nil
}

// Append writes a command to the WAL and fsyncs to disk.
// This MUST be called BEFORE applying the command to the KV store.
// If this function returns nil, the data is guaranteed to survive a crash.
func (w *WAL) Append(cmd kvstore.Command) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := Entry{
		Timestamp: time.Now(),
		Index:     w.nextIdx,
		Command:   cmd,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return 0, fmt.Errorf("wal marshal: %w", err)
	}

	// Write entry as a single line (newline-delimited JSON)
	data = append(data, '\n')

	n, err := w.file.Write(data)
	if err != nil {
		return 0, fmt.Errorf("wal write: %w", err)
	}

	// fsync: force the OS to flush to disk.
	// Without this, data could be lost if the machine loses power
	// before the OS flushes its page cache.
	if err := w.file.Sync(); err != nil {
		return 0, fmt.Errorf("wal fsync: %w", err)
	}

	idx := w.nextIdx
	w.nextIdx++
	w.entryCount++
	w.fsyncCount++
	w.totalBytes += uint64(n)

	return idx, nil
}

// Replay reads the entire WAL from disk and returns all entries in order.
// This is called once at startup to rebuild the in-memory KV store state.
//
// Corrupted entries (partial writes from a crash) are silently skipped —
// this is safe because:
//   - The entry was never fsynced (so it was never "committed")
//   - The client never received a success response
func (w *WAL) Replay() ([]Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to beginning of file for reading
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("wal seek: %w", err)
	}

	var entries []Entry
	scanner := bufio.NewScanner(w.file)

	// Increase scanner buffer for large entries
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry Entry
		if err := json.Unmarshal(line, &entry); err != nil {
			// Corrupted entry — skip it (see doc comment above)
			fmt.Printf("[WAL] WARNING: skipping corrupted entry at line %d: %v\n", lineNum, err)
			continue
		}

		entries = append(entries, entry)

		// Track the highest index seen for future Appends
		if entry.Index >= w.nextIdx {
			w.nextIdx = entry.Index + 1
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("wal scan: %w", err)
	}

	// Seek back to end for future Appends
	if _, err := w.file.Seek(0, 2); err != nil {
		return nil, fmt.Errorf("wal seek end: %w", err)
	}

	w.entryCount = uint64(len(entries))

	return entries, nil
}

// Truncate clears the WAL file. Used after snapshotting (Phase 4).
// All committed state has been captured in the snapshot, so old
// WAL entries can be safely discarded.
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("wal truncate: %w", err)
	}
	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("wal seek after truncate: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal fsync after truncate: %w", err)
	}

	w.entryCount = 0
	w.totalBytes = 0
	return nil
}

// Close flushes and closes the WAL file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal final fsync: %w", err)
	}
	return w.file.Close()
}

// Stats returns WAL metrics for Prometheus.
func (w *WAL) Stats() (entries, fsyncs, totalBytes uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.entryCount, w.fsyncCount, w.totalBytes
}

// Path returns the file path of the WAL.
func (w *WAL) Path() string {
	return w.filePath
}
