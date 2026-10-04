package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/KD-joshi/raft-kv/internal/kvstore"
)

func tempWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	return w, path
}

func TestOpenAndClose(t *testing.T) {
	w, _ := tempWAL(t)
	defer w.Close()

	if w.Path() == "" {
		t.Fatal("expected non-empty WAL path")
	}
}

func TestAppendAndReplay(t *testing.T) {
	w, _ := tempWAL(t)
	defer w.Close()

	// Append some commands
	cmds := []kvstore.Command{
		{Op: "PUT", Key: "x", Value: "100"},
		{Op: "PUT", Key: "y", Value: "200"},
		{Op: "DELETE", Key: "x"},
	}

	for _, cmd := range cmds {
		_, err := w.Append(cmd)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Replay
	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("Replay failed: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	// Verify order and content
	if entries[0].Command.Op != "PUT" || entries[0].Command.Key != "x" {
		t.Fatalf("entry 0 mismatch: %+v", entries[0].Command)
	}
	if entries[1].Command.Op != "PUT" || entries[1].Command.Key != "y" {
		t.Fatalf("entry 1 mismatch: %+v", entries[1].Command)
	}
	if entries[2].Command.Op != "DELETE" || entries[2].Command.Key != "x" {
		t.Fatalf("entry 2 mismatch: %+v", entries[2].Command)
	}

	// Verify monotonic indices
	for i := 1; i < len(entries); i++ {
		if entries[i].Index <= entries[i-1].Index {
			t.Fatalf("indices not monotonic: %d <= %d", entries[i].Index, entries[i-1].Index)
		}
	}
}

func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "crash.wal")

	// Phase 1: Write data and "crash" (just close the WAL)
	func() {
		w, err := Open(path)
		if err != nil {
			t.Fatalf("open failed: %v", err)
		}

		w.Append(kvstore.Command{Op: "PUT", Key: "a", Value: "1"})
		w.Append(kvstore.Command{Op: "PUT", Key: "b", Value: "2"})
		w.Append(kvstore.Command{Op: "PUT", Key: "c", Value: "3"})
		w.Close() // Simulate clean shutdown (or crash after fsync)
	}()

	// Phase 2: "Reboot" — open WAL and replay into a fresh store
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("replay after crash failed: %v", err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries after crash recovery, got %d", len(entries))
	}

	// Rebuild the store
	store := kvstore.New()
	for _, e := range entries {
		store.Apply(e.Command)
	}

	if store.Len() != 3 {
		t.Fatalf("expected 3 keys after recovery, got %d", store.Len())
	}

	val, ok := store.Get("b")
	if !ok || val != "2" {
		t.Fatalf("expected b=2, got %q (exists=%v)", val, ok)
	}
}

func TestCorruptedEntrySkipped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.wal")

	// Write a valid entry, then corrupt data, then another valid entry
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(`{"ts":"2024-01-01T00:00:00Z","idx":1,"cmd":{"op":"PUT","key":"good1","value":"yes"}}` + "\n")
	f.WriteString(`THIS IS CORRUPTED GARBAGE DATA` + "\n")
	f.WriteString(`{"ts":"2024-01-01T00:00:01Z","idx":3,"cmd":{"op":"PUT","key":"good2","value":"also yes"}}` + "\n")
	f.Close()

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	defer w.Close()

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("replay should succeed despite corruption: %v", err)
	}

	// Should have 2 valid entries (corrupt one skipped)
	if len(entries) != 2 {
		t.Fatalf("expected 2 valid entries, got %d", len(entries))
	}

	if entries[0].Command.Key != "good1" || entries[1].Command.Key != "good2" {
		t.Fatalf("unexpected entries: %+v", entries)
	}
}

func TestTruncate(t *testing.T) {
	w, _ := tempWAL(t)
	defer w.Close()

	w.Append(kvstore.Command{Op: "PUT", Key: "x", Value: "1"})
	w.Append(kvstore.Command{Op: "PUT", Key: "y", Value: "2"})

	if err := w.Truncate(); err != nil {
		t.Fatalf("truncate failed: %v", err)
	}

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("replay after truncate failed: %v", err)
	}

	if len(entries) != 0 {
		t.Fatalf("expected 0 entries after truncate, got %d", len(entries))
	}
}

func TestAppendAfterReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "resume.wal")

	// Write 2 entries, close
	func() {
		w, _ := Open(path)
		w.Append(kvstore.Command{Op: "PUT", Key: "a", Value: "1"})
		w.Append(kvstore.Command{Op: "PUT", Key: "b", Value: "2"})
		w.Close()
	}()

	// Reopen, replay, then append more
	w, _ := Open(path)
	defer w.Close()

	w.Replay()
	w.Append(kvstore.Command{Op: "PUT", Key: "c", Value: "3"})

	entries, _ := w.Replay()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	// Verify new entry has correct index (should be 3, not 1)
	if entries[2].Index != 3 {
		t.Fatalf("expected index 3 for new entry, got %d", entries[2].Index)
	}
}

func TestStats(t *testing.T) {
	w, _ := tempWAL(t)
	defer w.Close()

	w.Append(kvstore.Command{Op: "PUT", Key: "x", Value: "1"})
	w.Append(kvstore.Command{Op: "PUT", Key: "y", Value: "2"})

	entries, fsyncs, totalBytes := w.Stats()
	if entries != 2 {
		t.Fatalf("expected 2 entries, got %d", entries)
	}
	if fsyncs != 2 {
		t.Fatalf("expected 2 fsyncs, got %d", fsyncs)
	}
	if totalBytes == 0 {
		t.Fatal("expected non-zero total bytes")
	}
}
