package kvstore

import (
	"sync"
	"testing"
)

func TestNew(t *testing.T) {
	s := New()
	if s == nil {
		t.Fatal("New() returned nil")
	}
	if s.Len() != 0 {
		t.Fatalf("expected empty store, got %d keys", s.Len())
	}
}

func TestPutAndGet(t *testing.T) {
	s := New()

	// PUT a key
	_, err := s.Apply(Command{Op: "PUT", Key: "name", Value: "kuldeep"})
	if err != nil {
		t.Fatalf("Apply PUT failed: %v", err)
	}

	// GET it back
	val, ok := s.Get("name")
	if !ok {
		t.Fatal("expected key 'name' to exist")
	}
	if val != "kuldeep" {
		t.Fatalf("expected 'kuldeep', got %q", val)
	}
}

func TestGetNonExistent(t *testing.T) {
	s := New()

	_, ok := s.Get("ghost")
	if ok {
		t.Fatal("expected key 'ghost' to not exist")
	}
}

func TestPutOverwrite(t *testing.T) {
	s := New()

	s.Apply(Command{Op: "PUT", Key: "x", Value: "1"})
	s.Apply(Command{Op: "PUT", Key: "x", Value: "2"})

	val, _ := s.Get("x")
	if val != "2" {
		t.Fatalf("expected overwritten value '2', got %q", val)
	}
	if s.Len() != 1 {
		t.Fatalf("expected 1 key after overwrite, got %d", s.Len())
	}
}

func TestDelete(t *testing.T) {
	s := New()

	s.Apply(Command{Op: "PUT", Key: "temp", Value: "data"})
	s.Apply(Command{Op: "DELETE", Key: "temp"})

	_, ok := s.Get("temp")
	if ok {
		t.Fatal("expected key 'temp' to be deleted")
	}
	if s.Len() != 0 {
		t.Fatalf("expected 0 keys after delete, got %d", s.Len())
	}
}

func TestDeleteNonExistent(t *testing.T) {
	s := New()

	// Deleting a non-existent key should not error
	_, err := s.Apply(Command{Op: "DELETE", Key: "ghost"})
	if err != nil {
		t.Fatalf("delete non-existent key should not error: %v", err)
	}
}

func TestUnknownOperation(t *testing.T) {
	s := New()

	_, err := s.Apply(Command{Op: "PATCH", Key: "x", Value: "y"})
	if err == nil {
		t.Fatal("expected error for unknown operation 'PATCH'")
	}
}

func TestSnapshotAndRestore(t *testing.T) {
	// Create store with data
	s1 := New()
	s1.Apply(Command{Op: "PUT", Key: "a", Value: "1"})
	s1.Apply(Command{Op: "PUT", Key: "b", Value: "2"})
	s1.Apply(Command{Op: "PUT", Key: "c", Value: "3"})

	// Take snapshot
	snap, err := s1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Restore into a fresh store
	s2 := New()
	if err := s2.Restore(snap); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	// Verify all data is present
	if s2.Len() != 3 {
		t.Fatalf("expected 3 keys after restore, got %d", s2.Len())
	}
	for _, key := range []string{"a", "b", "c"} {
		v1, _ := s1.Get(key)
		v2, _ := s2.Get(key)
		if v1 != v2 {
			t.Fatalf("key %q: expected %q, got %q", key, v1, v2)
		}
	}
}

func TestConcurrentReadWrite(t *testing.T) {
	s := New()
	var wg sync.WaitGroup

	// 100 concurrent writers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key" + string(rune('A'+i%26))
			s.Apply(Command{Op: "PUT", Key: key, Value: "val"})
		}(i)
	}

	// 100 concurrent readers
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key" + string(rune('A'+i%26))
			s.Get(key) // Should not panic
		}(i)
	}

	wg.Wait()

	// The store should have at most 26 keys (A-Z)
	if s.Len() > 26 {
		t.Fatalf("unexpected key count: %d", s.Len())
	}
}

func TestCommandEncoding(t *testing.T) {
	original := Command{Op: "PUT", Key: "test", Value: "hello world"}

	data, err := EncodeCommand(original)
	if err != nil {
		t.Fatalf("EncodeCommand failed: %v", err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("DecodeCommand failed: %v", err)
	}

	if decoded.Op != original.Op || decoded.Key != original.Key || decoded.Value != original.Value {
		t.Fatalf("decoded command doesn't match: %+v vs %+v", decoded, original)
	}
}

func TestKeys(t *testing.T) {
	s := New()
	s.Apply(Command{Op: "PUT", Key: "x", Value: "1"})
	s.Apply(Command{Op: "PUT", Key: "y", Value: "2"})

	keys := s.Keys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys, got %d", len(keys))
	}

	// Keys can be in any order
	found := make(map[string]bool)
	for _, k := range keys {
		found[k] = true
	}
	if !found["x"] || !found["y"] {
		t.Fatalf("missing keys: %v", keys)
	}
}
