// raft-kv: A distributed key-value store with Raft consensus.
//
// Phase 1: Single-node persistent KV store with REST API.
//
// This is the main entrypoint. It:
//   1. Parses command-line flags (node ID, port, data directory)
//   2. Opens the WAL file
//   3. Replays WAL entries to rebuild in-memory state (crash recovery)
//   4. Starts the HTTP server
//
// Usage:
//
//	go run ./cmd/server --id=node1 --port=8080 --data-dir=./data/node1
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/KD-joshi/raft-kv/internal/kvstore"
	"github.com/KD-joshi/raft-kv/internal/server"
	wal "github.com/KD-joshi/raft-kv/internal/storage"
)

func main() {
	// ── Parse flags ──
	nodeID := flag.String("id", "node1", "Unique node identifier")
	port := flag.Int("port", 8080, "HTTP server port")
	dataDir := flag.String("data-dir", "./data/node1", "Directory for WAL and snapshots")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║         raft-kv: Distributed Key-Value Store     ║")
	fmt.Println("║         Phase 1: Single-Node Persistent Store    ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	log.Printf("[%s] Starting node...", *nodeID)
	log.Printf("[%s] Data directory: %s", *nodeID, *dataDir)

	// ── Create data directory ──
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("[%s] FATAL: cannot create data directory: %v", *nodeID, err)
	}

	// ── Open WAL ──
	walPath := filepath.Join(*dataDir, "wal.log")
	walLog, err := wal.Open(walPath)
	if err != nil {
		log.Fatalf("[%s] FATAL: cannot open WAL: %v", *nodeID, err)
	}
	defer walLog.Close()
	log.Printf("[%s] WAL opened: %s", *nodeID, walPath)

	// ── Create KV store ──
	store := kvstore.New()

	// ── Crash Recovery: Replay WAL ──
	start := time.Now()
	entries, err := walLog.Replay()
	if err != nil {
		log.Fatalf("[%s] FATAL: WAL replay failed: %v", *nodeID, err)
	}

	if len(entries) > 0 {
		log.Printf("[%s] Replaying %d WAL entries...", *nodeID, len(entries))
		for i, entry := range entries {
			_, err := store.Apply(entry.Command)
			if err != nil {
				log.Printf("[%s] WARNING: failed to replay entry %d: %v", *nodeID, i, err)
			}
		}
		log.Printf("[%s] WAL replay complete: %d entries in %s (recovered %d keys)",
			*nodeID, len(entries), time.Since(start), store.Len())
	} else {
		log.Printf("[%s] WAL is empty — starting fresh", *nodeID)
	}

	// ── Start HTTP server ──
	addr := fmt.Sprintf(":%d", *port)
	srv := server.New(server.Config{
		NodeID:  *nodeID,
		Addr:    addr,
		DataDir: *dataDir,
	}, store, walLog)

	// ── Graceful shutdown on SIGINT/SIGTERM ──
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("[%s] Received signal %v, shutting down gracefully...", *nodeID, sig)

		// Ensure WAL is flushed
		if err := walLog.Close(); err != nil {
			log.Printf("[%s] WARNING: WAL close error: %v", *nodeID, err)
		}

		log.Printf("[%s] Goodbye.", *nodeID)
		os.Exit(0)
	}()

	// Start serving (blocks)
	log.Fatal(srv.Start())
}
