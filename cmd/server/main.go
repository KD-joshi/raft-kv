// raft-kv: A distributed key-value store with Raft consensus.
//
// Phase 1: Single-node persistent KV store with REST API.
//
// This is the main entrypoint. It:
//  1. Parses command-line flags (node ID, port, data directory)
//  2. Opens the WAL file
//  3. Replays WAL entries to rebuild in-memory state (crash recovery)
//  4. Starts the HTTP server
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
	"strings"
	"syscall"
	"time"

	"github.com/KD-joshi/raft-kv/internal/kvstore"
	"github.com/KD-joshi/raft-kv/internal/raft"
	"github.com/KD-joshi/raft-kv/internal/server"
	wal "github.com/KD-joshi/raft-kv/internal/storage"
	"github.com/KD-joshi/raft-kv/internal/transport"
)

func main() {
	// ── Parse flags ──
	nodeID := flag.String("id", "node1", "Unique node identifier")
	httpPort := flag.Int("port", 8080, "HTTP server port (for clients)")
	grpcPort := flag.Int("grpc-port", 9080, "gRPC server port (for node-to-node RPC)")
	peersFlag := flag.String("peers", "", "Comma-separated list of peers (e.g. node2:9081,node3:9082)")
	dataDir := flag.String("data-dir", "./data/node1", "Directory for WAL and snapshots")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║         raft-kv: Distributed Key-Value Store     ║")
	fmt.Println("║         Phase 2: RPC Networking (gRPC)           ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	log.Printf("[%s] Starting node...", *nodeID)
	log.Printf("[%s] Data directory: %s", *nodeID, *dataDir)

	// ── 0. Create Raft Node ──
	raftNode := raft.NewNode(*nodeID)

	// ── 1. Start gRPC Server (Node-to-Node) ──
	grpcAddr := fmt.Sprintf(":%d", *grpcPort)
	grpcSrv, err := transport.StartGRPCServer(*nodeID, grpcAddr, raftNode)
	if err != nil {
		log.Fatalf("[%s] FATAL: failed to start gRPC server: %v", *nodeID, err)
	}
	defer grpcSrv.GracefulStop()

	// ── 2. Connect to Peers ──
	var peers []*transport.Peer
	if *peersFlag != "" {
		peerAddrs := strings.Split(*peersFlag, ",")
		for i, addr := range peerAddrs {
			peerID := fmt.Sprintf("peer%d", i+1)

			log.Printf("[%s] Attempting to connect to peer %s at %s...", *nodeID, peerID, addr)

			// We don't block here, gRPC handles reconnects in the background
			peer, err := transport.ConnectPeer(peerID, addr)
			if err != nil {
				log.Printf("[%s] WARNING: could not connect to %s: %v", *nodeID, peerID, err)
				continue
			}
			peers = append(peers, peer)
			log.Printf("[%s] Connected to peer %s", *nodeID, peerID)
		}
	}

	// ── 3. Start Raft Consensus Loop ──
	raftNode.SetPeers(peers)
	raftNode.Run()

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

	// ── Start HTTP server (Client-Facing) ──
	httpAddr := fmt.Sprintf(":%d", *httpPort)
	srv := server.New(server.Config{
		NodeID:  *nodeID,
		Addr:    httpAddr,
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
