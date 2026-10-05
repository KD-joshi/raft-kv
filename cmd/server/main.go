// raft-kv: A distributed key-value store with Raft consensus.
//
// Phase 4: Fully distributed KV store with log replication.
//
// This is the main entrypoint. It:
//  1. Parses command-line flags
//  2. Creates the Raft node and gRPC transport
//  3. Connects to peer nodes
//  4. Opens the WAL and replays for crash recovery
//  5. Starts the HTTP server for client-facing API
//
// Usage:
//
//	./raft-kv-server --id=node1 --port=8001 --grpc-port=9001 \
//	  --peers=localhost:9002,localhost:9003 --data-dir=./data/node1
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
	peersFlag := flag.String("peers", "", "Comma-separated list of peer gRPC addresses (e.g. localhost:9002,localhost:9003)")
	peerIDsFlag := flag.String("peer-ids", "", "Comma-separated list of peer node IDs (e.g. node2,node3)")
	peerHTTPFlag := flag.String("peer-http", "", "Comma-separated list of peer HTTP addresses (e.g. localhost:8002,localhost:8003)")
	dataDir := flag.String("data-dir", "./data/node1", "Directory for WAL and snapshots")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║         raft-kv: Distributed Key-Value Store     ║")
	fmt.Println("║         Phase 4: Log Replication & Consensus     ║")
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
	peerHTTPAddrs := make(map[string]string)

	if *peersFlag != "" {
		peerAddrs := strings.Split(*peersFlag, ",")

		// Parse peer IDs if provided, otherwise generate them
		var peerIDs []string
		if *peerIDsFlag != "" {
			peerIDs = strings.Split(*peerIDsFlag, ",")
		}

		// Parse peer HTTP addresses if provided
		var peerHTTPs []string
		if *peerHTTPFlag != "" {
			peerHTTPs = strings.Split(*peerHTTPFlag, ",")
		}

		for i, addr := range peerAddrs {
			peerID := fmt.Sprintf("peer%d", i+1)
			if i < len(peerIDs) {
				peerID = peerIDs[i]
			}

			log.Printf("[%s] Connecting to peer %s at %s...", *nodeID, peerID, addr)

			peer, err := transport.ConnectPeer(peerID, addr)
			if err != nil {
				log.Printf("[%s] WARNING: could not connect to %s: %v", *nodeID, peerID, err)
				continue
			}
			peers = append(peers, peer)
			log.Printf("[%s] Connected to peer %s", *nodeID, peerID)

			// Map peer HTTP address for redirects
			if i < len(peerHTTPs) {
				peerHTTPAddrs[peerID] = peerHTTPs[i]
			}
		}
	}

	// Also add our own HTTP address to the map
	peerHTTPAddrs[*nodeID] = fmt.Sprintf("localhost:%d", *httpPort)

	// ── 3. Start Raft Consensus Loop ──
	raftNode.SetPeers(peers)

	// ── 4. Create data directory ──
	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("[%s] FATAL: cannot create data directory: %v", *nodeID, err)
	}

	// ── 5. Open WAL ──
	walPath := filepath.Join(*dataDir, "wal.log")
	walLog, err := wal.Open(walPath)
	if err != nil {
		log.Fatalf("[%s] FATAL: cannot open WAL: %v", *nodeID, err)
	}
	defer walLog.Close()
	log.Printf("[%s] WAL opened: %s", *nodeID, walPath)

	// ── 6. Create KV store ──
	store := kvstore.New()

	// ── 7. Crash Recovery: Replay WAL ──
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

	// ── 8. Wire Raft node to the KV store and start consensus ──
	raftNode.SetStore(store)
	raftNode.Run()

	// ── 9. Start HTTP server (Client-Facing) ──
	httpAddr := fmt.Sprintf(":%d", *httpPort)
	srv := server.New(server.Config{
		NodeID:        *nodeID,
		Addr:          httpAddr,
		DataDir:       *dataDir,
		PeerHTTPAddrs: peerHTTPAddrs,
	}, store, walLog, raftNode)

	// ── 10. Graceful shutdown on SIGINT/SIGTERM ──
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("[%s] Received signal %v, shutting down gracefully...", *nodeID, sig)

		if err := walLog.Close(); err != nil {
			log.Printf("[%s] WARNING: WAL close error: %v", *nodeID, err)
		}

		log.Printf("[%s] Goodbye.", *nodeID)
		os.Exit(0)
	}()

	// Start serving (blocks)
	log.Fatal(srv.Start())
}
