// Package server implements the HTTP REST API layer for the KV store.
//
// Routes:
//   - PUT /kv/{key}?val={value}  → Write a key-value pair (routed through Raft)
//   - GET /kv/{key}              → Read a value by key (served locally)
//   - DELETE /kv/{key}           → Delete a key (routed through Raft)
//   - GET /health                → Health check + node status
//   - GET /keys                  → List all keys (debug)
//   - GET /metrics               → Prometheus-compatible metrics endpoint
//
// If this node is NOT the leader, PUT/DELETE will return an HTTP redirect
// telling the client which node is the current leader.
package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/KD-joshi/raft-kv/internal/kvstore"
	"github.com/KD-joshi/raft-kv/internal/raft"
	wal "github.com/KD-joshi/raft-kv/internal/storage"
)

// Server holds the HTTP server, KV store, and WAL references.
type Server struct {
	store    *kvstore.Store
	wal      *wal.WAL
	raftNode *raft.Node
	nodeID   string
	addr     string
	mux      *http.ServeMux
	start    time.Time

	// peerHTTPAddrs maps peer nodeIDs to their HTTP addresses for redirects.
	// e.g. {"node1": "localhost:8001", "node2": "localhost:8002"}
	peerHTTPAddrs map[string]string
}

// Config holds server configuration.
type Config struct {
	NodeID        string            // Unique identifier for this node (e.g., "node1")
	Addr          string            // Listen address (e.g., ":8080")
	DataDir       string            // Directory for WAL and snapshots
	PeerHTTPAddrs map[string]string // Map of nodeID -> HTTP address
}

// APIResponse is the standard JSON response envelope.
type APIResponse struct {
	Success  bool   `json:"success"`
	Key      string `json:"key,omitempty"`
	Value    string `json:"value,omitempty"`
	Message  string `json:"message,omitempty"`
	NodeID   string `json:"node_id"`
	LeaderID string `json:"leader_id,omitempty"`
}

// New creates a new HTTP server with all routes registered.
func New(cfg Config, store *kvstore.Store, walLog *wal.WAL, raftNode *raft.Node) *Server {
	s := &Server{
		store:         store,
		wal:           walLog,
		raftNode:      raftNode,
		nodeID:        cfg.NodeID,
		addr:          cfg.Addr,
		mux:           http.NewServeMux(),
		start:         time.Now(),
		peerHTTPAddrs: cfg.PeerHTTPAddrs,
	}

	s.registerRoutes()
	return s
}

// registerRoutes sets up all HTTP handlers.
func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/kv/", s.handleKV)
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/keys", s.handleKeys)
	s.mux.HandleFunc("/metrics", s.handleMetrics)
	s.mux.HandleFunc("/", s.handleRoot)
}

// Start begins listening for HTTP requests. Blocks until the server is stopped.
func (s *Server) Start() error {
	log.Printf("[%s] HTTP server starting on %s", s.nodeID, s.addr)
	log.Printf("[%s] Endpoints:", s.nodeID)
	log.Printf("[%s]   PUT    /kv/{key}?val={value}  - Set a key", s.nodeID)
	log.Printf("[%s]   GET    /kv/{key}              - Get a key", s.nodeID)
	log.Printf("[%s]   DELETE /kv/{key}              - Delete a key", s.nodeID)
	log.Printf("[%s]   GET    /health                - Health check", s.nodeID)
	log.Printf("[%s]   GET    /keys                  - List all keys", s.nodeID)
	log.Printf("[%s]   GET    /metrics               - Prometheus metrics", s.nodeID)

	return http.ListenAndServe(s.addr, s.withLogging(s.mux))
}

// withLogging wraps the handler with request logging middleware.
func (s *Server) withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("[%s] %s %s %s", s.nodeID, r.Method, r.URL.Path, time.Since(start))
	})
}

// ─── KV Handler ────────────────────────────────────────────────

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	key = strings.TrimSpace(key)

	if key == "" {
		s.respondError(w, http.StatusBadRequest, "key is required in URL path: /kv/{key}")
		return
	}

	switch r.Method {
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		s.respondError(w, http.StatusMethodNotAllowed,
			fmt.Sprintf("method %s not allowed; use GET, PUT, or DELETE", r.Method))
	}
}

// handlePut writes a key-value pair via the Raft consensus pipeline.
//
// Flow:
//  1. If not the leader → respond with a redirect to the leader
//  2. Propose the command to the Raft log
//  3. Raft replicates to a quorum of followers
//  4. Once committed, the applyLoop applies it to the KV store
//  5. Respond 200 OK to the client
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	val := r.URL.Query().Get("val")
	if val == "" {
		s.respondError(w, http.StatusBadRequest, "val query parameter is required: /kv/{key}?val={value}")
		return
	}

	cmd := kvstore.Command{
		Op:    "PUT",
		Key:   key,
		Value: val,
	}

	// Route through Raft consensus
	err := s.raftNode.Propose(cmd, 5*time.Second)
	if err != nil {
		if err == raft.ErrNotLeader {
			s.redirectToLeader(w, r)
			return
		}
		s.respondError(w, http.StatusInternalServerError, fmt.Sprintf("consensus failed: %v", err))
		return
	}

	log.Printf("[%s] PUT key=%q val=%q (committed via Raft)", s.nodeID, key, val)

	s.respondJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Key:     key,
		Value:   val,
		Message: "committed via Raft consensus",
		NodeID:  s.nodeID,
	})
}

// handleGet reads a value by key. Served locally (reads don't need consensus).
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	val, exists := s.store.Get(key)

	if !exists {
		s.respondJSON(w, http.StatusNotFound, APIResponse{
			Success: false,
			Key:     key,
			Message: "key not found",
			NodeID:  s.nodeID,
		})
		return
	}

	s.respondJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Key:     key,
		Value:   val,
		NodeID:  s.nodeID,
	})
}

// handleDelete removes a key via the Raft consensus pipeline.
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	cmd := kvstore.Command{
		Op:  "DELETE",
		Key: key,
	}

	err := s.raftNode.Propose(cmd, 5*time.Second)
	if err != nil {
		if err == raft.ErrNotLeader {
			s.redirectToLeader(w, r)
			return
		}
		s.respondError(w, http.StatusInternalServerError, fmt.Sprintf("consensus failed: %v", err))
		return
	}

	log.Printf("[%s] DELETE key=%q (committed via Raft)", s.nodeID, key)

	s.respondJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Key:     key,
		Message: "deleted via Raft consensus",
		NodeID:  s.nodeID,
	})
}

// redirectToLeader sends a JSON response telling the client who the leader is.
func (s *Server) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leaderID := s.raftNode.LeaderID()
	msg := "not the leader"
	if leaderID != "" {
		msg = fmt.Sprintf("not the leader, try node %s", leaderID)
		// If we know the leader's HTTP address, provide it
		if addr, ok := s.peerHTTPAddrs[leaderID]; ok {
			msg = fmt.Sprintf("not the leader, redirect to http://%s%s", addr, r.URL.String())
		}
	}

	s.respondJSON(w, http.StatusTemporaryRedirect, APIResponse{
		Success:  false,
		Message:  msg,
		NodeID:   s.nodeID,
		LeaderID: leaderID,
	})
}

// ─── Utility Endpoints ────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	puts, gets, deletes := s.store.Stats()
	walEntries, walFsyncs, walBytes := s.wal.Stats()
	state, term := s.raftNode.GetState()

	resp := map[string]interface{}{
		"status":      "healthy",
		"node_id":     s.nodeID,
		"uptime":      time.Since(s.start).String(),
		"keys_stored": s.store.Len(),
		"raft": map[string]interface{}{
			"state":     state.String(),
			"term":      term,
			"leader_id": s.raftNode.LeaderID(),
			"is_leader": s.raftNode.IsLeader(),
		},
		"store_stats": map[string]uint64{
			"puts":    puts,
			"gets":    gets,
			"deletes": deletes,
		},
		"wal_stats": map[string]uint64{
			"entries":     walEntries,
			"fsyncs":      walFsyncs,
			"total_bytes": walBytes,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	keys := s.store.Keys()

	resp := map[string]interface{}{
		"count": len(keys),
		"keys":  keys,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	puts, gets, deletes := s.store.Stats()
	walEntries, walFsyncs, walBytes := s.wal.Stats()
	uptime := time.Since(s.start).Seconds()
	state, term := s.raftNode.GetState()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	fmt.Fprintf(w, "# HELP raftkv_store_operations_total Total KV store operations by type.\n")
	fmt.Fprintf(w, "# TYPE raftkv_store_operations_total counter\n")
	fmt.Fprintf(w, "raftkv_store_operations_total{op=\"put\",node=\"%s\"} %d\n", s.nodeID, puts)
	fmt.Fprintf(w, "raftkv_store_operations_total{op=\"get\",node=\"%s\"} %d\n", s.nodeID, gets)
	fmt.Fprintf(w, "raftkv_store_operations_total{op=\"delete\",node=\"%s\"} %d\n", s.nodeID, deletes)
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "# HELP raftkv_store_keys_total Current number of keys in the store.\n")
	fmt.Fprintf(w, "# TYPE raftkv_store_keys_total gauge\n")
	fmt.Fprintf(w, "raftkv_store_keys_total{node=\"%s\"} %d\n", s.nodeID, s.store.Len())
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "# HELP raftkv_wal_entries_total Total WAL entries written.\n")
	fmt.Fprintf(w, "# TYPE raftkv_wal_entries_total counter\n")
	fmt.Fprintf(w, "raftkv_wal_entries_total{node=\"%s\"} %d\n", s.nodeID, walEntries)
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "# HELP raftkv_wal_fsyncs_total Total WAL fsync operations.\n")
	fmt.Fprintf(w, "# TYPE raftkv_wal_fsyncs_total counter\n")
	fmt.Fprintf(w, "raftkv_wal_fsyncs_total{node=\"%s\"} %d\n", s.nodeID, walFsyncs)
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "# HELP raftkv_wal_bytes_total Total bytes written to WAL.\n")
	fmt.Fprintf(w, "# TYPE raftkv_wal_bytes_total counter\n")
	fmt.Fprintf(w, "raftkv_wal_bytes_total{node=\"%s\"} %d\n", s.nodeID, walBytes)
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "# HELP raftkv_uptime_seconds Node uptime in seconds.\n")
	fmt.Fprintf(w, "# TYPE raftkv_uptime_seconds gauge\n")
	fmt.Fprintf(w, "raftkv_uptime_seconds{node=\"%s\"} %.2f\n", s.nodeID, uptime)
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "# HELP raftkv_raft_state Current Raft state (0=Follower, 1=Candidate, 2=Leader).\n")
	fmt.Fprintf(w, "# TYPE raftkv_raft_state gauge\n")
	fmt.Fprintf(w, "raftkv_raft_state{node=\"%s\"} %d\n", s.nodeID, state)
	fmt.Fprintf(w, "\n")
	fmt.Fprintf(w, "# HELP raftkv_raft_term Current Raft term.\n")
	fmt.Fprintf(w, "# TYPE raftkv_raft_term gauge\n")
	fmt.Fprintf(w, "raftkv_raft_term{node=\"%s\"} %d\n", s.nodeID, term)
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	state, term := s.raftNode.GetState()

	resp := map[string]interface{}{
		"name":    "raft-kv",
		"node_id": s.nodeID,
		"version": "0.4.0 (Phase 4: Distributed Consensus)",
		"raft": map[string]interface{}{
			"state":     state.String(),
			"term":      term,
			"leader_id": s.raftNode.LeaderID(),
		},
		"endpoints": map[string]string{
			"PUT /kv/{key}?val={value}": "Set a key-value pair (routed through Raft leader)",
			"GET /kv/{key}":            "Get a value by key",
			"DELETE /kv/{key}":         "Delete a key (routed through Raft leader)",
			"GET /health":             "Health check with Raft state",
			"GET /keys":               "List all keys",
			"GET /metrics":            "Prometheus metrics",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// ─── Response Helpers ──────────────────────────────────────────

func (s *Server) respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (s *Server) respondError(w http.ResponseWriter, status int, msg string) {
	s.respondJSON(w, status, APIResponse{
		Success: false,
		Message: msg,
		NodeID:  s.nodeID,
	})
}
