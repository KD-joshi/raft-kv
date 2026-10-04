// Package server implements the HTTP REST API layer for the KV store.
//
// This is the client-facing interface:
//   - PUT /key?val=value  → Write a key-value pair
//   - GET /key            → Read a value by key
//   - DELETE /key         → Delete a key
//   - GET /health         → Health check + node status
//   - GET /keys           → List all keys (debug)
//   - GET /metrics        → Prometheus-compatible metrics endpoint
//
// In Phase 4, writes will be routed through the Raft leader.
// If this node is not the leader, PUT/DELETE will return a redirect.
package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/kuldeep-joshi/raft-kv/internal/kvstore"
	wal "github.com/kuldeep-joshi/raft-kv/internal/storage"
)

// Server holds the HTTP server, KV store, and WAL references.
type Server struct {
	store  *kvstore.Store
	wal    *wal.WAL
	nodeID string
	addr   string
	mux    *http.ServeMux
	start  time.Time
}

// Config holds server configuration.
type Config struct {
	NodeID  string // Unique identifier for this node (e.g., "node1")
	Addr    string // Listen address (e.g., ":8080")
	DataDir string // Directory for WAL and snapshots
}

// APIResponse is the standard JSON response envelope.
type APIResponse struct {
	Success bool   `json:"success"`
	Key     string `json:"key,omitempty"`
	Value   string `json:"value,omitempty"`
	Message string `json:"message,omitempty"`
	NodeID  string `json:"node_id"`
}

// New creates a new HTTP server with all routes registered.
func New(cfg Config, store *kvstore.Store, walLog *wal.WAL) *Server {
	s := &Server{
		store:  store,
		wal:    walLog,
		nodeID: cfg.NodeID,
		addr:   cfg.Addr,
		mux:    http.NewServeMux(),
		start:  time.Now(),
	}

	s.registerRoutes()
	return s
}

// registerRoutes sets up all HTTP handlers.
func (s *Server) registerRoutes() {
	// KV operations — route by HTTP method
	s.mux.HandleFunc("/kv/", s.handleKV)

	// Health check
	s.mux.HandleFunc("/health", s.handleHealth)

	// Debug: list all keys
	s.mux.HandleFunc("/keys", s.handleKeys)

	// Prometheus metrics
	s.mux.HandleFunc("/metrics", s.handleMetrics)

	// Root
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

// handleKV dispatches to PUT, GET, or DELETE based on HTTP method.
// URL pattern: /kv/{key}
func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	// Extract key from URL path: /kv/mykey → "mykey"
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

// handlePut writes a key-value pair.
// Flow: Validate → WAL write (fsync) → Apply to KV store → Respond
//
// The order is critical for crash safety:
//   1. WAL first (persisted to disk)
//   2. KV store second (in-memory)
//   3. Response to client (only after both succeed)
//
// If we crash between 1 and 2, replay will re-apply the command.
// If we crash before 1, the client never got a success response.
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

	// Step 1: Write to WAL (crash safety)
	idx, err := s.wal.Append(cmd)
	if err != nil {
		s.respondError(w, http.StatusInternalServerError, fmt.Sprintf("WAL write failed: %v", err))
		return
	}

	// Step 2: Apply to in-memory store
	_, err = s.store.Apply(cmd)
	if err != nil {
		s.respondError(w, http.StatusInternalServerError, fmt.Sprintf("apply failed: %v", err))
		return
	}

	log.Printf("[%s] PUT key=%q val=%q wal_idx=%d", s.nodeID, key, val, idx)

	s.respondJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Key:     key,
		Value:   val,
		Message: fmt.Sprintf("stored at WAL index %d", idx),
		NodeID:  s.nodeID,
	})
}

// handleGet reads a value by key.
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

// handleDelete removes a key.
// Same flow as PUT: WAL → Apply → Respond.
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	cmd := kvstore.Command{
		Op:  "DELETE",
		Key: key,
	}

	// Step 1: WAL
	_, err := s.wal.Append(cmd)
	if err != nil {
		s.respondError(w, http.StatusInternalServerError, fmt.Sprintf("WAL write failed: %v", err))
		return
	}

	// Step 2: Apply
	_, err = s.store.Apply(cmd)
	if err != nil {
		s.respondError(w, http.StatusInternalServerError, fmt.Sprintf("apply failed: %v", err))
		return
	}

	log.Printf("[%s] DELETE key=%q", s.nodeID, key)

	s.respondJSON(w, http.StatusOK, APIResponse{
		Success: true,
		Key:     key,
		Message: "deleted",
		NodeID:  s.nodeID,
	})
}

// ─── Utility Endpoints ────────────────────────────────────────

// handleHealth returns node status for monitoring.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	puts, gets, deletes := s.store.Stats()
	walEntries, walFsyncs, walBytes := s.wal.Stats()

	resp := map[string]interface{}{
		"status":      "healthy",
		"node_id":     s.nodeID,
		"uptime":      time.Since(s.start).String(),
		"keys_stored": s.store.Len(),
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

// handleKeys lists all keys in the store (for debugging).
func (s *Server) handleKeys(w http.ResponseWriter, r *http.Request) {
	keys := s.store.Keys()

	resp := map[string]interface{}{
		"count": len(keys),
		"keys":  keys,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// handleMetrics exposes Prometheus-compatible metrics.
// Prometheus scrapes this endpoint periodically.
//
// Metrics exposed:
//   - raftkv_store_operations_total{op="put|get|delete"}
//   - raftkv_store_keys_total
//   - raftkv_wal_entries_total
//   - raftkv_wal_fsyncs_total
//   - raftkv_wal_bytes_total
//   - raftkv_uptime_seconds
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	puts, gets, deletes := s.store.Stats()
	walEntries, walFsyncs, walBytes := s.wal.Stats()
	uptime := time.Since(s.start).Seconds()

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
}

// handleRoot shows a welcome message with API docs.
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	resp := map[string]interface{}{
		"name":    "raft-kv",
		"node_id": s.nodeID,
		"version": "0.1.0 (Phase 1: Single-Node KV Store)",
		"endpoints": map[string]string{
			"PUT /kv/{key}?val={value}": "Set a key-value pair",
			"GET /kv/{key}":            "Get a value by key",
			"DELETE /kv/{key}":         "Delete a key",
			"GET /health":             "Health check with stats",
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
