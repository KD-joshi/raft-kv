# raft-kv Makefile
# ──────────────────────────────────────────────────────────────
# Targets for building, testing, and running the distributed KV store.

SHELL := /bin/bash
GO := $(HOME)/go/bin/go
BINARY := raft-kv-server
DATA_DIR := ./data

.PHONY: all build run test clean fmt vet lint help proto

# ── Protobuf ─────────────────────────────────────────────────

proto:  ## Generate Go code from Protobuf definitions
	$(HOME)/.local/bin/protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/raft/raft.proto

# ── Default ──────────────────────────────────────────────────

all: fmt vet test build  ## Build everything (format, vet, test, compile)

# ── Build ────────────────────────────────────────────────────

build:  ## Compile the server binary
	$(GO) build -o $(BINARY) ./cmd/server

# ── Run ──────────────────────────────────────────────────────

run: build  ## Build and run a single node on port 8080
	./$(BINARY) --id=node1 --port=8080 --data-dir=$(DATA_DIR)/node1

run-node1: build  ## Run node1 on port 8001
	./$(BINARY) --id=node1 --port=8001 --grpc-port=9001 --peers=node2:9002,node3:9003 --data-dir=$(DATA_DIR)/node1

run-node2: build  ## Run node2 on port 8002
	./$(BINARY) --id=node2 --port=8002 --grpc-port=9002 --peers=node1:9001,node3:9003 --data-dir=$(DATA_DIR)/node2

run-node3: build  ## Run node3 on port 8003
	./$(BINARY) --id=node3 --port=8003 --grpc-port=9003 --peers=node1:9001,node2:9002 --data-dir=$(DATA_DIR)/node3

# ── Test ─────────────────────────────────────────────────────

test:  ## Run all tests with verbose output and race detector
	$(GO) test -v -race -count=1 ./...

test-cover:  ## Run tests with coverage report
	$(GO) test -v -race -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

test-kvstore:  ## Run only KV store tests
	$(GO) test -v -race ./internal/kvstore/...

test-wal:  ## Run only WAL tests
	$(GO) test -v -race ./internal/storage/...

# ── Code Quality ─────────────────────────────────────────────

fmt:  ## Format all Go source files
	$(GO) fmt ./...

vet:  ## Run Go vet static analysis
	$(GO) vet ./...

# ── Cleanup ──────────────────────────────────────────────────

clean:  ## Remove build artifacts and data
	rm -f $(BINARY)
	rm -f coverage.out coverage.html
	rm -rf $(DATA_DIR)

clean-data:  ## Remove only data files (WAL, snapshots), keep binary
	rm -rf $(DATA_DIR)

# ── Quick smoke test ─────────────────────────────────────────

smoke:  ## Run the server and test PUT/GET/DELETE via curl
	@echo "Starting server in background..."
	@./$(BINARY) --id=smoke --port=9999 --data-dir=$(DATA_DIR)/smoke &
	@sleep 1
	@echo ""
	@echo "── PUT name=kuldeep ──"
	@curl -s -X PUT 'http://localhost:9999/kv/name?val=kuldeep' | python3 -m json.tool
	@echo ""
	@echo "── GET name ──"
	@curl -s 'http://localhost:9999/kv/name' | python3 -m json.tool
	@echo ""
	@echo "── DELETE name ──"
	@curl -s -X DELETE 'http://localhost:9999/kv/name' | python3 -m json.tool
	@echo ""
	@echo "── GET name (should be 404) ──"
	@curl -s 'http://localhost:9999/kv/name' | python3 -m json.tool
	@echo ""
	@echo "── Health ──"
	@curl -s 'http://localhost:9999/health' | python3 -m json.tool
	@echo ""
	@echo "── Metrics ──"
	@curl -s 'http://localhost:9999/metrics'
	@echo ""
	@kill %1 2>/dev/null || true
	@rm -rf $(DATA_DIR)/smoke
	@echo "Smoke test complete."

# ── Help ─────────────────────────────────────────────────────

help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'
