# Raft-KV: Distributed Key-Value Store

A distributed, highly available key-value store implemented in Go. This project features a scratch-built implementation of the **Raft consensus algorithm**, demonstrating core distributed systems engineering principles without relying on external consensus libraries.

## Architecture & Components

The system is built upon five foundational layers:

| Component | Description | Implementation Details |
| :--- | :--- | :--- |
| **Consensus Engine** | The Raft algorithm implementation. | Handles Leader Election, Log Replication, Heartbeats, Quorum validation, and Split-Brain resolution. |
| **State Machine** | The core database layer. | Thread-safe, in-memory `map[string]string` protected by `sync.RWMutex`. Mutations only occur after consensus is reached. |
| **Write-Ahead Log (WAL)** | Persistent storage for crash recovery. | Append-only log on disk. Every command is fsynced to disk before being acknowledged, ensuring zero data loss on node failure. |
| **RPC Layer** | Node-to-node communication. | Built using **gRPC** and Protobufs (`AppendEntries`, `RequestVote`). Handles network partitions and node reconnections. |
| **HTTP API Layer** | Client-facing interface. | Exposes REST endpoints (`PUT`, `GET`, `DELETE`). Implements automatic client routing (Followers redirect writes to the Leader). |

## Core Capabilities Demonstrated

This codebase serves as a functional demonstration of the mechanisms that power industry-standard distributed systems like **etcd**, **Consul**, and **Zookeeper**.

1. **State Synchronization**: Replicating log entries across a distributed cluster over an unreliable network.
2. **Fault Tolerance**: The cluster remains fully operational as long as a majority (quorum) of nodes are alive.
3. **Crash Recovery**: Nodes can crash, restart, and completely recover their state by replaying the Write-Ahead Log.
4. **Consistency**: Adheres to strict Raft consistency guarantees. Reads and writes reflect the latest committed state of the cluster.

## Development Phases

The project was constructed in four systematic phases:

| Phase | Focus | Result |
| :---: | :--- | :--- |
| **1** | Single-Node Foundation | Built the in-memory KV store, persistent WAL with recovery, and REST API. |
| **2** | RPC Networking | Implemented gRPC definitions and bidirectional peer-to-peer connection management. |
| **3** | Leader Election | Implemented randomized election timers, Candidate voting, Quorum validation, and Leader heartbeats. |
| **4** | Log Replication | Implemented the `AppendEntries` pipeline, 2-phase commits, client request routing, and state machine application. |

## Quick Start

### Prerequisites
* Go 1.25+
* Protocol Buffers compiler (`protoc`)
* Make

### Running a 3-Node Cluster Locally

1. **Build the binary**:
   ```bash
   make build
   ```

2. **Start the nodes** (in separate terminal windows/tabs):
   ```bash
   make run-node1
   make run-node2
   make run-node3
   ```

### Client Interactions

You can interact with any node. If you attempt to write to a Follower, it will automatically redirect you (HTTP 307) to the current Leader.

**Write a value (PUT):**
```bash
curl -L -X PUT 'http://localhost:8001/kv/database?val=raft'
```
*(Note the `-L` flag tells curl to follow the redirect if Node 1 is not the leader)*

**Read a value (GET):**
```bash
curl 'http://localhost:8001/kv/database'
```
*(Reads can be served locally by any node)*

**Check Node Health & Raft State:**
```bash
curl 'http://localhost:8001/health'
```

### Metrics & Observability
Every node exposes a Prometheus-compatible `/metrics` endpoint tracking operation counts, WAL sizes, and Raft state transitions.

```bash
curl 'http://localhost:8001/metrics'
```
