package raft

import "errors"

// Sentinel errors for the Raft consensus layer.
var (
	// ErrNotLeader is returned when a write is proposed to a non-leader node.
	ErrNotLeader = errors.New("raft: not the leader")

	// ErrCommitTimeout is returned when a proposed entry doesn't get committed
	// within the specified timeout (the cluster may be partitioned or unhealthy).
	ErrCommitTimeout = errors.New("raft: commit timeout, entry may not have been replicated")
)
