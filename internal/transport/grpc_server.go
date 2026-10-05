// Package transport provides the gRPC networking layer for node-to-node communication.
package transport

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/KD-joshi/raft-kv/proto/raft"
	"google.golang.org/grpc"
)

// RaftHandler is an interface to break the circular dependency between transport and raft packages.
// In a larger app, we'd define this in a separate interface package, but here it works nicely.
type RaftHandler interface {
	HandleRequestVote(req *raft.RequestVoteRequest) *raft.RequestVoteResponse
	ResetElectionTimer()
}

// RaftGRPCServer implements the RaftService gRPC interface.
type RaftGRPCServer struct {
	raft.UnimplementedRaftServiceServer
	nodeID  string
	handler RaftHandler
}

// Ping is a simple heartbeat receiver.
func (s *RaftGRPCServer) Ping(ctx context.Context, req *raft.PingRequest) (*raft.PingResponse, error) {
	// Let the Raft node know we heard from the leader
	s.handler.ResetElectionTimer()
	
	// Comment out the log so it doesn't spam the console 5 times a second
	// log.Printf("[%s] Received Ping from %s", s.nodeID, req.SenderId)

	return &raft.PingResponse{
		ReceiverId: s.nodeID,
		Success:    true,
	}, nil
}

// RequestVote handles an incoming election vote request.
func (s *RaftGRPCServer) RequestVote(ctx context.Context, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	resp := s.handler.HandleRequestVote(req)
	return resp, nil
}

// StartGRPCServer initializes and starts a gRPC server on the given address.
func StartGRPCServer(nodeID, addr string, handler RaftHandler) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer()
	
	raftServer := &RaftGRPCServer{
		nodeID:  nodeID,
		handler: handler,
	}

	// Register our implementation with the generated gRPC server code
	raft.RegisterRaftServiceServer(grpcServer, raftServer)

	// Start serving in a background goroutine
	go func() {
		log.Printf("[%s] gRPC Server listening on %s", nodeID, addr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("[%s] gRPC server failed to serve: %v", nodeID, err)
		}
	}()

	return grpcServer, nil
}
