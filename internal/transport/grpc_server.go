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

// RaftGRPCServer implements the RaftService gRPC interface.
type RaftGRPCServer struct {
	raft.UnimplementedRaftServiceServer
	nodeID string
}

// Ping is a simple heartbeat receiver.
func (s *RaftGRPCServer) Ping(ctx context.Context, req *raft.PingRequest) (*raft.PingResponse, error) {
	// For Phase 2, just log that we received a ping.
	log.Printf("[%s] Received Ping from %s", s.nodeID, req.SenderId)

	return &raft.PingResponse{
		ReceiverId: s.nodeID,
		Success:    true,
	}, nil
}

// StartGRPCServer initializes and starts a gRPC server on the given address.
func StartGRPCServer(nodeID, addr string) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	grpcServer := grpc.NewServer()
	
	raftServer := &RaftGRPCServer{
		nodeID: nodeID,
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
