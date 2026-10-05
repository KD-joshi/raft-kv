// Package transport provides the gRPC networking layer for node-to-node communication.
package transport

import (
	"context"
	"fmt"
	"log"
	"net"

	pb "github.com/KD-joshi/raft-kv/proto/raft"
	"google.golang.org/grpc"
)

// RaftHandler is an interface to break the circular dependency between transport and raft packages.
type RaftHandler interface {
	HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse
	HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse
	ResetElectionTimer()
}

// RaftGRPCServer implements the RaftService gRPC interface.
type RaftGRPCServer struct {
	pb.UnimplementedRaftServiceServer
	nodeID  string
	handler RaftHandler
}

// AppendEntries handles incoming AppendEntries RPCs (heartbeat + log replication).
func (s *RaftGRPCServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	resp := s.handler.HandleAppendEntries(req)
	return resp, nil
}

// RequestVote handles an incoming election vote request.
func (s *RaftGRPCServer) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
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

	pb.RegisterRaftServiceServer(grpcServer, raftServer)

	go func() {
		log.Printf("[%s] gRPC Server listening on %s", nodeID, addr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("[%s] gRPC server failed to serve: %v", nodeID, err)
		}
	}()

	return grpcServer, nil
}
