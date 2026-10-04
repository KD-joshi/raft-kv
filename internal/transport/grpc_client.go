package transport

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/KD-joshi/raft-kv/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Peer represents a connection to another node in the cluster.
type Peer struct {
	ID      string
	Address string
	Client  raft.RaftServiceClient
	conn    *grpc.ClientConn
}

// ConnectPeer dials a gRPC connection to a peer node.
func ConnectPeer(id, address string) (*Peer, error) {
	// For this phase, we use insecure credentials (no TLS).
	// gRPC connections are multiplexed and long-lived, so we dial once.
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer %s: %w", id, err)
	}

	client := raft.NewRaftServiceClient(conn)

	return &Peer{
		ID:      id,
		Address: address,
		Client:  client,
		conn:    conn,
	}, nil
}

// Close closes the underlying gRPC connection.
func (p *Peer) Close() error {
	return p.conn.Close()
}

// SendPing wraps the Ping RPC call with a timeout.
func (p *Peer) SendPing(senderID string) (*raft.PingResponse, error) {
	// Set a short timeout for the RPC. Heartbeats must be fast.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	req := &raft.PingRequest{
		SenderId: senderID,
	}

	resp, err := p.Client.Ping(ctx, req)
	if err != nil {
		return nil, err
	}
	
	log.Printf("[%s] Received successful Ping response from %s", senderID, resp.ReceiverId)
	return resp, nil
}
