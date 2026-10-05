package transport

import (
	"fmt"

	pb "github.com/KD-joshi/raft-kv/proto/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Peer represents a connection to another node in the cluster.
type Peer struct {
	ID      string
	Address string
	Client  pb.RaftServiceClient
	conn    *grpc.ClientConn
}

// ConnectPeer dials a gRPC connection to a peer node.
func ConnectPeer(id, address string) (*Peer, error) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to dial peer %s: %w", id, err)
	}

	client := pb.NewRaftServiceClient(conn)

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
