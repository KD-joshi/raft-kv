package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/KD-joshi/raft-kv/internal/transport"
	pb "github.com/KD-joshi/raft-kv/proto/raft"
)

// State defines the current role of the node.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Node represents a single Raft consensus node.
type Node struct {
	mu sync.Mutex

	id    string
	peers []*transport.Peer

	// Persistent state on all servers (in memory for now, should be persisted to WAL later)
	currentTerm uint64
	votedFor    string

	// Volatile state
	state State

	// Channels for signaling
	electionResetEvent chan struct{}
}

// NewNode initializes a new Raft node.
func NewNode(id string) *Node {
	return &Node{
		id:                 id,
		state:              Follower,
		currentTerm:        0,
		votedFor:           "",
		electionResetEvent: make(chan struct{}, 1),
	}
}

// SetPeers sets the connections to other nodes.
func (n *Node) SetPeers(peers []*transport.Peer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers = peers
}

// Run starts the node's background consensus loops.
func (n *Node) Run() {
	go n.runElectionTimer()
}

// runElectionTimer handles the randomized election timeout.
func (n *Node) runElectionTimer() {
	for {
		// Randomized timeout between 150ms and 300ms
		timeoutDuration := time.Duration(150+rand.Intn(150)) * time.Millisecond
		n.mu.Lock()
		state := n.state
		n.mu.Unlock()

		if state == Leader {
			// Leaders don't need election timers; they send heartbeats.
			// We just sleep for a bit and check again later if we step down.
			time.Sleep(100 * time.Millisecond)
			continue
		}

		timer := time.NewTimer(timeoutDuration)

		select {
		case <-timer.C:
			n.mu.Lock()
			currentState := n.state
			n.mu.Unlock()
			
			// If we became Leader while the timer was ticking, ignore the timeout.
			if currentState != Leader {
				log.Printf("[%s] Election timeout fired (waited %v)", n.id, timeoutDuration)
				n.startElection()
			}
		case <-n.electionResetEvent:
			// We received a heartbeat or voted for someone else, reset timer.
			timer.Stop()
		}
	}
}

// startElection transitions to Candidate state and requests votes.
func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	currentTerm := n.currentTerm
	n.votedFor = n.id
	log.Printf("[%s] Becomes Candidate, term=%d", n.id, currentTerm)
	peers := n.peers
	n.mu.Unlock()

	votesReceived := 1 // Vote for self

	for _, peer := range peers {
		go func(p *transport.Peer) {
			req := &pb.RequestVoteRequest{
				Term:        currentTerm,
				CandidateId: n.id,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			resp, err := p.Client.RequestVote(ctx, req)
			if err != nil {
				// Failed to reach peer, ignore
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			// If our state changed while waiting for RPC, ignore
			if n.state != Candidate || n.currentTerm != currentTerm {
				return
			}

			// If they have a higher term, we must step down
			if resp.Term > currentTerm {
				log.Printf("[%s] Discovered higher term (%d) from %s, stepping down to Follower", n.id, resp.Term, p.ID)
				n.state = Follower
				n.currentTerm = resp.Term
				n.votedFor = ""
				return
			}

			if resp.VoteGranted {
				votesReceived++
				log.Printf("[%s] Received vote from %s (total: %d)", n.id, p.ID, votesReceived)
				
				// Check for quorum: (total peers + 1) / 2 + 1
				quorum := (len(peers) + 1) / 2 + 1
				if votesReceived >= quorum && n.state == Candidate {
					log.Printf("[%s] Won election! Becoming LEADER for term %d", n.id, currentTerm)
					n.state = Leader
					go n.runHeartbeats()
				}
			}
		}(peer)
	}
}

// runHeartbeats sends periodic pings to assert leadership.
func (n *Node) runHeartbeats() {
	n.mu.Lock()
	peers := n.peers
	n.mu.Unlock()

	ticker := time.NewTicker(50 * time.Millisecond) // Fast heartbeat
	defer ticker.Stop()

	for {
		<-ticker.C

		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		for _, peer := range peers {
			go func(p *transport.Peer) {
				req := &pb.PingRequest{
					SenderId: n.id,
					// Term: term (We should add Term to PingRequest eventually)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
				
				_, _ = p.Client.Ping(ctx, req)
				// For now, we don't handle ping responses deeply
			}(peer)
		}
	}
}

// HandleRequestVote processes incoming vote requests.
func (n *Node) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term > n.currentTerm {
		// New term seen, become follower
		log.Printf("[%s] Stepping down to Follower: seen term %d > current term %d", n.id, req.Term, n.currentTerm)
		n.currentTerm = req.Term
		n.state = Follower
		n.votedFor = ""
	}

	voteGranted := false
	if req.Term == n.currentTerm && (n.votedFor == "" || n.votedFor == req.CandidateId) {
		voteGranted = true
		n.votedFor = req.CandidateId
		// Reset election timer since we voted for someone
		select {
		case n.electionResetEvent <- struct{}{}:
		default:
		}
		log.Printf("[%s] Voted for %s in term %d", n.id, req.CandidateId, n.currentTerm)
	}

	return &pb.RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: voteGranted,
	}
}

// ResetElectionTimer is called when a heartbeat is received from the leader.
func (n *Node) ResetElectionTimer() {
	select {
	case n.electionResetEvent <- struct{}{}:
	default:
	}
}
