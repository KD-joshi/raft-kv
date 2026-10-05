// Package raft implements the Raft consensus algorithm.
//
// This is the brain of the distributed KV store. It handles:
//   - Leader election (Phase 3)
//   - Log replication via AppendEntries (Phase 4)
//   - Commit tracking and state machine application
package raft

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/KD-joshi/raft-kv/internal/kvstore"
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

// String returns a human-readable name for the state.
func (s State) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// logEntry is our internal representation of a Raft log entry.
type logEntry struct {
	Term    uint64
	Index   uint64
	Command kvstore.Command
}

// pendingProposal tracks a client write waiting for consensus.
type pendingProposal struct {
	index    uint64
	resultCh chan error
}

// Node represents a single Raft consensus node.
type Node struct {
	mu sync.Mutex

	id    string
	peers []*transport.Peer

	// Persistent Raft state
	currentTerm uint64
	votedFor    string
	log         []logEntry // 0-indexed internally, but log[i].Index = i+1

	// Volatile state on all servers
	state       State
	commitIndex uint64 // highest log entry known to be committed
	lastApplied uint64 // highest log entry applied to state machine

	// Volatile state on leaders (reinitialized after election)
	nextIndex  map[string]uint64 // for each peer: index of next log entry to send
	matchIndex map[string]uint64 // for each peer: highest log entry known to be replicated

	// Leader identity (so followers can redirect clients)
	leaderID string

	// The state machine to apply committed entries to
	store *kvstore.Store

	// Channels for signaling
	electionResetEvent chan struct{}

	// Pending proposals waiting for commit
	pendingMu       sync.Mutex
	pendingProposals []pendingProposal
}

// NewNode initializes a new Raft node.
func NewNode(id string) *Node {
	return &Node{
		id:                 id,
		state:              Follower,
		currentTerm:        0,
		votedFor:           "",
		log:                make([]logEntry, 0),
		commitIndex:        0,
		lastApplied:        0,
		nextIndex:          make(map[string]uint64),
		matchIndex:         make(map[string]uint64),
		leaderID:           "",
		electionResetEvent: make(chan struct{}, 1),
	}
}

// SetPeers sets the connections to other nodes.
func (n *Node) SetPeers(peers []*transport.Peer) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers = peers
}

// SetStore sets the KV store that committed entries are applied to.
func (n *Node) SetStore(store *kvstore.Store) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.store = store
}

// Run starts the node's background consensus loops.
func (n *Node) Run() {
	go n.runElectionTimer()
	go n.applyLoop()
}

// IsLeader returns whether this node is currently the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// LeaderID returns the ID of the current known leader.
func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// GetState returns the current state and term (for health/metrics).
func (n *Node) GetState() (State, uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.currentTerm
}

// ─── Propose (Client Write Path) ────────────────────────────────

// Propose submits a command to the Raft leader for consensus.
// It blocks until the entry is committed or a timeout occurs.
// Only the Leader may call this; followers should redirect.
func (n *Node) Propose(cmd kvstore.Command, timeout time.Duration) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}

	// Append to our own log
	entry := logEntry{
		Term:    n.currentTerm,
		Index:   uint64(len(n.log)) + 1,
		Command: cmd,
	}
	n.log = append(n.log, entry)
	log.Printf("[%s] Leader appended log entry index=%d term=%d cmd=%s:%s",
		n.id, entry.Index, entry.Term, cmd.Op, cmd.Key)

	// Create a channel that will be signaled when this entry is committed
	resultCh := make(chan error, 1)
	n.pendingMu.Lock()
	n.pendingProposals = append(n.pendingProposals, pendingProposal{
		index:    entry.Index,
		resultCh: resultCh,
	})
	n.pendingMu.Unlock()

	// Update our own matchIndex
	n.matchIndex[n.id] = entry.Index

	n.mu.Unlock()

	// Trigger immediate replication to peers (don't wait for next heartbeat tick)
	n.replicateToAll()

	// Wait for commit or timeout
	select {
	case err := <-resultCh:
		return err
	case <-time.After(timeout):
		return ErrCommitTimeout
	}
}

// ─── Election Timer ─────────────────────────────────────────────

func (n *Node) runElectionTimer() {
	for {
		timeoutDuration := time.Duration(300+rand.Intn(300)) * time.Millisecond

		n.mu.Lock()
		state := n.state
		n.mu.Unlock()

		if state == Leader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		timer := time.NewTimer(timeoutDuration)

		select {
		case <-timer.C:
			n.mu.Lock()
			currentState := n.state
			n.mu.Unlock()

			if currentState != Leader {
				log.Printf("[%s] Election timeout fired (waited %v)", n.id, timeoutDuration)
				n.startElection()
			}
		case <-n.electionResetEvent:
			timer.Stop()
		}
	}
}

// lastLogIndexAndTerm returns the index and term of the last log entry.
func (n *Node) lastLogIndexAndTerm() (uint64, uint64) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	currentTerm := n.currentTerm
	n.votedFor = n.id
	lastLogIndex, lastLogTerm := n.lastLogIndexAndTerm()
	log.Printf("[%s] Becomes Candidate, term=%d", n.id, currentTerm)
	peers := n.peers
	n.mu.Unlock()

	votesReceived := 1 // Vote for self

	for _, peer := range peers {
		go func(p *transport.Peer) {
			req := &pb.RequestVoteRequest{
				Term:         currentTerm,
				CandidateId:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()

			resp, err := p.Client.RequestVote(ctx, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Candidate || n.currentTerm != currentTerm {
				return
			}

			if resp.Term > currentTerm {
				log.Printf("[%s] Discovered higher term (%d) from %s, stepping down", n.id, resp.Term, p.ID)
				n.becomeFollower(resp.Term)
				return
			}

			if resp.VoteGranted {
				votesReceived++
				log.Printf("[%s] Received vote from %s (total: %d)", n.id, p.ID, votesReceived)

				quorum := (len(peers)+1)/2 + 1
				if votesReceived >= quorum && n.state == Candidate {
					log.Printf("[%s] ★ Won election! Becoming LEADER for term %d", n.id, currentTerm)
					n.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeFollower transitions to follower state. Must be called with n.mu held.
func (n *Node) becomeFollower(term uint64) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
}

// becomeLeader transitions to leader state. Must be called with n.mu held.
func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id

	// Reinitialize nextIndex and matchIndex for all peers
	lastLogIndex, _ := n.lastLogIndexAndTerm()
	for _, p := range n.peers {
		n.nextIndex[p.ID] = lastLogIndex + 1
		n.matchIndex[p.ID] = 0
	}
	// Leader's own matchIndex
	n.matchIndex[n.id] = lastLogIndex

	go n.runHeartbeats()
}

// ─── Heartbeats (Leader Loop) ───────────────────────────────────

func (n *Node) runHeartbeats() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		n.replicateToAll()
	}
}

// replicateToAll sends AppendEntries to all peers in parallel.
func (n *Node) replicateToAll() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	peers := n.peers
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(peer)
	}
}

// replicateToPeer sends an AppendEntries RPC to a single peer.
func (n *Node) replicateToPeer(p *transport.Peer) {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[p.ID]
	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64
	if prevLogIndex > 0 && prevLogIndex <= uint64(len(n.log)) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}

	// Collect entries to send
	var entries []*pb.LogEntry
	if nextIdx <= uint64(len(n.log)) {
		for i := nextIdx; i <= uint64(len(n.log)); i++ {
			entry := n.log[i-1]
			cmdBytes, _ := json.Marshal(entry.Command)
			entries = append(entries, &pb.LogEntry{
				Term:    entry.Term,
				Index:   entry.Index,
				Command: cmdBytes,
			})
		}
	}

	req := &pb.AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	currentTerm := n.currentTerm
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	resp, err := p.Client.AppendEntries(ctx, req)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader || n.currentTerm != currentTerm {
		return
	}

	if resp.Term > currentTerm {
		log.Printf("[%s] Discovered higher term (%d) from %s in AppendEntries response, stepping down",
			n.id, resp.Term, p.ID)
		n.becomeFollower(resp.Term)
		return
	}

	if resp.Success {
		// Update nextIndex and matchIndex for this peer
		if len(entries) > 0 {
			lastSent := entries[len(entries)-1].Index
			n.nextIndex[p.ID] = lastSent + 1
			n.matchIndex[p.ID] = lastSent
			n.advanceCommitIndex()
		}
	} else {
		// Log inconsistency: decrement nextIndex and retry
		if n.nextIndex[p.ID] > 1 {
			n.nextIndex[p.ID]--
		}
	}
}

// advanceCommitIndex checks if we can advance commitIndex based on
// matchIndex values from all servers (including self). Must be called with n.mu held.
func (n *Node) advanceCommitIndex() {
	// For each N > commitIndex, if a majority of matchIndex[i] >= N
	// and log[N].term == currentTerm, set commitIndex = N
	for idx := n.commitIndex + 1; idx <= uint64(len(n.log)); idx++ {
		if n.log[idx-1].Term != n.currentTerm {
			continue
		}

		// Count how many servers have this entry replicated
		replicatedCount := 0
		// Count self
		if n.matchIndex[n.id] >= idx {
			replicatedCount++
		}
		for _, p := range n.peers {
			if n.matchIndex[p.ID] >= idx {
				replicatedCount++
			}
		}

		quorum := (len(n.peers)+1)/2 + 1
		if replicatedCount >= quorum {
			log.Printf("[%s] Commit index advanced: %d → %d (quorum=%d, replicated=%d)",
				n.id, n.commitIndex, idx, quorum, replicatedCount)
			n.commitIndex = idx
		}
	}
}

// ─── Apply Loop (State Machine) ─────────────────────────────────

// applyLoop runs in the background and applies committed entries to the KV store.
func (n *Node) applyLoop() {
	for {
		time.Sleep(10 * time.Millisecond)

		n.mu.Lock()
		commitIndex := n.commitIndex
		lastApplied := n.lastApplied
		store := n.store

		if store == nil || commitIndex <= lastApplied {
			n.mu.Unlock()
			continue
		}

		// Collect entries to apply
		var toApply []logEntry
		for i := lastApplied + 1; i <= commitIndex; i++ {
			if i <= uint64(len(n.log)) {
				toApply = append(toApply, n.log[i-1])
			}
		}
		n.lastApplied = commitIndex
		n.mu.Unlock()

		// Apply entries outside the lock to avoid holding it during I/O
		for _, entry := range toApply {
			_, err := store.Apply(entry.Command)
			if err != nil {
				log.Printf("[%s] ERROR applying entry index=%d: %v", n.id, entry.Index, err)
			} else {
				log.Printf("[%s] Applied entry index=%d: %s %s",
					n.id, entry.Index, entry.Command.Op, entry.Command.Key)
			}
		}

		// Notify any pending proposals that have been committed
		n.pendingMu.Lock()
		remaining := make([]pendingProposal, 0)
		for _, pp := range n.pendingProposals {
			if pp.index <= commitIndex {
				pp.resultCh <- nil // Success!
			} else {
				remaining = append(remaining, pp)
			}
		}
		n.pendingProposals = remaining
		n.pendingMu.Unlock()
	}
}

// ─── RPC Handlers (called by transport layer) ───────────────────

// HandleAppendEntries processes incoming AppendEntries RPCs.
func (n *Node) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Rule 1: Reply false if term < currentTerm
	if req.Term < n.currentTerm {
		return &pb.AppendEntriesResponse{
			Term:    n.currentTerm,
			Success: false,
		}
	}

	// If we see a term >= ours, become follower
	if req.Term > n.currentTerm {
		log.Printf("[%s] Stepping down: seen term %d from leader %s (current: %d)",
			n.id, req.Term, req.LeaderId, n.currentTerm)
		n.becomeFollower(req.Term)
	}

	// Reset election timer (we heard from the leader)
	select {
	case n.electionResetEvent <- struct{}{}:
	default:
	}

	// Track who the leader is
	n.leaderID = req.LeaderId

	// Rule 2: Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > uint64(len(n.log)) {
			return &pb.AppendEntriesResponse{
				Term:    n.currentTerm,
				Success: false,
			}
		}
		if n.log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			// Rule 3: Delete the conflicting entry and all that follow
			n.log = n.log[:req.PrevLogIndex-1]
			return &pb.AppendEntriesResponse{
				Term:    n.currentTerm,
				Success: false,
			}
		}
	}

	// Rule 4: Append any new entries not already in the log
	for _, entry := range req.Entries {
		var cmd kvstore.Command
		json.Unmarshal(entry.Command, &cmd)

		if entry.Index <= uint64(len(n.log)) {
			// Entry already exists, check for conflict
			if n.log[entry.Index-1].Term != entry.Term {
				// Conflict: delete this and all following entries
				n.log = n.log[:entry.Index-1]
				n.log = append(n.log, logEntry{
					Term:    entry.Term,
					Index:   entry.Index,
					Command: cmd,
				})
			}
			// Else: identical entry, skip
		} else {
			// New entry, append
			n.log = append(n.log, logEntry{
				Term:    entry.Term,
				Index:   entry.Index,
				Command: cmd,
			})
		}
	}

	// Rule 5: If leaderCommit > commitIndex, set commitIndex
	if req.LeaderCommit > n.commitIndex {
		lastNewIndex := uint64(len(n.log))
		if req.LeaderCommit < lastNewIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIndex
		}
	}

	return &pb.AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: true,
	}
}

// HandleRequestVote processes incoming vote requests.
func (n *Node) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term > n.currentTerm {
		log.Printf("[%s] Stepping down to Follower: seen term %d > current term %d",
			n.id, req.Term, n.currentTerm)
		n.becomeFollower(req.Term)
	}

	voteGranted := false
	if req.Term == n.currentTerm && (n.votedFor == "" || n.votedFor == req.CandidateId) {
		// Check if candidate's log is at least as up-to-date as ours
		lastIdx, lastTerm := n.lastLogIndexAndTerm()
		logOk := req.LastLogTerm > lastTerm ||
			(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)

		if logOk {
			voteGranted = true
			n.votedFor = req.CandidateId
			select {
			case n.electionResetEvent <- struct{}{}:
			default:
			}
			log.Printf("[%s] Voted for %s in term %d", n.id, req.CandidateId, n.currentTerm)
		}
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
