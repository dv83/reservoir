package cluster

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func voteMsg(t *testing.T, resp VoteResponse) *Message {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(resp); err != nil {
		t.Fatalf("encode vote response: %v", err)
	}
	return &Message{Type: MsgVoteResponse, Payload: buf.Bytes()}
}

// TestVoteDeduplication verifies that duplicate VoteResponses from the same node
// are not double-counted, and that becomeLeader runs at most once per election.
func TestVoteDeduplication(t *testing.T) {
	cm := NewClusterManager("127.0.0.1", 0, NewUUIDv7())
	defer cm.localNode.OptimizedVectorClock.Stop()

	// Enter candidate state for a fresh election needing 3 votes (majority of 5).
	cm.localNode.SetState(StateCandidate)
	term := cm.localNode.CurrentTerm.Add(1)
	cm.electionTerm.Store(term)
	cm.electionWon.Store(false)
	cm.votersMu.Lock()
	cm.voters = map[UUIDv7]struct{}{cm.localNode.NodeID: {}} // self vote
	cm.votersMu.Unlock()
	cm.votesNeeded.Store(3)

	voterA := NewUUIDv7()

	// The same voter granting twice must count once: self + A = 2 < 3.
	_ = cm.handleVoteResponse(voteMsg(t, VoteResponse{NodeID: voterA, Term: term, VoteGranted: true}))
	_ = cm.handleVoteResponse(voteMsg(t, VoteResponse{NodeID: voterA, Term: term, VoteGranted: true}))

	if got := cm.localNode.GetState(); got == StateLeader {
		t.Fatalf("became leader with only 2 distinct votes (needed 3)")
	}
	cm.votersMu.Lock()
	nVoters := len(cm.voters)
	cm.votersMu.Unlock()
	if nVoters != 2 {
		t.Fatalf("distinct voters = %d, want 2 (self + A)", nVoters)
	}

	// A distinct voter B reaches the majority.
	voterB := NewUUIDv7()
	_ = cm.handleVoteResponse(voteMsg(t, VoteResponse{NodeID: voterB, Term: term, VoteGranted: true}))
	if got := cm.localNode.GetState(); got != StateLeader {
		t.Fatalf("did not become leader after reaching majority; state=%v", got)
	}
	if !cm.electionWon.Load() {
		t.Fatalf("electionWon not set after becoming leader")
	}
}
