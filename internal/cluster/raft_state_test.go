package cluster

import (
	"path/filepath"
	"testing"
)

// TestRaftStatePersistence verifies that currentTerm and votedFor are persisted
// and restored across a simulated restart, so a node does not lose its vote.
func TestRaftStatePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-state.json")

	cm1 := NewClusterManager("127.0.0.1", 0, NewUUIDv7())
	defer cm1.localNode.OptimizedVectorClock.Stop()
	cm1.SetStatePath(path)

	candidate := NewUUIDv7()
	cm1.localNode.CurrentTerm.Store(7)
	cm1.localNode.VotedFor.Store(&candidate)
	cm1.persistState()

	// Simulate a restart: a fresh manager loads the persisted state.
	cm2 := NewClusterManager("127.0.0.1", 0, NewUUIDv7())
	defer cm2.localNode.OptimizedVectorClock.Stop()
	cm2.SetStatePath(path)
	cm2.loadState()

	if got := cm2.localNode.CurrentTerm.Load(); got != 7 {
		t.Fatalf("restored term = %d, want 7", got)
	}
	v := cm2.localNode.VotedFor.Load()
	if v == nil {
		t.Fatalf("votedFor not restored")
	}
	id, ok := v.(*UUIDv7)
	if !ok || *id != candidate {
		t.Fatalf("restored votedFor = %v, want %v", v, candidate)
	}
}

// TestRaftStateNoPathIsNoop verifies persistence is disabled (no panic, no file)
// when no path is configured.
func TestRaftStateNoPathIsNoop(t *testing.T) {
	cm := NewClusterManager("127.0.0.1", 0, NewUUIDv7())
	defer cm.localNode.OptimizedVectorClock.Stop()
	cm.localNode.CurrentTerm.Store(3)
	cm.persistState() // must be a no-op
	cm.loadState()    // must be a no-op
	if got := cm.localNode.CurrentTerm.Load(); got != 3 {
		t.Fatalf("term changed unexpectedly: %d", got)
	}
}
