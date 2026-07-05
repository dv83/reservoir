package cluster

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func heartbeatMsg(t *testing.T, hb Heartbeat) *Message {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(hb); err != nil {
		t.Fatalf("encode heartbeat: %v", err)
	}
	return &Message{Type: MsgHeartbeat, Payload: buf.Bytes()}
}

func (cm *ClusterManager) addTestNode(n *Node) {
	cm.nodesMu.Lock()
	cm.nodes[n.NodeID] = n
	cm.nodesMu.Unlock()
}

// TestHeartbeatConflictingLeaderIgnored verifies that a second node claiming
// leadership for a term already assigned to a different leader is ignored (no
// LeaderID flapping), and that a peer's recorded term never regresses.
func TestHeartbeatConflictingLeaderIgnored(t *testing.T) {
	cm := NewClusterManager("127.0.0.1", 0, NewUUIDv7())
	defer cm.localNode.OptimizedVectorClock.Stop()

	clusterID := cm.localNode.ClusterID
	nodeA := newRemoteNode(NewUUIDv7(), "127.0.0.1", 7601, clusterID)
	nodeB := newRemoteNode(NewUUIDv7(), "127.0.0.1", 7602, clusterID)
	cm.addTestNode(nodeA)
	cm.addTestNode(nodeB)

	cm.localNode.CurrentTerm.Store(5)

	// A legitimately leads term 5.
	_ = cm.handleHeartbeat(heartbeatMsg(t, Heartbeat{NodeID: nodeA.NodeID, Term: 5, State: StateLeader}))
	if id, ok := cm.currentLeaderID(); !ok || id != nodeA.NodeID {
		t.Fatalf("leader after A's heartbeat = %v (ok=%v), want A", id, ok)
	}

	// B claims leadership for the SAME term 5 — must be ignored.
	_ = cm.handleHeartbeat(heartbeatMsg(t, Heartbeat{NodeID: nodeB.NodeID, Term: 5, State: StateLeader}))
	if id, _ := cm.currentLeaderID(); id != nodeA.NodeID {
		t.Fatalf("conflicting leader claim from B flapped LeaderID to %v, want A", id)
	}

	// A legitimately leads a newer term 6 — accepted.
	cm.localNode.CurrentTerm.Store(6)
	_ = cm.handleHeartbeat(heartbeatMsg(t, Heartbeat{NodeID: nodeB.NodeID, Term: 6, State: StateLeader}))
	if id, _ := cm.currentLeaderID(); id != nodeB.NodeID {
		t.Fatalf("leader for newer term 6 = %v, want B", id)
	}

	// A peer's recorded term must not regress.
	nodeA.CurrentTerm.Store(5)
	_ = cm.handleHeartbeat(heartbeatMsg(t, Heartbeat{NodeID: nodeA.NodeID, Term: 3, State: StateFollower}))
	if got := nodeA.CurrentTerm.Load(); got != 5 {
		t.Fatalf("peer term regressed to %d, want 5", got)
	}
}
