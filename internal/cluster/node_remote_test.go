package cluster

import "testing"

// TestRemoteNodeHasNoOVC verifies remote-node bookkeeping objects do not carry a
// running OptimizedVectorClock. Each OVC starts a background goroutine, and one
// remote node is created per join and per node-update broadcast, so starting an
// OVC for them leaked a goroutine on every membership event.
func TestRemoteNodeHasNoOVC(t *testing.T) {
	clusterID := NewUUIDv7()
	remote := newRemoteNode(NewUUIDv7(), "127.0.0.1", 7400, clusterID)

	if remote.OptimizedVectorClock != nil {
		t.Fatalf("remote node must not start an OptimizedVectorClock goroutine")
	}

	// The nil-OVC path must remain safe to use.
	remote.UpdateVectorClock(NewUUIDv7(), 5)
	if got := remote.GetVectorClock(); got == nil {
		t.Fatalf("GetVectorClock returned nil for a remote node")
	}

	// The local node, by contrast, does run an OVC (and must be stoppable).
	local := NewNode("127.0.0.1", 7401, clusterID)
	if local.OptimizedVectorClock == nil {
		t.Fatalf("local node should have an OptimizedVectorClock")
	}
	local.OptimizedVectorClock.Stop()
}
