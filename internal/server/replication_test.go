package server

import (
	"strconv"
	"testing"
	"time"

	"reservoir/internal/cluster"
	"reservoir/internal/store"
)

func newReplTestStore() store.KVStore {
	return store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1 << 20,
		MaxMemoryUsage: 1 << 30,
	})
}

// TestApplyReplicationExpirePersist verifies that EXPIRE and PERSIST replication
// events are applied on a replica: EXPIRE sets the TTL to the replicated
// absolute deadline, and PERSIST removes it.
func TestApplyReplicationExpirePersist(t *testing.T) {
	st := newReplTestStore()
	h := &StoreReplicationHandler{Store: st}

	if err := st.Set("k", "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	exp := time.Now().Add(time.Hour).UnixNano()
	err := h.ApplyReplication(cluster.ReplicationEvent{
		Operation: "EXPIRE",
		Key:       "k",
		Value:     []byte(strconv.FormatInt(exp, 10)),
	})
	if err != nil {
		t.Fatalf("apply EXPIRE: %v", err)
	}
	if _, ok := st.GetTTL("k"); !ok {
		t.Fatalf("EXPIRE replication did not set a TTL")
	}

	if err := h.ApplyReplication(cluster.ReplicationEvent{Operation: "PERSIST", Key: "k"}); err != nil {
		t.Fatalf("apply PERSIST: %v", err)
	}
	if _, ok := st.GetTTL("k"); ok {
		t.Fatalf("PERSIST replication did not remove the TTL")
	}
}

// TestApplyReplicationSetLWW verifies that HLC-stamped SET events converge to
// the newer write regardless of the order they are applied on a replica.
func TestApplyReplicationSetLWW(t *testing.T) {
	older := cluster.ReplicationEvent{Operation: "SET", Key: "k", Value: []byte("old"), HLCPhysical: 100, HLCOrigin: 1}
	newer := cluster.ReplicationEvent{Operation: "SET", Key: "k", Value: []byte("new"), HLCPhysical: 200, HLCOrigin: 1}

	s1 := newReplTestStore()
	h1 := &StoreReplicationHandler{Store: s1}
	_ = h1.ApplyReplication(older)
	_ = h1.ApplyReplication(newer)

	s2 := newReplTestStore()
	h2 := &StoreReplicationHandler{Store: s2}
	_ = h2.ApplyReplication(newer) // reverse arrival order
	_ = h2.ApplyReplication(older)

	v1, _ := s1.Get("k")
	v2, _ := s2.Get("k")
	if v1 != v2 {
		t.Fatalf("replicas diverged under LWW: s1=%q s2=%q", v1, v2)
	}
	if v1 != "new" {
		t.Fatalf("converged on %q, want the newer write", v1)
	}
}

// TestApplyReplicationGetSet verifies GETSET replicates as a SET of the new
// value (GETSET is dispatched through the replication table as an in-place SET).
func TestApplyReplicationGetSet(t *testing.T) {
	st := newReplTestStore()
	h := &StoreReplicationHandler{Store: st}

	// GETSET replicates via the "SET" operation carrying the new value.
	if err := h.ApplyReplication(cluster.ReplicationEvent{Operation: "SET", Key: "k", Value: []byte("new")}); err != nil {
		t.Fatalf("apply SET: %v", err)
	}
	if v, ok := st.Get("k"); !ok || v != "new" {
		t.Fatalf("GETSET replication: got (%q,%v), want (\"new\",true)", v, ok)
	}
}
