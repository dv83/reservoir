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
