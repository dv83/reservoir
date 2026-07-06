package server

import (
	"sort"
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

// reconcile pulls every shard's digest from `from` into `into` under LWW, the
// same reconciliation handleSyncResponse performs (including set-element
// routing).
func reconcile(into *StoreReplicationHandler, from *StoreReplicationHandler) {
	for shard := 0; shard < from.ShardCount(); shard++ {
		for _, e := range from.LocalDigest(shard) {
			var op string
			value := e.Value
			switch {
			case e.Counter:
				op = "COUNTER"
			case e.Hash:
				if e.Deleted {
					op = "HDEL"
					value = []byte(e.Key + "\x00" + e.Member)
				} else {
					op = "HSET"
					value = []byte(e.Key + "\x00" + e.Member + "\x00" + string(e.Value))
				}
			case e.Member != "":
				if e.Deleted {
					op = "SREM"
				} else {
					op = "SADD"
				}
				value = []byte(e.Key + "\x00" + e.Member)
			case e.Deleted:
				op = "DEL"
			default:
				op = "SET"
			}
			_ = into.ApplyReplication(cluster.ReplicationEvent{
				Operation:   op,
				Key:         e.Key,
				Value:       value,
				HLCPhysical: e.HLCPhysical,
				HLCLogical:  e.HLCLogical,
				HLCOrigin:   e.HLCOrigin,
			})
		}
	}
}

// TestAntiEntropyReconciles verifies that anti-entropy heals divergence: missing
// keys are pulled, a stale value is upgraded to the newer one, and a tombstone
// propagates a delete — all under last-write-wins.
func TestAntiEntropyReconciles(t *testing.T) {
	a := &StoreReplicationHandler{Store: newReplTestStore()}
	b := &StoreReplicationHandler{Store: newReplTestStore()}

	// a: has "missing" (b lacks it), a newer "shared", and a deleted "gone".
	a.Store.SetLWW("missing", "mv", 100, 0, 1)
	a.Store.SetLWW("shared", "new", 200, 0, 1)
	a.Store.SetLWW("gone", "x", 50, 0, 1)
	a.Store.DeleteLWW("gone", 300, 0, 1)

	// b: has an older "shared" and still holds "gone".
	b.Store.SetLWW("shared", "old", 100, 0, 1)
	b.Store.SetLWW("gone", "x", 50, 0, 1)

	// Reconcile b from a.
	reconcile(b, a)

	if v, ok := b.Store.Get("missing"); !ok || v != "mv" {
		t.Errorf("missing key not pulled: got (%q,%v)", v, ok)
	}
	if v, _ := b.Store.Get("shared"); v != "new" {
		t.Errorf("stale value not upgraded: got %q, want new", v)
	}
	if _, ok := b.Store.Get("gone"); ok {
		t.Errorf("tombstone did not propagate: 'gone' still present")
	}
}

// TestAntiEntropyReconcilesSet verifies anti-entropy heals set divergence from a
// dropped element op: a missing member is pulled, and a member removed on the
// source (tombstoned) is removed on the target even though the target still held
// it — all through the set digest under last-write-wins.
func TestAntiEntropyReconcilesSet(t *testing.T) {
	a := &StoreReplicationHandler{Store: newReplTestStore()}
	b := &StoreReplicationHandler{Store: newReplTestStore()}

	// a: added x and y, then removed y (tombstone at 200).
	a.Store.SAddLWW("s", 100, 0, 1, "x", "y")
	a.Store.SRemLWW("s", 200, 0, 1, "y")

	// b: only ever saw the original add of x and y (missed a's later ops).
	b.Store.SAddLWW("s", 100, 0, 1, "x", "y")

	reconcile(b, a)

	got, _ := b.Store.SMembers("s")
	sort.Strings(got)
	if len(got) != 1 || got[0] != "x" {
		t.Fatalf("after reconcile, members = %v, want [x]", got)
	}
	// The tombstone must persist: a stale re-add of y cannot resurrect it.
	b.Store.SAddLWW("s", 150, 0, 1, "y")
	if m, _ := b.Store.SIsMember("s", "y"); m {
		t.Fatal("stale add resurrected tombstoned member 'y' after reconcile")
	}
}

// setReplEvent builds a stamped SADD/SREM replication event using the same
// null-separated encoding the command layer emits.
func setReplEvent(op, key string, physical int64, origin uint64, members ...string) cluster.ReplicationEvent {
	payload := key
	for _, m := range members {
		payload += "\x00" + m
	}
	return cluster.ReplicationEvent{
		Operation:   op,
		Key:         key,
		Value:       []byte(payload),
		HLCPhysical: physical,
		HLCOrigin:   origin,
	}
}

// TestApplyReplicationSetCRDTConverges verifies that stamped SADD/SREM events
// converge to the same membership on two replicas regardless of arrival order,
// including the key property that a remove blocks a concurrent older add.
func TestApplyReplicationSetCRDTConverges(t *testing.T) {
	events := []cluster.ReplicationEvent{
		setReplEvent("SADD", "s", 100, 1, "a", "b"),
		setReplEvent("SREM", "s", 200, 2, "a"),
		setReplEvent("SADD", "s", 150, 3, "a"), // older than the remove — must not resurrect
		setReplEvent("SADD", "s", 300, 1, "c"),
	}

	members := func(h *StoreReplicationHandler) []string {
		m, _ := h.Store.SMembers("s")
		sort.Strings(m)
		return m
	}

	h1 := &StoreReplicationHandler{Store: newReplTestStore()}
	for _, e := range events {
		_ = h1.ApplyReplication(e)
	}

	h2 := &StoreReplicationHandler{Store: newReplTestStore()}
	for i := len(events) - 1; i >= 0; i-- { // reverse order
		_ = h2.ApplyReplication(events[i])
	}

	m1, m2 := members(h1), members(h2)
	if len(m1) != len(m2) {
		t.Fatalf("replicas diverged: h1=%v h2=%v", m1, m2)
	}
	for i := range m1 {
		if m1[i] != m2[i] {
			t.Fatalf("replicas diverged: h1=%v h2=%v", m1, m2)
		}
	}
	// "a" removed at 200 must stay gone; "b" and "c" present.
	if got := m1; !(len(got) == 2 && got[0] == "b" && got[1] == "c") {
		t.Fatalf("converged membership = %v, want [b c]", got)
	}
}

// TestAntiEntropyReconcilesHash verifies anti-entropy heals hash divergence from
// a dropped field op: a missing field is pulled, a stale value is upgraded, and
// a deleted field (tombstoned) is removed on the target that still held it.
func TestAntiEntropyReconcilesHash(t *testing.T) {
	a := &StoreReplicationHandler{Store: newReplTestStore()}
	b := &StoreReplicationHandler{Store: newReplTestStore()}

	// a: has "missing" (b lacks it), a newer "shared", and a deleted "gone".
	a.Store.HSetLWW("h", 100, 0, 1, "missing", "mv")
	a.Store.HSetLWW("h", 200, 0, 1, "shared", "new")
	a.Store.HSetLWW("h", 50, 0, 1, "gone", "x")
	a.Store.HDelLWW("h", 300, 0, 1, "gone")

	// b: has an older "shared" and still holds "gone".
	b.Store.HSetLWW("h", 100, 0, 1, "shared", "old")
	b.Store.HSetLWW("h", 50, 0, 1, "gone", "x")

	reconcile(b, a)

	if v, ok, _ := b.Store.HGet("h", "missing"); !ok || v != "mv" {
		t.Errorf("missing field not pulled: got (%q,%v)", v, ok)
	}
	if v, _, _ := b.Store.HGet("h", "shared"); v != "new" {
		t.Errorf("stale value not upgraded: got %q, want new", v)
	}
	if _, ok, _ := b.Store.HGet("h", "gone"); ok {
		t.Errorf("tombstone did not propagate: 'gone' still present")
	}
}

// hashReplEvent builds a stamped HSET/HDEL replication event using the same
// null-separated encoding the command layer emits.
func hashReplEvent(op, key string, physical int64, origin uint64, args ...string) cluster.ReplicationEvent {
	payload := key
	for _, a := range args {
		payload += "\x00" + a
	}
	return cluster.ReplicationEvent{
		Operation:   op,
		Key:         key,
		Value:       []byte(payload),
		HLCPhysical: physical,
		HLCOrigin:   origin,
	}
}

// TestApplyReplicationHashCRDTConverges verifies that stamped HSET/HDEL events
// converge to the same fields and values on two replicas regardless of arrival
// order, including a delete blocking a concurrent older set.
func TestApplyReplicationHashCRDTConverges(t *testing.T) {
	events := []cluster.ReplicationEvent{
		hashReplEvent("HSET", "h", 100, 1, "a", "1", "b", "2"),
		hashReplEvent("HDEL", "h", 200, 2, "a"),
		hashReplEvent("HSET", "h", 150, 3, "a", "stale"), // older than delete — must not resurrect
		hashReplEvent("HSET", "h", 300, 1, "c", "3"),
	}

	fields := func(h *StoreReplicationHandler) []string {
		all, _ := h.Store.HGetAll("h")
		sort.Strings(all)
		return all
	}

	h1 := &StoreReplicationHandler{Store: newReplTestStore()}
	for _, e := range events {
		_ = h1.ApplyReplication(e)
	}
	h2 := &StoreReplicationHandler{Store: newReplTestStore()}
	for i := len(events) - 1; i >= 0; i-- {
		_ = h2.ApplyReplication(events[i])
	}

	f1, f2 := fields(h1), fields(h2)
	if len(f1) != len(f2) {
		t.Fatalf("replicas diverged: h1=%v h2=%v", f1, f2)
	}
	for i := range f1 {
		if f1[i] != f2[i] {
			t.Fatalf("replicas diverged: h1=%v h2=%v", f1, f2)
		}
	}
	// "a" deleted at 200 must stay gone; b=2 and c=3 present.
	if v, ok, _ := h1.Store.HGet("h", "a"); ok {
		t.Fatalf("deleted field 'a' resurrected as %q", v)
	}
	if v, _, _ := h1.Store.HGet("h", "b"); v != "2" {
		t.Fatalf("field b = %q, want 2", v)
	}
	if v, _, _ := h1.Store.HGet("h", "c"); v != "3" {
		t.Fatalf("field c = %q, want 3", v)
	}
}

// TestAntiEntropyReconcilesCounter verifies anti-entropy heals a counter that
// missed a live COUNTER event: after reconciliation the lagging node reflects
// the peer's increments, merged with its own.
func TestAntiEntropyReconcilesCounter(t *testing.T) {
	sa := newReplTestStore()
	sa.SetLocalOrigin(0xA)
	sb := newReplTestStore()
	sb.SetLocalOrigin(0xB)

	// A did +10 then +5 (peer missed both live). B did its own +2.
	sa.IncrBy("c", 10)
	sa.IncrBy("c", 5)
	sb.IncrBy("c", 2)

	a := &StoreReplicationHandler{Store: sa}
	b := &StoreReplicationHandler{Store: sb}

	// b pulls a's digest.
	reconcile(b, a)

	if v, _ := sb.Get("c"); v != "17" {
		t.Fatalf("after reconcile b = %q, want \"17\" (15 from A + 2 from B)", v)
	}
	// Idempotent: a second reconcile changes nothing.
	reconcile(b, a)
	if v, _ := sb.Get("c"); v != "17" {
		t.Fatalf("second reconcile changed b to %q, want \"17\"", v)
	}
}

// TestApplyReplicationCounterConverges verifies that two nodes' INCR streams,
// replicated as COUNTER state and applied via ApplyReplication in opposite
// orders, converge to the summed value with no increment lost.
func TestApplyReplicationCounterConverges(t *testing.T) {
	// Node A and node B each maintain their own counter with a distinct origin.
	sa := newReplTestStore()
	sa.SetLocalOrigin(0xA)
	sb := newReplTestStore()
	sb.SetLocalOrigin(0xB)

	// Independent local increments.
	sa.IncrBy("c", 5) // A: +5
	sb.IncrBy("c", 7) // B: +7

	// Snapshot each node's state and encode it as the wire payload.
	stA, _ := sa.CounterState("c")
	stB, _ := sb.CounterState("c")
	encA, err := encodeCounterState(stA)
	if err != nil {
		t.Fatalf("encode A: %v", err)
	}
	encB, err := encodeCounterState(stB)
	if err != nil {
		t.Fatalf("encode B: %v", err)
	}

	hA := &StoreReplicationHandler{Store: sa}
	hB := &StoreReplicationHandler{Store: sb}

	// Cross-apply: B learns A's state, A learns B's state.
	if err := hB.ApplyReplication(cluster.ReplicationEvent{Operation: "COUNTER", Key: "c", Value: encA}); err != nil {
		t.Fatalf("apply A->B: %v", err)
	}
	if err := hA.ApplyReplication(cluster.ReplicationEvent{Operation: "COUNTER", Key: "c", Value: encB}); err != nil {
		t.Fatalf("apply B->A: %v", err)
	}

	va, _ := sa.Get("c")
	vb, _ := sb.Get("c")
	if va != vb {
		t.Fatalf("counters diverged: a=%q b=%q", va, vb)
	}
	if va != "12" {
		t.Fatalf("converged value = %q, want \"12\" (5 + 7, nothing lost)", va)
	}

	// Duplicate delivery must not double-count.
	if err := hA.ApplyReplication(cluster.ReplicationEvent{Operation: "COUNTER", Key: "c", Value: encB}); err != nil {
		t.Fatalf("re-apply B->A: %v", err)
	}
	if v, _ := sa.Get("c"); v != "12" {
		t.Fatalf("duplicate COUNTER changed value to %q, want \"12\"", v)
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
