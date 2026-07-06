package store

import (
	"reflect"
	"testing"
	"time"
)

func newListCRDTStore(origin uint64) *LockFreeStore {
	s := NewStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	s.SetLocalOrigin(origin)
	return s
}

func lrange(t *testing.T, s *LockFreeStore, key string) []string {
	t.Helper()
	v, err := s.LRange(key, 0, -1)
	if err != nil {
		t.Fatalf("LRange(%q): %v", key, err)
	}
	return v
}

// TestListCRDTLocalSemantics verifies CRDT-mode list ops preserve Redis
// semantics on a single node: push order, pop, index, set, rem, trim.
func TestListCRDTLocalSemantics(t *testing.T) {
	s := newListCRDTStore(1)
	defer s.Stop()

	if n, _ := s.RPush("l", "a", "b", "c"); n != 3 {
		t.Fatalf("RPush len = %d, want 3", n)
	}
	if n, _ := s.LPush("l", "z"); n != 4 {
		t.Fatalf("LPush len = %d, want 4", n)
	}
	if got := lrange(t, s, "l"); !reflect.DeepEqual(got, []string{"z", "a", "b", "c"}) {
		t.Fatalf("LRange = %v, want [z a b c]", got)
	}
	if n, _ := s.LLen("l"); n != 4 {
		t.Fatalf("LLen = %d, want 4", n)
	}
	if v, ok, _ := s.LIndex("l", 1); !ok || v != "a" {
		t.Fatalf("LIndex 1 = (%q,%v), want (a,true)", v, ok)
	}
	if v, _ := s.LPop("l", 1); !reflect.DeepEqual(v, []string{"z"}) {
		t.Fatalf("LPop = %v, want [z]", v)
	}
	if v, _ := s.RPop("l", 1); !reflect.DeepEqual(v, []string{"c"}) {
		t.Fatalf("RPop = %v, want [c]", v)
	}
	if err := s.LSet("l", 0, "A"); err != nil {
		t.Fatalf("LSet: %v", err)
	}
	if got := lrange(t, s, "l"); !reflect.DeepEqual(got, []string{"A", "b"}) {
		t.Fatalf("after LSet = %v, want [A b]", got)
	}
	s.RPush("l", "b", "b")
	if n, _ := s.LRem("l", 0, "b"); n != 3 {
		t.Fatalf("LRem removed = %d, want 3", n)
	}
	if got := lrange(t, s, "l"); !reflect.DeepEqual(got, []string{"A"}) {
		t.Fatalf("after LRem = %v, want [A]", got)
	}
	s.RPush("l", "x", "y", "z")
	if err := s.LTrim("l", 1, 2); err != nil {
		t.Fatalf("LTrim: %v", err)
	}
	if got := lrange(t, s, "l"); !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Fatalf("after LTrim = %v, want [x y]", got)
	}
}

// applyDrain drains from's pending delta for key and applies it to into.
func applyDrain(t *testing.T, from, into *LockFreeStore, key string) {
	t.Helper()
	if d, ok := from.DrainListDelta(key); ok {
		if err := into.ApplyListDelta(key, d); err != nil {
			t.Fatalf("ApplyListDelta: %v", err)
		}
	}
}

// TestListCRDTConvergence verifies two nodes' concurrent pushes converge to the
// same order on both replicas after exchanging deltas — no element lost, unlike
// the old value-replay path.
func TestListCRDTConvergence(t *testing.T) {
	a := newListCRDTStore(1)
	defer a.Stop()
	b := newListCRDTStore(2)
	defer b.Stop()

	a.RPush("l", "a", "b") // node A appends a,b
	b.RPush("l", "c", "d") // node B appends c,d concurrently

	// Exchange deltas both ways.
	applyDrain(t, a, b, "l")
	applyDrain(t, b, a, "l")

	la, lb := lrange(t, a, "l"), lrange(t, b, "l")
	if !reflect.DeepEqual(la, lb) {
		t.Fatalf("replicas diverged: a=%v b=%v", la, lb)
	}
	if len(la) != 4 {
		t.Fatalf("lost elements: %v (want all 4 of a,b,c,d)", la)
	}
	// pos 0: {a(o1),c(o2)} -> a,c ; pos 1: {b,d} -> b,d.
	if !reflect.DeepEqual(la, []string{"a", "c", "b", "d"}) {
		t.Fatalf("converged order = %v, want [a c b d]", la)
	}
}

// TestListCRDTPopTombstoneConverges verifies a pop on one node propagates as a
// tombstone: the element is removed on the other node and a re-delivered insert
// cannot resurrect it.
func TestListCRDTPopTombstoneConverges(t *testing.T) {
	a := newListCRDTStore(1)
	defer a.Stop()
	b := newListCRDTStore(2)
	defer b.Stop()

	a.RPush("l", "x", "y")
	pushDelta, _ := a.DrainListDelta("l")
	b.ApplyListDelta("l", pushDelta) // b now has x,y

	a.LPop("l", 1) // pop x on a
	popDelta, _ := a.DrainListDelta("l")
	b.ApplyListDelta("l", popDelta) // b tombstones x

	if got := lrange(t, b, "l"); !reflect.DeepEqual(got, []string{"y"}) {
		t.Fatalf("b after pop = %v, want [y]", got)
	}
	// Re-deliver the original insert of x: the tombstone must keep it gone.
	b.ApplyListDelta("l", pushDelta)
	if got := lrange(t, b, "l"); !reflect.DeepEqual(got, []string{"y"}) {
		t.Fatalf("re-delivered insert resurrected x: %v", got)
	}
}

// TestListCRDTTombstoneGC verifies aged-out list tombstones are pruned and a
// fully-emptied list is reclaimed, while fresh tombstones are retained.
func TestListCRDTTombstoneGC(t *testing.T) {
	s := newListCRDTStore(1)
	defer s.Stop()

	// "gone": build and empty it, then backdate its tombstones so GC reclaims it.
	s.RPush("gone", "a", "b")
	s.LPop("gone", 2) // fully emptied; tombstones retained
	shard := s.getShard("gone")
	shard.mu.Lock()
	if sv, ok := shard.data["gone"]; ok && sv.ListVal != nil && sv.ListVal.rga != nil {
		for _, e := range sv.ListVal.rga.elems {
			e.delTime = time.Now().Add(-2 * time.Hour).UnixNano()
		}
	}
	shard.mu.Unlock()

	// "live" list keeps its elements and a fresh tombstone.
	s.RPush("live", "x", "y", "z")
	s.LRem("live", 1, "y") // fresh tombstone for y

	s.gcTombstones(time.Hour)

	if _, ok := s.GetStoredValue("gone"); ok {
		t.Error("fully-aged-out empty list 'gone' was not reclaimed")
	}
	if got := lrange(t, s, "live"); !reflect.DeepEqual(got, []string{"x", "z"}) {
		t.Errorf("live list after GC = %v, want [x z]", got)
	}
}

// TestListCRDTLPushXNonexistent verifies LPUSHX/RPUSHX do not create a missing
// list in CRDT mode.
func TestListCRDTLPushXNonexistent(t *testing.T) {
	s := newListCRDTStore(1)
	defer s.Stop()
	if n, _ := s.LPushX("missing", "v"); n != 0 {
		t.Fatalf("LPushX on missing = %d, want 0", n)
	}
	if _, ok := s.GetStoredValue("missing"); ok {
		t.Fatal("LPushX created a missing list")
	}
}
