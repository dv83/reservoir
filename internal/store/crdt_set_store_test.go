package store

import (
	"sort"
	"testing"
	"time"
)

func newCRDTTestStore() *LockFreeStore {
	return NewStore(&Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1 << 20,
		MaxMemoryUsage: 1 << 30,
	})
}

func membersSorted(t *testing.T, s *LockFreeStore, key string) []string {
	t.Helper()
	m, err := s.SMembers(key)
	if err != nil {
		t.Fatalf("SMembers(%q): %v", key, err)
	}
	sort.Strings(m)
	return m
}

func eqStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// setOp is a stamped SADD/SREM, replayable in any order.
type setOp struct {
	add      bool
	member   string
	physical int64
	origin   uint64
}

func applySetOps(t *testing.T, s *LockFreeStore, key string, ops []setOp) {
	t.Helper()
	for _, o := range ops {
		var err error
		if o.add {
			_, err = s.SAddLWW(key, o.physical, 0, o.origin, o.member)
		} else {
			_, err = s.SRemLWW(key, o.physical, 0, o.origin, o.member)
		}
		if err != nil {
			t.Fatalf("op %+v: %v", o, err)
		}
	}
}

// TestSAddLWWBasic verifies a stamped add is a member, a newer stamped remove
// hides it, and the count returned reflects membership changes.
func TestSAddLWWBasic(t *testing.T) {
	s := newCRDTTestStore()
	defer s.Stop()

	if n, _ := s.SAddLWW("k", 100, 0, 1, "a", "b"); n != 2 {
		t.Fatalf("SAddLWW added %d, want 2", n)
	}
	if n, _ := s.SAddLWW("k", 110, 0, 1, "a"); n != 0 {
		t.Fatalf("re-adding present member counted %d, want 0", n)
	}
	if n, _ := s.SRemLWW("k", 200, 0, 1, "a"); n != 1 {
		t.Fatalf("SRemLWW removed %d, want 1", n)
	}
	if got := membersSorted(t, s, "k"); !eqStrings(got, []string{"b"}) {
		t.Fatalf("members = %v, want [b]", got)
	}
}

// TestSRemLWWBlocksStaleAdd verifies the core CRDT guarantee that plain SREM
// cannot give: a remove that arrives before a concurrent older add still wins,
// so the element does not resurrect regardless of delivery order.
func TestSRemLWWBlocksStaleAdd(t *testing.T) {
	s := newCRDTTestStore()
	defer s.Stop()

	// Remove of "x" at t=200 arrives on a node that has never seen "x".
	if _, err := s.SRemLWW("k", 200, 0, 2, "x"); err != nil {
		t.Fatalf("SRemLWW: %v", err)
	}
	// A stale add at t=100 arrives late — it must not resurrect "x".
	if _, err := s.SAddLWW("k", 100, 0, 1, "x"); err != nil {
		t.Fatalf("SAddLWW: %v", err)
	}
	if m, _ := s.SIsMember("k", "x"); m {
		t.Fatal("stale add older than the tombstone resurrected 'x'")
	}
}

// TestCRDTSetConvergence is the convergence property at the store level: two
// replicas that receive the same stamped SADD/SREM ops in different orders end
// with identical membership.
func TestCRDTSetConvergence(t *testing.T) {
	ops := []setOp{
		{true, "a", 100, 1},
		{true, "b", 110, 2},
		{false, "a", 120, 1},
		{true, "a", 130, 3},
		{true, "c", 90, 1},
		{false, "c", 140, 2},
		{false, "b", 105, 1}, // older than b's add — no effect
		{true, "d", 150, 1},
		{false, "d", 80, 2}, // older than d's add — no effect
	}

	ref := newCRDTTestStore()
	defer ref.Stop()
	applySetOps(t, ref, "k", ops)
	want := membersSorted(t, ref, "k")

	for shift := 0; shift < len(ops); shift++ {
		rotated := make([]setOp, 0, len(ops))
		rotated = append(rotated, ops[shift:]...)
		rotated = append(rotated, ops[:shift]...)

		s := newCRDTTestStore()
		applySetOps(t, s, "k", rotated)
		got := membersSorted(t, s, "k")
		s.Stop()

		if !eqStrings(got, want) {
			t.Fatalf("shift %d diverged: got %v, want %v", shift, got, want)
		}
	}
}

// TestCRDTSetTombstoneGC verifies that once a set's element tombstones age out,
// the GC pass prunes them and reclaims the now-empty set, while a still-fresh
// tombstone (and any live member) is preserved.
func TestCRDTSetTombstoneGC(t *testing.T) {
	s := newCRDTTestStore()
	defer s.Stop()

	past := time.Now().Add(-2 * time.Hour).UnixNano()
	now := time.Now().UnixNano()

	// "old" set: a member added and removed long ago — fully tombstoned.
	s.SAddLWW("old", past, 0, 1, "a")
	s.SRemLWW("old", past+1, 0, 1, "a")

	// "mixed" set: a live member plus a member removed just now (fresh tombstone).
	s.SAddLWW("mixed", now, 0, 1, "live")
	s.SAddLWW("mixed", past, 0, 1, "gone")
	s.SRemLWW("mixed", now, 0, 1, "gone")

	// Retain one hour: the past tombstones are stale, the "now" one is fresh.
	s.gcTombstones(time.Hour)

	// "old" aged out entirely and should be reclaimed from the keyspace.
	if _, ok := s.GetStoredValue("old"); ok {
		t.Error("fully-aged-out empty set 'old' was not reclaimed")
	}
	// A stale re-add after GC is allowed to recreate the element (tombstone gone).
	// This is the accepted GC tradeoff; assert the set is live and holds it.
	if m, _ := s.SIsMember("mixed", "live"); !m {
		t.Error("live member pruned by GC")
	}
	// "mixed" still holds a fresh tombstone for "gone": a stale re-add must fail.
	s.SAddLWW("mixed", past+5, 0, 1, "gone")
	if m, _ := s.SIsMember("mixed", "gone"); m {
		t.Error("fresh tombstone for 'gone' was incorrectly GC'd (stale add resurrected it)")
	}
}

// TestCRDTSetRetainsTombstonesWhenEmpty verifies that removing every member
// keeps the set alive (holding tombstones) so a later stale add still cannot
// resurrect a removed element — the property a delete-on-empty set would lose.
func TestCRDTSetRetainsTombstonesWhenEmpty(t *testing.T) {
	s := newCRDTTestStore()
	defer s.Stop()

	s.SAddLWW("k", 100, 0, 1, "a")
	s.SRemLWW("k", 200, 0, 1, "a") // set is now materially empty

	if c, _ := s.SCard("k"); c != 0 {
		t.Fatalf("SCard = %d, want 0 after removing all members", c)
	}
	// A stale re-add older than the tombstone must not bring "a" back.
	s.SAddLWW("k", 150, 0, 1, "a")
	if m, _ := s.SIsMember("k", "a"); m {
		t.Fatal("stale add resurrected 'a' after empty — tombstone not retained")
	}
	// A fresh add newer than the tombstone must succeed.
	s.SAddLWW("k", 300, 0, 1, "a")
	if m, _ := s.SIsMember("k", "a"); !m {
		t.Fatal("newer add after tombstone did not restore 'a'")
	}
}
