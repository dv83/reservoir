package store

import (
	"sort"
	"testing"

	"reservoir/pkg/types"
)

func st(physical int64, logical uint32, origin uint64) hlcStamp {
	return hlcStamp{TS: types.HLCTimestamp{Physical: physical, Logical: logical}, Origin: origin}
}

func sortedMembers(s *lwwSet) []string {
	m := s.members()
	sort.Strings(m)
	return m
}

// op is one add/remove operation, replayable in any order.
type op struct {
	add   bool
	elem  string
	stamp hlcStamp
}

func apply(s *lwwSet, ops []op) {
	for _, o := range ops {
		if o.add {
			s.add(o.elem, o.stamp)
		} else {
			s.remove(o.elem, o.stamp)
		}
	}
}

// TestLWWSetAddRemoveBasic covers the single-element state machine: an add makes
// it a member, a newer remove hides it, a still-newer add brings it back.
func TestLWWSetAddRemoveBasic(t *testing.T) {
	s := newLWWSet()
	if s.contains("a") {
		t.Fatal("empty set should not contain 'a'")
	}
	s.add("a", st(100, 0, 1))
	if !s.contains("a") {
		t.Fatal("after add, 'a' should be a member")
	}
	s.remove("a", st(200, 0, 1))
	if s.contains("a") {
		t.Fatal("after newer remove, 'a' should be gone")
	}
	s.add("a", st(300, 0, 1))
	if !s.contains("a") {
		t.Fatal("after still-newer add, 'a' should return")
	}
	if s.card() != 1 {
		t.Fatalf("card = %d, want 1", s.card())
	}
}

// TestLWWSetStaleOpsIgnored verifies that an out-of-order (older) add or remove
// never overrides a newer one, so delivery order cannot change the result.
func TestLWWSetStaleOpsIgnored(t *testing.T) {
	s := newLWWSet()
	s.remove("a", st(200, 0, 1)) // remove arrives first
	s.add("a", st(100, 0, 1))    // older add arrives late
	if s.contains("a") {
		t.Fatal("a stale add older than the remove must not resurrect 'a'")
	}

	s2 := newLWWSet()
	s2.add("b", st(200, 0, 1)) // newer add first
	s2.remove("b", st(100, 0, 1))
	if !s2.contains("b") {
		t.Fatal("a stale remove older than the add must not delete 'b'")
	}
}

// TestLWWSetAddWinsOnTie verifies the add-wins bias on an exact stamp tie:
// when an add and a remove carry the identical stamp, the element stays.
func TestLWWSetAddWinsOnTie(t *testing.T) {
	s := newLWWSet()
	tie := st(100, 0, 1)
	s.remove("a", tie)
	s.add("a", tie)
	if !s.contains("a") {
		t.Fatal("on an exact add/remove tie, add must win")
	}
}

// TestLWWSetConvergence is the core CRDT property: applying the same set of
// operations in any permutation yields identical membership on every replica.
func TestLWWSetConvergence(t *testing.T) {
	ops := []op{
		{true, "a", st(100, 0, 1)},
		{true, "b", st(110, 0, 2)},
		{false, "a", st(120, 0, 1)},
		{true, "a", st(130, 0, 3)},
		{true, "c", st(90, 0, 1)},
		{false, "c", st(140, 0, 2)},
		{false, "b", st(105, 0, 1)}, // older than b's add — no effect
		{true, "d", st(150, 0, 1)},
	}

	// Reference order.
	ref := newLWWSet()
	apply(ref, ops)
	want := sortedMembers(ref)

	// Every rotation of the op list must converge to the same membership.
	for shift := 0; shift < len(ops); shift++ {
		rotated := make([]op, 0, len(ops))
		rotated = append(rotated, ops[shift:]...)
		rotated = append(rotated, ops[:shift]...)

		s := newLWWSet()
		apply(s, rotated)
		got := sortedMembers(s)
		if len(got) != len(want) {
			t.Fatalf("shift %d: membership size %d, want %d (%v vs %v)", shift, len(got), len(want), got, want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("shift %d: diverged: got %v, want %v", shift, got, want)
			}
		}
	}
}

// TestLWWSetOriginTiebreak verifies that when two ops share an identical HLC
// timestamp, the origin node id breaks the tie deterministically — so add vs
// remove at the same physical/logical time resolves the same way everywhere.
func TestLWWSetOriginTiebreak(t *testing.T) {
	// remove has the higher origin, so it wins the timestamp tie over the add.
	s := newLWWSet()
	s.add("a", st(100, 0, 1))
	s.remove("a", st(100, 0, 5))
	if s.contains("a") {
		t.Fatal("remove with higher origin should win the tie and delete 'a'")
	}

	// Reverse arrival order must reach the same state.
	s2 := newLWWSet()
	s2.remove("a", st(100, 0, 5))
	s2.add("a", st(100, 0, 1))
	if s2.contains("a") {
		t.Fatal("origin tiebreak must be order-independent")
	}
}
