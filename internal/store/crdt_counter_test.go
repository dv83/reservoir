package store

import "testing"

// TestPNCounterBasic covers value accounting: increments, decrements, and a
// seeded base combine into Base + ΣP − ΣN.
func TestPNCounterBasic(t *testing.T) {
	c := newPNCounter()
	if c.value() != 0 {
		t.Fatalf("empty counter = %d, want 0", c.value())
	}
	c.add(1, 5)  // node 1: +5
	c.add(2, 3)  // node 2: +3
	c.add(1, -2) // node 1: -2
	if c.value() != 6 {
		t.Fatalf("value = %d, want 6", c.value())
	}
	c.Base = 10
	if c.value() != 16 {
		t.Fatalf("value with base = %d, want 16", c.value())
	}
}

// TestPNCounterMergeIdempotent verifies merging the same state twice changes
// nothing (max of equal slots), so duplicate delivery is harmless.
func TestPNCounterMergeIdempotent(t *testing.T) {
	a := newPNCounter()
	a.add(1, 7)
	a.add(2, 4)

	b := newPNCounter()
	b.mergeFrom(a)
	before := b.value()
	b.mergeFrom(a)
	if b.value() != before || before != 11 {
		t.Fatalf("idempotent merge broke: before=%d after=%d, want 11", before, b.value())
	}
}

// TestPNCounterConcurrentIncrementsNoLoss is the property the old result-based
// replication lacked: two nodes each increment the same counter independently,
// and after they exchange state (in either order) both see the sum — no
// increment is lost.
func TestPNCounterConcurrentIncrementsNoLoss(t *testing.T) {
	// Node A and node B both start from a shared base of 10 (a replicated SET).
	a := &pnCounter{Base: 10, P: map[uint64]int64{}, N: map[uint64]int64{}}
	b := &pnCounter{Base: 10, P: map[uint64]int64{}, N: map[uint64]int64{}}

	// Concurrent, independent increments on each node.
	a.add(0xA, 1)
	a.add(0xA, 1) // A: +2
	b.add(0xB, 1)
	b.add(0xB, 1)
	b.add(0xB, 1) // B: +3

	// Exchange state in opposite orders.
	a2 := a.clone()
	a2.mergeFrom(b)
	b2 := b.clone()
	b2.mergeFrom(a)

	if a2.value() != b2.value() {
		t.Fatalf("replicas diverged: a=%d b=%d", a2.value(), b2.value())
	}
	if a2.value() != 15 { // 10 base + 2 + 3
		t.Fatalf("converged value = %d, want 15 (base 10 + 2 + 3, nothing lost)", a2.value())
	}
}

// TestPNCounterMergeCommutative verifies the merge order does not matter across
// three replicas with disjoint and overlapping contributions.
func TestPNCounterMergeCommutative(t *testing.T) {
	mk := func(origin uint64, p, n int64) *pnCounter {
		c := newPNCounter()
		if p != 0 {
			c.P[origin] = p
		}
		if n != 0 {
			c.N[origin] = n
		}
		return c
	}
	x := mk(1, 5, 1)
	y := mk(2, 3, 0)
	z := mk(1, 8, 0) // higher P[1] than x — max should win

	// (x ∪ y) ∪ z
	left := x.clone()
	left.mergeFrom(y)
	left.mergeFrom(z)

	// z ∪ (y ∪ x)
	right := z.clone()
	right.mergeFrom(y)
	right.mergeFrom(x)

	if left.value() != right.value() {
		t.Fatalf("merge not commutative: left=%d right=%d", left.value(), right.value())
	}
	// P[1]=max(5,8)=8, N[1]=1, P[2]=3 → 8 - 1 + 3 = 10
	if left.value() != 10 {
		t.Fatalf("value = %d, want 10", left.value())
	}
}
