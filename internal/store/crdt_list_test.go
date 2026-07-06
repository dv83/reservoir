package store

import (
	"reflect"
	"testing"
)

// TestRGAListLocalOrder verifies single-node intention: appends go to the tail,
// prepends to the head, in the expected order.
func TestRGAListLocalOrder(t *testing.T) {
	r := newRGAList()
	r.localAppend(st(100, 0, 1), "b")
	r.localAppend(st(101, 0, 1), "c")
	r.localPrepend(st(102, 0, 1), "a")
	r.localAppend(st(103, 0, 1), "d")
	if got := r.values(); !reflect.DeepEqual(got, []string{"a", "b", "c", "d"}) {
		t.Fatalf("order = %v, want [a b c d]", got)
	}
}

// TestRGAListDeleteBlocksLateInsert verifies the tombstone guarantee: an insert
// that arrives after its own delete does not resurrect the element.
func TestRGAListDeleteBlocksLateInsert(t *testing.T) {
	id := st(100, 0, 1)

	// Delete arrives before the insert.
	r := newRGAList()
	r.applyDelete(id)
	r.applyInsert(rgaElem{id: id, pos: 0, value: "x", valStamp: id})
	if got := r.values(); len(got) != 0 {
		t.Fatalf("late insert resurrected a deleted element: %v", got)
	}
}

// applyOps replays a set of element inserts and deletes onto a fresh list; used
// to check order-independent convergence.
type listOp struct {
	del  bool
	elem rgaElem
}

func replay(ops []listOp) *rgaList {
	r := newRGAList()
	for _, o := range ops {
		if o.del {
			r.applyDelete(o.elem.id)
		} else {
			r.applyInsert(o.elem)
		}
	}
	return r
}

// TestRGAListConvergence verifies that replaying the same insert/delete events in
// any rotation yields identical visible values — the core CRDT property.
func TestRGAListConvergence(t *testing.T) {
	// Elements as they would have been assigned on their origin nodes.
	e := func(p, l int64, o uint64, pos int64, v string) rgaElem {
		id := st(p, uint32(l), o)
		return rgaElem{id: id, pos: pos, value: v, valStamp: id}
	}
	ops := []listOp{
		{false, e(100, 0, 1, 0, "a")},
		{false, e(101, 0, 1, 1, "b")},
		{false, e(102, 0, 2, 1, "x")}, // concurrent with b at pos 1 (tiebreak by id)
		{false, e(103, 0, 1, 2, "c")},
		{true, e(101, 0, 1, 0, "")},    // delete b
		{false, e(104, 0, 3, -1, "z")}, // prepend on another node
	}

	ref := replay(ops).values()
	for shift := 0; shift < len(ops); shift++ {
		rotated := append(append([]listOp{}, ops[shift:]...), ops[:shift]...)
		got := replay(rotated).values()
		if !reflect.DeepEqual(got, ref) {
			t.Fatalf("shift %d diverged: %v vs %v", shift, got, ref)
		}
	}
	// Sanity on the converged content: b deleted, x/c ordered, z prepended.
	if !reflect.DeepEqual(ref, []string{"z", "a", "x", "c"}) {
		t.Fatalf("converged order = %v, want [z a x c]", ref)
	}
}

// TestRGAListConcurrentPushTiebreak verifies two nodes appending at the same
// position converge on the id-ordered result regardless of arrival order.
func TestRGAListConcurrentPushTiebreak(t *testing.T) {
	// Both nodes append their first element to an empty list: both get pos 0.
	x := rgaElem{id: st(100, 0, 1), pos: 0, value: "x", valStamp: st(100, 0, 1)}
	y := rgaElem{id: st(100, 0, 2), pos: 0, value: "y", valStamp: st(100, 0, 2)}

	r1 := newRGAList()
	r1.applyInsert(x)
	r1.applyInsert(y)

	r2 := newRGAList()
	r2.applyInsert(y)
	r2.applyInsert(x)

	if !reflect.DeepEqual(r1.values(), r2.values()) {
		t.Fatalf("diverged on tie: %v vs %v", r1.values(), r2.values())
	}
	// Origin 2 > origin 1, so on the pos tie y sorts after x.
	if got := r1.values(); !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Fatalf("tie order = %v, want [x y]", got)
	}
}

// TestRGAListSetValueLWW verifies LSET replaces a value under last-write-wins.
func TestRGAListSetValueLWW(t *testing.T) {
	r := newRGAList()
	id := st(100, 0, 1)
	r.localAppend(id, "v1")

	if !r.setValue(id, "v2", st(200, 0, 1)) {
		t.Fatal("newer setValue not applied")
	}
	if r.setValue(id, "stale", st(150, 0, 1)) {
		t.Fatal("stale setValue should be rejected")
	}
	if got := r.values(); !reflect.DeepEqual(got, []string{"v2"}) {
		t.Fatalf("value = %v, want [v2]", got)
	}
	// setValue on a deleted element is a no-op.
	r.applyDelete(id)
	if r.setValue(id, "v3", st(300, 0, 1)) {
		t.Fatal("setValue on a deleted element should be rejected")
	}
}
