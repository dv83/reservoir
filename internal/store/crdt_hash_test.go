package store

import (
	"sort"
	"testing"
)

// hashOp is one field set/delete, replayable in any order.
type hashOp struct {
	set   bool
	field string
	value string
	stamp hlcStamp
}

func applyHashOps(h *lwwHash, ops []hashOp) {
	for _, o := range ops {
		if o.set {
			h.set(o.field, o.value, o.stamp)
		} else {
			h.del(o.field, o.stamp)
		}
	}
}

func hashKeysSorted(h *lwwHash) []string {
	out := []string{}
	for f := range h.fields {
		if _, ok := h.get(f); ok {
			out = append(out, f)
		}
	}
	sort.Strings(out)
	return out
}

// TestLWWHashBasic covers a field's state machine: set makes it present with a
// value, a newer delete hides it, a still-newer set restores it with a new value.
func TestLWWHashBasic(t *testing.T) {
	h := newLWWHash()
	if _, ok := h.get("f"); ok {
		t.Fatal("empty hash should not have field 'f'")
	}
	h.set("f", "v1", st(100, 0, 1))
	if v, ok := h.get("f"); !ok || v != "v1" {
		t.Fatalf("after set, get = (%q,%v), want (v1,true)", v, ok)
	}
	h.set("f", "v2", st(200, 0, 1))
	if v, _ := h.get("f"); v != "v2" {
		t.Fatalf("after newer set, value = %q, want v2", v)
	}
	h.del("f", st(300, 0, 1))
	if _, ok := h.get("f"); ok {
		t.Fatal("after newer delete, 'f' should be gone")
	}
	h.set("f", "v3", st(400, 0, 1))
	if v, _ := h.get("f"); v != "v3" {
		t.Fatalf("after still-newer set, value = %q, want v3", v)
	}
}

// TestLWWHashStaleOpsIgnored verifies that an out-of-order (older) set or delete
// never overrides a newer one, so delivery order cannot change the result.
func TestLWWHashStaleOpsIgnored(t *testing.T) {
	h := newLWWHash()
	h.del("a", st(200, 0, 1)) // delete arrives first
	h.set("a", "x", st(100, 0, 1))
	if _, ok := h.get("a"); ok {
		t.Fatal("a stale set older than the delete must not resurrect 'a'")
	}

	h2 := newLWWHash()
	h2.set("b", "new", st(200, 0, 1))
	h2.set("b", "old", st(100, 0, 1)) // older set must not overwrite
	if v, _ := h2.get("b"); v != "new" {
		t.Fatalf("stale set overwrote newer value: got %q, want new", v)
	}
}

// TestLWWHashConvergence is the CRDT property: every rotation of a mixed op
// sequence yields identical field state and values.
func TestLWWHashConvergence(t *testing.T) {
	ops := []hashOp{
		{true, "a", "a1", st(100, 0, 1)},
		{true, "b", "b1", st(110, 0, 2)},
		{false, "a", "", st(120, 0, 1)},
		{true, "a", "a2", st(130, 0, 3)},
		{true, "c", "c1", st(90, 0, 1)},
		{false, "c", "", st(140, 0, 2)},
		{true, "b", "b0", st(105, 0, 1)}, // older than b's set — no effect
		{true, "d", "d1", st(150, 0, 1)},
	}

	ref := newLWWHash()
	applyHashOps(ref, ops)
	wantKeys := hashKeysSorted(ref)
	wantVals := map[string]string{}
	for _, k := range wantKeys {
		v, _ := ref.get(k)
		wantVals[k] = v
	}

	for shift := 0; shift < len(ops); shift++ {
		rotated := append(append([]hashOp{}, ops[shift:]...), ops[:shift]...)
		h := newLWWHash()
		applyHashOps(h, rotated)
		gotKeys := hashKeysSorted(h)
		if len(gotKeys) != len(wantKeys) {
			t.Fatalf("shift %d: keys %v, want %v", shift, gotKeys, wantKeys)
		}
		for i, k := range wantKeys {
			if gotKeys[i] != k {
				t.Fatalf("shift %d: keys diverged: %v vs %v", shift, gotKeys, wantKeys)
			}
			if v, _ := h.get(k); v != wantVals[k] {
				t.Fatalf("shift %d: field %q = %q, want %q", shift, k, v, wantVals[k])
			}
		}
	}
}

// TestLWWHashOriginTiebreak verifies that an exact timestamp tie between a set
// and a delete resolves deterministically by origin, order-independently.
func TestLWWHashOriginTiebreak(t *testing.T) {
	h := newLWWHash()
	h.set("f", "v", st(100, 0, 1))
	h.del("f", st(100, 0, 5)) // higher origin wins the tie
	if _, ok := h.get("f"); ok {
		t.Fatal("delete with higher origin should win the tie")
	}

	h2 := newLWWHash()
	h2.del("f", st(100, 0, 5))
	h2.set("f", "v", st(100, 0, 1))
	if _, ok := h2.get("f"); ok {
		t.Fatal("origin tiebreak must be order-independent")
	}
}
