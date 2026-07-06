package store

import (
	"testing"

	"reservoir/pkg/types"
)

func stamp(physical int64, origin uint64) hlcStamp {
	return hlcStamp{TS: types.HLCTimestamp{Physical: physical}, Origin: origin}
}

func TestSetWithHLCLastWriteWins(t *testing.T) {
	s := createTestStore()

	if applied, _ := s.SetWithHLC("k", "older", stamp(100, 1)); !applied {
		t.Fatalf("first write not applied")
	}
	if applied, _ := s.SetWithHLC("k", "newer", stamp(200, 1)); !applied {
		t.Fatalf("newer write not applied")
	}
	if v, _ := s.Get("k"); v != "newer" {
		t.Fatalf("value = %q, want newer", v)
	}

	// A stale (older) write must be dropped, not overwrite the newer value.
	if applied, _ := s.SetWithHLC("k", "stale", stamp(100, 1)); applied {
		t.Fatalf("stale write was applied")
	}
	if v, _ := s.Get("k"); v != "newer" {
		t.Fatalf("stale write overwrote value: %q", v)
	}
}

func TestSetWithHLCOriginTiebreak(t *testing.T) {
	s := createTestStore()
	// Identical timestamp: the higher origin id wins deterministically.
	s.SetWithHLC("k", "from1", stamp(100, 1))
	if applied, _ := s.SetWithHLC("k", "from2", stamp(100, 2)); !applied {
		t.Fatalf("higher-origin write at equal timestamp should win")
	}
	if applied, _ := s.SetWithHLC("k", "from1again", stamp(100, 1)); applied {
		t.Fatalf("lower-origin write at equal timestamp should lose")
	}
	if v, _ := s.Get("k"); v != "from2" {
		t.Fatalf("value = %q, want from2", v)
	}
}

// TestSetWithHLCConvergence is the core LWW property: applying the same two
// writes in opposite orders yields the same value, so replicas that receive
// events in any order converge.
func TestSetWithHLCConvergence(t *testing.T) {
	type write struct {
		val string
		st  hlcStamp
	}
	w1 := write{"a", stamp(100, 1)}
	w2 := write{"b", stamp(150, 2)} // strictly newer

	s1 := createTestStore()
	s1.SetWithHLC("k", w1.val, w1.st)
	s1.SetWithHLC("k", w2.val, w2.st)

	s2 := createTestStore()
	s2.SetWithHLC("k", w2.val, w2.st) // reverse order
	s2.SetWithHLC("k", w1.val, w1.st)

	v1, _ := s1.Get("k")
	v2, _ := s2.Get("k")
	if v1 != v2 {
		t.Fatalf("replicas diverged: s1=%q s2=%q", v1, v2)
	}
	if v1 != "b" {
		t.Fatalf("converged on %q, want the newer write b", v1)
	}
}
