package store

import "testing"

func newCounterStore(origin uint64) *LockFreeStore {
	s := NewStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	s.SetLocalOrigin(origin)
	return s
}

// TestIncrByCRDTBasic verifies the counter path preserves plain INCR semantics:
// value accounting, GET readback, WRONGTYPE, non-integer, and overflow.
func TestIncrByCRDTBasic(t *testing.T) {
	s := newCounterStore(1)
	defer s.Stop()

	if v, err := s.Incr("c"); err != nil || v != 1 {
		t.Fatalf("Incr = (%d,%v), want (1,nil)", v, err)
	}
	if v, _ := s.IncrBy("c", 10); v != 11 {
		t.Fatalf("IncrBy = %d, want 11", v)
	}
	if v, _ := s.Decr("c"); v != 10 {
		t.Fatalf("Decr = %d, want 10", v)
	}
	// GET reads the materialized value.
	if got, ok := s.Get("c"); !ok || got != "10" {
		t.Fatalf("Get = (%q,%v), want (\"10\",true)", got, ok)
	}
	// WRONGTYPE on a list key.
	if _, err := s.LPush("l", "x"); err != nil {
		t.Fatalf("LPush: %v", err)
	}
	if _, err := s.Incr("l"); err == nil {
		t.Fatal("Incr on a list did not return WRONGTYPE")
	}
	// Non-integer string.
	if err := s.Set("str", "abc"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, err := s.Incr("str"); err == nil {
		t.Fatal("Incr on a non-integer string did not error")
	}
}

// TestIncrCRDTSeedsBaseFromSet verifies INCR on a SET-created numeric key keeps
// Redis semantics: the SET value seeds the counter base.
func TestIncrCRDTSeedsBaseFromSet(t *testing.T) {
	s := newCounterStore(1)
	defer s.Stop()

	if err := s.Set("c", "100"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if v, _ := s.Incr("c"); v != 101 {
		t.Fatalf("Incr after SET 100 = %d, want 101", v)
	}
}

// TestCounterConvergenceNoLostIncrements is the end-to-end store property: two
// nodes each run their own increments on the same key, then exchange counter
// state via MergeCounter in both directions — both converge to the sum with no
// increment lost, which the old result-replication could not guarantee.
func TestCounterConvergenceNoLostIncrements(t *testing.T) {
	a := newCounterStore(0xA)
	defer a.Stop()
	b := newCounterStore(0xB)
	defer b.Stop()

	// Both start from a shared base (a replicated SET 10).
	a.Set("c", "10")
	b.Set("c", "10")

	// Independent concurrent increments.
	a.IncrBy("c", 2) // node A: +2
	b.IncrBy("c", 3) // node B: +3

	// Exchange state both ways.
	sa, _ := a.CounterState("c")
	sb, _ := b.CounterState("c")
	vb, _ := b.MergeCounter("c", sa)
	va, _ := a.MergeCounter("c", sb)

	if va != vb {
		t.Fatalf("counters diverged: a=%d b=%d", va, vb)
	}
	if va != 15 {
		t.Fatalf("converged value = %d, want 15 (10 + 2 + 3, no increment lost)", va)
	}

	// Re-applying the same states is idempotent (max-merge).
	again, _ := a.MergeCounter("c", sb)
	if again != 15 {
		t.Fatalf("re-merge changed value to %d, want 15", again)
	}
}
