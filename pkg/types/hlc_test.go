package types

import "testing"

func TestHLCTimestampCompare(t *testing.T) {
	a := HLCTimestamp{Physical: 100, Logical: 0}
	b := HLCTimestamp{Physical: 100, Logical: 1}
	c := HLCTimestamp{Physical: 200, Logical: 0}

	if !b.After(a) {
		t.Errorf("(100,1) should be after (100,0)")
	}
	if !c.After(b) {
		t.Errorf("(200,0) should be after (100,1)")
	}
	if a.After(a) {
		t.Errorf("timestamp should not be after itself")
	}
	if a.Compare(a) != 0 {
		t.Errorf("equal timestamps should compare 0")
	}
	if !(HLCTimestamp{}).IsZero() {
		t.Errorf("zero value should be zero")
	}
}

// TestHLCMonotonicSamePhysical verifies that when physical time does not advance,
// successive Now() calls increment the logical counter and stay strictly ordered.
func TestHLCMonotonicSamePhysical(t *testing.T) {
	phys := int64(1000)
	h := newHLCWithClock(func() int64 { return phys })

	prev := h.Now()
	for i := 0; i < 5; i++ {
		cur := h.Now()
		if !cur.After(prev) {
			t.Fatalf("Now() not strictly increasing: %v then %v", prev, cur)
		}
		prev = cur
	}
}

// TestHLCPhysicalAdvanceResetsLogical verifies the logical counter resets when
// physical time advances.
func TestHLCPhysicalAdvanceResetsLogical(t *testing.T) {
	phys := int64(1000)
	h := newHLCWithClock(func() int64 { return phys })

	_ = h.Now() // (1000,0)
	_ = h.Now() // (1000,1)
	phys = 2000
	ts := h.Now()
	if ts.Physical != 2000 || ts.Logical != 0 {
		t.Fatalf("after physical advance got %v, want (2000,0)", ts)
	}
}

// TestHLCUpdateTracksRemote verifies Update produces a timestamp strictly after
// both the local clock and a remote timestamp from the future.
func TestHLCUpdateTracksRemote(t *testing.T) {
	phys := int64(1000)
	h := newHLCWithClock(func() int64 { return phys })

	local := h.Now() // (1000,0)

	// A remote timestamp from further ahead in physical time.
	remote := HLCTimestamp{Physical: 5000, Logical: 3}
	merged := h.Update(remote)

	if !merged.After(local) {
		t.Errorf("merged %v not after local %v", merged, local)
	}
	if !merged.After(remote) {
		t.Errorf("merged %v not after remote %v", merged, remote)
	}
	if merged.Physical != 5000 || merged.Logical != 4 {
		t.Errorf("merged = %v, want (5000,4)", merged)
	}

	// A subsequent local event stays ahead.
	next := h.Now()
	if !next.After(merged) {
		t.Errorf("next %v not after merged %v", next, merged)
	}
}

// TestHLCUpdateStalePastRemote verifies a remote timestamp from the past does not
// pull the local clock backwards.
func TestHLCUpdateStalePastRemote(t *testing.T) {
	phys := int64(5000)
	h := newHLCWithClock(func() int64 { return phys })
	local := h.Now() // (5000,0)

	merged := h.Update(HLCTimestamp{Physical: 100, Logical: 0})
	if merged.Compare(local) <= 0 {
		t.Fatalf("merged %v should be strictly after local %v (no regression)", merged, local)
	}
}
