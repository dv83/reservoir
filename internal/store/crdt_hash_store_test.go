package store

import (
	"sort"
	"testing"
	"time"
)

func hkeys(t *testing.T, s *LockFreeStore, key string) []string {
	t.Helper()
	ks, err := s.HKeys(key)
	if err != nil {
		t.Fatalf("HKeys(%q): %v", key, err)
	}
	sort.Strings(ks)
	return ks
}

// hashSetOp is a stamped HSET/HDEL of one field, replayable in any order.
type hashSetOp struct {
	set      bool
	field    string
	value    string
	physical int64
	origin   uint64
}

func applyHashStoreOps(t *testing.T, s *LockFreeStore, key string, ops []hashSetOp) {
	t.Helper()
	for _, o := range ops {
		var err error
		if o.set {
			_, err = s.HSetLWW(key, o.physical, 0, o.origin, o.field, o.value)
		} else {
			_, err = s.HDelLWW(key, o.physical, 0, o.origin, o.field)
		}
		if err != nil {
			t.Fatalf("op %+v: %v", o, err)
		}
	}
}

// TestHSetLWWBasic verifies stamped set/delete semantics and the newly-created
// count HSET reports.
func TestHSetLWWBasic(t *testing.T) {
	s := newCRDTTestStore()
	defer s.Stop()

	if n, _ := s.HSetLWW("h", 100, 0, 1, "a", "1", "b", "2"); n != 2 {
		t.Fatalf("HSetLWW new fields = %d, want 2", n)
	}
	if n, _ := s.HSetLWW("h", 110, 0, 1, "a", "9"); n != 0 {
		t.Fatalf("updating existing field counted %d, want 0", n)
	}
	if v, ok, _ := s.HGet("h", "a"); !ok || v != "9" {
		t.Fatalf("HGet a = (%q,%v), want (9,true)", v, ok)
	}
	if n, _ := s.HDelLWW("h", 200, 0, 1, "a"); n != 1 {
		t.Fatalf("HDelLWW = %d, want 1", n)
	}
	if got := hkeys(t, s, "h"); !(len(got) == 1 && got[0] == "b") {
		t.Fatalf("keys = %v, want [b]", got)
	}
}

// TestHDelLWWBlocksStaleSet verifies the CRDT guarantee plain HDEL lacks: a
// delete that arrives before a concurrent older set still wins, so the field
// does not resurrect regardless of delivery order.
func TestHDelLWWBlocksStaleSet(t *testing.T) {
	s := newCRDTTestStore()
	defer s.Stop()

	if _, err := s.HDelLWW("h", 200, 0, 2, "x"); err != nil {
		t.Fatalf("HDelLWW: %v", err)
	}
	if _, err := s.HSetLWW("h", 100, 0, 1, "x", "v"); err != nil {
		t.Fatalf("HSetLWW: %v", err)
	}
	if _, ok, _ := s.HGet("h", "x"); ok {
		t.Fatal("stale set older than the tombstone resurrected field 'x'")
	}
}

// TestCRDTHashConvergence is the convergence property at the store level: two
// replicas that receive the same stamped HSET/HDEL ops in different orders end
// with identical fields and values.
func TestCRDTHashConvergence(t *testing.T) {
	ops := []hashSetOp{
		{true, "a", "a1", 100, 1},
		{true, "b", "b1", 110, 2},
		{false, "a", "", 120, 1},
		{true, "a", "a2", 130, 3},
		{true, "c", "c1", 90, 1},
		{false, "c", "", 140, 2},
		{true, "b", "b0", 105, 1}, // older than b's set — no effect
		{true, "d", "d1", 150, 1},
	}

	ref := newCRDTTestStore()
	defer ref.Stop()
	applyHashStoreOps(t, ref, "h", ops)
	wantKeys := hkeys(t, ref, "h")
	wantVals := map[string]string{}
	for _, k := range wantKeys {
		v, _, _ := ref.HGet("h", k)
		wantVals[k] = v
	}

	for shift := 0; shift < len(ops); shift++ {
		rotated := append(append([]hashSetOp{}, ops[shift:]...), ops[:shift]...)
		cur := newCRDTTestStore()
		applyHashStoreOps(t, cur, "h", rotated)
		gotKeys := hkeys(t, cur, "h")
		if len(gotKeys) != len(wantKeys) {
			cur.Stop()
			t.Fatalf("shift %d: keys %v, want %v", shift, gotKeys, wantKeys)
		}
		for i, k := range wantKeys {
			if gotKeys[i] != k {
				cur.Stop()
				t.Fatalf("shift %d: keys diverged %v vs %v", shift, gotKeys, wantKeys)
			}
			if v, _, _ := cur.HGet("h", k); v != wantVals[k] {
				cur.Stop()
				t.Fatalf("shift %d: field %q = %q, want %q", shift, k, v, wantVals[k])
			}
		}
		cur.Stop()
	}
}

// TestCRDTHashTombstoneGC verifies that once a hash's field tombstones age out,
// the GC pass prunes them and reclaims the now-empty hash, while a still-fresh
// tombstone and any live field are preserved.
func TestCRDTHashTombstoneGC(t *testing.T) {
	s := newCRDTTestStore()
	defer s.Stop()

	past := time.Now().Add(-2 * time.Hour).UnixNano()
	now := time.Now().UnixNano()

	// "old" hash: a field set and deleted long ago — fully tombstoned.
	s.HSetLWW("old", past, 0, 1, "a", "1")
	s.HDelLWW("old", past+1, 0, 1, "a")

	// "mixed" hash: a live field plus a field deleted just now (fresh tombstone).
	s.HSetLWW("mixed", now, 0, 1, "live", "v")
	s.HSetLWW("mixed", past, 0, 1, "gone", "x")
	s.HDelLWW("mixed", now, 0, 1, "gone")

	s.gcTombstones(time.Hour)

	if _, ok := s.GetStoredValue("old"); ok {
		t.Error("fully-aged-out empty hash 'old' was not reclaimed")
	}
	if v, ok, _ := s.HGet("mixed", "live"); !ok || v != "v" {
		t.Errorf("live field pruned by GC: (%q,%v)", v, ok)
	}
	// Fresh tombstone for "gone" must survive: a stale re-set must fail.
	s.HSetLWW("mixed", past+5, 0, 1, "gone", "stale")
	if _, ok, _ := s.HGet("mixed", "gone"); ok {
		t.Error("fresh tombstone for 'gone' was incorrectly GC'd (stale set resurrected it)")
	}
}

// TestCRDTHashPersistedToCommitLog verifies HSetLWW/HDelLWW append HSET/HDEL
// records with the null-separated payload the recovery path decodes.
func TestCRDTHashPersistedToCommitLog(t *testing.T) {
	s := newCRDTTestStore()
	defer s.Stop()
	fake := &fakeCommitLog{}
	s.SetCommitLog(fake)

	s.HSetLWW("h", 100, 0, 1, "a", "1", "b", "2")
	s.HDelLWW("h", 200, 0, 1, "a")

	var setVal, delVal string
	for _, w := range fake.writes {
		switch {
		case w.op == "HSET" && w.key == "h":
			setVal = w.value
		case w.op == "HDEL" && w.key == "h":
			delVal = w.value
		}
	}
	if setVal != "h\x00a\x001\x00b\x002" {
		t.Fatalf("HSET payload = %q, want %q", setVal, "h\x00a\x001\x00b\x002")
	}
	if delVal != "h\x00a" {
		t.Fatalf("HDEL payload = %q, want %q", delVal, "h\x00a")
	}
}

// TestCRDTHashRetainsTombstonesWhenEmpty verifies that deleting every field
// keeps the hash alive (holding tombstones), so a later stale set cannot
// resurrect a removed field.
func TestCRDTHashRetainsTombstonesWhenEmpty(t *testing.T) {
	s := newCRDTTestStore()
	defer s.Stop()

	s.HSetLWW("h", 100, 0, 1, "a", "1")
	s.HDelLWW("h", 200, 0, 1, "a") // hash now materially empty

	if n, _ := s.HLen("h"); n != 0 {
		t.Fatalf("HLen = %d, want 0", n)
	}
	s.HSetLWW("h", 150, 0, 1, "a", "stale") // older than tombstone
	if _, ok, _ := s.HGet("h", "a"); ok {
		t.Fatal("stale set resurrected 'a' after empty — tombstone not retained")
	}
	s.HSetLWW("h", 300, 0, 1, "a", "fresh") // newer than tombstone
	if v, ok, _ := s.HGet("h", "a"); !ok || v != "fresh" {
		t.Fatalf("newer set after tombstone: (%q,%v), want (fresh,true)", v, ok)
	}
}
