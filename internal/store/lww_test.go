package store

import (
	"testing"
	"time"

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

// TestDeleteWithHLCTombstoneBlocksStaleCreate verifies a delete leaves a
// tombstone so a later-arriving OLDER create cannot resurrect the key.
func TestDeleteWithHLCTombstoneBlocksStaleCreate(t *testing.T) {
	s := createTestStore()

	// A create at t=100, then a delete at t=200.
	if applied, _ := s.SetWithHLC("k", "v", stamp(100, 1)); !applied {
		t.Fatalf("create not applied")
	}
	if !s.DeleteWithHLC("k", stamp(200, 1)) {
		t.Fatalf("delete not applied")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatalf("key present after delete")
	}

	// A stale create at t=150 (older than the delete) must be rejected.
	if applied, _ := s.SetWithHLC("k", "resurrected", stamp(150, 1)); applied {
		t.Fatalf("stale create resurrected a deleted key")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatalf("deleted key came back")
	}

	// A create newer than the delete (t=300) does apply.
	if applied, _ := s.SetWithHLC("k", "recreated", stamp(300, 1)); !applied {
		t.Fatalf("newer create should apply over a tombstone")
	}
	if v, _ := s.Get("k"); v != "recreated" {
		t.Fatalf("value = %q, want recreated", v)
	}
}

// TestGCTombstones verifies old tombstones are reaped while recent ones survive.
func TestGCTombstones(t *testing.T) {
	s := createTestStore()
	s.SetWithHLC("old", "v", stamp(100, 1))
	s.DeleteWithHLC("old", stamp(200, 1))
	s.SetWithHLC("recent", "v", stamp(100, 1))
	s.DeleteWithHLC("recent", stamp(200, 1))

	// Backdate the "old" tombstone beyond the retention window.
	shard := s.shards[int(FastHash("old")&s.shardMask)]
	shard.mu.Lock()
	tomb := shard.tombstones["old"]
	tomb.deleted = tomb.deleted.Add(-2 * time.Hour)
	shard.tombstones["old"] = tomb
	shard.mu.Unlock()

	if removed := s.gcTombstones(time.Hour); removed != 1 {
		t.Fatalf("gcTombstones removed %d, want 1", removed)
	}

	// The reaped tombstone no longer blocks an older create; the recent one does.
	if applied, _ := s.SetWithHLC("old", "back", stamp(150, 1)); !applied {
		t.Errorf("create should succeed after its tombstone was GC'd")
	}
	if applied, _ := s.SetWithHLC("recent", "back", stamp(150, 1)); applied {
		t.Errorf("recent tombstone should still block a stale create")
	}
}

// TestDeleteSetConvergence verifies a concurrent set and delete converge to the
// same result regardless of arrival order on two replicas.
func TestDeleteSetConvergence(t *testing.T) {
	set := struct {
		st hlcStamp
	}{stamp(100, 1)}
	del := stamp(200, 2) // delete is newer, should win

	s1 := createTestStore()
	s1.SetWithHLC("k", "v", set.st)
	s1.DeleteWithHLC("k", del)

	s2 := createTestStore()
	s2.DeleteWithHLC("k", del) // reverse order: delete first
	s2.SetWithHLC("k", "v", set.st)

	_, ok1 := s1.Get("k")
	_, ok2 := s2.Get("k")
	if ok1 != ok2 {
		t.Fatalf("replicas diverged: s1 present=%v s2 present=%v", ok1, ok2)
	}
	if ok1 {
		t.Fatalf("newer delete should have won, but key is present")
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
