package store

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

// Stamps used below: t1 < tDel < t3. Origin 1 is a remote writer.
const (
	t1   = int64(100)
	tDel = int64(200)
	t3   = int64(300)
)

func membersOf(t *testing.T, s *LockFreeStore, key string) []string {
	t.Helper()
	m, _ := s.SMembers(key)
	sort.Strings(m)
	if m == nil {
		m = []string{}
	}
	return m
}

func listOf(s *LockFreeStore, key string) []string {
	r, _ := s.LRange(key, 0, -1)
	if r == nil {
		r = []string{}
	}
	return r
}

// TestDeleteNotUndoneByStaleWrite (task 1.1): once a key of any type is deleted,
// a delayed or re-sent write from before the delete must not bring it back,
// while a write from after the delete still applies.
func TestDeleteNotUndoneByStaleWrite(t *testing.T) {
	t.Run("set", func(t *testing.T) {
		s := createTestStore()
		defer s.Stop()
		s.SetLocalOrigin(7)
		s.SAddLWW("s", t1, 0, 1, "a")
		s.DeleteLWW("s", tDel, 0, 1)
		s.SAddLWW("s", t1, 0, 1, "a") // anti-entropy re-sends the old add
		if got := membersOf(t, s, "s"); len(got) != 0 {
			t.Fatalf("set after DEL + stale SADD = %v, want empty", got)
		}
		s.SAddLWW("s", t3, 0, 1, "b")
		if got := membersOf(t, s, "s"); !reflect.DeepEqual(got, []string{"b"}) {
			t.Fatalf("set after newer SADD = %v, want [b]", got)
		}
	})

	t.Run("set keeps members added after the delete", func(t *testing.T) {
		s := createTestStore()
		defer s.Stop()
		s.SetLocalOrigin(7)
		s.SAddLWW("s", t1, 0, 1, "old")
		s.SAddLWW("s", t3, 0, 1, "new") // written after the delete, arrived first
		s.DeleteLWW("s", tDel, 0, 1)
		if got := membersOf(t, s, "s"); !reflect.DeepEqual(got, []string{"new"}) {
			t.Fatalf("set after late DEL = %v, want [new]", got)
		}
	})

	t.Run("hash", func(t *testing.T) {
		s := createTestStore()
		defer s.Stop()
		s.SetLocalOrigin(7)
		s.HSetLWW("h", t1, 0, 1, "f", "v")
		s.HSetLWW("h", t3, 0, 1, "g", "w") // after the delete
		s.DeleteLWW("h", tDel, 0, 1)
		s.HSetLWW("h", t1, 0, 1, "f", "v") // stale re-send
		if _, ok, _ := s.HGet("h", "f"); ok {
			t.Fatal("stale field f came back after DEL")
		}
		if v, ok, _ := s.HGet("h", "g"); !ok || v != "w" {
			t.Fatalf("field g written after DEL = (%q, %v), want (w, true)", v, ok)
		}
	})

	t.Run("counter", func(t *testing.T) {
		s := createTestStore()
		defer s.Stop()
		s.SetLocalOrigin(7)
		s.Incr("c")
		stale, _ := s.CounterState("c")
		del := time.Now().Add(time.Hour).UnixNano() // after every local stamp
		s.DeleteLWW("c", del, 0, 1)
		s.MergeCounter("c", stale)
		if v, ok := s.Get("c"); ok {
			t.Fatalf("counter after DEL + stale merge = %q, want missing", v)
		}
		// Increments after the delete start a new counter and merge with
		// peers' increments of the same generation.
		if n, _ := s.Incr("c"); n != 1 {
			t.Fatalf("INCR after DEL = %d, want 1", n)
		}
		fresh, _ := s.CounterState("c")
		peer := CounterState{P: map[uint64]int64{9: 5}, GenP: fresh.GenP, GenL: fresh.GenL, GenO: fresh.GenO}
		if n, _ := s.MergeCounter("c", peer); n != 6 {
			t.Fatalf("merge of same-generation peer state = %d, want 6", n)
		}
	})

	t.Run("counter learns a newer delete from a peer", func(t *testing.T) {
		s := createTestStore()
		defer s.Stop()
		s.SetLocalOrigin(7)
		s.Incr("c") // generation 0: this node never saw the delete
		// A peer saw a delete at tDel and counted 2 after it.
		peer := CounterState{P: map[uint64]int64{9: 2}, GenP: tDel, GenO: 1}
		if n, _ := s.MergeCounter("c", peer); n != 2 {
			t.Fatalf("merge of newer generation = %d, want 2 (local pre-delete increment dropped)", n)
		}
		// The old generation is now stale here too.
		if n, _ := s.MergeCounter("c", CounterState{P: map[uint64]int64{7: 1}}); n != 2 {
			t.Fatalf("merge of older generation = %d, want 2", n)
		}
	})

	t.Run("list", func(t *testing.T) {
		s := createTestStore()
		defer s.Stop()
		s.SetLocalOrigin(7)
		s.RPush("l", "x")
		stale, _ := s.DrainListDelta("l")
		del := time.Now().Add(time.Hour).UnixNano()
		s.DeleteLWW("l", del, 0, 1)
		s.ApplyListDelta("l", stale)
		if got := listOf(s, "l"); len(got) != 0 {
			t.Fatalf("list after DEL + stale delta = %v, want empty", got)
		}
		s.RPush("l", "y")
		if got := listOf(s, "l"); !reflect.DeepEqual(got, []string{"y"}) {
			t.Fatalf("list after RPUSH following DEL = %v, want [y]", got)
		}
	})
}

// TestFlushIsAStampedReset (task 1.2): a stamped FLUSHALL removes every key
// written before it and blocks their stale re-delivery, but keeps — and still
// accepts — writes stamped after it.
func TestFlushIsAStampedReset(t *testing.T) {
	s := createTestStore()
	defer s.Stop()
	s.SetLocalOrigin(7)

	s.SetLWW("str", "old", t1, 0, 1)
	s.SAddLWW("s", t1, 0, 1, "a")
	s.HSetLWW("h", t1, 0, 1, "f", "v")
	s.Incr("c")
	ctr, _ := s.CounterState("c")
	s.RPush("l", "x")
	lst, _ := s.DrainListDelta("l")

	flush := time.Now().Add(time.Hour).UnixNano() // after every local stamp
	s.FlushLWW(flush, 0, 1)

	for _, k := range []string{"str", "s", "h", "c", "l"} {
		if s.Exists(k) != 0 {
			t.Errorf("%s survived the flush", k)
		}
	}

	// Stale re-delivery of everything that was flushed.
	s.SetLWW("str", "old", t1, 0, 1)
	s.SAddLWW("s", t1, 0, 1, "a")
	s.HSetLWW("h", t1, 0, 1, "f", "v")
	s.MergeCounter("c", ctr)
	s.ApplyListDelta("l", lst)
	for _, k := range []string{"str", "s", "h", "c", "l"} {
		if s.Exists(k) != 0 {
			t.Errorf("%s came back after the flush", k)
		}
	}

	// Writes stamped after the flush apply.
	s.SetLWW("str", "new", flush+1, 0, 1)
	s.SAddLWW("s", flush+1, 0, 1, "b")
	if v, _ := s.Get("str"); v != "new" {
		t.Errorf("SET after flush = %q, want new", v)
	}
	if got := membersOf(t, s, "s"); !reflect.DeepEqual(got, []string{"b"}) {
		t.Errorf("SADD after flush = %v, want [b]", got)
	}

	// A flush that arrives late keeps what was written after it.
	s.SetLWW("after", "kept", flush+10, 0, 1)
	s.FlushLWW(flush+5, 0, 1)
	if v, _ := s.Get("after"); v != "kept" {
		t.Errorf("write stamped after a late flush = %q, want kept", v)
	}

	// The flush stamp travels in every shard digest.
	if p, _, _, _ := s.FlushStampLWW(); p != flush+5 {
		t.Errorf("FlushStamp physical = %d, want %d", p, flush+5)
	}
}

// TestExpiredContainerLeavesTombstone (task 2.5 regression): reclaiming an
// expired container records a tombstone at its deadline, so writes from the
// expired incarnation cannot return while writes after the deadline apply.
func TestExpiredContainerLeavesTombstone(t *testing.T) {
	s := createTestStore()
	defer s.Stop()
	s.SetLocalOrigin(7)
	s.SAddLWW("s", t1, 0, 1, "a")
	s.SetExpiry("s", time.Now().Add(time.Hour))
	expireNow(t, s, "s")
	deadline := func() int64 {
		shard := s.getShard("s")
		shard.expiryMu.RLock()
		defer shard.expiryMu.RUnlock()
		return shard.expiry["s"].UnixNano()
	}()
	s.SCard("s") // reclaims

	s.SAddLWW("s", deadline-1, 0, 1, "late-old")
	if got := membersOf(t, s, "s"); len(got) != 0 {
		t.Fatalf("write from the expired incarnation came back: %v", got)
	}
	s.SAddLWW("s", deadline+1, 0, 1, "new")
	if got := membersOf(t, s, "s"); !reflect.DeepEqual(got, []string{"new"}) {
		t.Fatalf("write after the deadline = %v, want [new]", got)
	}
}
