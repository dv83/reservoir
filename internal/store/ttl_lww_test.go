package store

import (
	"testing"
	"time"
)

func newTTLStore(t *testing.T) *LockFreeStore {
	t.Helper()
	s := NewStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	t.Cleanup(s.Stop)
	return s
}

func future() int64 { return time.Now().Add(time.Hour).UnixNano() }

// TestExpireLWWBeforeSetStillApplies: an EXPIRE issued after a SET but delivered
// before it (key absent) is remembered and applied once the SET lands — both
// delivery orders end with the TTL.
func TestExpireLWWBeforeSetStillApplies(t *testing.T) {
	d := future()

	inOrder := newTTLStore(t)
	inOrder.SetLWW("k", "v", 100, 0, 1)
	inOrder.ExpireLWW("k", d, 200, 0, 1)

	reversed := newTTLStore(t)
	reversed.ExpireLWW("k", d, 200, 0, 1) // arrives first, key absent
	reversed.SetLWW("k", "v", 100, 0, 1)

	for name, s := range map[string]*LockFreeStore{"in-order": inOrder, "reversed": reversed} {
		if _, ok := s.GetTTL("k"); !ok {
			t.Errorf("%s: TTL lost — EXPIRE@200 must survive the older SET@100", name)
		}
	}
}

// TestSetNewerThanExpireClearsTTL: a SET written after an EXPIRE clears the TTL
// (Redis semantics) in either delivery order, and the stale EXPIRE is dropped.
func TestSetNewerThanExpireClearsTTL(t *testing.T) {
	d := future()

	inOrder := newTTLStore(t)
	inOrder.SetLWW("k", "v1", 100, 0, 1)
	inOrder.ExpireLWW("k", d, 200, 0, 1)
	inOrder.SetLWW("k", "v2", 300, 0, 1)

	reversed := newTTLStore(t)
	reversed.SetLWW("k", "v2", 300, 0, 1)
	if reversed.ExpireLWW("k", d, 200, 0, 1) {
		t.Fatal("EXPIRE@200 applied over a value written at 300")
	}
	reversed.SetLWW("k", "v1", 100, 0, 1)

	for name, s := range map[string]*LockFreeStore{"in-order": inOrder, "reversed": reversed} {
		if _, ok := s.GetTTL("k"); ok {
			t.Errorf("%s: TTL survived a newer SET", name)
		}
		if v, _ := s.Get("k"); v != "v2" {
			t.Errorf("%s: value = %q, want v2", name, v)
		}
	}
}

// TestExpireVsPersistConverges: concurrent EXPIRE and PERSIST resolve by stamp,
// identically in both delivery orders.
func TestExpireVsPersistConverges(t *testing.T) {
	d := future()
	cases := []struct {
		name      string
		expireAt  int64
		persistAt int64
		wantTTL   bool
	}{
		{"persist newer", 200, 300, false},
		{"expire newer", 300, 200, true},
	}
	for _, c := range cases {
		a := newTTLStore(t)
		a.SetLWW("k", "v", 100, 0, 1)
		a.ExpireLWW("k", d, c.expireAt, 0, 1)
		a.ExpireLWW("k", 0, c.persistAt, 0, 2)

		b := newTTLStore(t)
		b.SetLWW("k", "v", 100, 0, 1)
		b.ExpireLWW("k", 0, c.persistAt, 0, 2)
		b.ExpireLWW("k", d, c.expireAt, 0, 1)

		_, ta := a.GetTTL("k")
		_, tb := b.GetTTL("k")
		if ta != tb {
			t.Fatalf("%s: diverged: a hasTTL=%v b hasTTL=%v", c.name, ta, tb)
		}
		if ta != c.wantTTL {
			t.Fatalf("%s: hasTTL=%v, want %v", c.name, ta, c.wantTTL)
		}
	}
}

// TestExpiryTombstoneBlocksResurrection: once an LWW value is reaped by expiry, a
// peer handing the same value back (anti-entropy) must not resurrect it, while a
// genuinely newer write still wins.
func TestExpiryTombstoneBlocksResurrection(t *testing.T) {
	s := newTTLStore(t)
	s.SetLWW("k", "v", 100, 0, 1)
	s.ExpireLWW("k", time.Now().Add(-time.Second).UnixNano(), 200, 0, 1) // already past

	shard := s.getShard("k")
	s.expiryBatch.deleteExpiredDirect(shard, "k") // reap

	if _, ok := s.Get("k"); ok {
		t.Fatal("expired key still present after reaping")
	}
	// Anti-entropy from a peer that has not reaped yet re-sends value@100.
	if applied, _ := s.SetLWW("k", "v", 100, 0, 1); applied {
		t.Fatal("expired value resurrected by a re-sent older write")
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("expired key resurrected")
	}
	// A newer write is a legitimate new value.
	if applied, _ := s.SetLWW("k", "new", 300, 0, 1); !applied {
		t.Fatal("newer write after expiry was blocked")
	}
	if v, _ := s.Get("k"); v != "new" {
		t.Fatalf("value = %q, want new", v)
	}
}

// TestDigestCarriesTTL: the shard digest includes the TTL register so a peer
// that missed an EXPIRE learns it via anti-entropy.
func TestDigestCarriesTTL(t *testing.T) {
	s := newTTLStore(t)
	d := future()
	s.SetLWW("k", "v", 100, 0, 1)
	s.ExpireLWW("k", d, 200, 0, 1)

	idx := int(FastHash("k") & s.shardMask)
	found := false
	for _, e := range s.ShardLWWDigest(idx) {
		if e.Key == "k" && e.TTL {
			found = true
			if e.Deadline != d || e.Physical != 200 {
				t.Fatalf("TTL entry = deadline %d stamp %d, want %d / 200", e.Deadline, e.Physical, d)
			}
		}
	}
	if !found {
		t.Fatal("digest has no TTL entry for k")
	}
}
