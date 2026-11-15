package store

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// checkContainerBytes fails if any set's or hash's incremental byte counts
// differ from a full recount.
func checkContainerBytes(t *testing.T, s *LockFreeStore, when string) {
	t.Helper()
	for _, shard := range s.shards {
		shard.mu.RLock()
		for key, sv := range shard.data {
			if v := sv.SetVal; v != nil {
				elems, crdt := v.recountBytes()
				var have int64
				if v.crdt != nil {
					have = v.crdt.bytes
				}
				if v.bytes != elems || have != crdt {
					t.Fatalf("%s: set %s bytes %d/%d, recount %d/%d", when, key, v.bytes, have, elems, crdt)
				}
			}
			if v := sv.HashVal; v != nil {
				fields, crdt := v.recountBytes()
				var have int64
				if v.crdt != nil {
					have = v.crdt.bytes
				}
				if v.bytes != fields || have != crdt {
					t.Fatalf("%s: hash %s bytes %d/%d, recount %d/%d", when, key, v.bytes, have, fields, crdt)
				}
			}
		}
		shard.mu.RUnlock()
	}
}

// TestContainerBytesMatchRecount: the O(1) set and hash sizes stay equal to a
// full recount through every kind of write, reset, tombstone GC and a codec
// round trip.
func TestContainerBytesMatchRecount(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for seed := int64(1); seed <= 20; seed++ {
			s := createTestStore()
			if cluster {
				s.SetLocalOrigin(7)
			}
			rng := rand.New(rand.NewSource(seed))
			randomWorkload(s, rng, 400)
			for i := 0; i < 50; i++ {
				n := rng.Intn(4)
				s.SPop(fmt.Sprintf("set%d", n))
				s.HDel(fmt.Sprintf("h%d", n), fmt.Sprintf("m%d", rng.Intn(6)))
				s.HIncrBy(fmt.Sprintf("h%d", n), "ctr", int64(rng.Intn(1000)))
				s.SRem(fmt.Sprintf("set%d", n), fmt.Sprintf("m%d", rng.Intn(6)))
			}
			checkContainerBytes(t, s, fmt.Sprintf("cluster=%v seed=%d after writes", cluster, seed))
			s.gcTombstones(-time.Hour) // every tombstone is old enough
			checkContainerBytes(t, s, fmt.Sprintf("cluster=%v seed=%d after GC", cluster, seed))
			for _, shard := range s.shards {
				for key, sv := range shard.data {
					enc, err := encodeValue(sv)
					if err != nil {
						t.Fatal(err)
					}
					dec, err := decodeValue(enc)
					if err != nil {
						t.Fatal(err)
					}
					if dec.MemoryUsage() != sv.MemoryUsage() {
						t.Fatalf("%s: decoded size %d, original %d", key, dec.MemoryUsage(), sv.MemoryUsage())
					}
				}
			}
			s.FlushLWW(time.Now().UnixNano(), 0, 1)
			checkContainerBytes(t, s, fmt.Sprintf("cluster=%v seed=%d after flush", cluster, seed))
			s.Stop()
		}
	}
}

// TestSAddCostIndependentOfSetSize: adding to a large set costs the same as
// adding to a small one (it used to walk the whole set on every write).
func TestSAddCostIndependentOfSetSize(t *testing.T) {
	if lsmTestDir != "" {
		// The LSM record holds the whole container, so a write there is still
		// O(size) (add-lsm-storage, task 5.3); this checks the in-memory accounting.
		t.Skip("LSM mode re-encodes the whole container on every write")
	}
	s := createTestStore()
	defer s.Stop()
	timeAdds := func(key string, from int) time.Duration {
		start := time.Now()
		for i := from; i < from+2000; i++ {
			s.SAdd(key, fmt.Sprintf("member-%d", i))
			s.HSet(key+"h", fmt.Sprintf("field-%d", i), "v")
		}
		return time.Since(start)
	}
	small := timeAdds("small", 0)
	for i := 0; i < 200000; i += 1000 {
		batch := make([]string, 1000)
		for j := range batch {
			batch[j] = fmt.Sprintf("member-%d", i+j)
			s.HSet("largeh", batch[j], "v")
		}
		s.SAdd("large", batch...)
	}
	large := timeAdds("large", 200000)
	if large > 20*small+50*time.Millisecond {
		t.Fatalf("2000 adds to a 200K-element set took %v, to a small set %v", large, small)
	}
}
