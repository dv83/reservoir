package store

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// normalize returns a comparable picture of everything a StoredValue's
// behavior depends on (the encoded state), independent of map iteration order,
// deque layout and nil-versus-empty maps.
func normalize(sv *StoredValue) map[string]interface{} { return normalizeOpt(sv, true) }

// normalizeOpt can leave out list tombstone times: they record the local wall
// clock when an element was deleted, so two stores running the same operations
// at different moments differ there and nowhere else.
func normalizeOpt(sv *StoredValue, withDelTime bool) map[string]interface{} {
	out := map[string]interface{}{
		"type": sv.Type, "hlc": sv.hlc, "mem": sv.MemoryUsage(), "str": sv.StringVal,
	}
	if c := sv.counter; c != nil {
		out["ctr"] = fmt.Sprint(c.Base, c.gen, sortedSlots(c.P), sortedSlots(c.N))
	}
	if s := sv.SetVal; s != nil {
		out["elems"] = sortedKeys(s.Elements)
		out["size"] = s.totalSize
		if s.crdt != nil {
			out["adds"] = sortedStamps(s.crdt.adds)
			out["removes"] = sortedStamps(s.crdt.removes)
		}
	}
	if h := sv.HashVal; h != nil {
		out["fields"] = fmt.Sprint(sortedMap(h.Fields))
		if h.crdt != nil {
			var fs []string
			for f, fl := range h.crdt.fields {
				fs = append(fs, fmt.Sprint(f, fl.stamp, fl.deleted, fl.value))
			}
			sort.Strings(fs)
			out["hcrdt"] = fs
		}
	}
	if l := sv.ListVal; l != nil {
		out["list"] = l.deque.ToSlice()
		if l.rga != nil {
			var es []string
			for _, e := range l.rga.elems {
				dt := e.delTime
				if !withDelTime && dt != 0 {
					dt = 1
				}
				es = append(es, fmt.Sprint(e.id, e.pos, e.value, e.valStamp, e.deleted, dt))
			}
			sort.Strings(es)
			out["rga"] = es
			out["gen"] = l.gen
		}
	}
	return out
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedStamps(m map[string]hlcStamp) []string {
	var out []string
	for k, v := range m {
		out = append(out, fmt.Sprint(k, v))
	}
	sort.Strings(out)
	return out
}

func sortedSlots(m map[uint64]int64) []string {
	var out []string
	for k, v := range m {
		out = append(out, fmt.Sprint(k, v))
	}
	sort.Strings(out)
	return out
}

func sortedMap(m map[string]string) []string {
	var out []string
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

// randomWorkload drives a store through a random mix of the operations that
// shape stored values, on a handful of keys per type.
func randomWorkload(s *LockFreeStore, rng *rand.Rand, ops int) {
	ts := int64(1000)
	stamp := func() (int64, uint32, uint64) {
		ts += int64(rng.Intn(3))
		return ts, uint32(rng.Intn(3)), uint64(1 + rng.Intn(3))
	}
	m := func() string { return fmt.Sprintf("m%d", rng.Intn(6)) }
	for i := 0; i < ops; i++ {
		n := rng.Intn(4)
		switch rng.Intn(16) {
		case 0:
			s.Set(fmt.Sprintf("s%d", n), fmt.Sprintf("v%d", rng.Int()))
		case 1:
			p, l, o := stamp()
			s.SetLWW(fmt.Sprintf("s%d", n), "lww", p, l, o)
		case 2:
			s.IncrBy(fmt.Sprintf("c%d", n), int64(rng.Intn(20)-10))
		case 3:
			s.SAdd(fmt.Sprintf("set%d", n), m(), m())
		case 4:
			p, l, o := stamp()
			s.SAddLWW(fmt.Sprintf("set%d", n), p, l, o, m())
		case 5:
			p, l, o := stamp()
			s.SRemLWW(fmt.Sprintf("set%d", n), p, l, o, m())
		case 6:
			s.HSet(fmt.Sprintf("h%d", n), m(), fmt.Sprint(rng.Int()))
		case 7:
			p, l, o := stamp()
			s.HSetLWW(fmt.Sprintf("h%d", n), p, l, o, m(), "x")
		case 8:
			p, l, o := stamp()
			s.HDelLWW(fmt.Sprintf("h%d", n), p, l, o, m())
		case 9:
			s.RPush(fmt.Sprintf("l%d", n), m(), m())
		case 10:
			s.LPush(fmt.Sprintf("l%d", n), m())
		case 11:
			s.LPop(fmt.Sprintf("l%d", n), 1)
		case 12:
			s.LSet(fmt.Sprintf("l%d", n), 0, m())
		case 13:
			s.LRem(fmt.Sprintf("l%d", n), 1, m())
		case 14:
			s.Append(fmt.Sprintf("s%d", n), "+")
		default:
			p, l, o := stamp()
			s.DeleteLWW(fmt.Sprintf("set%d", n), p, l, o)
		}
	}
}

// TestCodecRoundTrip (add-lsm-storage, task 1.2): every value the store builds
// — plain and CRDT, every type — decodes to the same state it was encoded from.
func TestCodecRoundTrip(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for seed := int64(1); seed <= 40; seed++ {
			s := createTestStore()
			if cluster {
				s.SetLocalOrigin(7)
			}
			randomWorkload(s, rand.New(rand.NewSource(seed)), 300)
			checked := 0
			for _, shard := range s.shards {
				shard.mu.Lock()
				for key, sv := range shard.data {
					b, err := encodeValue(sv)
					if err != nil {
						t.Fatalf("encode %s: %v", key, err)
					}
					got, err := decodeValue(b)
					if err != nil {
						t.Fatalf("decode %s: %v", key, err)
					}
					if want, have := normalize(sv), normalize(got); !reflect.DeepEqual(want, have) {
						t.Fatalf("cluster=%v seed=%d key %s round trip differs:\nwant %v\ngot  %v", cluster, seed, key, want, have)
					}
					checked++
				}
				shard.mu.Unlock()
			}
			if checked == 0 {
				t.Fatal("workload built no keys")
			}
			s.Stop()
		}
	}
}

// TestDecodedValueBehavesTheSame: after a round trip, further operations give
// the same replies and state as on the original.
func TestDecodedValueBehavesTheSame(t *testing.T) {
	a, b := createTestStore(), createTestStore()
	defer a.Stop()
	defer b.Stop()
	a.SetLocalOrigin(7)
	b.SetLocalOrigin(7)
	randomWorkload(a, rand.New(rand.NewSource(99)), 400)
	// Copy every key into b through the codec.
	for i, shard := range a.shards {
		shard.mu.Lock()
		for key, sv := range shard.data {
			enc, err := encodeValue(sv)
			if err != nil {
				t.Fatal(err)
			}
			dec, err := decodeValue(enc)
			if err != nil {
				t.Fatal(err)
			}
			b.shards[i].mu.Lock()
			b.shards[i].put(key, dec)
			b.shards[i].mu.Unlock()
		}
		// Side state the value codec does not cover (the LSM record does),
		// copied the way the store changes it: load, change, mark dirty.
		bs := b.shards[i]
		bs.mu.Lock()
		for k, v := range shard.tombstones {
			bs.ensure(k)
			bs.tombstones[k] = v
			bs.markDirty(k)
		}
		for k, v := range shard.ttlRecs {
			bs.ensure(k)
			bs.ttlRecs[k] = v
			bs.markDirty(k)
		}
		for k, v := range shard.expiry {
			bs.ensure(k)
			bs.expiry[k] = v
			bs.markDirty(k)
		}
		bs.mu.Unlock()
		shard.mu.Unlock()
	}
	b.listSeq = a.listSeq
	// The same continuation on both.
	randomWorkload(a, rand.New(rand.NewSource(100)), 400)
	randomWorkload(b, rand.New(rand.NewSource(100)), 400)
	for i := range a.shards {
		for key, sv := range a.shards[i].data {
			other, ok := b.shards[i].data[key]
			if !ok {
				t.Fatalf("%s missing in the decoded copy", key)
			}
			if want, have := normalizeOpt(sv, false), normalizeOpt(other, false); !reflect.DeepEqual(want, have) {
				t.Fatalf("%s diverged after a round trip:\nwant %v\ngot  %v", key, want, have)
			}
		}
		if len(a.shards[i].data) != len(b.shards[i].data) {
			t.Fatalf("shard %d key count differs", i)
		}
	}
}

// TestCodecRejectsCorruptInput: truncated or altered encodings fail cleanly,
// never panic.
func TestCodecRejectsCorruptInput(t *testing.T) {
	s := createTestStore()
	defer s.Stop()
	s.SetLocalOrigin(7)
	randomWorkload(s, rand.New(rand.NewSource(5)), 300)
	rng := rand.New(rand.NewSource(6))
	for _, shard := range s.shards {
		for _, sv := range shard.data {
			enc, err := encodeValue(sv)
			if err != nil {
				t.Fatal(err)
			}
			for cut := 0; cut < len(enc); cut++ {
				if _, err := decodeValue(enc[:cut]); err == nil {
					t.Fatalf("truncation to %d of %d bytes decoded without error", cut, len(enc))
				}
			}
			for i := 0; i < 50; i++ {
				bad := append([]byte(nil), enc...)
				bad[rng.Intn(len(bad))] ^= byte(1 + rng.Intn(255))
				decodeValue(bad) // must not panic; it may decode to other state
			}
		}
	}
	if _, err := decodeValue([]byte{9, 0, 0, 0}); err == nil {
		t.Fatal("unknown codec version accepted")
	}
}
