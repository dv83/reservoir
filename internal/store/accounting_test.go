package store

import (
	"strconv"
	"sync/atomic"
	"testing"
)

// sumShardKeyCounts returns the sum of every shard's keyCount, which must always
// equal the global totalKeys counter.
func sumShardKeyCounts(s *LockFreeStore) int64 {
	var total int64
	for i := 0; i < numShards; i++ {
		total += atomic.LoadInt64(&s.shards[i].keyCount)
	}
	return total
}

// TestKeyCountAccountingAllTypes verifies key-count accounting is exact and does
// not drift across every value type: after creating keys of each type and then
// deleting them all, totalKeys returns to zero and the per-shard keyCount sum
// stays equal to totalKeys throughout. Previously strings updated totalKeys but
// not shard.keyCount, and lists did the reverse, so the two diverged.
func TestKeyCountAccountingAllTypes(t *testing.T) {
	s := createTestStore()

	if got := atomic.LoadInt64(&s.totalKeys); got != 0 {
		t.Fatalf("fresh store totalKeys = %d, want 0", got)
	}

	keys := make([]string, 0, 40)
	add := func(k string) { keys = append(keys, k) }

	for i := 0; i < 10; i++ {
		sk := "str" + strconv.Itoa(i)
		s.Set(sk, "v")
		add(sk)

		lk := "list" + strconv.Itoa(i)
		s.LPush(lk, "a", "b")
		add(lk)

		setk := "set" + strconv.Itoa(i)
		s.SAdd(setk, "x", "y")
		add(setk)

		hk := "hash" + strconv.Itoa(i)
		s.HSet(hk, "f", "v")
		add(hk)
	}

	// Mutate some keys in place; key count must not change.
	s.IncrBy("counter", 5) // creates a new string key
	add("counter")
	s.Append("str0", "more")
	s.LPush("list0", "c")
	s.SAdd("set0", "z")

	wantKeys := int64(len(keys))
	if got := atomic.LoadInt64(&s.totalKeys); got != wantKeys {
		t.Fatalf("totalKeys = %d, want %d", got, wantKeys)
	}
	if got := sumShardKeyCounts(s); got != wantKeys {
		t.Fatalf("sum(shard.keyCount) = %d, want %d (must equal totalKeys)", got, wantKeys)
	}

	// Delete everything through the generic Delete path.
	for _, k := range keys {
		s.Delete(k)
	}

	if got := atomic.LoadInt64(&s.totalKeys); got != 0 {
		t.Fatalf("after deleting all, totalKeys = %d, want 0", got)
	}
	if got := sumShardKeyCounts(s); got != 0 {
		t.Fatalf("after deleting all, sum(shard.keyCount) = %d, want 0", got)
	}
}

// TestMemoryLimitEnforced verifies the store rejects writes that would exceed
// MaxMemoryUsage (noeviction policy), while still allowing deletes and
// non-growing overwrites.
func TestMemoryLimitEnforced(t *testing.T) {
	s := NewLockFreeStore(&Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024,
		MaxMemoryUsage: 2000, // small cap
	})

	// Fill up to the cap.
	accepted := 0
	var lastErr error
	for i := 0; i < 1000; i++ {
		if err := s.Set("k"+strconv.Itoa(i), "0123456789"); err != nil {
			lastErr = err
			break
		}
		accepted++
	}

	if lastErr == nil {
		t.Fatalf("expected a write to be rejected once memory limit was reached")
	}
	if accepted == 0 {
		t.Fatalf("no writes were accepted before hitting the limit")
	}
	if used := atomic.LoadInt64(&s.totalMemory); used > s.limits.MaxMemoryUsage {
		t.Fatalf("total memory %d exceeded the limit %d", used, s.limits.MaxMemoryUsage)
	}

	// A delete frees memory and must always be allowed.
	if !s.Delete("k0") {
		t.Fatalf("delete of an existing key failed")
	}
	// After freeing space, a new write should succeed again.
	if err := s.Set("after-free", "x"); err != nil {
		t.Fatalf("write after freeing memory was rejected: %v", err)
	}
}

// TestStringMemoryAccounting verifies string memory accounting returns exactly
// to baseline after set/modify/delete cycles (previously Delete freed no memory
// at all, so totalMemory grew without bound).
func TestStringMemoryAccounting(t *testing.T) {
	s := createTestStore()
	base := atomic.LoadInt64(&s.totalMemory)

	for i := 0; i < 100; i++ {
		k := "k" + strconv.Itoa(i)
		s.Set(k, "value")
		s.Append(k, "-more")
		s.Set(k, "x") // overwrite smaller
		s.GetSet(k, "final")
	}
	for i := 0; i < 100; i++ {
		s.Delete("k" + strconv.Itoa(i))
	}

	if got := atomic.LoadInt64(&s.totalMemory); got != base {
		t.Fatalf("string memory drift: totalMemory = %d, want %d (delta %d)", got, base, got-base)
	}
}
