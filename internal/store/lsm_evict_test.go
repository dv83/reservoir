package store

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"reservoir/internal/lsmengine"
)

func newLSMStore(t *testing.T, budget int64) *LockFreeStore {
	t.Helper()
	s := NewLockFreeStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: budget})
	if err := s.EnableLSM(t.TempDir(), lsmengine.Options{CacheBytes: 4 << 20}); err != nil {
		t.Fatal(err)
	}
	s.lsmCheck = checkShardAgainstEngine
	t.Cleanup(s.Stop)
	return s
}

func cachedKeys(s *LockFreeStore) int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += len(sh.data)
		sh.mu.RUnlock()
	}
	return n
}

// TestLSMEvictsAtBudget (add-lsm-storage, task 2.3): with a cache far smaller
// than the data, every write succeeds, memory stays near the budget, the cache
// holds a fraction of the keys, and every key — of every type — reads back
// with its value and TTL after being evicted.
func TestLSMEvictsAtBudget(t *testing.T) {
	const budget, n = 128 << 10, 8000
	s := newLSMStore(t, budget)
	deadline := time.Now().Add(time.Hour)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%d", i)
		if err := s.Set(k, strings.Repeat("v", 40)+fmt.Sprint(i)); err != nil {
			t.Fatalf("Set %s: %v", k, err)
		}
		if i%100 == 0 {
			s.SetExpiry(k, deadline)
		}
	}
	for i := 0; i < 200; i++ {
		s.SAdd(fmt.Sprintf("set%d", i), "a", "b", fmt.Sprint(i))
		s.HSet(fmt.Sprintf("h%d", i), "f", fmt.Sprint(i))
		s.RPush(fmt.Sprintf("l%d", i), "x", fmt.Sprint(i))
	}
	if used := s.GetMemoryUsage(); used > budget*12/10 {
		t.Errorf("cache memory %d, budget %d", used, budget)
	}
	if c := cachedKeys(s); c > n/2 {
		t.Errorf("%d of %d keys still cached: nothing was evicted", c, n)
	}
	if s.evictions == 0 {
		t.Fatal("no evictions")
	}
	if got := s.GetKeyCount(); got < n {
		t.Errorf("key count %d, want at least %d (counts cover evicted keys)", got, n)
	}

	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%d", i)
		if v, ok := s.Get(k); !ok || v != strings.Repeat("v", 40)+fmt.Sprint(i) {
			t.Fatalf("%s after eviction = (%q, %v)", k, v, ok)
		}
		if i%100 == 0 {
			if ttl, ok := s.GetTTL(k); !ok || ttl < 59*time.Minute {
				t.Fatalf("%s lost its TTL: %v %v", k, ttl, ok)
			}
		}
	}
	for i := 0; i < 200; i++ {
		if c, _ := s.SCard(fmt.Sprintf("set%d", i)); c != 3 {
			t.Fatalf("set%d card %d after eviction", i, c)
		}
		if v, ok, _ := s.HGet(fmt.Sprintf("h%d", i), "f"); !ok || v != fmt.Sprint(i) {
			t.Fatalf("h%d after eviction = (%q, %v)", i, v, ok)
		}
		if r, _ := s.LRange(fmt.Sprintf("l%d", i), 0, -1); fmt.Sprint(r) != fmt.Sprintf("[x %d]", i) {
			t.Fatalf("l%d after eviction = %v", i, r)
		}
	}
	// Writes and deletes of evicted keys reach the engine.
	s.Delete("k1")
	s.Set("k2", "new")
	s.SAdd("set3", "z")
	if _, ok := s.Get("k1"); ok {
		t.Fatal("deleted evicted key is back")
	}
	if v, _ := s.Get("k2"); v != "new" {
		t.Fatalf("k2 = %q", v)
	}
	if c, _ := s.SCard("set3"); c != 4 {
		t.Fatalf("set3 card %d after adding to an evicted set", c)
	}
}

// TestLSMReadKeepsKeyCached: a key that keeps being read survives eviction
// passes (CLOCK second chance), while unread keys go.
func TestLSMReadKeepsKeyCached(t *testing.T) {
	s := newLSMStore(t, 1<<30)
	for i := 0; i < 200; i++ {
		s.Set(fmt.Sprintf("k%d", i), "value")
	}
	s.Get("k7")
	for _, sh := range s.shards {
		sh.mu.Lock()
		s.evictShard(sh, 1<<40)
		sh.mu.Unlock()
	}
	sh := s.getShard("k7")
	sh.mu.RLock()
	_, cached := sh.data["k7"]
	sh.mu.RUnlock()
	if !cached {
		t.Fatal("recently read key was evicted on the first pass")
	}
	if c := cachedKeys(s); c != 1 {
		t.Fatalf("%d keys cached after a full pass, want only the read one", c)
	}
}

// TestLSMConcurrentWithEviction: concurrent writers and readers with a tiny
// cache under -race; every key ends with its last value.
func TestLSMConcurrentWithEviction(t *testing.T) {
	s := newLSMStore(t, 64<<10)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 3000; i++ {
				k := fmt.Sprintf("w%d-%d", w, i%500)
				s.Set(k, fmt.Sprintf("%d-%s", i, strings.Repeat("p", 30)))
				s.Get(fmt.Sprintf("w%d-%d", w, (i*7)%500))
				if i%50 == 0 {
					s.SAdd(fmt.Sprintf("ws%d", w), fmt.Sprint(i))
				}
			}
		}(w)
	}
	wg.Wait()
	for w := 0; w < 4; w++ {
		for i := 2500; i < 3000; i++ {
			k := fmt.Sprintf("w%d-%d", w, i%500)
			if v, _ := s.Get(k); v != fmt.Sprintf("%d-%s", i, strings.Repeat("p", 30)) {
				t.Fatalf("%s = %q", k, v)
			}
		}
		if c, _ := s.SCard(fmt.Sprintf("ws%d", w)); c != 60 {
			t.Fatalf("ws%d card %d, want 60", w, c)
		}
	}
}

// TestLSMStaleWritesDoNotGrowCache: replicated or anti-entropy writes that
// load an evicted key and are then dropped as stale must not grow the cache
// past its budget (they never reach the write-path budget check).
func TestLSMStaleWritesDoNotGrowCache(t *testing.T) {
	const budget = 128 << 10
	s := newLSMStore(t, budget)
	s.SetLocalOrigin(5)
	for i := 0; i < 5000; i++ {
		s.SetLWW(fmt.Sprintf("k%d", i), strings.Repeat("v", 40), 1000, 0, 1)
	}
	for i := 0; i < 5000; i++ { // anti-entropy re-sending everything, all stale
		if applied, _ := s.SetLWW(fmt.Sprintf("k%d", i), "old", 500, 0, 1); applied {
			t.Fatal("stale write applied")
		}
	}
	if used := s.GetMemoryUsage(); used > budget*11/10 {
		t.Fatalf("cache usage %d after stale writes, budget %d", used, budget)
	}
}
