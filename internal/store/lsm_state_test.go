package store

import (
	"fmt"
	"testing"
	"time"

	"reservoir/internal/lsmengine"
)

// evictAll drops every clean entry from the cache, clearing reference bits
// first, so later reads must come from the engine.
func evictAll(s *LockFreeStore) {
	for pass := 0; pass < 2; pass++ {
		for _, sh := range s.shards {
			sh.mu.Lock()
			s.evictShard(sh, 1<<62)
			sh.mu.Unlock()
		}
	}
}

func openLSMAt(t *testing.T, dir string) *LockFreeStore {
	t.Helper()
	s := NewLockFreeStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 30})
	if err := s.EnableLSM(dir, lsmengine.Options{}); err != nil {
		t.Fatal(err)
	}
	s.lsmCheck = checkShardAgainstEngine
	return s
}

// checkSideState asserts what must survive eviction and restart: values, TTLs,
// expiry, tombstones and TTL registers.
func checkSideState(t *testing.T, s *LockFreeStore, when string) {
	t.Helper()
	if v, ok := s.Get("plain"); !ok || v != "v" {
		t.Errorf("%s: plain = (%q, %v)", when, v, ok)
	}
	if ttl, ok := s.GetTTL("ttl"); !ok || ttl < 59*time.Minute {
		t.Errorf("%s: ttl key lost its TTL: %v %v", when, ttl, ok)
	}
	if _, ok := s.Get("expired"); ok {
		t.Errorf("%s: expired key served", when)
	}
	if s.Exists("expired") != 0 {
		t.Errorf("%s: expired key exists", when)
	}
	// A stale write after the delete is rejected by the persisted tombstone.
	if applied, _ := s.SetLWW("deleted", "stale", 100, 0, 1); applied {
		t.Errorf("%s: stale write resurrected a deleted key", when)
	}
	if applied, _ := s.SetLWW("deleted", "new", 900, 0, 1); !applied {
		t.Errorf("%s: newer write after the delete was rejected", when)
	}
	// The TTL register recorded for an absent key still applies when the
	// write it follows arrives.
	if _, err := s.SetLWW("reg", "v", 300, 0, 1); err != nil {
		t.Fatal(err)
	}
	if ttl, ok := s.GetTTL("reg"); !ok || ttl < 59*time.Minute {
		t.Errorf("%s: TTL register for reg was lost: %v %v", when, ttl, ok)
	}
}

func writeSideState(t *testing.T, s *LockFreeStore) {
	t.Helper()
	s.SetLocalOrigin(7)
	s.Set("plain", "v")
	s.Set("ttl", "v")
	s.SetExpiry("ttl", time.Now().Add(time.Hour))
	s.Set("expired", "v")
	s.SetExpiry("expired", time.Now().Add(50*time.Millisecond))
	s.SetLWW("deleted", "old", 200, 0, 1)
	s.DeleteLWW("deleted", 500, 0, 1)
	// EXPIRE stamped after a SET that has not arrived yet.
	s.ExpireLWW("reg", time.Now().Add(time.Hour).UnixNano(), 400, 0, 1)
}

// TestLSMSideStateSurvivesEviction (add-lsm-storage, task 3.1).
func TestLSMSideStateSurvivesEviction(t *testing.T) {
	s := openLSMAt(t, t.TempDir())
	defer s.Stop()
	writeSideState(t, s)
	time.Sleep(80 * time.Millisecond)
	evictAll(s)
	checkSideState(t, s, "after eviction")
}

// TestLSMSideStateSurvivesRestart (add-lsm-storage, task 3.1): a store
// reopened on the same engine keeps values, TTLs, tombstones and TTL
// registers — no log replay involved.
func TestLSMSideStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s := openLSMAt(t, dir)
	writeSideState(t, s)
	s.Stop()
	time.Sleep(80 * time.Millisecond)

	s2 := openLSMAt(t, dir)
	defer s2.Stop()
	if n := s2.GetKeyCount(); n != 3 {
		// plain, ttl, expired (not yet reclaimed): deleted and reg have no value.
		t.Errorf("key count after restart = %d, want 3", n)
	}
	checkSideState(t, s2, "after restart")
	if n := s2.GetKeyCount(); n != 4 {
		// expired was reclaimed on access; deleted and reg now hold values.
		t.Errorf("key count after accesses = %d, want 4", n)
	}
}

// TestLSMFlushStampSurvivesRestart: after a stamped FLUSHALL and a restart,
// stale re-deliveries of flushed data are still rejected.
func TestLSMFlushStampSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	s := openLSMAt(t, dir)
	s.SetLocalOrigin(7)
	s.SetLWW("k", "old", 100, 0, 1)
	s.FlushLWW(500, 0, 1)
	s.Stop()

	s2 := openLSMAt(t, dir)
	defer s2.Stop()
	if p, _, _, ok := s2.FlushStampLWW(); !ok || p != 500 {
		t.Fatalf("flush stamp after restart = %d (%v), want 500", p, ok)
	}
	if applied, _ := s2.SetLWW("k", "old", 100, 0, 1); applied {
		t.Fatal("stale write before the flush was accepted after a restart")
	}
	if applied, _ := s2.SetLWW("k", "new", 600, 0, 1); !applied {
		t.Fatal("write after the flush was rejected")
	}
}

// TestLSMExpiryIndexReapsColdKeys (add-lsm-storage, task 3.2): expired keys
// that are not in the cache are reclaimed from the expiry index without being
// accessed, while a key whose TTL was extended survives its stale entry.
func TestLSMExpiryIndexReapsColdKeys(t *testing.T) {
	s := openLSMAt(t, t.TempDir())
	defer s.Stop()
	soon := time.Now().Add(500 * time.Millisecond)
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("short%d", i)
		s.Set(k, "v")
		s.SetExpiry(k, soon)
		if i == 0 {
			// Extended right away (before it can expire, even under -race):
			// its first index entry is now stale.
			s.SetExpiry(k, time.Now().Add(time.Hour))
		}
		s.Set(fmt.Sprintf("keep%d", i), "v")
	}
	evictAll(s)
	if n := s.GetKeyCount(); n != 1000 {
		t.Fatalf("key count before reaping = %d, want 1000", n)
	}

	if ttl, ok := s.GetTTL("short0"); !ok || ttl < 59*time.Minute {
		t.Fatalf("extended key lost its TTL before reaping: %v %v", ttl, ok)
	}
	time.Sleep(time.Until(soon) + 20*time.Millisecond) // let the short TTLs pass
	n := s.reapExpiryIndex(time.Now(), 1<<20)
	if n < 500 {
		t.Fatalf("reaper handled %d index entries, want at least 500", n)
	}
	if got := s.GetKeyCount(); got != 501 {
		t.Fatalf("key count after reaping = %d, want 501 (500 kept + the extended one)", got)
	}
	if ttl, ok := s.GetTTL("short0"); !ok || ttl < 59*time.Minute {
		t.Fatalf("extended key lost its TTL: %v %v", ttl, ok)
	}
	if _, ok := s.Get("short1"); ok {
		t.Fatal("reaped key still readable")
	}
	// The reaped keys' records hold no value any more.
	values := 0
	for _, sh := range s.shards {
		s.engine.ScanPrefix([]byte{lsmKindValue, sh.idx}, func(_, v []byte) bool {
			if len(v) > 1 && v[1]&recValue != 0 {
				values++
			}
			return true
		})
	}
	if values != 501 {
		t.Fatalf("%d records still hold a value, want 501", values)
	}
	// Processed entries are gone: a second pass finds only the extended key's
	// live entry, which is not due.
	if n := s.reapExpiryIndex(time.Now(), 1<<20); n != 0 {
		t.Fatalf("second pass handled %d entries, want 0", n)
	}
}
