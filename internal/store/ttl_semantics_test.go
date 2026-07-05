package store

import (
	"testing"
	"time"
)

// TestGetHonorsExpiry verifies that a read does not serve a key whose TTL has
// already elapsed, even before the background cleaner has removed it.
func TestGetHonorsExpiry(t *testing.T) {
	s := createTestStore()

	if err := s.SetWithExpiry("k", "v", 20*time.Millisecond); err != nil {
		t.Fatalf("SetWithExpiry: %v", err)
	}
	if v, ok := s.Get("k"); !ok || v != "v" {
		t.Fatalf("before expiry: got (%q,%v), want (\"v\",true)", v, ok)
	}

	time.Sleep(40 * time.Millisecond)

	if v, ok := s.Get("k"); ok {
		t.Fatalf("after expiry: Get returned (%q,%v), want not found", v, ok)
	}
	if n := s.Exists("k"); n != 0 {
		t.Fatalf("after expiry: Exists = %d, want 0", n)
	}
}

// TestGetWrongType verifies GET returns "not found" (so the handler emits
// WRONGTYPE) for a key that holds a non-string value, instead of an empty
// string that looks like a successful read.
func TestGetWrongType(t *testing.T) {
	s := createTestStore()

	if _, err := s.LPush("mylist", "a"); err != nil {
		t.Fatalf("LPush: %v", err)
	}
	if v, ok := s.Get("mylist"); ok {
		t.Fatalf("Get on list returned (%q,%v), want not found", v, ok)
	}
}

// TestSetClearsTTL verifies that a plain SET removes any previously attached
// TTL, so the key is not later reaped by the expiry cleaner.
func TestSetClearsTTL(t *testing.T) {
	s := createTestStore()

	if err := s.SetWithExpiry("k", "v1", 20*time.Millisecond); err != nil {
		t.Fatalf("SetWithExpiry: %v", err)
	}
	if err := s.Set("k", "v2"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, ok := s.GetTTL("k"); ok {
		t.Fatalf("TTL still present after plain SET, want cleared")
	}

	time.Sleep(40 * time.Millisecond)

	if v, ok := s.Get("k"); !ok || v != "v2" {
		t.Fatalf("key reaped by stale TTL after SET: got (%q,%v), want (\"v2\",true)", v, ok)
	}
}

// TestExpiredDeletionSkipsRenewedKey reproduces the race where a key is queued
// for expired-deletion but then rewritten before the deletion runs: the fresh
// value must survive.
func TestExpiredDeletionSkipsRenewedKey(t *testing.T) {
	s := createTestStore()
	shard := s.getShardFast("k")

	// Create a key whose TTL is already in the past and queue its deletion,
	// exactly as the cleaner would.
	if err := s.Set("k", "old"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	shard.expiryMu.Lock()
	shard.expiry["k"] = time.Now().Add(-time.Second)
	shard.expiryMu.Unlock()

	// A SET lands before the queued deletion is processed: it rewrites the
	// value and (plain SET) clears the TTL.
	if err := s.Set("k", "fresh"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Now the stale deletion fires. It must be a no-op because the key is no
	// longer expired.
	s.expiryBatch.deleteExpiredDirect(shard, "k")

	if v, ok := s.Get("k"); !ok || v != "fresh" {
		t.Fatalf("renewed key destroyed by stale expiry deletion: got (%q,%v), want (\"fresh\",true)", v, ok)
	}
}

// TestDeleteRemovesExpiry verifies DEL drops the expiry entry, so recreating
// the key does not inherit a stale TTL.
func TestDeleteRemovesExpiry(t *testing.T) {
	s := createTestStore()

	if err := s.SetWithExpiry("k", "v1", time.Hour); err != nil {
		t.Fatalf("SetWithExpiry: %v", err)
	}
	if !s.Delete("k") {
		t.Fatalf("Delete returned false")
	}
	if _, ok := s.GetTTL("k"); ok {
		t.Fatalf("expiry entry survived DEL")
	}

	if err := s.Set("k", "v2"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if _, ok := s.GetTTL("k"); ok {
		t.Fatalf("recreated key inherited a stale TTL")
	}
}
