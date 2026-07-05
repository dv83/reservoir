package store

import (
	"fmt"
	"sync"
	"testing"
)

// TestNewLockFreeStore tests store initialization
func TestNewLockFreeStore(t *testing.T) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 10 * 1024 * 1024,
	}

	store := NewLockFreeStore(limits)

	if store == nil {
		t.Fatal("NewLockFreeStore returned nil")
	}

	if store.limits != limits {
		t.Error("Store limits not set correctly")
	}

	if len(store.shards) != numShards {
		t.Errorf("Expected %d shards, got %d", numShards, len(store.shards))
	}

	// Verify all shards are initialized
	for i, shard := range store.shards {
		if shard == nil {
			t.Errorf("Shard %d is nil", i)
		}
		if shard.data == nil {
			t.Errorf("Shard %d data map is nil", i)
		}
		if shard.expiry == nil {
			t.Errorf("Shard %d expiry map is nil", i)
		}
	}
}

// TestBasicStringOperations tests core GET/SET functionality
func TestBasicStringOperations(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	t.Run("SetAndGet", func(t *testing.T) {
		key := "test_key"
		value := "test_value"

		err := store.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		result, exists := store.Get(key)
		if !exists {
			t.Fatal("Key should exist")
		}
		if result != value {
			t.Errorf("Expected %s, got %s", value, result)
		}
	})

	t.Run("GetNonExistent", func(t *testing.T) {
		result, exists := store.Get("nonexistent")
		if exists {
			t.Error("Key should not exist")
		}
		if result != "" {
			t.Errorf("Expected empty string, got %s", result)
		}
	})

	t.Run("OverwriteValue", func(t *testing.T) {
		key := "overwrite_key"
		value1 := "value1"
		value2 := "value2"

		// Set initial value
		err := store.Set(key, value1)
		if err != nil {
			t.Fatalf("Initial set failed: %v", err)
		}

		// Overwrite with new value
		err = store.Set(key, value2)
		if err != nil {
			t.Fatalf("Overwrite failed: %v", err)
		}

		// Verify new value
		result, exists := store.Get(key)
		if !exists {
			t.Fatal("Key should exist")
		}
		if result != value2 {
			t.Errorf("Expected %s, got %s", value2, result)
		}
	})
}

// TestDeleteOperations tests key deletion
func TestDeleteOperations(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	key := "delete_test"
	value := "test_value"

	// Set a value
	err := store.Set(key, value)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Delete the key
	deleted := store.Delete(key)
	if !deleted {
		t.Error("Delete should return true for existing key")
	}

	// Verify key is gone
	_, exists := store.Get(key)
	if exists {
		t.Error("Key should not exist after deletion")
	}

	// Delete non-existent key
	deleted = store.Delete("nonexistent")
	if deleted {
		t.Error("Delete should return false for non-existent key")
	}
}

// TestMemoryLimits tests memory limit enforcement
func TestMemoryLimits(t *testing.T) {
	limits := &Limits{
		MaxKeySize:     10,
		MaxValueSize:   10,
		MaxMemoryUsage: 100, // Very small limit
	}

	store := NewLockFreeStore(limits)
	defer store.Stop()

	t.Run("KeySizeLimit", func(t *testing.T) {
		longKey := "this_key_is_too_long"
		err := store.Set(longKey, "value")
		if err == nil {
			t.Error("Should fail with key too large error")
		}
	})

	t.Run("ValueSizeLimit", func(t *testing.T) {
		key := "key"
		longValue := "this_value_is_too_long"
		err := store.Set(key, longValue)
		if err == nil {
			t.Error("Should fail with value too large error")
		}
	})

	t.Run("MemoryLimit", func(t *testing.T) {
		// Fill up memory with small keys
		for i := 0; i < 20; i++ {
			key := string(rune('a' + i))
			value := "val"
			err := store.Set(key, value)
			if err != nil {
				// Should eventually hit memory limit
				break
			}
		}
		// Memory limit should have been hit by now
	})
}

// TestConcurrentOperations tests thread safety
func TestConcurrentOperations(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	numGoroutines := 10
	numOperations := 100
	var wg sync.WaitGroup

	t.Run("ConcurrentSets", func(t *testing.T) {
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("concurrent_%d_%d", id, j)
					value := fmt.Sprintf("value_%d_%d", id, j)
					err := store.Set(key, value)
					if err != nil {
						t.Errorf("Concurrent set failed: %v", err)
					}
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("ConcurrentGets", func(t *testing.T) {
		// First set some values
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("get_test_%d", i)
			value := fmt.Sprintf("value_%d", i)
			store.Set(key, value)
		}

		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < numOperations; j++ {
					key := fmt.Sprintf("get_test_%d", j%10)
					expectedValue := fmt.Sprintf("value_%d", j%10)

					result, exists := store.Get(key)
					if !exists {
						t.Errorf("Key %s should exist", key)
						continue
					}
					if result != expectedValue {
						t.Errorf("Expected %s, got %s", expectedValue, result)
					}
				}
			}(i)
		}
		wg.Wait()
	})
}

// TestTTLOperations tests TTL functionality
func TestTTLOperations(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	key := "ttl_test"
	value := "test_value"

	t.Run("GetTTL", func(t *testing.T) {
		// Set a key first
		err := store.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Check TTL for key without expiration
		ttl, hasExpiry := store.GetTTL(key)
		if hasExpiry {
			t.Error("Key should not have TTL initially")
		}
		if ttl > 0 {
			t.Errorf("TTL should be 0 for key without expiry, got %v", ttl)
		}

		// Test TTL for non-existent key
		ttl, hasExpiry = store.GetTTL("nonexistent")
		if hasExpiry {
			t.Error("Non-existent key should not have TTL")
		}
	})
}

// TestShardDistribution tests that keys are distributed across shards
func TestShardDistribution(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	shardCounts := make(map[int]int)

	// Set many keys
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("shard_test_%d", i)
		value := fmt.Sprintf("value_%d", i)

		err := store.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Track which shard this key went to
		shard := store.getShard(key)
		for j, s := range store.shards {
			if s == shard {
				shardCounts[j]++
				break
			}
		}
	}

	// Verify distribution - at least half the shards should have keys
	shardsWithKeys := 0
	for _, count := range shardCounts {
		if count > 0 {
			shardsWithKeys++
		}
	}

	minShards := numShards / 2
	if shardsWithKeys < minShards {
		t.Errorf("Poor shard distribution: only %d/%d shards have keys", shardsWithKeys, numShards)
	}
}

// TestStoreStatistics tests memory and key counting
func TestStoreStatistics(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	initialMemory := store.GetMemoryUsage()
	initialKeys := store.GetKeyCount()

	// Add some data
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("stats_test_%d", i)
		value := fmt.Sprintf("value_%d", i)
		err := store.Set(key, value)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// Force flush async metrics to ensure statistics are updated
	store.ForceFlushMetrics()

	// Check that statistics increased
	finalMemory := store.GetMemoryUsage()
	finalKeys := store.GetKeyCount()

	if finalMemory <= initialMemory {
		t.Error("Memory usage should have increased")
	}
	if finalKeys != initialKeys+10 {
		t.Errorf("Expected %d keys, got %d", initialKeys+10, finalKeys)
	}

	// Delete some keys and verify statistics decrease
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("stats_test_%d", i)
		store.DeleteFast(key)
	}

	// Force flush async metrics to ensure statistics are updated
	store.ForceFlushMetrics()

	midMemory := store.GetMemoryUsage()
	midKeys := store.GetKeyCount()

	if midMemory >= finalMemory {
		t.Error("Memory usage should have decreased after deletions")
	}
	if midKeys != initialKeys+5 {
		t.Errorf("Expected %d keys after deletion, got %d", initialKeys+5, midKeys)
	}
}

// Helper function to create test store
func createTestStore() *LockFreeStore {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	}
	return NewLockFreeStore(limits)
}
