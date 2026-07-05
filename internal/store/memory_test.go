package store

import (
	"math"
	"math/rand"
	"strconv"
	"sync/atomic"
	"testing"
)

// TestMemoryUsageCalculation tests memory usage calculations for correctness
func TestMemoryUsageCalculation(t *testing.T) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	// Test basic SET operation
	err := store.Set("test_key", "test_value")
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}

	memory := store.GetMemoryUsage()
	if memory < 0 {
		t.Errorf("Memory usage is negative: %d", memory)
	}

	// Test DELETE operation
	deleted := store.Delete("test_key")
	if !deleted {
		t.Error("Expected key to be deleted")
	}

	memory = store.GetMemoryUsage()
	if memory < 0 {
		t.Errorf("Memory usage is negative after delete: %d", memory)
	}
}

// TestMemoryUsageWithRandomKeys tests with random key patterns like __rand_int__
func TestMemoryUsageWithRandomKeys(t *testing.T) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	// Test keys similar to the ones in the screenshot
	keys := []string{
		"key__rand_int__",
		"key__rand_int__VXK",
		"__rand_int__123",
		"test__rand_int__456",
	}

	for _, key := range keys {
		// Set value
		err := store.Set(key, "test_value")
		if err != nil {
			t.Fatalf("SET failed for key %s: %v", key, err)
		}

		memory := store.GetMemoryUsage()
		if memory < 0 {
			t.Errorf("Memory usage is negative after setting %s: %d", key, memory)
		}
	}

	// Test deletion
	for _, key := range keys {
		deleted := store.Delete(key)
		if !deleted {
			t.Errorf("Expected key %s to be deleted", key)
		}

		memory := store.GetMemoryUsage()
		if memory < 0 {
			t.Errorf("Memory usage is negative after deleting %s: %d", key, memory)
		}
	}
}

// TestMemoryUsageStressTest performs heavy operations to trigger overflow
func TestMemoryUsageStressTest(t *testing.T) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	// Set a lot of keys with different sizes
	for i := 0; i < 1000; i++ {
		key := "stress_test_key_" + strconv.Itoa(i)
		// Create values of different sizes
		valueSize := rand.Intn(1000) + 100
		value := string(make([]byte, valueSize))

		err := store.Set(key, value)
		if err != nil {
			t.Fatalf("SET failed: %v", err)
		}

		memory := store.GetMemoryUsage()
		if memory < 0 {
			t.Errorf("Memory usage became negative at iteration %d: %d", i, memory)
			break
		}
	}

	// Delete in random order
	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = "stress_test_key_" + strconv.Itoa(i)
	}

	// Shuffle keys
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	for i, key := range keys {
		store.Delete(key)
		memory := store.GetMemoryUsage()
		if memory < 0 {
			t.Errorf("Memory usage became negative during deletion at iteration %d: %d", i, memory)
			break
		}
	}
}

// TestMemoryUsageAtomicity tests concurrent access to memory counters
func TestMemoryUsageAtomicity(t *testing.T) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	// Track minimum memory seen
	var minMemory int64 = math.MaxInt64

	// Run concurrent operations
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			for j := 0; j < 100; j++ {
				key := "concurrent_" + strconv.Itoa(id) + "_" + strconv.Itoa(j)

				// SET
				store.Set(key, "test_value")
				memory := store.GetMemoryUsage()
				if memory < atomic.LoadInt64(&minMemory) {
					atomic.StoreInt64(&minMemory, memory)
				}

				// DEL
				store.Delete(key)
				memory = store.GetMemoryUsage()
				if memory < atomic.LoadInt64(&minMemory) {
					atomic.StoreInt64(&minMemory, memory)
				}
			}
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	finalMinMemory := atomic.LoadInt64(&minMemory)
	if finalMinMemory < 0 {
		t.Errorf("Memory usage went negative during concurrent operations: %d", finalMinMemory)
	}
}

// TestSpecificKeysFromScreenshot tests the exact keys from the screenshot
func TestSpecificKeysFromScreenshot(t *testing.T) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	// The exact keys from the screenshot that show negative memory
	keys := []string{
		"key__rand_int__",
		"key__rand_int__VXK",
	}

	t.Logf("Initial memory usage: %d", store.GetMemoryUsage())

	for _, key := range keys {
		t.Logf("Setting key: %s", key)
		err := store.Set(key, "test_value")
		if err != nil {
			t.Fatalf("Failed to set key %s: %v", key, err)
		}

		memory := store.GetMemoryUsage()
		t.Logf("Memory after setting %s: %d", key, memory)

		if memory < 0 {
			t.Errorf("CRITICAL: Memory usage is negative after setting %s: %d", key, memory)
		}
	}

	// Test individual deletion
	for _, key := range keys {
		t.Logf("Deleting key: %s", key)
		deleted := store.Delete(key)
		if !deleted {
			t.Errorf("Failed to delete key %s", key)
		}

		memory := store.GetMemoryUsage()
		t.Logf("Memory after deleting %s: %d", key, memory)

		if memory < 0 {
			t.Errorf("CRITICAL: Memory usage is negative after deleting %s: %d", key, memory)
		}
	}
}
