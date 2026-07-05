package store

import (
	"fmt"
	"sync"
	"testing"
)

// TestSCardComprehensive tests SCARD command thoroughly
func TestSCardComprehensive(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	t.Run("SCard_EmptySet", func(t *testing.T) {
		// Test SCARD on non-existent key
		count, err := store.SCard("nonexistent_set")
		if err != nil {
			t.Fatalf("SCard failed on non-existent key: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected 0 for non-existent set, got %d", count)
		}
	})

	t.Run("SCard_AfterSAdd", func(t *testing.T) {
		key := "scard_test_1"

		// Add 1 element
		added, err := store.SAdd(key, "element1")
		if err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
		if added != 1 {
			t.Errorf("Expected 1 added, got %d", added)
		}

		// Check cardinality
		count, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected cardinality 1, got %d", count)
		}

		// Add more elements
		added, err = store.SAdd(key, "element2", "element3", "element4", "element5")
		if err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
		if added != 4 {
			t.Errorf("Expected 4 added, got %d", added)
		}

		// Check updated cardinality
		count, err = store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 5 {
			t.Errorf("Expected cardinality 5, got %d", count)
		}
	})

	t.Run("SCard_WithDuplicates", func(t *testing.T) {
		key := "scard_test_2"

		// Add elements including duplicates
		added, err := store.SAdd(key, "a", "b", "c", "a", "b") // a, b are duplicates
		if err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
		if added != 3 { // Only unique elements should be added
			t.Errorf("Expected 3 unique elements added, got %d", added)
		}

		// Check cardinality
		count, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected cardinality 3, got %d", count)
		}

		// Add more duplicates
		added, err = store.SAdd(key, "a", "d", "e", "c")
		if err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
		if added != 2 { // Only d, e should be added
			t.Errorf("Expected 2 new elements added, got %d", added)
		}

		// Check final cardinality
		count, err = store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 5 {
			t.Errorf("Expected cardinality 5, got %d", count)
		}
	})

	t.Run("SCard_AfterSRem", func(t *testing.T) {
		key := "scard_test_3"

		// Add elements
		store.SAdd(key, "1", "2", "3", "4", "5", "6", "7", "8", "9", "10")

		// Verify initial cardinality
		count, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 10 {
			t.Errorf("Expected initial cardinality 10, got %d", count)
		}

		// Remove some elements
		removed, err := store.SRem(key, "1", "3", "5", "7", "9")
		if err != nil {
			t.Fatalf("SRem failed: %v", err)
		}
		if removed != 5 {
			t.Errorf("Expected 5 removed, got %d", removed)
		}

		// Check cardinality after removal
		count, err = store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 5 {
			t.Errorf("Expected cardinality 5 after removal, got %d", count)
		}

		// Remove non-existent elements
		removed, err = store.SRem(key, "1", "11", "12") // 1 was already removed, 11,12 don't exist
		if err != nil {
			t.Fatalf("SRem failed: %v", err)
		}
		if removed != 0 {
			t.Errorf("Expected 0 removed (non-existent), got %d", removed)
		}

		// Cardinality should remain the same
		count, err = store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 5 {
			t.Errorf("Expected cardinality still 5, got %d", count)
		}
	})

	t.Run("SCard_AfterSPop", func(t *testing.T) {
		key := "scard_test_4"

		// Add elements
		store.SAdd(key, "x", "y", "z")

		// Verify initial cardinality
		count, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected initial cardinality 3, got %d", count)
		}

		// Pop an element
		popped, exists, err := store.SPop(key)
		if err != nil {
			t.Fatalf("SPop failed: %v", err)
		}
		if !exists {
			t.Error("SPop should return existing element")
		}
		if popped == "" {
			t.Error("SPop should return non-empty element")
		}

		// Check cardinality after pop
		count, err = store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected cardinality 2 after pop, got %d", count)
		}

		// Pop all remaining elements
		for i := 0; i < 2; i++ {
			_, exists, err := store.SPop(key)
			if err != nil {
				t.Fatalf("SPop failed: %v", err)
			}
			if !exists {
				t.Errorf("SPop should return existing element on iteration %d", i)
			}
		}

		// Check final cardinality (should be 0)
		count, err = store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected cardinality 0 after popping all, got %d", count)
		}

		// Pop from empty set
		_, exists, err = store.SPop(key)
		if err != nil {
			t.Fatalf("SPop failed on empty set: %v", err)
		}
		if exists {
			t.Error("SPop on empty set should not return existing element")
		}
	})

	t.Run("SCard_WrongType", func(t *testing.T) {
		stringKey := "string_key"

		// Set a string value
		err := store.Set(stringKey, "this is a string")
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}

		// Try SCARD on string key
		_, err = store.SCard(stringKey)
		if err == nil {
			t.Error("SCard should fail on string key (WRONGTYPE)")
		}
		if err.Error() != "WRONGTYPE Operation against a key holding the wrong kind of value" {
			t.Errorf("Expected WRONGTYPE error, got: %v", err)
		}
	})
}

// TestSCardConcurrency tests SCARD under concurrent operations
func TestSCardConcurrency(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	key := "concurrent_scard_test"
	numGoroutines := 10
	elementsPerGoroutine := 20

	// Pre-populate the set
	for i := 0; i < 50; i++ {
		store.SAdd(key, fmt.Sprintf("initial_%d", i))
	}

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Run concurrent operations
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()

			for i := 0; i < elementsPerGoroutine; i++ {
				switch i % 3 {
				case 0: // Add elements
					member := fmt.Sprintf("g%d_add_%d", goroutineID, i)
					store.SAdd(key, member)
				case 1: // Remove elements
					member := fmt.Sprintf("initial_%d", (goroutineID*elementsPerGoroutine+i)%50)
					store.SRem(key, member)
				case 2: // Check cardinality
					count, err := store.SCard(key)
					if err != nil {
						t.Errorf("Concurrent SCard failed: %v", err)
					}
					if count < 0 {
						t.Errorf("SCard returned negative count: %d", count)
					}
				}
			}
		}(g)
	}

	wg.Wait()

	// Final cardinality check
	finalCount, err := store.SCard(key)
	if err != nil {
		t.Fatalf("Final SCard failed: %v", err)
	}
	if finalCount < 0 {
		t.Errorf("Final SCard should not be negative, got %d", finalCount)
	}

	t.Logf("Final cardinality after concurrent operations: %d", finalCount)
}

// TestSCardConsistency tests SCARD consistency across operations
func TestSCardConsistency(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	key := "consistency_test"

	// Test consistency across multiple operations
	expectedElements := make(map[string]bool)

	// Add elements and track them
	for i := 0; i < 100; i++ {
		element := fmt.Sprintf("element_%d", i)
		expectedElements[element] = true

		added, err := store.SAdd(key, element)
		if err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
		if added != 1 {
			t.Errorf("Expected 1 added for new element, got %d", added)
		}

		// Check cardinality matches expected
		count, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		expectedCount := int64(len(expectedElements))
		if count != expectedCount {
			t.Errorf("Cardinality mismatch at step %d: expected %d, got %d", i, expectedCount, count)
		}
	}

	// Remove elements and track them
	elementsToRemove := []string{"element_10", "element_25", "element_50", "element_75", "element_99"}
	for _, element := range elementsToRemove {
		delete(expectedElements, element)

		removed, err := store.SRem(key, element)
		if err != nil {
			t.Fatalf("SRem failed: %v", err)
		}
		if removed != 1 {
			t.Errorf("Expected 1 removed, got %d", removed)
		}

		// Check cardinality matches expected
		count, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		expectedCount := int64(len(expectedElements))
		if count != expectedCount {
			t.Errorf("Cardinality mismatch after removal: expected %d, got %d", expectedCount, count)
		}
	}

	// Verify final state with SMembers
	members, err := store.SMembers(key)
	if err != nil {
		t.Fatalf("SMembers failed: %v", err)
	}

	if len(members) != len(expectedElements) {
		t.Errorf("SMembers count mismatch: expected %d, got %d", len(expectedElements), len(members))
	}

	// Verify all members are expected
	for _, member := range members {
		if !expectedElements[member] {
			t.Errorf("Unexpected member found: %s", member)
		}
	}
}

// TestSCardEdgeCases tests edge cases for SCARD
func TestSCardEdgeCases(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	t.Run("SCard_EmptyStringMember", func(t *testing.T) {
		key := "empty_string_test"

		// Add empty string as member
		added, err := store.SAdd(key, "")
		if err != nil {
			t.Fatalf("SAdd with empty string failed: %v", err)
		}
		if added != 1 {
			t.Errorf("Expected 1 added (empty string), got %d", added)
		}

		// Check cardinality
		count, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 1 {
			t.Errorf("Expected cardinality 1 with empty string member, got %d", count)
		}
	})

	t.Run("SCard_LongStringMembers", func(t *testing.T) {
		key := "long_string_test"

		// Add very long string members
		longString1 := make([]byte, 1000)
		for i := range longString1 {
			longString1[i] = 'A'
		}

		longString2 := make([]byte, 1000)
		for i := range longString2 {
			longString2[i] = 'B'
		}

		added, err := store.SAdd(key, string(longString1), string(longString2))
		if err != nil {
			t.Fatalf("SAdd with long strings failed: %v", err)
		}
		if added != 2 {
			t.Errorf("Expected 2 added (long strings), got %d", added)
		}

		// Check cardinality
		count, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected cardinality 2 with long string members, got %d", count)
		}
	})

	t.Run("SCard_UnicodeMembers", func(t *testing.T) {
		key := "unicode_test"

		// Add Unicode string members
		unicodeMembers := []string{"🚀", "🎯", "💡", "🔥", "⭐", "🌟", "🎉", "🎊"}

		added, err := store.SAdd(key, unicodeMembers...)
		if err != nil {
			t.Fatalf("SAdd with Unicode strings failed: %v", err)
		}
		if added != int64(len(unicodeMembers)) {
			t.Errorf("Expected %d added (Unicode strings), got %d", len(unicodeMembers), added)
		}

		// Check cardinality
		count, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != int64(len(unicodeMembers)) {
			t.Errorf("Expected cardinality %d with Unicode members, got %d", len(unicodeMembers), count)
		}
	})
}
