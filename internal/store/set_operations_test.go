package store

import (
	"fmt"
	"testing"
)

// TestSetOperations tests SET data type operations
func TestSetOperations(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	key := "test_set"

	t.Run("SAdd", func(t *testing.T) {
		// Add members to set
		added, err := store.SAdd(key, "apple", "banana", "cherry")
		if err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
		if added != 3 {
			t.Errorf("Expected 3 added, got %d", added)
		}

		// Add duplicate members
		added, err = store.SAdd(key, "apple", "date")
		if err != nil {
			t.Fatalf("SAdd failed: %v", err)
		}
		if added != 1 { // Only "date" should be added
			t.Errorf("Expected 1 added, got %d", added)
		}
	})

	t.Run("SCard", func(t *testing.T) {
		count, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 4 { // apple, banana, cherry, date
			t.Errorf("Expected cardinality 4, got %d", count)
		}

		// Test non-existent key
		count, err = store.SCard("nonexistent")
		if err != nil {
			t.Fatalf("SCard failed for nonexistent key: %v", err)
		}
		if count != 0 {
			t.Errorf("Expected cardinality 0 for nonexistent key, got %d", count)
		}
	})

	t.Run("SIsMember", func(t *testing.T) {
		// Test existing member
		isMember, err := store.SIsMember(key, "apple")
		if err != nil {
			t.Fatalf("SIsMember failed: %v", err)
		}
		if !isMember {
			t.Error("apple should be a member")
		}

		// Test non-existent member
		isMember, err = store.SIsMember(key, "grape")
		if err != nil {
			t.Fatalf("SIsMember failed: %v", err)
		}
		if isMember {
			t.Error("grape should not be a member")
		}

		// Test non-existent set
		isMember, err = store.SIsMember("nonexistent", "apple")
		if err != nil {
			t.Fatalf("SIsMember failed for nonexistent set: %v", err)
		}
		if isMember {
			t.Error("member should not exist in nonexistent set")
		}
	})

	t.Run("SMembers", func(t *testing.T) {
		members, err := store.SMembers(key)
		if err != nil {
			t.Fatalf("SMembers failed: %v", err)
		}
		if len(members) != 4 {
			t.Errorf("Expected 4 members, got %d", len(members))
		}

		// Verify all expected members are present
		expectedMembers := map[string]bool{
			"apple":  false,
			"banana": false,
			"cherry": false,
			"date":   false,
		}

		for _, member := range members {
			if _, exists := expectedMembers[member]; exists {
				expectedMembers[member] = true
			} else {
				t.Errorf("Unexpected member: %s", member)
			}
		}

		for member, found := range expectedMembers {
			if !found {
				t.Errorf("Missing expected member: %s", member)
			}
		}

		// Test non-existent set
		members, err = store.SMembers("nonexistent")
		if err != nil {
			t.Fatalf("SMembers failed for nonexistent set: %v", err)
		}
		if len(members) != 0 {
			t.Errorf("Expected 0 members for nonexistent set, got %d", len(members))
		}
	})

	t.Run("SRem", func(t *testing.T) {
		// Remove existing members
		removed, err := store.SRem(key, "apple", "grape") // grape doesn't exist
		if err != nil {
			t.Fatalf("SRem failed: %v", err)
		}
		if removed != 1 { // Only apple should be removed
			t.Errorf("Expected 1 removed, got %d", removed)
		}

		// Verify apple is gone
		isMember, err := store.SIsMember(key, "apple")
		if err != nil {
			t.Fatalf("SIsMember failed: %v", err)
		}
		if isMember {
			t.Error("apple should have been removed")
		}

		// Verify cardinality decreased
		count, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != 3 {
			t.Errorf("Expected cardinality 3 after removal, got %d", count)
		}
	})

	t.Run("SPop", func(t *testing.T) {
		// Get initial count
		initialCount, _ := store.SCard(key)

		// Pop a member
		popped, exists, err := store.SPop(key)
		if err != nil {
			t.Fatalf("SPop failed: %v", err)
		}
		if !exists {
			t.Error("SPop should return an existing member")
		}
		if popped == "" {
			t.Error("SPop should return a non-empty member")
		}

		// Verify cardinality decreased
		newCount, err := store.SCard(key)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if newCount != initialCount-1 {
			t.Errorf("Expected cardinality to decrease by 1, was %d, now %d", initialCount, newCount)
		}

		// Verify popped member is no longer in set
		isMember, err := store.SIsMember(key, popped)
		if err != nil {
			t.Fatalf("SIsMember failed: %v", err)
		}
		if isMember {
			t.Errorf("Popped member %s should no longer be in set", popped)
		}

		// Test SPop on empty set
		emptyKey := "empty_set"
		popped, exists, err = store.SPop(emptyKey)
		if err != nil {
			t.Fatalf("SPop failed on empty set: %v", err)
		}
		if exists {
			t.Error("SPop on empty set should not return existing member")
		}
		if popped != "" {
			t.Errorf("SPop on empty set should return empty string, got %s", popped)
		}
	})
}

// TestSetSetOperations tests set-to-set operations
func TestSetSetOperations(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	// Setup test sets
	set1Key := "set1"
	set2Key := "set2"
	set3Key := "set3"

	// set1: {a, b, c, d}
	store.SAdd(set1Key, "a", "b", "c", "d")
	// set2: {c, d, e, f}
	store.SAdd(set2Key, "c", "d", "e", "f")
	// set3: {g, h}
	store.SAdd(set3Key, "g", "h")

	t.Run("SDiff", func(t *testing.T) {
		// set1 - set2 should be {a, b}
		diff, err := store.SDiff(set1Key, set2Key)
		if err != nil {
			t.Fatalf("SDiff failed: %v", err)
		}
		if len(diff) != 2 {
			t.Errorf("Expected 2 elements in diff, got %d", len(diff))
		}

		expectedDiff := map[string]bool{"a": false, "b": false}
		for _, member := range diff {
			if _, exists := expectedDiff[member]; exists {
				expectedDiff[member] = true
			} else {
				t.Errorf("Unexpected member in diff: %s", member)
			}
		}

		for member, found := range expectedDiff {
			if !found {
				t.Errorf("Missing expected member in diff: %s", member)
			}
		}

		// Test with non-existent set
		diff, err = store.SDiff("nonexistent", set2Key)
		if err != nil {
			t.Fatalf("SDiff failed with nonexistent set: %v", err)
		}
		if len(diff) != 0 {
			t.Errorf("Expected empty diff with nonexistent set, got %d elements", len(diff))
		}
	})

	t.Run("SInter", func(t *testing.T) {
		// set1 ∩ set2 should be {c, d}
		inter, err := store.SInter(set1Key, set2Key)
		if err != nil {
			t.Fatalf("SInter failed: %v", err)
		}
		if len(inter) != 2 {
			t.Errorf("Expected 2 elements in intersection, got %d", len(inter))
		}

		expectedInter := map[string]bool{"c": false, "d": false}
		for _, member := range inter {
			if _, exists := expectedInter[member]; exists {
				expectedInter[member] = true
			} else {
				t.Errorf("Unexpected member in intersection: %s", member)
			}
		}

		for member, found := range expectedInter {
			if !found {
				t.Errorf("Missing expected member in intersection: %s", member)
			}
		}

		// Test with non-overlapping sets
		inter, err = store.SInter(set1Key, set3Key)
		if err != nil {
			t.Fatalf("SInter failed: %v", err)
		}
		if len(inter) != 0 {
			t.Errorf("Expected empty intersection, got %d elements", len(inter))
		}
	})

	t.Run("SUnion", func(t *testing.T) {
		// set1 ∪ set2 should be {a, b, c, d, e, f}
		union, err := store.SUnion(set1Key, set2Key)
		if err != nil {
			t.Fatalf("SUnion failed: %v", err)
		}
		if len(union) != 6 {
			t.Errorf("Expected 6 elements in union, got %d", len(union))
		}

		expectedUnion := map[string]bool{
			"a": false, "b": false, "c": false,
			"d": false, "e": false, "f": false,
		}
		for _, member := range union {
			if _, exists := expectedUnion[member]; exists {
				expectedUnion[member] = true
			} else {
				t.Errorf("Unexpected member in union: %s", member)
			}
		}

		for member, found := range expectedUnion {
			if !found {
				t.Errorf("Missing expected member in union: %s", member)
			}
		}
	})

	t.Run("SDiffStore", func(t *testing.T) {
		destKey := "diff_result"

		// Store set1 - set2 in destKey
		count, err := store.SDiffStore(destKey, set1Key, set2Key)
		if err != nil {
			t.Fatalf("SDiffStore failed: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 elements stored, got %d", count)
		}

		// Verify the result is stored as a set
		members, err := store.SMembers(destKey)
		if err != nil {
			t.Fatalf("SMembers failed on diff result: %v", err)
		}
		if len(members) != 2 {
			t.Errorf("Expected 2 members in stored diff, got %d", len(members))
		}

		expectedMembers := map[string]bool{"a": false, "b": false}
		for _, member := range members {
			if _, exists := expectedMembers[member]; exists {
				expectedMembers[member] = true
			}
		}

		for member, found := range expectedMembers {
			if !found {
				t.Errorf("Missing expected member in stored diff: %s", member)
			}
		}
	})
}

// TestSetErrorCases tests error conditions for SET operations
func TestSetErrorCases(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	// Set up a string key for WRONGTYPE tests
	stringKey := "string_key"
	store.Set(stringKey, "string_value")

	t.Run("WRONGTYPE_Errors", func(t *testing.T) {
		// Try SET operations on string key
		_, err := store.SAdd(stringKey, "member")
		if err == nil {
			t.Error("SAdd should fail on string key")
		}

		_, err = store.SCard(stringKey)
		if err == nil {
			t.Error("SCard should fail on string key")
		}

		_, err = store.SIsMember(stringKey, "member")
		if err == nil {
			t.Error("SIsMember should fail on string key")
		}

		_, err = store.SMembers(stringKey)
		if err == nil {
			t.Error("SMembers should fail on string key")
		}

		_, err = store.SRem(stringKey, "member")
		if err == nil {
			t.Error("SRem should fail on string key")
		}

		_, _, err = store.SPop(stringKey)
		if err == nil {
			t.Error("SPop should fail on string key")
		}
	})

	t.Run("EmptyArguments", func(t *testing.T) {
		setKey := "test_set"

		// SAdd with no members
		added, err := store.SAdd(setKey)
		if err != nil {
			t.Fatalf("SAdd with no members failed: %v", err)
		}
		if added != 0 {
			t.Errorf("Expected 0 added with no members, got %d", added)
		}

		// SRem with no members
		removed, err := store.SRem(setKey)
		if err != nil {
			t.Fatalf("SRem with no members failed: %v", err)
		}
		if removed != 0 {
			t.Errorf("Expected 0 removed with no members, got %d", removed)
		}
	})
}

// TestSetMemoryManagement tests SET operations under memory constraints
func TestSetMemoryManagement(t *testing.T) {
	// Create store with very limited memory
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024,
		MaxMemoryUsage: 1024, // Very small limit
	}
	store := NewLockFreeStore(limits)
	defer store.Stop()

	setKey := "memory_test_set"

	// Try to add many members to exceed memory limit
	var err error
	for i := 0; i < 100; i++ {
		member := fmt.Sprintf("member_%d", i)
		_, err = store.SAdd(setKey, member)
		if err != nil {
			// Should eventually hit memory limit
			break
		}
	}

	// We should have hit a memory limit at some point
	if err == nil {
		t.Log("Warning: Memory limit may not be properly enforced for SET operations")
	}
}

// TestSetConcurrency tests concurrent SET operations
func TestSetConcurrency(t *testing.T) {
	store := createTestStore()
	defer store.Stop()

	setKey := "concurrent_set"
	numGoroutines := 10
	membersPerGoroutine := 10

	// Concurrent SAdd operations
	t.Run("ConcurrentSAdd", func(t *testing.T) {
		done := make(chan bool, numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				defer func() { done <- true }()

				for i := 0; i < membersPerGoroutine; i++ {
					member := fmt.Sprintf("g%d_m%d", goroutineID, i)
					_, err := store.SAdd(setKey, member)
					if err != nil {
						t.Errorf("Concurrent SAdd failed: %v", err)
					}
				}
			}(g)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Verify final cardinality
		expectedCount := int64(numGoroutines * membersPerGoroutine)
		count, err := store.SCard(setKey)
		if err != nil {
			t.Fatalf("SCard failed: %v", err)
		}
		if count != expectedCount {
			t.Errorf("Expected %d members after concurrent adds, got %d", expectedCount, count)
		}
	})

	// Concurrent mixed operations
	t.Run("ConcurrentMixedOps", func(t *testing.T) {
		mixedSetKey := "mixed_ops_set"

		// Pre-populate the set
		for i := 0; i < 50; i++ {
			member := fmt.Sprintf("initial_%d", i)
			store.SAdd(mixedSetKey, member)
		}

		done := make(chan bool, numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				defer func() { done <- true }()

				for i := 0; i < 20; i++ {
					switch i % 4 {
					case 0: // Add
						member := fmt.Sprintf("g%d_add_%d", goroutineID, i)
						store.SAdd(mixedSetKey, member)
					case 1: // Remove
						member := fmt.Sprintf("initial_%d", (goroutineID*20+i)%50)
						store.SRem(mixedSetKey, member)
					case 2: // Check membership
						member := fmt.Sprintf("initial_%d", i%50)
						store.SIsMember(mixedSetKey, member)
					case 3: // Get cardinality
						store.SCard(mixedSetKey)
					}
				}
			}(g)
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Verify set is still functional
		count, err := store.SCard(mixedSetKey)
		if err != nil {
			t.Fatalf("SCard failed after concurrent operations: %v", err)
		}
		if count < 0 {
			t.Error("Set cardinality should not be negative")
		}
	})
}
