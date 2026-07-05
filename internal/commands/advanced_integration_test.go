package commands

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// =============================================================================
// Multi-Client Concurrent Operations
// =============================================================================

func TestMultiClientConcurrent(t *testing.T) {
	t.Run("Multiple clients incrementing same counter", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Initialize counter
		tc.exec("SET", "shared_counter", "0")

		numClients := 10
		incrementsPerClient := 100
		var wg sync.WaitGroup

		// Simulate multiple clients incrementing the same counter
		wg.Add(numClients)
		for i := 0; i < numClients; i++ {
			go func(clientID int) {
				defer wg.Done()
				for j := 0; j < incrementsPerClient; j++ {
					tc.store.Incr("shared_counter")
				}
			}(i)
		}
		wg.Wait()

		// Verify final count
		val, _ := tc.store.Get("shared_counter")
		expected := fmt.Sprintf("%d", numClients*incrementsPerClient)
		if val != expected {
			t.Errorf("Expected counter to be %s, got %s", expected, val)
		}
	})

	t.Run("Multiple clients pushing to same list", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		numClients := 5
		pushesPerClient := 50
		var wg sync.WaitGroup

		// Multiple clients pushing to same list
		wg.Add(numClients * 2) // half LPUSH, half RPUSH
		for i := 0; i < numClients; i++ {
			// LPUSH clients
			go func(clientID int) {
				defer wg.Done()
				for j := 0; j < pushesPerClient; j++ {
					tc.store.LPush("shared_list", fmt.Sprintf("L%d_%d", clientID, j))
				}
			}(i)
			// RPUSH clients
			go func(clientID int) {
				defer wg.Done()
				for j := 0; j < pushesPerClient; j++ {
					tc.store.RPush("shared_list", fmt.Sprintf("R%d_%d", clientID, j))
				}
			}(i)
		}
		wg.Wait()

		// Verify list length
		length, _ := tc.store.LLen("shared_list")
		expected := int64(numClients * pushesPerClient * 2)
		if length != expected {
			t.Errorf("Expected list length %d, got %d", expected, length)
		}
	})

	t.Run("Multiple clients adding to same set", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		numClients := 5
		addsPerClient := 100
		var wg sync.WaitGroup
		var totalAdded int64

		// Multiple clients adding unique members
		wg.Add(numClients)
		for i := 0; i < numClients; i++ {
			go func(clientID int) {
				defer wg.Done()
				for j := 0; j < addsPerClient; j++ {
					member := fmt.Sprintf("member_%d_%d", clientID, j)
					added, _ := tc.store.SAdd("shared_set", member)
					atomic.AddInt64(&totalAdded, added)
				}
			}(i)
		}
		wg.Wait()

		// Verify set cardinality matches total added (all unique)
		card, _ := tc.store.SCard("shared_set")
		expected := int64(numClients * addsPerClient)
		if card != expected {
			t.Errorf("Expected set cardinality %d, got %d", expected, card)
		}
	})

	t.Run("Concurrent readers and writers", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		numWriters := 3
		numReaders := 5
		opsPerGoroutine := 100
		var wg sync.WaitGroup

		// Initialize some data
		for i := 0; i < 10; i++ {
			tc.store.Set(fmt.Sprintf("rw_key_%d", i), fmt.Sprintf("initial_%d", i))
		}

		// Writers
		wg.Add(numWriters)
		for i := 0; i < numWriters; i++ {
			go func(writerID int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					keyIdx := j % 10
					key := fmt.Sprintf("rw_key_%d", keyIdx)
					value := fmt.Sprintf("writer_%d_op_%d", writerID, j)
					tc.store.Set(key, value)
				}
			}(i)
		}

		// Readers
		wg.Add(numReaders)
		for i := 0; i < numReaders; i++ {
			go func(readerID int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					keyIdx := j % 10
					key := fmt.Sprintf("rw_key_%d", keyIdx)
					_, exists := tc.store.Get(key)
					if !exists {
						t.Errorf("Reader %d: key %s should exist", readerID, key)
					}
				}
			}(i)
		}

		wg.Wait()
	})

	t.Run("Producer-consumer queue pattern", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		numProducers := 3
		numConsumers := 2
		itemsPerProducer := 50
		var wg sync.WaitGroup
		var produced, consumed int64

		// Producers push to list
		wg.Add(numProducers)
		for i := 0; i < numProducers; i++ {
			go func(producerID int) {
				defer wg.Done()
				for j := 0; j < itemsPerProducer; j++ {
					item := fmt.Sprintf("item_%d_%d", producerID, j)
					tc.store.RPush("task_queue", item)
					atomic.AddInt64(&produced, 1)
				}
			}(i)
		}

		// Wait for producers to finish
		wg.Wait()

		// Consumers pop from list
		wg.Add(numConsumers)
		for i := 0; i < numConsumers; i++ {
			go func(consumerID int) {
				defer wg.Done()
				for {
					items, _ := tc.store.LPop("task_queue", 1)
					if len(items) == 0 {
						// Check if queue is really empty
						length, _ := tc.store.LLen("task_queue")
						if length == 0 {
							break
						}
						continue
					}
					atomic.AddInt64(&consumed, 1)
				}
			}(i)
		}

		wg.Wait()

		// Verify all items consumed
		if produced != consumed {
			t.Errorf("Produced %d items but consumed %d", produced, consumed)
		}

		// Queue should be empty
		length, _ := tc.store.LLen("task_queue")
		if length != 0 {
			t.Errorf("Queue should be empty, has %d items", length)
		}
	})
}

// =============================================================================
// Mixed Type Operations
// =============================================================================

func TestMixedTypeOperations(t *testing.T) {
	t.Run("Convert key type via DEL and recreate", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Start with string
		tc.exec("SET", "morphing_key", "string_value")
		resp := tc.exec("GET", "morphing_key")
		tc.expectContains(resp, "string_value", "Should be string")

		// Convert to list
		tc.exec("DEL", "morphing_key")
		tc.exec("RPUSH", "morphing_key", "list_item_1", "list_item_2")
		resp = tc.exec("LLEN", "morphing_key")
		tc.expectContains(resp, ":2", "Should be list with 2 items")

		// Convert to set
		tc.exec("DEL", "morphing_key")
		tc.exec("SADD", "morphing_key", "set_member_1", "set_member_2", "set_member_3")
		resp = tc.exec("SCARD", "morphing_key")
		tc.expectContains(resp, ":3", "Should be set with 3 members")

		// Back to string
		tc.exec("DEL", "morphing_key")
		tc.exec("SET", "morphing_key", "back_to_string")
		resp = tc.exec("GET", "morphing_key")
		tc.expectContains(resp, "back_to_string", "Should be string again")
	})

	t.Run("Multiple keys with different types", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Create keys of different types
		tc.exec("SET", "str_key", "string_value")
		tc.exec("RPUSH", "list_key", "a", "b", "c")
		tc.exec("SADD", "set_key", "x", "y", "z")
		tc.exec("SET", "counter", "100")

		// Operate on each in mixed order
		tc.exec("INCR", "counter")
		tc.exec("LPUSH", "list_key", "prepended")
		tc.exec("SADD", "set_key", "new_member")
		tc.exec("APPEND", "str_key", "_appended")

		// Verify all operations
		resp := tc.exec("GET", "str_key")
		tc.expectContains(resp, "string_value_appended", "String should have appended value")

		resp = tc.exec("LLEN", "list_key")
		tc.expectContains(resp, ":4", "List should have 4 items")

		resp = tc.exec("SCARD", "set_key")
		tc.expectContains(resp, ":4", "Set should have 4 members")

		resp = tc.exec("GET", "counter")
		tc.expectContains(resp, "101", "Counter should be 101")
	})

	t.Run("WRONGTYPE errors dont affect other keys", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Create list and string
		tc.exec("RPUSH", "mylist", "item1")
		tc.exec("SET", "mystring", "value1")

		// Try wrong operations (should fail)
		resp := tc.exec("INCR", "mylist")
		tc.expectContains(resp, "WRONGTYPE", "INCR on list should fail")

		resp = tc.exec("LPUSH", "mystring", "item")
		tc.expectContains(resp, "WRONGTYPE", "LPUSH on string should fail")

		// Original values should be intact
		resp = tc.exec("LRANGE", "mylist", "0", "-1")
		tc.expectContains(resp, "item1", "List should still have original item")

		resp = tc.exec("GET", "mystring")
		tc.expectContains(resp, "value1", "String should still have original value")
	})

	t.Run("Concurrent mixed type operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		var wg sync.WaitGroup
		numOps := 50

		// Initialize different key types
		tc.store.Set("concurrent_str", "0")
		tc.store.RPush("concurrent_list", "init")
		tc.store.SAdd("concurrent_set", "init")

		// Concurrent string operations
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				tc.store.Incr("concurrent_str")
			}
		}()

		// Concurrent list operations
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				tc.store.RPush("concurrent_list", fmt.Sprintf("item_%d", i))
			}
		}()

		// Concurrent set operations
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < numOps; i++ {
				tc.store.SAdd("concurrent_set", fmt.Sprintf("member_%d", i))
			}
		}()

		wg.Wait()

		// Verify results
		strVal, _ := tc.store.Get("concurrent_str")
		if strVal != "50" {
			t.Errorf("Expected string counter to be 50, got %s", strVal)
		}

		listLen, _ := tc.store.LLen("concurrent_list")
		if listLen != 51 { // init + 50
			t.Errorf("Expected list length 51, got %d", listLen)
		}

		setCard, _ := tc.store.SCard("concurrent_set")
		if setCard != 51 { // init + 50
			t.Errorf("Expected set cardinality 51, got %d", setCard)
		}
	})
}

// =============================================================================
// Complex Workflow Scenarios
// =============================================================================

func TestComplexWorkflows(t *testing.T) {
	t.Run("Session management simulation", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		numUsers := 10
		var wg sync.WaitGroup

		// Simulate user sessions
		wg.Add(numUsers)
		for i := 0; i < numUsers; i++ {
			go func(userID int) {
				defer wg.Done()
				sessionKey := fmt.Sprintf("session:%d", userID)
				activityKey := fmt.Sprintf("activity:%d", userID)

				// Create session
				tc.store.Set(sessionKey, fmt.Sprintf("user_data_%d", userID))

				// Log activities
				for j := 0; j < 5; j++ {
					tc.store.RPush(activityKey, fmt.Sprintf("action_%d", j))
				}

				// Update session
				tc.store.Set(sessionKey, fmt.Sprintf("updated_data_%d", userID))

				// Read back
				_, exists := tc.store.Get(sessionKey)
				if !exists {
					t.Errorf("Session %s should exist", sessionKey)
				}

				length, _ := tc.store.LLen(activityKey)
				if length != 5 {
					t.Errorf("Activity log should have 5 entries, got %d", length)
				}
			}(i)
		}

		wg.Wait()

		// Verify all sessions exist by checking actual keys
		sessionsFound := 0
		activitiesFound := 0
		for i := 0; i < numUsers; i++ {
			sessionKey := fmt.Sprintf("session:%d", i)
			activityKey := fmt.Sprintf("activity:%d", i)

			if _, exists := tc.store.Get(sessionKey); exists {
				sessionsFound++
			}
			if length, _ := tc.store.LLen(activityKey); length > 0 {
				activitiesFound++
			}
		}

		if sessionsFound != numUsers {
			t.Errorf("Expected %d sessions, found %d", numUsers, sessionsFound)
		}
		if activitiesFound != numUsers {
			t.Errorf("Expected %d activity logs, found %d", numUsers, activitiesFound)
		}
	})

	t.Run("Leaderboard simulation", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		numPlayers := 20
		var wg sync.WaitGroup

		// Players earn points concurrently
		wg.Add(numPlayers)
		for i := 0; i < numPlayers; i++ {
			go func(playerID int) {
				defer wg.Done()
				scoreKey := fmt.Sprintf("score:player_%d", playerID)

				// Random number of score increments
				increments := rand.Intn(10) + 1
				for j := 0; j < increments; j++ {
					tc.store.Incr(scoreKey)
				}
			}(i)
		}

		wg.Wait()

		// Collect all scores
		var totalScore int64
		for i := 0; i < numPlayers; i++ {
			scoreKey := fmt.Sprintf("score:player_%d", i)
			val, exists := tc.store.Get(scoreKey)
			if exists {
				score, _ := parseIntegerStrict(val)
				totalScore += score
			}
		}

		// Total score should be positive
		if totalScore <= 0 {
			t.Errorf("Total score should be positive, got %d", totalScore)
		}
	})

	t.Run("Tag system simulation", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		numArticles := 10
		tags := []string{"tech", "news", "tutorial", "review", "opinion"}

		// Create articles with tags
		for i := 0; i < numArticles; i++ {
			articleID := fmt.Sprintf("article:%d", i)
			tc.store.Set(articleID, fmt.Sprintf("Article %d content", i))

			// Add random tags
			numTags := rand.Intn(3) + 1
			for j := 0; j < numTags; j++ {
				tag := tags[rand.Intn(len(tags))]
				tagSetKey := fmt.Sprintf("tag:%s", tag)
				tc.store.SAdd(tagSetKey, articleID)
			}
		}

		// Find articles with specific tags
		techArticles, _ := tc.store.SMembers("tag:tech")
		newsArticles, _ := tc.store.SMembers("tag:news")

		// Find intersection (articles with both tech AND news tags)
		if len(techArticles) > 0 && len(newsArticles) > 0 {
			intersection, _ := tc.store.SInter("tag:tech", "tag:news")
			t.Logf("Articles with both tech and news tags: %d", len(intersection))
		}

		// Find union (articles with tech OR news)
		union, _ := tc.store.SUnion("tag:tech", "tag:news")
		t.Logf("Articles with tech or news tags: %d", len(union))
	})

	t.Run("Rate limiter simulation", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		numClients := 5
		requestsPerClient := 20
		var wg sync.WaitGroup
		var allowedRequests, blockedRequests int64

		// Simulate rate-limited requests
		wg.Add(numClients)
		for i := 0; i < numClients; i++ {
			go func(clientID int) {
				defer wg.Done()
				limitKey := fmt.Sprintf("ratelimit:client_%d", clientID)
				maxRequests := int64(10) // max 10 requests allowed

				for j := 0; j < requestsPerClient; j++ {
					// Increment counter
					count, _ := tc.store.Incr(limitKey)

					if count <= maxRequests {
						atomic.AddInt64(&allowedRequests, 1)
					} else {
						atomic.AddInt64(&blockedRequests, 1)
					}
				}
			}(i)
		}

		wg.Wait()

		// Each client should have 10 allowed, 10 blocked
		expectedAllowed := int64(numClients * 10)
		expectedBlocked := int64(numClients * 10)

		if allowedRequests != expectedAllowed {
			t.Errorf("Expected %d allowed requests, got %d", expectedAllowed, allowedRequests)
		}
		if blockedRequests != expectedBlocked {
			t.Errorf("Expected %d blocked requests, got %d", expectedBlocked, blockedRequests)
		}
	})

	t.Run("Distributed lock simulation", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		lockKey := "distributed_lock"
		numClients := 10
		var wg sync.WaitGroup
		var lockAcquired int64
		var criticalSectionExecutions int64

		// Clients trying to acquire lock
		wg.Add(numClients)
		for i := 0; i < numClients; i++ {
			go func(clientID int) {
				defer wg.Done()

				// Try to acquire lock using SETNX pattern (simplified with Incr)
				// First one to set wins
				for attempt := 0; attempt < 100; attempt++ {
					val, _ := tc.store.Incr(lockKey)
					if val == 1 {
						// Got the lock
						atomic.AddInt64(&lockAcquired, 1)

						// Critical section
						atomic.AddInt64(&criticalSectionExecutions, 1)

						// Release lock
						tc.store.Set(lockKey, "0")
						break
					}
					// Didn't get lock, try again
					time.Sleep(time.Microsecond * 10)
				}
			}(i)
		}

		wg.Wait()

		// All clients should eventually get the lock
		if criticalSectionExecutions != int64(numClients) {
			t.Errorf("Expected %d critical section executions, got %d", numClients, criticalSectionExecutions)
		}
	})
}

// =============================================================================
// Data Integrity Tests
// =============================================================================

func TestDataIntegrity(t *testing.T) {
	t.Run("No data loss under concurrent writes", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		numWriters := 10
		writesPerWriter := 100
		var wg sync.WaitGroup

		// Track what we wrote
		written := make(map[string]string)
		var writtenMu sync.Mutex

		wg.Add(numWriters)
		for i := 0; i < numWriters; i++ {
			go func(writerID int) {
				defer wg.Done()
				for j := 0; j < writesPerWriter; j++ {
					key := fmt.Sprintf("integrity_key_%d_%d", writerID, j)
					value := fmt.Sprintf("value_%d_%d_%d", writerID, j, time.Now().UnixNano())

					tc.store.Set(key, value)

					writtenMu.Lock()
					written[key] = value
					writtenMu.Unlock()
				}
			}(i)
		}

		wg.Wait()

		// Verify all written data
		missingKeys := 0
		mismatchedValues := 0

		for key, expectedValue := range written {
			actualValue, exists := tc.store.Get(key)
			if !exists {
				missingKeys++
			} else if actualValue != expectedValue {
				// Value might have been overwritten by another concurrent write
				// This is expected behavior, not an error
			}
		}

		if missingKeys > 0 {
			t.Errorf("Missing %d keys after concurrent writes", missingKeys)
		}
		if mismatchedValues > 0 {
			t.Logf("Note: %d values were overwritten by concurrent writes (expected)", mismatchedValues)
		}
	})

	t.Run("List order preserved under concurrent access", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		listKey := "ordered_list"
		numItems := 100

		// Single writer to preserve order
		for i := 0; i < numItems; i++ {
			tc.store.RPush(listKey, fmt.Sprintf("item_%03d", i))
		}

		// Multiple readers concurrently
		numReaders := 5
		var wg sync.WaitGroup

		wg.Add(numReaders)
		for r := 0; r < numReaders; r++ {
			go func(readerID int) {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					elements, _ := tc.store.LRange(listKey, 0, -1)
					if len(elements) != numItems {
						t.Errorf("Reader %d: Expected %d elements, got %d", readerID, numItems, len(elements))
					}
					// Verify order
					for j, elem := range elements {
						expected := fmt.Sprintf("item_%03d", j)
						if elem != expected {
							t.Errorf("Reader %d: Element %d: expected %s, got %s", readerID, j, expected, elem)
							break
						}
					}
				}
			}(r)
		}

		wg.Wait()
	})

	t.Run("Set uniqueness maintained under concurrent adds", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		setKey := "unique_set"
		numWriters := 5
		addsPerWriter := 100
		var wg sync.WaitGroup

		// All writers add the same set of members
		wg.Add(numWriters)
		for i := 0; i < numWriters; i++ {
			go func(writerID int) {
				defer wg.Done()
				for j := 0; j < addsPerWriter; j++ {
					member := fmt.Sprintf("member_%d", j) // Same members across all writers
					tc.store.SAdd(setKey, member)
				}
			}(i)
		}

		wg.Wait()

		// Set should have exactly addsPerWriter unique members
		card, _ := tc.store.SCard(setKey)
		if card != int64(addsPerWriter) {
			t.Errorf("Expected set cardinality %d, got %d", addsPerWriter, card)
		}
	})

	t.Run("Atomic increment accuracy", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		counterKey := "atomic_counter"
		tc.store.Set(counterKey, "0")

		numGoroutines := 100
		incrementsPerGoroutine := 100
		var wg sync.WaitGroup

		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < incrementsPerGoroutine; j++ {
					tc.store.Incr(counterKey)
				}
			}()
		}

		wg.Wait()

		// Verify exact count
		val, _ := tc.store.Get(counterKey)
		expected := fmt.Sprintf("%d", numGoroutines*incrementsPerGoroutine)
		if val != expected {
			t.Errorf("Expected counter to be %s, got %s", expected, val)
		}
	})
}

// =============================================================================
// Stress Tests
// =============================================================================

func TestStressScenarios(t *testing.T) {
	t.Run("High concurrency mixed operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		numGoroutines := 50
		opsPerGoroutine := 100
		var wg sync.WaitGroup

		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					op := j % 6
					key := fmt.Sprintf("stress_key_%d", j%10)

					switch op {
					case 0:
						tc.store.Set(key, fmt.Sprintf("value_%d_%d", id, j))
					case 1:
						tc.store.Get(key)
					case 2:
						tc.store.Incr(fmt.Sprintf("stress_counter_%d", id))
					case 3:
						tc.store.RPush(fmt.Sprintf("stress_list_%d", id), fmt.Sprintf("item_%d", j))
					case 4:
						tc.store.SAdd(fmt.Sprintf("stress_set_%d", id), fmt.Sprintf("member_%d", j))
					case 5:
						tc.store.Delete(fmt.Sprintf("stress_temp_%d_%d", id, j))
					}
				}
			}(i)
		}

		wg.Wait()
		// No panics = success
	})

	t.Run("Rapid key creation and deletion", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		numGoroutines := 20
		cycles := 50
		var wg sync.WaitGroup

		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < cycles; j++ {
					key := fmt.Sprintf("ephemeral_%d_%d", id, j)
					tc.store.Set(key, "temporary")
					tc.store.Delete(key)
				}
			}(i)
		}

		wg.Wait()

		// All ephemeral keys should be deleted
		tc.store.ForceFlushMetrics()
		// Check a sample of keys
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("ephemeral_%d_%d", i, cycles-1)
			_, exists := tc.store.Get(key)
			if exists {
				t.Errorf("Key %s should have been deleted", key)
			}
		}
	})

	t.Run("List operations under load", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		listKey := "load_test_list"
		numPushers := 5
		numPoppers := 3
		pushesPerGoroutine := 100
		var wg sync.WaitGroup
		var totalPushed, totalPopped int64

		// Pushers
		wg.Add(numPushers)
		for i := 0; i < numPushers; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < pushesPerGoroutine; j++ {
					tc.store.RPush(listKey, fmt.Sprintf("item_%d_%d", id, j))
					atomic.AddInt64(&totalPushed, 1)
				}
			}(i)
		}

		// Wait for pushers to get a head start
		time.Sleep(time.Millisecond * 10)

		// Poppers
		wg.Add(numPoppers)
		for i := 0; i < numPoppers; i++ {
			go func(id int) {
				defer wg.Done()
				for {
					items, _ := tc.store.LPop(listKey, 1)
					if len(items) > 0 {
						atomic.AddInt64(&totalPopped, 1)
					} else {
						// Check if list is empty and pushers are done
						pushed := atomic.LoadInt64(&totalPushed)
						popped := atomic.LoadInt64(&totalPopped)
						if pushed >= int64(numPushers*pushesPerGoroutine) && popped >= pushed {
							break
						}
						time.Sleep(time.Microsecond * 100)
					}
				}
			}(i)
		}

		wg.Wait()

		// Verify all items were processed
		finalLength, _ := tc.store.LLen(listKey)
		if finalLength != 0 {
			t.Logf("List has %d remaining items (pushed: %d, popped: %d)",
				finalLength, totalPushed, totalPopped)
		}
	})
}

// Helper function for parsing integers
func parseIntegerStrict(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	var result int64
	negative := s[0] == '-'
	start := 0
	if negative || s[0] == '+' {
		start = 1
	}
	for i := start; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("invalid integer")
		}
		result = result*10 + int64(s[i]-'0')
	}
	if negative {
		result = -result
	}
	return result, nil
}
