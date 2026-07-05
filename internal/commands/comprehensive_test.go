package commands

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"

	"reservoir/internal/store"
)

// =============================================================================
// Test Utilities
// =============================================================================

type testContext struct {
	store    *store.LockFreeStore
	registry *ZeroCopyRegistry
	t        *testing.T
}

func newTestContext(t *testing.T) *testContext {
	limits := &store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	}
	return &testContext{
		store:    store.NewLockFreeStore(limits),
		registry: NewZeroCopyRegistry(),
		t:        t,
	}
}

func (tc *testContext) cleanup() {
	tc.store.Stop()
}

func (tc *testContext) exec(command string, args ...string) string {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)

	byteArgs := make([][]byte, len(args))
	for i, arg := range args {
		byteArgs[i] = []byte(arg)
	}

	cmd := createTestCommand(byteArgs)
	handler, exists := tc.registry.GetHandler(command)
	if !exists {
		tc.t.Fatalf("%s handler not found", command)
	}

	err := handler(tc.store, cmd, writer)
	if err != nil {
		tc.t.Fatalf("%s failed: %v", command, err)
	}

	writer.Flush()
	return buf.String()
}

func (tc *testContext) expectContains(response, expected, description string) {
	if !strings.Contains(response, expected) {
		tc.t.Errorf("%s: expected response to contain %q, got: %q", description, expected, response)
	}
}

func (tc *testContext) expectNotContains(response, notExpected, description string) {
	if strings.Contains(response, notExpected) {
		tc.t.Errorf("%s: expected response NOT to contain %q, got: %q", description, notExpected, response)
	}
}

func (tc *testContext) expectEquals(response, expected, description string) {
	if response != expected {
		tc.t.Errorf("%s: expected %q, got: %q", description, expected, response)
	}
}

// =============================================================================
// STRING COMMANDS - Comprehensive Tests
// =============================================================================

func TestStringCommands_Comprehensive(t *testing.T) {
	t.Run("SET and GET basic operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Basic SET/GET
		resp := tc.exec("SET", "key1", "value1")
		tc.expectEquals(resp, "+OK\r\n", "SET should return OK")

		resp = tc.exec("GET", "key1")
		tc.expectContains(resp, "value1", "GET should return value")

		// GET non-existent key
		resp = tc.exec("GET", "nonexistent")
		tc.expectEquals(resp, "$-1\r\n", "GET nonexistent should return nil")

		// Overwrite existing key
		tc.exec("SET", "key1", "newvalue")
		resp = tc.exec("GET", "key1")
		tc.expectContains(resp, "newvalue", "GET should return overwritten value")
	})

	t.Run("SET with empty value", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("SET", "emptykey", "")
		tc.expectEquals(resp, "+OK\r\n", "SET with empty value should work")

		resp = tc.exec("GET", "emptykey")
		tc.expectContains(resp, "$0", "GET empty value should return zero-length bulk string")
	})

	t.Run("SET with special characters", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Value with spaces
		tc.exec("SET", "spacey", "hello world")
		resp := tc.exec("GET", "spacey")
		tc.expectContains(resp, "hello world", "GET should preserve spaces")

		// Value with unicode
		tc.exec("SET", "unicode", "привіт світ")
		resp = tc.exec("GET", "unicode")
		tc.expectContains(resp, "привіт світ", "GET should preserve unicode")

		// Value with newlines (as part of bulk string)
		tc.exec("SET", "multiline", "line1\nline2")
		resp = tc.exec("GET", "multiline")
		tc.expectContains(resp, "line1\nline2", "GET should preserve newlines")
	})

	t.Run("DEL operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Delete existing key
		tc.exec("SET", "delme", "value")
		resp := tc.exec("DEL", "delme")
		tc.expectContains(resp, ":1", "DEL existing key should return 1")

		// Verify deletion
		resp = tc.exec("GET", "delme")
		tc.expectEquals(resp, "$-1\r\n", "GET deleted key should return nil")

		// Delete non-existent key
		resp = tc.exec("DEL", "nonexistent")
		tc.expectContains(resp, ":0", "DEL nonexistent key should return 0")

		// Delete multiple keys
		tc.exec("SET", "k1", "v1")
		tc.exec("SET", "k2", "v2")
		tc.exec("SET", "k3", "v3")
		resp = tc.exec("DEL", "k1", "k2", "k3", "nonexistent")
		tc.expectContains(resp, ":3", "DEL multiple should return count of deleted")
	})

	t.Run("MSET and MGET operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// MSET multiple keys
		resp := tc.exec("MSET", "mk1", "mv1", "mk2", "mv2", "mk3", "mv3")
		tc.expectEquals(resp, "+OK\r\n", "MSET should return OK")

		// MGET all keys
		resp = tc.exec("MGET", "mk1", "mk2", "mk3")
		tc.expectContains(resp, "*3", "MGET should return array of 3")
		tc.expectContains(resp, "mv1", "MGET should contain mv1")
		tc.expectContains(resp, "mv2", "MGET should contain mv2")
		tc.expectContains(resp, "mv3", "MGET should contain mv3")

		// MGET with some non-existent keys
		resp = tc.exec("MGET", "mk1", "nonexistent", "mk3")
		tc.expectContains(resp, "*3", "MGET should return array of 3")
		tc.expectContains(resp, "$-1", "MGET should contain nil for nonexistent")
	})

	t.Run("MSET odd number of arguments", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("MSET", "key1", "value1", "key2")
		tc.expectContains(resp, "-ERR", "MSET with odd args should error")
	})

	t.Run("APPEND operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// APPEND to non-existent key (creates it)
		resp := tc.exec("APPEND", "appendkey", "hello")
		tc.expectContains(resp, ":5", "APPEND to new key should return length 5")

		// APPEND to existing key
		resp = tc.exec("APPEND", "appendkey", " world")
		tc.expectContains(resp, ":11", "APPEND should return new length")

		// Verify value
		resp = tc.exec("GET", "appendkey")
		tc.expectContains(resp, "hello world", "GET should return appended value")
	})

	t.Run("STRLEN operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SET", "strkey", "hello")
		resp := tc.exec("STRLEN", "strkey")
		tc.expectContains(resp, ":5", "STRLEN should return 5")

		// STRLEN on non-existent key
		resp = tc.exec("STRLEN", "nonexistent")
		tc.expectContains(resp, ":0", "STRLEN nonexistent should return 0")

		// STRLEN on empty value
		tc.exec("SET", "emptystr", "")
		resp = tc.exec("STRLEN", "emptystr")
		tc.expectContains(resp, ":0", "STRLEN empty should return 0")
	})

	t.Run("INCR and DECR operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// INCR on non-existent key (starts at 0)
		resp := tc.exec("INCR", "counter")
		tc.expectContains(resp, ":1", "INCR from 0 should return 1")

		resp = tc.exec("INCR", "counter")
		tc.expectContains(resp, ":2", "INCR should return 2")

		resp = tc.exec("DECR", "counter")
		tc.expectContains(resp, ":1", "DECR should return 1")

		// DECR below zero
		tc.exec("SET", "negcounter", "0")
		resp = tc.exec("DECR", "negcounter")
		tc.expectContains(resp, ":-1", "DECR should go negative")

		// INCR on non-numeric string
		tc.exec("SET", "notanumber", "hello")
		resp = tc.exec("INCR", "notanumber")
		tc.expectContains(resp, "-ERR", "INCR on non-number should error")
	})

	t.Run("INCRBY and DECRBY operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SET", "incrbykey", "10")
		resp := tc.exec("INCRBY", "incrbykey", "5")
		tc.expectContains(resp, ":15", "INCRBY 5 should return 15")

		resp = tc.exec("DECRBY", "incrbykey", "3")
		tc.expectContains(resp, ":12", "DECRBY 3 should return 12")

		// INCRBY with negative (acts like DECRBY)
		resp = tc.exec("INCRBY", "incrbykey", "-7")
		tc.expectContains(resp, ":5", "INCRBY -7 should return 5")
	})
}

// =============================================================================
// LIST COMMANDS - Comprehensive Tests
// =============================================================================

func TestListCommands_Comprehensive(t *testing.T) {
	t.Run("LPUSH and RPUSH basic operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// LPUSH creates list
		resp := tc.exec("LPUSH", "mylist", "c", "b", "a")
		tc.expectContains(resp, ":3", "LPUSH should return length 3")

		// Check order (LPUSH pushes from left, so a is first)
		resp = tc.exec("LRANGE", "mylist", "0", "-1")
		tc.expectContains(resp, "*3", "LRANGE should return 3 elements")

		// RPUSH adds to end
		resp = tc.exec("RPUSH", "mylist", "d", "e")
		tc.expectContains(resp, ":5", "RPUSH should return length 5")
	})

	t.Run("LPUSH and RPUSH on empty list", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("LPUSH", "newlist", "first")
		tc.expectContains(resp, ":1", "LPUSH on new key should return 1")

		resp = tc.exec("RPUSH", "anotherlist", "first")
		tc.expectContains(resp, ":1", "RPUSH on new key should return 1")
	})

	t.Run("LPOP and RPOP operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("RPUSH", "poplist", "a", "b", "c", "d")

		// LPOP
		resp := tc.exec("LPOP", "poplist")
		tc.expectContains(resp, "a", "LPOP should return first element")

		// RPOP
		resp = tc.exec("RPOP", "poplist")
		tc.expectContains(resp, "d", "RPOP should return last element")

		// LPOP with count
		resp = tc.exec("LPOP", "poplist", "2")
		tc.expectContains(resp, "*2", "LPOP with count should return array")
		tc.expectContains(resp, "b", "LPOP should contain b")
		tc.expectContains(resp, "c", "LPOP should contain c")

		// POP from empty list
		resp = tc.exec("LPOP", "poplist")
		tc.expectEquals(resp, "$-1\r\n", "LPOP empty list should return nil")
	})

	t.Run("LPOP and RPOP on non-existent key", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("LPOP", "nonexistent")
		tc.expectEquals(resp, "$-1\r\n", "LPOP nonexistent should return nil")

		resp = tc.exec("RPOP", "nonexistent")
		tc.expectEquals(resp, "$-1\r\n", "RPOP nonexistent should return nil")
	})

	t.Run("LLEN operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// LLEN on non-existent key
		resp := tc.exec("LLEN", "nonexistent")
		tc.expectContains(resp, ":0", "LLEN nonexistent should return 0")

		tc.exec("RPUSH", "lenlist", "a", "b", "c")
		resp = tc.exec("LLEN", "lenlist")
		tc.expectContains(resp, ":3", "LLEN should return 3")
	})

	t.Run("LRANGE operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("RPUSH", "rangelist", "a", "b", "c", "d", "e")

		// Full range
		resp := tc.exec("LRANGE", "rangelist", "0", "-1")
		tc.expectContains(resp, "*5", "LRANGE 0 -1 should return all 5")

		// Partial range
		resp = tc.exec("LRANGE", "rangelist", "1", "3")
		tc.expectContains(resp, "*3", "LRANGE 1 3 should return 3")
		tc.expectContains(resp, "b", "LRANGE should contain b")
		tc.expectContains(resp, "c", "LRANGE should contain c")
		tc.expectContains(resp, "d", "LRANGE should contain d")

		// Negative indices
		resp = tc.exec("LRANGE", "rangelist", "-3", "-1")
		tc.expectContains(resp, "*3", "LRANGE -3 -1 should return last 3")
		tc.expectContains(resp, "c", "LRANGE should contain c")
		tc.expectContains(resp, "d", "LRANGE should contain d")
		tc.expectContains(resp, "e", "LRANGE should contain e")

		// Out of range (should clamp)
		resp = tc.exec("LRANGE", "rangelist", "0", "100")
		tc.expectContains(resp, "*5", "LRANGE beyond end should return all")

		// Empty range (start > stop)
		resp = tc.exec("LRANGE", "rangelist", "3", "1")
		tc.expectContains(resp, "*0", "LRANGE with start > stop should return empty")
	})

	t.Run("LINDEX operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("RPUSH", "indexlist", "a", "b", "c", "d")

		resp := tc.exec("LINDEX", "indexlist", "0")
		tc.expectContains(resp, "a", "LINDEX 0 should return a")

		resp = tc.exec("LINDEX", "indexlist", "2")
		tc.expectContains(resp, "c", "LINDEX 2 should return c")

		// Negative index
		resp = tc.exec("LINDEX", "indexlist", "-1")
		tc.expectContains(resp, "d", "LINDEX -1 should return last element")

		resp = tc.exec("LINDEX", "indexlist", "-2")
		tc.expectContains(resp, "c", "LINDEX -2 should return second to last")

		// Out of range
		resp = tc.exec("LINDEX", "indexlist", "100")
		tc.expectEquals(resp, "$-1\r\n", "LINDEX out of range should return nil")

		resp = tc.exec("LINDEX", "indexlist", "-100")
		tc.expectEquals(resp, "$-1\r\n", "LINDEX negative out of range should return nil")
	})

	t.Run("LSET operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("RPUSH", "setlist", "a", "b", "c")

		resp := tc.exec("LSET", "setlist", "1", "B")
		tc.expectEquals(resp, "+OK\r\n", "LSET should return OK")

		resp = tc.exec("LINDEX", "setlist", "1")
		tc.expectContains(resp, "B", "LINDEX after LSET should return B")

		// LSET with negative index
		resp = tc.exec("LSET", "setlist", "-1", "C")
		tc.expectEquals(resp, "+OK\r\n", "LSET with negative index should work")

		resp = tc.exec("LINDEX", "setlist", "-1")
		tc.expectContains(resp, "C", "LINDEX -1 after LSET should return C")

		// LSET out of range
		resp = tc.exec("LSET", "setlist", "100", "X")
		tc.expectContains(resp, "-ERR", "LSET out of range should error")

		// LSET on non-existent key
		resp = tc.exec("LSET", "nonexistent", "0", "X")
		tc.expectContains(resp, "-ERR", "LSET on nonexistent should error")
	})

	t.Run("LREM operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("RPUSH", "remlist", "a", "b", "a", "c", "a", "d", "a")

		// Remove 2 occurrences from head
		resp := tc.exec("LREM", "remlist", "2", "a")
		tc.expectContains(resp, ":2", "LREM count 2 should remove 2")

		// Remove 1 from tail (negative count)
		resp = tc.exec("LREM", "remlist", "-1", "a")
		tc.expectContains(resp, ":1", "LREM count -1 should remove 1 from tail")

		// Remove all remaining
		resp = tc.exec("LREM", "remlist", "0", "a")
		tc.expectContains(resp, ":1", "LREM count 0 should remove all remaining")

		// Verify no 'a' left
		resp = tc.exec("LRANGE", "remlist", "0", "-1")
		tc.expectNotContains(resp, "$1\r\na", "List should not contain 'a'")
	})

	t.Run("LTRIM operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("RPUSH", "trimlist", "a", "b", "c", "d", "e")

		resp := tc.exec("LTRIM", "trimlist", "1", "3")
		tc.expectEquals(resp, "+OK\r\n", "LTRIM should return OK")

		resp = tc.exec("LRANGE", "trimlist", "0", "-1")
		tc.expectContains(resp, "*3", "LTRIM should leave 3 elements")
		tc.expectContains(resp, "b", "LTRIM should keep b")
		tc.expectContains(resp, "c", "LTRIM should keep c")
		tc.expectContains(resp, "d", "LTRIM should keep d")

		// LTRIM with negative indices
		tc.exec("RPUSH", "trimlist2", "1", "2", "3", "4", "5")
		resp = tc.exec("LTRIM", "trimlist2", "-3", "-1")
		tc.expectEquals(resp, "+OK\r\n", "LTRIM with negative should work")

		resp = tc.exec("LRANGE", "trimlist2", "0", "-1")
		tc.expectContains(resp, "*3", "LTRIM -3 -1 should leave last 3")

		// LTRIM that empties list
		tc.exec("RPUSH", "trimlist3", "a", "b", "c")
		resp = tc.exec("LTRIM", "trimlist3", "5", "10")
		tc.expectEquals(resp, "+OK\r\n", "LTRIM out of range should work")

		resp = tc.exec("LLEN", "trimlist3")
		tc.expectContains(resp, ":0", "LTRIM should result in empty list")
	})

	t.Run("LPUSHX and RPUSHX operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// LPUSHX on non-existent key should return 0
		resp := tc.exec("LPUSHX", "nonexistent", "value")
		tc.expectContains(resp, ":0", "LPUSHX on nonexistent should return 0")

		// Create list first
		tc.exec("RPUSH", "pushxlist", "initial")

		// Now LPUSHX should work
		resp = tc.exec("LPUSHX", "pushxlist", "prepended")
		tc.expectContains(resp, ":2", "LPUSHX on existing should return new length")

		// RPUSHX should also work
		resp = tc.exec("RPUSHX", "pushxlist", "appended")
		tc.expectContains(resp, ":3", "RPUSHX on existing should return new length")

		// Verify order
		resp = tc.exec("LRANGE", "pushxlist", "0", "-1")
		tc.expectContains(resp, "prepended", "List should have prepended")
		tc.expectContains(resp, "initial", "List should have initial")
		tc.expectContains(resp, "appended", "List should have appended")
	})

	t.Run("LPUSHUNIQUE and RPUSHUNIQUE operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Create list with some values
		tc.exec("RPUSH", "uniquelist", "a", "b", "c")

		// LPUSHUNIQUE with mix of existing and new
		resp := tc.exec("LPUSHUNIQUE", "uniquelist", "a", "d", "b", "e")
		// Should only add d and e (a and b already exist)
		tc.expectContains(resp, ":5", "LPUSHUNIQUE should return new length 5")

		// RPUSHUNIQUE with duplicates
		resp = tc.exec("RPUSHUNIQUE", "uniquelist", "c", "f", "a", "g")
		// Should only add f and g
		tc.expectContains(resp, ":7", "RPUSHUNIQUE should return new length 7")

		// Verify final state
		resp = tc.exec("LLEN", "uniquelist")
		tc.expectContains(resp, ":7", "List should have 7 unique elements")
	})

	t.Run("List edge cases - large list", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Push many elements
		args := make([]string, 1001)
		args[0] = "largelist"
		for i := 1; i <= 1000; i++ {
			args[i] = fmt.Sprintf("item%d", i)
		}

		resp := tc.exec("RPUSH", args...)
		tc.expectContains(resp, ":1000", "RPUSH 1000 items should return 1000")

		// LRANGE with limit
		resp = tc.exec("LRANGE", "largelist", "0", "9")
		tc.expectContains(resp, "*10", "LRANGE 0 9 should return 10")

		// LINDEX at end
		resp = tc.exec("LINDEX", "largelist", "999")
		tc.expectContains(resp, "item1000", "LINDEX 999 should return last item")
	})
}

// =============================================================================
// SET COMMANDS - Comprehensive Tests
// =============================================================================

func TestSetCommands_Comprehensive(t *testing.T) {
	t.Run("SADD basic operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Add members to new set
		resp := tc.exec("SADD", "myset", "a", "b", "c")
		tc.expectContains(resp, ":3", "SADD should return 3 (all new)")

		// Add mix of existing and new
		resp = tc.exec("SADD", "myset", "c", "d", "e")
		tc.expectContains(resp, ":2", "SADD should return 2 (only d, e are new)")

		// Add only existing members
		resp = tc.exec("SADD", "myset", "a", "b")
		tc.expectContains(resp, ":0", "SADD existing members should return 0")
	})

	t.Run("SREM operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "remset", "a", "b", "c", "d")

		// Remove existing members
		resp := tc.exec("SREM", "remset", "a", "c")
		tc.expectContains(resp, ":2", "SREM should return 2")

		// Remove non-existing and existing
		resp = tc.exec("SREM", "remset", "b", "nonexistent")
		tc.expectContains(resp, ":1", "SREM should return 1")

		// Remove from non-existent set
		resp = tc.exec("SREM", "nonexistent", "member")
		tc.expectContains(resp, ":0", "SREM from nonexistent should return 0")
	})

	t.Run("SCARD operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "cardset", "a", "b", "c")
		resp := tc.exec("SCARD", "cardset")
		tc.expectContains(resp, ":3", "SCARD should return 3")

		// SCARD on non-existent set
		resp = tc.exec("SCARD", "nonexistent")
		tc.expectContains(resp, ":0", "SCARD nonexistent should return 0")
	})

	t.Run("SISMEMBER operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "ismemberset", "apple", "banana", "cherry")

		resp := tc.exec("SISMEMBER", "ismemberset", "apple")
		tc.expectContains(resp, ":1", "SISMEMBER existing should return 1")

		resp = tc.exec("SISMEMBER", "ismemberset", "grape")
		tc.expectContains(resp, ":0", "SISMEMBER nonexistent member should return 0")

		// SISMEMBER on non-existent set
		resp = tc.exec("SISMEMBER", "nonexistent", "member")
		tc.expectContains(resp, ":0", "SISMEMBER on nonexistent set should return 0")
	})

	t.Run("SMEMBERS operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "membersset", "x", "y", "z")

		resp := tc.exec("SMEMBERS", "membersset")
		tc.expectContains(resp, "*3", "SMEMBERS should return 3 members")
		tc.expectContains(resp, "x", "SMEMBERS should contain x")
		tc.expectContains(resp, "y", "SMEMBERS should contain y")
		tc.expectContains(resp, "z", "SMEMBERS should contain z")

		// SMEMBERS on non-existent set
		resp = tc.exec("SMEMBERS", "nonexistent")
		tc.expectContains(resp, "*0", "SMEMBERS nonexistent should return empty array")
	})

	t.Run("SPOP operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "popset", "a", "b", "c")

		resp := tc.exec("SPOP", "popset")
		// Response should be a bulk string with one of a, b, c
		if !strings.Contains(resp, "$1") {
			tc.t.Errorf("SPOP should return bulk string, got: %s", resp)
		}

		// Verify cardinality decreased
		resp = tc.exec("SCARD", "popset")
		tc.expectContains(resp, ":2", "SCARD after SPOP should return 2")

		// SPOP from non-existent set
		resp = tc.exec("SPOP", "nonexistent")
		tc.expectEquals(resp, "$-1\r\n", "SPOP nonexistent should return nil")
	})

	t.Run("SDIFF operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "set1", "a", "b", "c", "d")
		tc.exec("SADD", "set2", "c", "d", "e")

		resp := tc.exec("SDIFF", "set1", "set2")
		tc.expectContains(resp, "*2", "SDIFF should return 2 elements")
		tc.expectContains(resp, "a", "SDIFF should contain a")
		tc.expectContains(resp, "b", "SDIFF should contain b")

		// SDIFF with multiple sets
		tc.exec("SADD", "set3", "b")
		resp = tc.exec("SDIFF", "set1", "set2", "set3")
		tc.expectContains(resp, "*1", "SDIFF with 3 sets should return 1 element")
		tc.expectContains(resp, "a", "SDIFF should contain only a")
	})

	t.Run("SINTER operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "int1", "a", "b", "c")
		tc.exec("SADD", "int2", "b", "c", "d")
		tc.exec("SADD", "int3", "c", "d", "e")

		resp := tc.exec("SINTER", "int1", "int2")
		tc.expectContains(resp, "*2", "SINTER should return 2 elements")
		tc.expectContains(resp, "b", "SINTER should contain b")
		tc.expectContains(resp, "c", "SINTER should contain c")

		// SINTER with 3 sets
		resp = tc.exec("SINTER", "int1", "int2", "int3")
		tc.expectContains(resp, "*1", "SINTER with 3 sets should return 1")
		tc.expectContains(resp, "c", "SINTER should contain only c")

		// SINTER with empty set
		resp = tc.exec("SINTER", "int1", "nonexistent")
		tc.expectContains(resp, "*0", "SINTER with nonexistent should return empty")
	})

	t.Run("SUNION operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "un1", "a", "b")
		tc.exec("SADD", "un2", "b", "c")

		resp := tc.exec("SUNION", "un1", "un2")
		tc.expectContains(resp, "*3", "SUNION should return 3 unique elements")
		tc.expectContains(resp, "a", "SUNION should contain a")
		tc.expectContains(resp, "b", "SUNION should contain b")
		tc.expectContains(resp, "c", "SUNION should contain c")

		// SUNION with non-existent set
		resp = tc.exec("SUNION", "un1", "nonexistent")
		tc.expectContains(resp, "*2", "SUNION with nonexistent should return un1")
	})

	t.Run("SDIFFSTORE operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "ds1", "a", "b", "c")
		tc.exec("SADD", "ds2", "b", "c", "d")

		resp := tc.exec("SDIFFSTORE", "result", "ds1", "ds2")
		tc.expectContains(resp, ":1", "SDIFFSTORE should return 1 (element stored)")

		resp = tc.exec("SMEMBERS", "result")
		tc.expectContains(resp, "a", "Result set should contain a")
	})
}

// =============================================================================
// WRONGTYPE Error Tests
// =============================================================================

func TestWRONGTYPE_Errors(t *testing.T) {
	t.Run("List commands on string", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SET", "stringkey", "value")

		// Try list operations on string
		commands := []struct {
			cmd  string
			args []string
		}{
			{"LPUSH", []string{"stringkey", "elem"}},
			{"RPUSH", []string{"stringkey", "elem"}},
			{"LPOP", []string{"stringkey"}},
			{"RPOP", []string{"stringkey"}},
			{"LLEN", []string{"stringkey"}},
			{"LRANGE", []string{"stringkey", "0", "-1"}},
			{"LINDEX", []string{"stringkey", "0"}},
			{"LSET", []string{"stringkey", "0", "val"}},
		}

		for _, c := range commands {
			resp := tc.exec(c.cmd, c.args...)
			tc.expectContains(resp, "WRONGTYPE", fmt.Sprintf("%s on string should return WRONGTYPE", c.cmd))
		}
	})

	t.Run("Set commands on string", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SET", "stringkey", "value")

		commands := []struct {
			cmd  string
			args []string
		}{
			{"SADD", []string{"stringkey", "member"}},
			{"SREM", []string{"stringkey", "member"}},
			{"SCARD", []string{"stringkey"}},
			{"SISMEMBER", []string{"stringkey", "member"}},
			{"SMEMBERS", []string{"stringkey"}},
			{"SPOP", []string{"stringkey"}},
		}

		for _, c := range commands {
			resp := tc.exec(c.cmd, c.args...)
			tc.expectContains(resp, "WRONGTYPE", fmt.Sprintf("%s on string should return WRONGTYPE", c.cmd))
		}
	})

	t.Run("String commands on list", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("RPUSH", "listkey", "elem1", "elem2")

		// APPEND on list should fail
		resp := tc.exec("APPEND", "listkey", "text")
		tc.expectContains(resp, "WRONGTYPE", "APPEND on list should return WRONGTYPE")

		// STRLEN on list should fail
		resp = tc.exec("STRLEN", "listkey")
		tc.expectContains(resp, "WRONGTYPE", "STRLEN on list should return WRONGTYPE")

		// INCR on list should fail
		resp = tc.exec("INCR", "listkey")
		tc.expectContains(resp, "WRONGTYPE", "INCR on list should return WRONGTYPE")
	})

	t.Run("String commands on set", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "setkey", "member1", "member2")

		// APPEND on set should fail
		resp := tc.exec("APPEND", "setkey", "text")
		tc.expectContains(resp, "WRONGTYPE", "APPEND on set should return WRONGTYPE")

		// STRLEN on set should fail
		resp = tc.exec("STRLEN", "setkey")
		tc.expectContains(resp, "WRONGTYPE", "STRLEN on set should return WRONGTYPE")

		// INCR on set should fail
		resp = tc.exec("INCR", "setkey")
		tc.expectContains(resp, "WRONGTYPE", "INCR on set should return WRONGTYPE")
	})

	t.Run("List commands on set", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "setkey", "member")

		commands := []struct {
			cmd  string
			args []string
		}{
			{"LPUSH", []string{"setkey", "elem"}},
			{"RPUSH", []string{"setkey", "elem"}},
			{"LPOP", []string{"setkey"}},
			{"RPOP", []string{"setkey"}},
			{"LLEN", []string{"setkey"}},
			{"LRANGE", []string{"setkey", "0", "-1"}},
		}

		for _, c := range commands {
			resp := tc.exec(c.cmd, c.args...)
			tc.expectContains(resp, "WRONGTYPE", fmt.Sprintf("%s on set should return WRONGTYPE", c.cmd))
		}
	})

	t.Run("Set commands on list", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("RPUSH", "listkey", "elem")

		commands := []struct {
			cmd  string
			args []string
		}{
			{"SADD", []string{"listkey", "member"}},
			{"SREM", []string{"listkey", "member"}},
			{"SCARD", []string{"listkey"}},
			{"SISMEMBER", []string{"listkey", "member"}},
			{"SMEMBERS", []string{"listkey"}},
			{"SPOP", []string{"listkey"}},
		}

		for _, c := range commands {
			resp := tc.exec(c.cmd, c.args...)
			tc.expectContains(resp, "WRONGTYPE", fmt.Sprintf("%s on list should return WRONGTYPE", c.cmd))
		}
	})
}

// =============================================================================
// Concurrent Operations Tests
// =============================================================================

func TestConcurrentOperations(t *testing.T) {
	t.Run("Concurrent string operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		var wg sync.WaitGroup
		numGoroutines := 10
		opsPerGoroutine := 100

		// Concurrent SET
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					key := fmt.Sprintf("concurrent_key_%d_%d", id, j)
					value := fmt.Sprintf("value_%d_%d", id, j)
					tc.store.Set(key, value)
				}
			}(i)
		}
		wg.Wait()

		// Verify data integrity
		tc.store.ForceFlushMetrics()
		keyCount := tc.store.GetKeyCount()
		expected := numGoroutines * opsPerGoroutine
		if keyCount != int64(expected) {
			t.Errorf("Expected %d keys, got %d", expected, keyCount)
		}
	})

	t.Run("Concurrent list operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		var wg sync.WaitGroup
		numGoroutines := 10
		opsPerGoroutine := 50

		// Create initial list
		tc.exec("RPUSH", "concurrent_list", "initial")

		// Concurrent RPUSH
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					elem := fmt.Sprintf("elem_%d_%d", id, j)
					tc.store.RPush("concurrent_list", elem)
				}
			}(i)
		}
		wg.Wait()

		// Verify list length
		length, _ := tc.store.LLen("concurrent_list")
		expected := int64(1 + (numGoroutines * opsPerGoroutine)) // initial + concurrent
		if length != expected {
			t.Errorf("Expected list length %d, got %d", expected, length)
		}
	})

	t.Run("Concurrent set operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		var wg sync.WaitGroup
		numGoroutines := 10
		opsPerGoroutine := 50

		// Concurrent SADD with unique members
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < opsPerGoroutine; j++ {
					member := fmt.Sprintf("member_%d_%d", id, j)
					tc.store.SAdd("concurrent_set", member)
				}
			}(i)
		}
		wg.Wait()

		// Verify set cardinality
		cardinality, _ := tc.store.SCard("concurrent_set")
		expected := int64(numGoroutines * opsPerGoroutine)
		if cardinality != expected {
			t.Errorf("Expected set cardinality %d, got %d", expected, cardinality)
		}
	})

	t.Run("Mixed concurrent operations", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		var wg sync.WaitGroup
		numGoroutines := 5
		opsPerType := 20

		// Concurrent mixed operations
		wg.Add(numGoroutines * 3) // 3 operation types

		// String writers
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < opsPerType; j++ {
					key := fmt.Sprintf("str_%d_%d", id, j)
					tc.store.Set(key, "value")
				}
			}(i)
		}

		// List writers
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < opsPerType; j++ {
					key := fmt.Sprintf("list_%d", id)
					tc.store.RPush(key, fmt.Sprintf("elem_%d", j))
				}
			}(i)
		}

		// Set writers
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < opsPerType; j++ {
					key := fmt.Sprintf("set_%d", id)
					tc.store.SAdd(key, fmt.Sprintf("member_%d", j))
				}
			}(i)
		}

		wg.Wait()

		// No panics = success, data integrity verified by operation counts
	})
}

// =============================================================================
// Complex Scenarios
// =============================================================================

func TestComplexScenarios(t *testing.T) {
	t.Run("List as queue (FIFO)", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Enqueue items
		for i := 1; i <= 5; i++ {
			tc.exec("RPUSH", "queue", fmt.Sprintf("task%d", i))
		}

		// Dequeue and verify order
		for i := 1; i <= 5; i++ {
			resp := tc.exec("LPOP", "queue")
			expected := fmt.Sprintf("task%d", i)
			tc.expectContains(resp, expected, "Queue should dequeue in FIFO order")
		}

		// Queue should be empty
		resp := tc.exec("LLEN", "queue")
		tc.expectContains(resp, ":0", "Queue should be empty")
	})

	t.Run("List as stack (LIFO)", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Push items
		for i := 1; i <= 5; i++ {
			tc.exec("LPUSH", "stack", fmt.Sprintf("item%d", i))
		}

		// Pop and verify order (LIFO)
		for i := 5; i >= 1; i-- {
			resp := tc.exec("LPOP", "stack")
			expected := fmt.Sprintf("item%d", i)
			tc.expectContains(resp, expected, "Stack should pop in LIFO order")
		}
	})

	t.Run("Set as unique collection", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Add duplicates
		tc.exec("SADD", "tags", "go", "redis", "database")
		tc.exec("SADD", "tags", "go", "golang", "database")
		tc.exec("SADD", "tags", "redis", "cache", "nosql")

		// Should have unique values only
		resp := tc.exec("SCARD", "tags")
		tc.expectContains(resp, ":6", "Set should have 6 unique tags")
	})

	t.Run("Counter with expiry check", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Create counter
		tc.exec("SET", "visits", "0")

		// Increment multiple times
		for i := 0; i < 10; i++ {
			tc.exec("INCR", "visits")
		}

		resp := tc.exec("GET", "visits")
		tc.expectContains(resp, "10", "Counter should be 10")

		// Decrement
		tc.exec("DECRBY", "visits", "3")
		resp = tc.exec("GET", "visits")
		tc.expectContains(resp, "7", "Counter should be 7 after decrement")
	})

	t.Run("Overwrite key with different type", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Create string
		tc.exec("SET", "mykey", "string_value")
		resp := tc.exec("GET", "mykey")
		tc.expectContains(resp, "string_value", "Should be string")

		// Delete and create list
		tc.exec("DEL", "mykey")
		tc.exec("RPUSH", "mykey", "list_elem")
		resp = tc.exec("LLEN", "mykey")
		tc.expectContains(resp, ":1", "Should be list with 1 element")

		// Delete and create set
		tc.exec("DEL", "mykey")
		tc.exec("SADD", "mykey", "set_member")
		resp = tc.exec("SCARD", "mykey")
		tc.expectContains(resp, ":1", "Should be set with 1 member")
	})

	t.Run("Bulk operations performance", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Prepare bulk MSET args (100 key-value pairs = 200 args)
		args := make([]string, 200)
		for i := 0; i < 100; i++ {
			args[i*2] = fmt.Sprintf("bulk_key_%d", i)
			args[i*2+1] = fmt.Sprintf("bulk_value_%d", i)
		}

		// MSET 100 keys
		resp := tc.exec("MSET", args...)
		tc.expectEquals(resp, "+OK\r\n", "Bulk MSET should succeed")

		// MGET all keys
		getArgs := make([]string, 100)
		for i := 0; i < 100; i++ {
			getArgs[i] = fmt.Sprintf("bulk_key_%d", i)
		}
		resp = tc.exec("MGET", getArgs...)
		tc.expectContains(resp, "*100", "Bulk MGET should return 100 elements")
	})
}

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

func TestEdgeCases(t *testing.T) {
	t.Run("Empty key name", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Most commands should handle empty key gracefully
		resp := tc.exec("GET", "")
		tc.expectEquals(resp, "$-1\r\n", "GET empty key should return nil")

		resp = tc.exec("SET", "", "value")
		tc.expectEquals(resp, "+OK\r\n", "SET empty key should work")

		resp = tc.exec("GET", "")
		tc.expectContains(resp, "value", "GET empty key should return value")
	})

	t.Run("Very long key and value", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		longKey := strings.Repeat("k", 500)
		longValue := strings.Repeat("v", 10000)

		resp := tc.exec("SET", longKey, longValue)
		tc.expectEquals(resp, "+OK\r\n", "SET long key/value should work")

		resp = tc.exec("GET", longKey)
		tc.expectContains(resp, longValue, "GET should return long value")
	})

	t.Run("Binary-safe values", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Value with null bytes and special chars
		binaryValue := "hello\x00world\x01\x02\x03"

		tc.store.Set("binary", binaryValue)
		result, _ := tc.store.Get("binary")

		if result != binaryValue {
			t.Errorf("Binary value not preserved: expected %q, got %q", binaryValue, result)
		}
	})

	t.Run("Negative list indices edge cases", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("RPUSH", "neglist", "a", "b", "c")

		// LRANGE with both negative
		resp := tc.exec("LRANGE", "neglist", "-3", "-1")
		tc.expectContains(resp, "*3", "LRANGE -3 -1 should return all 3")

		// Single element access with negative
		resp = tc.exec("LINDEX", "neglist", "-1")
		tc.expectContains(resp, "c", "LINDEX -1 should return c")

		resp = tc.exec("LINDEX", "neglist", "-3")
		tc.expectContains(resp, "a", "LINDEX -3 should return a")
	})

	t.Run("LPOP/RPOP count edge cases", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("RPUSH", "poplist", "a", "b", "c")

		// Pop more than available
		resp := tc.exec("LPOP", "poplist", "10")
		tc.expectContains(resp, "*3", "LPOP count > len should return all available")

		// Pop from empty list
		resp = tc.exec("LPOP", "poplist", "1")
		tc.expectEquals(resp, "$-1\r\n", "LPOP from empty should return nil")
	})

	t.Run("INCR/DECR overflow protection", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Test with very large number
		tc.exec("SET", "bignum", "9223372036854775800")
		resp := tc.exec("INCR", "bignum")
		tc.expectContains(resp, ":", "INCR large number should work")

		// Test near minimum
		tc.exec("SET", "smallnum", "-9223372036854775800")
		resp = tc.exec("DECR", "smallnum")
		tc.expectContains(resp, ":", "DECR near minimum should work")
	})

	t.Run("Set operations with self", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("SADD", "selfset", "a", "b", "c")

		// SDIFF with itself should be empty
		resp := tc.exec("SDIFF", "selfset", "selfset")
		tc.expectContains(resp, "*0", "SDIFF with self should be empty")

		// SINTER with itself should return all
		resp = tc.exec("SINTER", "selfset", "selfset")
		tc.expectContains(resp, "*3", "SINTER with self should return all")

		// SUNION with itself should return all (no duplicates)
		resp = tc.exec("SUNION", "selfset", "selfset")
		tc.expectContains(resp, "*3", "SUNION with self should return all once")
	})
}

// =============================================================================
// Argument Validation Tests
// =============================================================================

func TestArgumentValidation(t *testing.T) {
	t.Run("Commands with wrong argument count", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Commands that need specific argument counts
		testCases := []struct {
			cmd      string
			args     []string
			hasError bool
		}{
			// SET needs exactly 2 args
			{"SET", []string{"key"}, true},
			{"SET", []string{}, true},

			// GET needs exactly 1 arg
			{"GET", []string{}, true},
			{"GET", []string{"key1", "key2"}, true},

			// DEL needs at least 1 arg
			{"DEL", []string{}, true},

			// LRANGE needs exactly 3 args
			{"LRANGE", []string{"key"}, true},
			{"LRANGE", []string{"key", "0"}, true},

			// LINDEX needs exactly 2 args
			{"LINDEX", []string{"key"}, true},

			// LSET needs exactly 3 args
			{"LSET", []string{"key", "0"}, true},
		}

		for _, tc2 := range testCases {
			resp := tc.exec(tc2.cmd, tc2.args...)
			if tc2.hasError {
				tc.expectContains(resp, "-ERR", fmt.Sprintf("%s with wrong args should error", tc2.cmd))
			}
		}
	})

	t.Run("Invalid numeric arguments", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("RPUSH", "numlist", "a", "b", "c")

		// Non-numeric index for LINDEX
		resp := tc.exec("LINDEX", "numlist", "notanumber")
		tc.expectContains(resp, "-ERR", "LINDEX with non-numeric should error")

		// Non-numeric for LSET
		resp = tc.exec("LSET", "numlist", "abc", "value")
		tc.expectContains(resp, "-ERR", "LSET with non-numeric index should error")

		// Non-numeric for INCRBY
		tc.exec("SET", "numkey", "10")
		resp = tc.exec("INCRBY", "numkey", "notanumber")
		tc.expectContains(resp, "-ERR", "INCRBY with non-numeric should error")
	})
}
