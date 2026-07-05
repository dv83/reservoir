package commands

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

func TestSetCommands(t *testing.T) {
	// Create store with reasonable limits
	limits := &store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024, // 100MB
	}
	kvStore := store.NewLockFreeStore(limits)
	defer kvStore.Stop()

	registry := NewZeroCopyRegistry()

	t.Run("SADD - Add members to set", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// Test SADD with multiple members
		cmd := createTestCommand([][]byte{[]byte("myset"), []byte("member1"), []byte("member2"), []byte("member3")})

		handler, exists := registry.GetHandler("SADD")
		if !exists {
			t.Fatal("SADD handler not found")
		}

		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SADD failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		// Should return :3 (3 members added)
		if !strings.Contains(response, ":3") {
			t.Errorf("Expected ':3' in response, got: %s", response)
		}
	})

	t.Run("SCARD - Get set cardinality", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		cmd := createTestCommand([][]byte{[]byte("myset")})

		handler, exists := registry.GetHandler("SCARD")
		if !exists {
			t.Fatal("SCARD handler not found")
		}

		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SCARD failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		// Should return :3 (3 members in set)
		if !strings.Contains(response, ":3") {
			t.Errorf("Expected ':3' in response, got: %s", response)
		}
	})

	t.Run("SISMEMBER - Check membership", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// Test existing member
		cmd := &protocol.ZeroCopyCommand{}
		cmd = createTestCommand([][]byte{[]byte("myset"), []byte("member1")})

		handler, exists := registry.GetHandler("SISMEMBER")
		if !exists {
			t.Fatal("SISMEMBER handler not found")
		}

		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SISMEMBER failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		// Should return :1 (member exists)
		if !strings.Contains(response, ":1") {
			t.Errorf("Expected ':1' in response, got: %s", response)
		}

		// Test non-existing member
		buf.Reset()
		cmd = createTestCommand([][]byte{[]byte("myset"), []byte("nonexistent")})

		err = handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SISMEMBER failed: %v", err)
		}

		writer.Flush()
		response = buf.String()

		// Should return :0 (member doesn't exist)
		if !strings.Contains(response, ":0") {
			t.Errorf("Expected ':0' in response, got: %s", response)
		}
	})

	t.Run("SMEMBERS - Get all members", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		cmd := createTestCommand([][]byte{[]byte("myset")})

		handler, exists := registry.GetHandler("SMEMBERS")
		if !exists {
			t.Fatal("SMEMBERS handler not found")
		}

		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SMEMBERS failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		// Should return array with 3 elements
		if !strings.Contains(response, "*3") {
			t.Errorf("Expected '*3' in response, got: %s", response)
		}

		// Should contain all members
		if !strings.Contains(response, "member1") || !strings.Contains(response, "member2") || !strings.Contains(response, "member3") {
			t.Errorf("Response should contain all members, got: %s", response)
		}
	})

	t.Run("SREM - Remove members", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// Remove one member
		cmd := &protocol.ZeroCopyCommand{}
		cmd = createTestCommand([][]byte{[]byte("myset"), []byte("member1")})

		handler, exists := registry.GetHandler("SREM")
		if !exists {
			t.Fatal("SREM handler not found")
		}

		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SREM failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		// Should return :1 (1 member removed)
		if !strings.Contains(response, ":1") {
			t.Errorf("Expected ':1' in response, got: %s", response)
		}

		// Verify cardinality is now 2
		buf.Reset()
		cmd = createTestCommand([][]byte{[]byte("myset")})
		handler, _ = registry.GetHandler("SCARD")
		err = handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SCARD failed: %v", err)
		}

		writer.Flush()
		response = buf.String()

		if !strings.Contains(response, ":2") {
			t.Errorf("Expected ':2' in response after removal, got: %s", response)
		}
	})

	t.Run("SPOP - Pop random member", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		cmd := createTestCommand([][]byte{[]byte("myset")})

		handler, exists := registry.GetHandler("SPOP")
		if !exists {
			t.Fatal("SPOP handler not found")
		}

		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SPOP failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		// Should return a bulk string (member2 or member3)
		if !strings.Contains(response, "$") {
			t.Errorf("Expected bulk string response, got: %s", response)
		}

		// Verify cardinality is now 1
		buf.Reset()
		cmd = createTestCommand([][]byte{[]byte("myset")})
		handler, _ = registry.GetHandler("SCARD")
		err = handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SCARD failed: %v", err)
		}

		writer.Flush()
		response = buf.String()

		if !strings.Contains(response, ":1") {
			t.Errorf("Expected ':1' in response after pop, got: %s", response)
		}
	})

	t.Run("Set operations on non-existent keys", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// SCARD on non-existent key
		cmd := &protocol.ZeroCopyCommand{}
		cmd = createTestCommand([][]byte{[]byte("nonexistent")})

		handler, _ := registry.GetHandler("SCARD")
		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SCARD on nonexistent key failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		if !strings.Contains(response, ":0") {
			t.Errorf("Expected ':0' for nonexistent key, got: %s", response)
		}

		// SMEMBERS on non-existent key
		buf.Reset()
		handler, _ = registry.GetHandler("SMEMBERS")
		err = handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SMEMBERS on nonexistent key failed: %v", err)
		}

		writer.Flush()
		response = buf.String()

		if !strings.Contains(response, "*0") {
			t.Errorf("Expected '*0' for nonexistent key, got: %s", response)
		}
	})

	t.Run("WRONGTYPE errors", func(t *testing.T) {
		// First set a string value
		kvStore.Set("stringkey", "stringvalue")

		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// Try SADD on string key
		cmd := &protocol.ZeroCopyCommand{}
		cmd = createTestCommand([][]byte{[]byte("stringkey"), []byte("member1")})

		handler, _ := registry.GetHandler("SADD")
		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SADD failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		if !strings.Contains(response, "WRONGTYPE") {
			t.Errorf("Expected WRONGTYPE error, got: %s", response)
		}
	})
}

func TestSetOperations(t *testing.T) {
	// Create store with reasonable limits
	limits := &store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024, // 100MB
	}
	kvStore := store.NewLockFreeStore(limits)
	defer kvStore.Stop()

	registry := NewZeroCopyRegistry()

	// Setup test sets
	setupTestSets := func() {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// Create set1: {a, b, c}
		cmd := createTestCommand([][]byte{[]byte("set1"), []byte("a"), []byte("b"), []byte("c")})
		handler, _ := registry.GetHandler("SADD")
		handler(kvStore, cmd, writer)

		// Create set2: {b, c, d}
		buf.Reset()
		cmd = createTestCommand([][]byte{[]byte("set2"), []byte("b"), []byte("c"), []byte("d")})
		handler(kvStore, cmd, writer)

		// Create set3: {c, d, e}
		buf.Reset()
		cmd = createTestCommand([][]byte{[]byte("set3"), []byte("c"), []byte("d"), []byte("e")})
		handler(kvStore, cmd, writer)
	}

	t.Run("SDIFF - Set difference", func(t *testing.T) {
		setupTestSets()

		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// SDIFF set1 set2 should return {a}
		cmd := createTestCommand([][]byte{[]byte("set1"), []byte("set2")})

		handler, exists := registry.GetHandler("SDIFF")
		if !exists {
			t.Fatal("SDIFF handler not found")
		}

		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SDIFF failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		// Should return array with 1 element
		if !strings.Contains(response, "*1") {
			t.Errorf("Expected '*1' in response, got: %s", response)
		}

		// Should contain 'a'
		if !strings.Contains(response, "a") {
			t.Errorf("Response should contain 'a', got: %s", response)
		}
	})

	t.Run("SINTER - Set intersection", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// SINTER set1 set2 should return {b, c}
		cmd := createTestCommand([][]byte{[]byte("set1"), []byte("set2")})

		handler, exists := registry.GetHandler("SINTER")
		if !exists {
			t.Fatal("SINTER handler not found")
		}

		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SINTER failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		// Should return array with 2 elements
		if !strings.Contains(response, "*2") {
			t.Errorf("Expected '*2' in response, got: %s", response)
		}

		// Should contain 'b' and 'c'
		if !strings.Contains(response, "b") || !strings.Contains(response, "c") {
			t.Errorf("Response should contain 'b' and 'c', got: %s", response)
		}
	})

	t.Run("SUNION - Set union", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// SUNION set1 set2 should return {a, b, c, d}
		cmd := createTestCommand([][]byte{[]byte("set1"), []byte("set2")})

		handler, exists := registry.GetHandler("SUNION")
		if !exists {
			t.Fatal("SUNION handler not found")
		}

		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SUNION failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		// Should return array with 4 elements
		if !strings.Contains(response, "*4") {
			t.Errorf("Expected '*4' in response, got: %s", response)
		}

		// Should contain all unique elements
		if !strings.Contains(response, "a") || !strings.Contains(response, "b") ||
			!strings.Contains(response, "c") || !strings.Contains(response, "d") {
			t.Errorf("Response should contain 'a', 'b', 'c', 'd', got: %s", response)
		}
	})

	t.Run("SDIFFSTORE - Store set difference", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// SDIFFSTORE result set1 set2 should store {a} in result
		cmd := createTestCommand([][]byte{[]byte("result"), []byte("set1"), []byte("set2")})

		handler, exists := registry.GetHandler("SDIFFSTORE")
		if !exists {
			t.Fatal("SDIFFSTORE handler not found")
		}

		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SDIFFSTORE failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		// Should return :1 (1 element stored)
		if !strings.Contains(response, ":1") {
			t.Errorf("Expected ':1' in response, got: %s", response)
		}

		// Verify the result set contains the expected element
		buf.Reset()
		cmd = createTestCommand([][]byte{[]byte("result")})
		handler, _ = registry.GetHandler("SMEMBERS")
		err = handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SMEMBERS on result set failed: %v", err)
		}

		writer.Flush()
		response = buf.String()

		// Should contain 'a'
		if !strings.Contains(response, "a") {
			t.Errorf("Result set should contain 'a', got: %s", response)
		}
	})

	t.Run("Set operations with non-existent keys", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// SDIFF with non-existent first key
		cmd := createTestCommand([][]byte{[]byte("nonexistent"), []byte("set1")})

		handler, _ := registry.GetHandler("SDIFF")
		err := handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SDIFF with nonexistent key failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		if !strings.Contains(response, "*0") {
			t.Errorf("Expected '*0' for nonexistent first key, got: %s", response)
		}

		// SINTER with non-existent key should return empty
		buf.Reset()
		cmd = createTestCommand([][]byte{[]byte("set1"), []byte("nonexistent")})
		handler, _ = registry.GetHandler("SINTER")
		err = handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("SINTER with nonexistent key failed: %v", err)
		}

		writer.Flush()
		response = buf.String()

		if !strings.Contains(response, "*0") {
			t.Errorf("Expected '*0' for intersection with nonexistent key, got: %s", response)
		}
	})
}

func TestSetCommandArguments(t *testing.T) {
	// Create store with reasonable limits
	limits := &store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024, // 100MB
	}
	kvStore := store.NewLockFreeStore(limits)
	defer kvStore.Stop()

	registry := NewZeroCopyRegistry()

	t.Run("Invalid argument counts", func(t *testing.T) {
		testCases := []struct {
			command string
			args    [][]byte
		}{
			{"SADD", [][]byte{[]byte("key")}},                                         // Need at least 2 args
			{"SREM", [][]byte{[]byte("key")}},                                         // Need at least 2 args
			{"SCARD", [][]byte{}},                                                     // Need exactly 1 arg
			{"SCARD", [][]byte{[]byte("key1"), []byte("key2")}},                       // Need exactly 1 arg
			{"SISMEMBER", [][]byte{[]byte("key")}},                                    // Need exactly 2 args
			{"SISMEMBER", [][]byte{[]byte("key"), []byte("member"), []byte("extra")}}, // Need exactly 2 args
			{"SPOP", [][]byte{}},                                                      // Need exactly 1 arg
			{"SMEMBERS", [][]byte{}},                                                  // Need exactly 1 arg
		}

		for _, tc := range testCases {
			t.Run(tc.command+" with invalid args", func(t *testing.T) {
				var buf bytes.Buffer
				writer := bufio.NewWriter(&buf)

				cmd := createTestCommand(tc.args)

				handler, exists := registry.GetHandler(tc.command)
				if !exists {
					t.Fatalf("%s handler not found", tc.command)
				}

				err := handler(kvStore, cmd, writer)
				if err != nil {
					t.Fatalf("%s failed: %v", tc.command, err)
				}

				writer.Flush()
				response := buf.String()

				if !strings.Contains(response, "-ERR") {
					t.Errorf("Expected error response for %s with invalid args, got: %s", tc.command, response)
				}
			})
		}
	})
}

// Helper function to create ZeroCopyCommand with args for testing
func createTestCommand(args [][]byte) *protocol.ZeroCopyCommand {
	return &protocol.ZeroCopyCommand{
		Command: []byte("TEST"), // Command name doesn't matter for these tests
		Args:    args,
		Buffer:  nil, // Buffer not needed for tests
	}
}
