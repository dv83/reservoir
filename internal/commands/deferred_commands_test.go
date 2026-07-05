package commands

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
	"time"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

// =============================================================================
// DEFER Command Tests
// =============================================================================

func TestDeferCommand(t *testing.T) {
	t.Run("DEFER basic SET command", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Schedule a SET command with 1 second delay
		resp := tc.exec("DEFER", "1", "SET", "deferred_key", "deferred_value")

		// Should return a command ID (bulk string)
		if !strings.HasPrefix(resp, "$") {
			t.Errorf("Expected bulk string response (command ID), got: %s", resp)
		}

		// Key should not exist yet
		val, exists := tc.store.Get("deferred_key")
		if exists {
			t.Errorf("Key should not exist yet, but got: %s", val)
		}

		// Wait for deferred command to execute
		time.Sleep(1500 * time.Millisecond)

		// Now key should exist
		val, exists = tc.store.Get("deferred_key")
		if !exists {
			t.Error("Key should exist after deferred command executed")
		}
		if val != "deferred_value" {
			t.Errorf("Expected 'deferred_value', got: %s", val)
		}
	})

	t.Run("DEFER INCR command", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Set initial value
		tc.store.Set("counter", "10")

		// Schedule INCR
		resp := tc.exec("DEFER", "1", "INCR", "counter")
		if !strings.HasPrefix(resp, "$") {
			t.Errorf("Expected command ID, got: %s", resp)
		}

		// Value should still be 10
		val, _ := tc.store.Get("counter")
		if val != "10" {
			t.Errorf("Counter should still be 10, got: %s", val)
		}

		// Wait for execution
		time.Sleep(1500 * time.Millisecond)

		// Now should be 11
		val, _ = tc.store.Get("counter")
		if val != "11" {
			t.Errorf("Counter should be 11 after INCR, got: %s", val)
		}
	})

	t.Run("DEFER INCRBY command", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.store.Set("counter", "100")

		tc.exec("DEFER", "1", "INCRBY", "counter", "50")

		time.Sleep(1500 * time.Millisecond)

		val, _ := tc.store.Get("counter")
		if val != "150" {
			t.Errorf("Counter should be 150, got: %s", val)
		}
	})

	t.Run("DEFER DEL command", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.store.Set("to_delete", "value")

		tc.exec("DEFER", "1", "DEL", "to_delete")

		// Key should still exist
		_, exists := tc.store.Get("to_delete")
		if !exists {
			t.Error("Key should still exist before deferred DEL")
		}

		time.Sleep(1500 * time.Millisecond)

		// Now should be deleted
		_, exists = tc.store.Get("to_delete")
		if exists {
			t.Error("Key should be deleted after deferred DEL")
		}
	})

	t.Run("DEFER APPEND command", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.store.Set("appendkey", "Hello")

		tc.exec("DEFER", "1", "APPEND", "appendkey", " World")

		val, _ := tc.store.Get("appendkey")
		if val != "Hello" {
			t.Errorf("Value should still be 'Hello', got: %s", val)
		}

		time.Sleep(1500 * time.Millisecond)

		val, _ = tc.store.Get("appendkey")
		if val != "Hello World" {
			t.Errorf("Value should be 'Hello World', got: %s", val)
		}
	})

	t.Run("DEFER with zero delay executes quickly", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		tc.exec("DEFER", "0", "SET", "immediate_key", "immediate_value")

		// Give scheduler time to process (scheduler runs every 1 second)
		time.Sleep(1500 * time.Millisecond)

		val, exists := tc.store.Get("immediate_key")
		if !exists {
			t.Error("Key should exist after zero-delay command")
		}
		if val != "immediate_value" {
			t.Errorf("Expected 'immediate_value', got: %s", val)
		}
	})

	t.Run("DEFER error - wrong number of arguments", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("DEFER", "1", "SET") // missing key and value
		tc.expectContains(resp, "-ERR", "Should return error for missing args")
	})

	t.Run("DEFER error - invalid delay", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("DEFER", "notanumber", "SET", "key", "value")
		tc.expectContains(resp, "-ERR", "Should return error for invalid delay")

		resp = tc.exec("DEFER", "-5", "SET", "key", "value")
		tc.expectContains(resp, "-ERR", "Should return error for negative delay")
	})

	t.Run("DEFER error - non-write command", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("DEFER", "1", "GET", "somekey")
		tc.expectContains(resp, "-ERR", "Should return error for non-write command")
		tc.expectContains(resp, "write commands", "Error should mention write commands")
	})

	t.Run("DEFER error - SET without value", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("DEFER", "1", "SET", "key") // missing value
		tc.expectContains(resp, "-ERR", "Should return error for SET without value")
	})

	t.Run("DEFER error - INCRBY with non-integer", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("DEFER", "1", "INCRBY", "key", "notanumber")
		tc.expectContains(resp, "-ERR", "Should return error for non-integer increment")
	})
}

// =============================================================================
// DEFER.CANCEL Command Tests
// =============================================================================

func TestDeferCancelCommand(t *testing.T) {
	t.Run("Cancel scheduled command", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Schedule a command with longer delay
		resp := tc.exec("DEFER", "5", "SET", "cancelled_key", "value")

		// Extract command ID from response
		commandID := extractBulkString(resp)
		if commandID == "" {
			t.Fatalf("Failed to get command ID from response: %s", resp)
		}

		// Cancel the command
		resp = tc.exec("DEFER.CANCEL", commandID)
		tc.expectContains(resp, ":1", "Cancel should return 1 for success")

		// Wait a bit and verify key was never created
		time.Sleep(500 * time.Millisecond)
		_, exists := tc.store.Get("cancelled_key")
		if exists {
			t.Error("Cancelled command should not have executed")
		}
	})

	t.Run("Cancel non-existent command", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("DEFER.CANCEL", "non_existent_id")
		tc.expectContains(resp, ":0", "Cancel should return 0 for non-existent ID")
	})

	t.Run("Cancel already executed command", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Schedule with zero delay (executes immediately)
		resp := tc.exec("DEFER", "0", "SET", "executed_key", "value")
		commandID := extractBulkString(resp)

		// Wait for execution (scheduler runs every 1 second)
		time.Sleep(1500 * time.Millisecond)

		// Try to cancel - should fail (already executed)
		resp = tc.exec("DEFER.CANCEL", commandID)
		tc.expectContains(resp, ":0", "Cancel should return 0 for already executed command")
	})

	t.Run("Cancel error - wrong number of arguments", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("DEFER.CANCEL") // missing command ID
		tc.expectContains(resp, "-ERR", "Should return error for missing args")
	})
}

// =============================================================================
// DEFER.LIST Command Tests
// =============================================================================

func TestDeferListCommand(t *testing.T) {
	t.Run("List scheduled commands", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Schedule multiple commands
		tc.exec("DEFER", "10", "SET", "key1", "value1")
		tc.exec("DEFER", "10", "SET", "key2", "value2")
		tc.exec("DEFER", "10", "INCR", "counter")

		// List all scheduled commands
		resp := tc.exec("DEFER.LIST")

		// Should return array with 3 commands
		tc.expectContains(resp, "*3", "Should have 3 scheduled commands")
	})

	t.Run("List empty when no scheduled commands", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("DEFER.LIST")
		tc.expectContains(resp, "*0", "Should return empty array")
	})

	t.Run("List error - wrong number of arguments", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("DEFER.LIST", "extra_arg")
		tc.expectContains(resp, "-ERR", "Should return error for extra args")
	})
}

// =============================================================================
// DEFER.STATS Command Tests
// =============================================================================

func TestDeferStatsCommand(t *testing.T) {
	t.Run("Get deferred command statistics", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Schedule some commands
		tc.exec("DEFER", "0", "SET", "key1", "value1")
		tc.exec("DEFER", "10", "SET", "key2", "value2")

		time.Sleep(200 * time.Millisecond) // Wait for zero-delay to execute

		resp := tc.exec("DEFER.STATS")

		// Should return array of key-value pairs
		if !strings.HasPrefix(resp, "*") {
			t.Errorf("Expected array response, got: %s", resp)
		}
	})

	t.Run("Stats error - wrong number of arguments", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		resp := tc.exec("DEFER.STATS", "extra_arg")
		tc.expectContains(resp, "-ERR", "Should return error for extra args")
	})
}

// =============================================================================
// DEFER Integration Tests
// =============================================================================

func TestDeferIntegration(t *testing.T) {
	t.Run("Multiple deferred commands execute in order", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Set initial value
		tc.store.Set("ordered_key", "0")

		// Schedule increments with different delays
		tc.exec("DEFER", "1", "SET", "ordered_key", "first")
		tc.exec("DEFER", "2", "SET", "ordered_key", "second")
		tc.exec("DEFER", "3", "SET", "ordered_key", "third")

		// Wait for all to execute
		time.Sleep(3500 * time.Millisecond)

		val, _ := tc.store.Get("ordered_key")
		if val != "third" {
			t.Errorf("Expected 'third' (last scheduled), got: %s", val)
		}
	})

	t.Run("Deferred command on non-existent key", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Schedule SET on non-existent key
		tc.exec("DEFER", "1", "SET", "new_key", "created")

		time.Sleep(1500 * time.Millisecond)

		val, exists := tc.store.Get("new_key")
		if !exists {
			t.Error("Key should exist after deferred SET")
		}
		if val != "created" {
			t.Errorf("Expected 'created', got: %s", val)
		}
	})

	t.Run("Cancel one of multiple scheduled commands", func(t *testing.T) {
		tc := newTestContext(t)
		defer tc.cleanup()

		// Schedule multiple commands
		resp1 := tc.exec("DEFER", "5", "SET", "key1", "value1")
		resp2 := tc.exec("DEFER", "5", "SET", "key2", "value2")
		tc.exec("DEFER", "5", "SET", "key3", "value3")

		id1 := extractBulkString(resp1)
		id2 := extractBulkString(resp2)

		// Cancel command 2
		tc.exec("DEFER.CANCEL", id2)

		// Verify list shows 2 remaining
		resp := tc.exec("DEFER.LIST")
		tc.expectContains(resp, "*2", "Should have 2 remaining commands after cancel")

		// Command 1 ID should still be in the list
		tc.expectContains(resp, id1, "Command 1 should still be scheduled")
	})
}

// =============================================================================
// Helper Functions
// =============================================================================

// extractBulkString extracts the string value from a RESP bulk string response
func extractBulkString(resp string) string {
	// Format: $<len>\r\n<data>\r\n
	if !strings.HasPrefix(resp, "$") {
		return ""
	}

	lines := strings.Split(resp, "\r\n")
	if len(lines) < 2 {
		return ""
	}

	return lines[1]
}

// TestDeferCommandDirect tests DEFER using direct handler calls
func TestDeferCommandDirect(t *testing.T) {
	limits := &store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	}
	kvStore := store.NewLockFreeStore(limits)
	defer kvStore.Stop()

	registry := NewZeroCopyRegistry()

	t.Run("Direct DEFER handler call", func(t *testing.T) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)

		// Create command
		parser := protocol.NewZeroCopyParser()
		cmdBytes := []byte("*5\r\n$5\r\nDEFER\r\n$1\r\n1\r\n$3\r\nSET\r\n$7\r\ntestkey\r\n$9\r\ntestvalue\r\n")
		reader := bufio.NewReader(bytes.NewReader(cmdBytes))

		cmd, err := parser.ParseCommand(reader)
		if err != nil {
			t.Fatalf("Failed to parse command: %v", err)
		}

		handler, exists := registry.GetHandler("DEFER")
		if !exists {
			t.Fatal("DEFER handler not found")
		}

		err = handler(kvStore, cmd, writer)
		if err != nil {
			t.Fatalf("DEFER handler failed: %v", err)
		}

		writer.Flush()
		response := buf.String()

		// Should return command ID
		if !strings.HasPrefix(response, "$") {
			t.Errorf("Expected bulk string response, got: %s", response)
		}

		// Wait and verify
		time.Sleep(1500 * time.Millisecond)
		val, exists := kvStore.Get("testkey")
		if !exists {
			t.Error("Key should exist after deferred command")
		}
		if val != "testvalue" {
			t.Errorf("Expected 'testvalue', got: %s", val)
		}
	})
}
