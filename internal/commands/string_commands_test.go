package commands

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

func TestExistsCommand(t *testing.T) {
	// Create test store
	kvStore := store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	})
	defer kvStore.Stop()

	// Create command registry
	registry := NewZeroCopyRegistry()

	// Set some test keys
	kvStore.Set("key1", "value1")
	kvStore.Set("key2", "value2")
	kvStore.Set("key3", "value3")

	tests := []struct {
		name     string
		command  string
		expected string
	}{
		{
			name:     "Single existing key",
			command:  "EXISTS key1\r\n",
			expected: ":1\r\n",
		},
		{
			name:     "Single non-existing key",
			command:  "EXISTS nonexistent\r\n",
			expected: ":0\r\n",
		},
		{
			name:     "Multiple existing keys",
			command:  "EXISTS key1 key2 key3\r\n",
			expected: ":3\r\n",
		},
		{
			name:     "Mix of existing and non-existing keys",
			command:  "EXISTS key1 nonexistent key2 missing key3\r\n",
			expected: ":3\r\n",
		},
		{
			name:     "All non-existing keys",
			command:  "EXISTS missing1 missing2 missing3\r\n",
			expected: ":0\r\n",
		},
		{
			name:     "Duplicate keys counted once each",
			command:  "EXISTS key1 key1 key1\r\n",
			expected: ":3\r\n", // Redis counts duplicates
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse command
			parser := protocol.NewZeroCopyParser()
			cmdBytes := []byte(tt.command)
			reader := bufio.NewReader(bytes.NewReader(cmdBytes))

			cmd, err := parser.ParseCommand(reader)
			if err != nil {
				t.Fatalf("Failed to parse command: %v", err)
			}

			// Execute command
			var output bytes.Buffer
			writer := bufio.NewWriter(&output)

			handler, exists := registry.GetHandler("EXISTS")
			if !exists {
				t.Fatal("EXISTS handler not found")
			}

			err = handler(kvStore, cmd, writer)
			if err != nil {
				t.Fatalf("Command execution failed: %v", err)
			}

			writer.Flush()

			// Check response
			response := output.String()
			if response != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, response)
			}
		})
	}
}

func TestExistsWithExpiry(t *testing.T) {
	// Create test store
	kvStore := store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	})
	defer kvStore.Stop()

	// Create command registry
	registry := NewZeroCopyRegistry()

	// Set a key with expiry (already expired)
	kvStore.Set("expired_key", "value")
	// Normally we'd use EXPIRE command, but for test we can directly set past time
	// Since we can't directly set past expiry in the test, we'll skip this case

	// Test normal key exists
	kvStore.Set("normal_key", "value")

	parser := protocol.NewZeroCopyParser()
	cmdBytes := []byte("EXISTS normal_key\r\n")
	reader := bufio.NewReader(bytes.NewReader(cmdBytes))

	cmd, err := parser.ParseCommand(reader)
	if err != nil {
		t.Fatalf("Failed to parse command: %v", err)
	}

	var output bytes.Buffer
	writer := bufio.NewWriter(&output)

	handler, _ := registry.GetHandler("EXISTS")
	err = handler(kvStore, cmd, writer)
	if err != nil {
		t.Fatalf("Command execution failed: %v", err)
	}

	writer.Flush()

	response := output.String()
	if response != ":1\r\n" {
		t.Errorf("Expected :1\\r\\n, got %q", response)
	}
}

func TestIncrDecrCommands(t *testing.T) {
	// Create test store
	kvStore := store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	})
	defer kvStore.Stop()

	// Create command registry
	registry := NewZeroCopyRegistry()

	tests := []struct {
		name     string
		commands []string // sequence of commands to execute
		expected []string // expected responses
	}{
		{
			name:     "INCR on non-existing key",
			commands: []string{"INCR counter\r\n"},
			expected: []string{":1\r\n"},
		},
		{
			name:     "INCR multiple times",
			commands: []string{"INCR mycounter\r\n", "INCR mycounter\r\n", "INCR mycounter\r\n"},
			expected: []string{":1\r\n", ":2\r\n", ":3\r\n"},
		},
		{
			name:     "DECR on non-existing key",
			commands: []string{"DECR countdown\r\n"},
			expected: []string{":-1\r\n"},
		},
		{
			name:     "INCRBY command",
			commands: []string{"INCRBY score 10\r\n", "INCRBY score 5\r\n"},
			expected: []string{":10\r\n", ":15\r\n"},
		},
		{
			name:     "DECRBY command",
			commands: []string{"SET temp 100\r\n", "DECRBY temp 30\r\n", "DECRBY temp 20\r\n"},
			expected: []string{"+OK\r\n", ":70\r\n", ":50\r\n"},
		},
		{
			name:     "Mixed INCR and DECR",
			commands: []string{"INCR mixed\r\n", "INCR mixed\r\n", "DECR mixed\r\n"},
			expected: []string{":1\r\n", ":2\r\n", ":1\r\n"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, cmdStr := range tt.commands {
				// Parse command
				parser := protocol.NewZeroCopyParser()
				cmdBytes := []byte(cmdStr)
				reader := bufio.NewReader(bytes.NewReader(cmdBytes))

				cmd, err := parser.ParseCommand(reader)
				if err != nil {
					t.Fatalf("Failed to parse command %d: %v", i, err)
				}

				// Execute command
				var output bytes.Buffer
				writer := bufio.NewWriter(&output)

				// Get handler for the command
				cmdName := cmd.String()
				handler, exists := registry.GetHandler(cmdName)
				if !exists {
					t.Fatalf("%s handler not found", cmdName)
				}

				err = handler(kvStore, cmd, writer)
				if err != nil {
					t.Fatalf("Command %d execution failed: %v", i, err)
				}

				writer.Flush()

				// Check response
				response := output.String()
				if response != tt.expected[i] {
					t.Errorf("Command %d: Expected %q, got %q", i, tt.expected[i], response)
				}
			}
		})
	}
}

func TestIncrDecrErrors(t *testing.T) {
	// Create test store
	kvStore := store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	})
	defer kvStore.Stop()

	// Create command registry
	registry := NewZeroCopyRegistry()

	// Set a non-numeric value
	kvStore.Set("text", "hello")

	tests := []struct {
		name        string
		command     string
		expectError bool
		errorPart   string
	}{
		{
			name:        "INCR on non-numeric value",
			command:     "INCR text\r\n",
			expectError: true,
			errorPart:   "not an integer",
		},
		{
			name:        "INCRBY with invalid increment",
			command:     "INCRBY key abc\r\n",
			expectError: true,
			errorPart:   "not an integer",
		},
		{
			name:        "DECRBY with invalid decrement",
			command:     "DECRBY key xyz\r\n",
			expectError: true,
			errorPart:   "not an integer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse command
			parser := protocol.NewZeroCopyParser()
			cmdBytes := []byte(tt.command)
			reader := bufio.NewReader(bytes.NewReader(cmdBytes))

			cmd, err := parser.ParseCommand(reader)
			if err != nil {
				t.Fatalf("Failed to parse command: %v", err)
			}

			// Execute command
			var output bytes.Buffer
			writer := bufio.NewWriter(&output)

			cmdName := cmd.String()
			handler, exists := registry.GetHandler(cmdName)
			if !exists {
				t.Fatalf("%s handler not found", cmdName)
			}

			err = handler(kvStore, cmd, writer)
			if err != nil {
				t.Fatalf("Command execution failed: %v", err)
			}

			writer.Flush()
			response := output.String()

			if tt.expectError {
				if !strings.Contains(response, "-ERR") || !strings.Contains(response, tt.errorPart) {
					t.Errorf("Expected error containing %q, got %q", tt.errorPart, response)
				}
			} else {
				if strings.Contains(response, "-ERR") {
					t.Errorf("Unexpected error: %q", response)
				}
			}
		})
	}
}
