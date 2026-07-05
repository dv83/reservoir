package commands

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

func TestAppendCommand(t *testing.T) {
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
		commands []string
		expected []string
	}{
		{
			name:     "APPEND to non-existing key",
			commands: []string{"APPEND newkey hello\r\n"},
			expected: []string{":5\r\n"}, // returns length of new string
		},
		{
			name:     "APPEND to existing key",
			commands: []string{"SET mykey hello\r\n", "APPEND mykey world\r\n"},
			expected: []string{"+OK\r\n", ":10\r\n"}, // "helloworld" = 10 chars
		},
		{
			name:     "Multiple APPEND operations",
			commands: []string{"APPEND text a\r\n", "APPEND text b\r\n", "APPEND text c\r\n"},
			expected: []string{":1\r\n", ":2\r\n", ":3\r\n"},
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

func TestStrLenCommand(t *testing.T) {
	// Create test store
	kvStore := store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	})
	defer kvStore.Stop()

	// Create command registry
	registry := NewZeroCopyRegistry()

	// Set up test data
	kvStore.Set("string1", "hello")
	kvStore.Set("string2", "world!")
	kvStore.Set("empty", "")

	tests := []struct {
		name     string
		command  string
		expected string
	}{
		{
			name:     "STRLEN existing string",
			command:  "STRLEN string1\r\n",
			expected: ":5\r\n",
		},
		{
			name:     "STRLEN longer string",
			command:  "STRLEN string2\r\n",
			expected: ":6\r\n",
		},
		{
			name:     "STRLEN empty string",
			command:  "STRLEN empty\r\n",
			expected: ":0\r\n",
		},
		{
			name:     "STRLEN non-existing key",
			command:  "STRLEN nonexistent\r\n",
			expected: ":0\r\n",
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

			handler, exists := registry.GetHandler("STRLEN")
			if !exists {
				t.Fatal("STRLEN handler not found")
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

func TestAppendStrLenErrors(t *testing.T) {
	// Create test store
	kvStore := store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	})
	defer kvStore.Stop()

	// Create command registry
	registry := NewZeroCopyRegistry()

	// Create a list value to test WRONGTYPE
	kvStore.LPush("mylist", "element")

	tests := []struct {
		name        string
		command     string
		expectError bool
		errorPart   string
	}{
		{
			name:        "APPEND on list type",
			command:     "APPEND mylist value\r\n",
			expectError: true,
			errorPart:   "WRONGTYPE",
		},
		{
			name:        "STRLEN on list type returns WRONGTYPE",
			command:     "STRLEN mylist\r\n",
			expectError: true,
			errorPart:   "WRONGTYPE",
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
