package commands

import (
	"bufio"
	"bytes"
	"strings"
	"testing"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

func TestGetSetCommand(t *testing.T) {
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
			name:     "GETSET on non-existing key",
			commands: []string{"GETSET newkey value1\r\n"},
			expected: []string{"$-1\r\n"}, // null response for non-existing key
		},
		{
			name: "GETSET on existing key",
			commands: []string{
				"SET mykey oldvalue\r\n",
				"GETSET mykey newvalue\r\n",
			},
			expected: []string{
				"+OK\r\n",
				"$8\r\noldvalue\r\n", // returns old value
			},
		},
		{
			name: "Multiple GETSET operations",
			commands: []string{
				"GETSET counter 1\r\n",
				"GETSET counter 2\r\n",
				"GETSET counter 3\r\n",
			},
			expected: []string{
				"$-1\r\n",     // first GETSET returns null
				"$1\r\n1\r\n", // second returns "1"
				"$1\r\n2\r\n", // third returns "2"
			},
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

func TestGetSetValueCheck(t *testing.T) {
	// Create test store
	kvStore := store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	})
	defer kvStore.Stop()

	// Create command registry
	registry := NewZeroCopyRegistry()

	// Test that GETSET actually sets the new value
	commands := []string{
		"GETSET testkey newvalue\r\n",
		"GET testkey\r\n",
	}
	expected := []string{
		"$-1\r\n",            // GETSET returns null for non-existing key
		"$8\r\nnewvalue\r\n", // GET returns the new value
	}

	for i, cmdStr := range commands {
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
		if response != expected[i] {
			t.Errorf("Command %d: Expected %q, got %q", i, expected[i], response)
		}
	}
}

func TestGetSetErrors(t *testing.T) {
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
			name:        "GETSET on list type",
			command:     "GETSET mylist value\r\n",
			expectError: true,
			errorPart:   "WRONGTYPE",
		},
		{
			name:        "GETSET with wrong arg count",
			command:     "GETSET onlykey\r\n",
			expectError: true,
			errorPart:   "wrong number of arguments",
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
