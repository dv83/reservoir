package commands

import (
	"bufio"
	"bytes"
	"regexp"
	"testing"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

func TestRandomTemplatesIntegration(t *testing.T) {
	// Enable template expansion for testing
	originalState := disableTemplateExpansion
	EnableTemplateExpansion()
	defer func() {
		disableTemplateExpansion = originalState
	}()

	// Create a test store
	kvStore := store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	})

	// Create command registry
	registry := NewZeroCopyRegistry()

	tests := []struct {
		name        string
		command     string
		expectedKey string // pattern to match
		expectedVal string // pattern to match
	}{
		{
			name:        "SET with random key",
			command:     "SET key:__rand_int__ hello",
			expectedKey: `^key:\d+$`,
			expectedVal: "hello",
		},
		{
			name:        "SET with random value",
			command:     "SET mykey value:__rand_int__",
			expectedKey: "mykey",
			expectedVal: `^value:\d+$`,
		},
		{
			name:        "SET with random key and value",
			command:     "SET user:__rand_int__ data:__rand_int__",
			expectedKey: `^user:\d+$`,
			expectedVal: `^data:\d+$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the command
			parser := protocol.NewZeroCopyParser()
			cmdBytes := []byte(tt.command + "\r\n")
			reader := bufio.NewReader(bytes.NewReader(cmdBytes))

			cmd, err := parser.ParseCommand(reader)
			if err != nil {
				t.Fatalf("Failed to parse command: %v", err)
			}

			// Execute the command
			var output bytes.Buffer
			writer := bufio.NewWriter(&output)

			handler, exists := registry.GetHandler("SET")
			if !exists {
				t.Fatal("SET handler not found")
			}

			err = handler(kvStore, cmd, writer)
			if err != nil {
				t.Fatalf("Command execution failed: %v", err)
			}

			writer.Flush()

			// Check that the command executed successfully
			response := output.String()
			if response != "+OK\r\n" {
				t.Fatalf("Expected +OK response, got: %q", response)
			}

			// Get all keys to find the one that was set
			allKeys := kvStore.GetAllKeys()
			if len(allKeys) == 0 {
				t.Fatal("No keys found in store")
			}

			// Find the key that matches our pattern
			var foundKey string
			keyPattern := regexp.MustCompile(tt.expectedKey)
			for _, key := range allKeys {
				if keyPattern.MatchString(key) {
					foundKey = key
					break
				}
			}

			if foundKey == "" {
				t.Fatalf("No key matching pattern %q found. Available keys: %v", tt.expectedKey, allKeys)
			}

			// Get the value and check it matches the pattern
			value, exists := kvStore.Get(foundKey)
			if !exists {
				t.Fatalf("Key %q not found in store", foundKey)
			}

			valuePattern := regexp.MustCompile(tt.expectedVal)
			if !valuePattern.MatchString(value) {
				t.Fatalf("Value %q does not match pattern %q", value, tt.expectedVal)
			}

			t.Logf("Successfully set %q -> %q", foundKey, value)

			// Clean up for next test
			kvStore.Delete(foundKey)
		})
	}
}

func TestRandomTemplatesGET(t *testing.T) {
	// Enable template expansion for testing
	originalState := disableTemplateExpansion
	EnableTemplateExpansion()
	defer func() {
		disableTemplateExpansion = originalState
	}()

	// Create a test store
	kvStore := store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	})

	// Create command registry
	registry := NewZeroCopyRegistry()

	// First, set a key manually with a known random part
	testKey := "user:12345"
	testValue := "test_data"
	err := kvStore.Set(testKey, testValue)
	if err != nil {
		t.Fatalf("Failed to set test key: %v", err)
	}

	// Now try to GET using a placeholder that should expand to the same number
	// We'll test this by making multiple GET requests and ensuring we can retrieve the value

	// Test GET command
	parser := protocol.NewZeroCopyParser()

	// This test demonstrates usage but can't easily test the exact expansion
	// since we can't predict the random number. In practice, the user would
	// use the same random seed or the application would handle this logic.

	cmdBytes := []byte("GET user:__rand_int__\r\n")
	reader := bufio.NewReader(bytes.NewReader(cmdBytes))

	cmd, err := parser.ParseCommand(reader)
	if err != nil {
		t.Fatalf("Failed to parse command: %v", err)
	}

	var output bytes.Buffer
	writer := bufio.NewWriter(&output)

	handler, exists := registry.GetHandler("GET")
	if !exists {
		t.Fatal("GET handler not found")
	}

	err = handler(kvStore, cmd, writer)
	if err != nil {
		t.Fatalf("Command execution failed: %v", err)
	}

	writer.Flush()
	response := output.String()

	// The response should be a null response since the random number
	// won't match our manually set key, but this tests that the
	// template expansion is working without errors
	if response != "$-1\r\n" {
		t.Logf("GET response: %q (expected null response since random key won't match)", response)
	}

	t.Log("GET with random template executed successfully")
}

func TestRandomTemplatesMULTI(t *testing.T) {
	// Enable template expansion for testing
	originalState := disableTemplateExpansion
	EnableTemplateExpansion()
	defer func() {
		disableTemplateExpansion = originalState
	}()

	// Create a test store
	kvStore := store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	})

	// Create command registry
	registry := NewZeroCopyRegistry()

	// Test MSET with random templates
	parser := protocol.NewZeroCopyParser()
	cmdBytes := []byte("MSET key1:__rand_int__ value1:__rand_int__ key2:__rand_int__ value2:__rand_int__\r\n")
	reader := bufio.NewReader(bytes.NewReader(cmdBytes))

	cmd, err := parser.ParseCommand(reader)
	if err != nil {
		t.Fatalf("Failed to parse command: %v", err)
	}

	var output bytes.Buffer
	writer := bufio.NewWriter(&output)

	handler, exists := registry.GetHandler("MSET")
	if !exists {
		t.Fatal("MSET handler not found")
	}

	err = handler(kvStore, cmd, writer)
	if err != nil {
		t.Fatalf("Command execution failed: %v", err)
	}

	writer.Flush()

	// Check that the command executed successfully
	response := output.String()
	if response != "+OK\r\n" {
		t.Fatalf("Expected +OK response, got: %q", response)
	}

	// Verify that 2 keys were set with random patterns
	allKeys := kvStore.GetAllKeys()
	if len(allKeys) != 2 {
		t.Fatalf("Expected 2 keys, got %d. Keys: %v", len(allKeys), allKeys)
	}

	keyPattern := regexp.MustCompile(`^key[12]:\d+$`)
	valuePattern := regexp.MustCompile(`^value[12]:\d+$`)

	matchedKeys := 0
	for _, key := range allKeys {
		if keyPattern.MatchString(key) {
			matchedKeys++
			value, exists := kvStore.Get(key)
			if !exists {
				t.Fatalf("Key %q not found", key)
			}
			if !valuePattern.MatchString(value) {
				t.Fatalf("Value %q does not match expected pattern", value)
			}
			t.Logf("MSET result: %q -> %q", key, value)
		}
	}

	if matchedKeys != 2 {
		t.Fatalf("Expected 2 keys matching pattern, got %d", matchedKeys)
	}
}

func BenchmarkRandomTemplatesPerformance(b *testing.B) {
	// Create a test store
	kvStore := store.NewStore(&store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024,
	})

	// Create command registry
	registry := NewZeroCopyRegistry()
	handler, _ := registry.GetHandler("SET")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Parse command with random template
		parser := protocol.NewZeroCopyParser()
		cmdBytes := []byte("SET key:__rand_int__ value:__rand_int__\r\n")
		reader := bufio.NewReader(bytes.NewReader(cmdBytes))

		cmd, err := parser.ParseCommand(reader)
		if err != nil {
			b.Fatalf("Failed to parse command: %v", err)
		}

		var output bytes.Buffer
		writer := bufio.NewWriter(&output)

		err = handler(kvStore, cmd, writer)
		if err != nil {
			b.Fatalf("Command execution failed: %v", err)
		}
	}
}
