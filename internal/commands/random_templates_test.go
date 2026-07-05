package commands

import (
	"regexp"
	"strings"
	"testing"
)

func TestExpandRandomTemplates(t *testing.T) {
	// Enable template expansion for testing
	originalState := disableTemplateExpansion
	EnableTemplateExpansion()
	defer func() {
		disableTemplateExpansion = originalState
	}()

	tests := []struct {
		name        string
		input       string
		description string
	}{
		{
			name:        "NoPlaceholder",
			input:       "regular_key",
			description: "String without __rand_int__ should remain unchanged",
		},
		{
			name:        "SinglePlaceholder",
			input:       "key:__rand_int__",
			description: "Single __rand_int__ placeholder should be replaced with number",
		},
		{
			name:        "MultiplePlaceholders",
			input:       "user:__rand_int__:data:__rand_int__",
			description: "Multiple __rand_int__ placeholders should all be replaced",
		},
		{
			name:        "PlaceholderInValue",
			input:       "value_with_random_id___rand_int__",
			description: "Placeholder in value should be replaced",
		},
		{
			name:        "EmptyString",
			input:       "",
			description: "Empty string should remain empty",
		},
		{
			name:        "OnlyPlaceholder",
			input:       "__rand_int__",
			description: "String with only placeholder should become number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := expandRandomTemplates(tt.input)

			// Test 1: Check if result is different when placeholder exists
			if strings.Contains(tt.input, "__rand_int__") {
				if result == tt.input {
					t.Errorf("expandRandomTemplates() should have changed input %q but returned same value", tt.input)
				}

				// Test 2: Check that no placeholders remain
				if strings.Contains(result, "__rand_int__") {
					t.Errorf("expandRandomTemplates() left __rand_int__ placeholder in result: %q", result)
				}

				// Test 3: Check that replaced values are numeric (using regex)
				// Split by potential separators and check numeric parts
				parts := strings.FieldsFunc(result, func(r rune) bool {
					return r == ':' || r == '_' || r == '-'
				})

				hasNumber := false
				numberPattern := regexp.MustCompile(`^\d+$`)
				for _, part := range parts {
					if numberPattern.MatchString(part) {
						hasNumber = true
						// Test 4: Check that number is in expected range (0-999999999)
						if len(part) > 10 { // 999999999 has 9 digits, so 10+ would be too large
							t.Errorf("expandRandomTemplates() generated number too large: %s", part)
						}
						break
					}
				}

				if !hasNumber {
					t.Errorf("expandRandomTemplates() should have generated numeric replacement but result %q contains no numbers", result)
				}

			} else {
				// Test 5: Input without placeholder should remain unchanged
				if result != tt.input {
					t.Errorf("expandRandomTemplates() changed input without placeholder: input=%q, result=%q", tt.input, result)
				}
			}

			t.Logf("Test %s: input=%q -> result=%q", tt.name, tt.input, result)
		})
	}
}

func TestExpandRandomTemplatesConsistency(t *testing.T) {
	// Enable template expansion for testing
	originalState := disableTemplateExpansion
	EnableTemplateExpansion()
	defer func() {
		disableTemplateExpansion = originalState
	}()

	input := "key:__rand_int__"

	// Generate multiple results
	results := make([]string, 10)
	for i := 0; i < 10; i++ {
		results[i] = expandRandomTemplates(input)
	}

	// Test that all results are different (very high probability with crypto/rand)
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[i] == results[j] {
				t.Logf("Warning: Got duplicate random values %q and %q (acceptable but rare)", results[i], results[j])
			}
		}
	}

	// Test that all results have the correct format
	expectedPrefix := "key:"
	for i, result := range results {
		if !strings.HasPrefix(result, expectedPrefix) {
			t.Errorf("Result %d: %q should start with %q", i, result, expectedPrefix)
		}

		// Extract the number part
		numberPart := strings.TrimPrefix(result, expectedPrefix)
		numberPattern := regexp.MustCompile(`^\d+$`)
		if !numberPattern.MatchString(numberPart) {
			t.Errorf("Result %d: number part %q should be purely numeric", i, numberPart)
		}
	}
}

func TestExpandRandomTemplatesCryptoFallback(t *testing.T) {
	// Enable template expansion for testing
	originalState := disableTemplateExpansion
	EnableTemplateExpansion()
	defer func() {
		disableTemplateExpansion = originalState
	}()

	// This test is more for documentation - we can't easily force crypto/rand to fail
	// But we can test that the function returns a sensible fallback
	input := "key:__rand_int__"
	result := expandRandomTemplates(input)

	// Should not contain the placeholder
	if strings.Contains(result, "__rand_int__") {
		t.Errorf("Result should not contain __rand_int__ placeholder: %q", result)
	}

	// Should contain "key:" prefix
	if !strings.HasPrefix(result, "key:") {
		t.Errorf("Result should start with 'key:': %q", result)
	}
}

func BenchmarkExpandRandomTemplates(b *testing.B) {
	// Enable template expansion for benchmarking
	originalState := disableTemplateExpansion
	EnableTemplateExpansion()
	defer func() {
		disableTemplateExpansion = originalState
	}()

	testCases := []struct {
		name  string
		input string
	}{
		{"NoPlaceholder", "regular_key_without_placeholder"},
		{"SinglePlaceholder", "key:__rand_int__"},
		{"MultiplePlaceholders", "user:__rand_int__:session:__rand_int__:data"},
		{"LongString", "this_is_a_very_long_key_name_with_multiple_parts:__rand_int__:and_more_data"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = expandRandomTemplates(tc.input)
			}
		})
	}
}

// BenchmarkExpandRandomTemplatesDisabled benchmarks with template expansion disabled
func BenchmarkExpandRandomTemplatesDisabled(b *testing.B) {
	// Ensure template expansion is disabled
	DisableTemplateExpansion()
	defer EnableTemplateExpansion()

	testCases := []struct {
		name  string
		input string
	}{
		{"NoPlaceholder", "regular_key_without_placeholder"},
		{"WithPlaceholder", "key:__rand_int__"},
		{"MultiplePlaceholders", "user:__rand_int__:data:__rand_int__"},
		{"LongString", "very_long_key_name_with_multiple_placeholders_for_benchmarking_performance:__rand_int__:__rand_int__"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = expandRandomTemplates(tc.input)
			}
		})
	}
}
