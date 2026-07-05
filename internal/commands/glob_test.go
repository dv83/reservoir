package commands

import (
	"strings"
	"testing"
)

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pattern, name string
		want          bool
	}{
		{"*", "", true},
		{"*", "anything", true},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h*llo", "hello", true},
		{"h*llo", "heeeello", true},
		{"h*llo", "hallox", false},
		{"user:*", "user:42", true},
		{"user:*", "admin:42", false},
		{"*:42", "user:42", true},
		{"a*b*c", "axxbyyc", true},
		{"a*b*c", "axxbyy", false},
		{"[hj]ello", "hello", true},
		{"[hj]ello", "jello", true},
		{"[hj]ello", "yello", false},
		{"[^a]bc", "xbc", true},
		{"[^a]bc", "abc", false},
		{"key[0-9]", "key5", true},
		{"key[0-9]", "keyx", false},
		{"key[a-c]", "keyb", true},
		{"key[a-c]", "keyd", false},
		{"foo\\*bar", "foo*bar", true},
		{"foo\\*bar", "fooXbar", false},
		{"**", "abc", true},
	}
	for _, c := range cases {
		if got := globMatch(c.pattern, c.name); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.pattern, c.name, got, c.want)
		}
	}
}

// TestKeysGlob verifies the KEYS command filters by glob pattern, not just "*".
func TestKeysGlob(t *testing.T) {
	tc := newTestContext(t)
	defer tc.cleanup()

	tc.exec("SET", "user:1", "a")
	tc.exec("SET", "user:2", "b")
	tc.exec("SET", "admin:1", "c")

	resp := tc.exec("KEYS", "user:*")
	tc.expectContains(resp, "user:1", "KEYS user:* returns user:1")
	tc.expectContains(resp, "user:2", "KEYS user:* returns user:2")
	if strings.Contains(resp, "admin:1") {
		t.Errorf("KEYS user:* should not include admin:1, got: %q", resp)
	}

	// "*" still returns everything.
	all := tc.exec("KEYS", "*")
	tc.expectContains(all, "admin:1", "KEYS * returns admin:1")
}
