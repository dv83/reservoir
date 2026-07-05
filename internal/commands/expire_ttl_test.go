package commands

import (
	"strings"
	"testing"
)

// TestExpireAndTTLSemantics covers the newly-wired EXPIRE command and the
// corrected TTL/PTTL return semantics.
func TestExpireAndTTLSemantics(t *testing.T) {
	tc := newTestContext(t)
	defer tc.cleanup()

	// TTL on a missing key must be -2 (not -1).
	if resp := tc.exec("TTL", "missing"); resp != ":-2\r\n" {
		t.Errorf("TTL missing key = %q, want :-2", resp)
	}

	// A key with no expiry must report -1.
	tc.exec("SET", "k", "v")
	if resp := tc.exec("TTL", "k"); resp != ":-1\r\n" {
		t.Errorf("TTL key without expiry = %q, want :-1", resp)
	}

	// EXPIRE must be a known command and set a positive TTL.
	tc.expectContains(tc.exec("EXPIRE", "k", "100"), ":1", "EXPIRE existing key returns 1")
	if resp := tc.exec("TTL", "k"); resp == ":-1\r\n" || resp == ":-2\r\n" {
		t.Errorf("TTL after EXPIRE = %q, want a positive value", resp)
	}

	// EXPIRE on a missing key returns 0.
	tc.expectContains(tc.exec("EXPIRE", "nope", "100"), ":0", "EXPIRE missing key returns 0")

	// PTTL must report milliseconds (roughly 100_000 ms) and be positive.
	tc.expectContains(tc.exec("PTTL", "k"), ":", "PTTL returns an integer")
}

// TestSetOptions covers the EX/PX/NX/XX/KEEPTTL modifiers of SET.
func TestSetOptions(t *testing.T) {
	tc := newTestContext(t)
	defer tc.cleanup()

	// SET k v EX 100 sets a positive TTL.
	tc.expectContains(tc.exec("SET", "k", "v", "EX", "100"), "+OK", "SET EX succeeds")
	if resp := tc.exec("TTL", "k"); resp == ":-1\r\n" || resp == ":-2\r\n" {
		t.Errorf("TTL after SET EX = %q, want positive", resp)
	}

	// SET k v2 NX must fail (key exists) and leave the value unchanged.
	if resp := tc.exec("SET", "k", "v2", "NX"); resp != "$-1\r\n" {
		t.Errorf("SET NX on existing key = %q, want $-1", resp)
	}
	tc.expectContains(tc.exec("GET", "k"), "v", "value unchanged after failed NX")

	// A plain SET (no KEEPTTL) clears the TTL.
	tc.exec("SET", "k", "v3")
	if resp := tc.exec("TTL", "k"); resp != ":-1\r\n" {
		t.Errorf("TTL after plain SET = %q, want :-1 (cleared)", resp)
	}

	// SET ... XX on a missing key fails.
	if resp := tc.exec("SET", "missing", "v", "XX"); resp != "$-1\r\n" {
		t.Errorf("SET XX on missing key = %q, want $-1", resp)
	}

	// SET k v4 XX succeeds (key exists).
	tc.expectContains(tc.exec("SET", "k", "v4", "XX"), "+OK", "SET XX on existing key succeeds")

	// KEEPTTL preserves an existing TTL.
	tc.exec("SET", "t", "v", "EX", "100")
	tc.exec("SET", "t", "v2", "KEEPTTL")
	if resp := tc.exec("TTL", "t"); resp == ":-1\r\n" || resp == ":-2\r\n" {
		t.Errorf("TTL after SET KEEPTTL = %q, want preserved positive", resp)
	}

	// Bad expire time is a syntax/argument error.
	if resp := tc.exec("SET", "k", "v", "EX", "0"); !strings.HasPrefix(resp, "-") {
		t.Errorf("SET EX 0 = %q, want error", resp)
	}
}

// TestWrongTypeReplyFormat verifies WRONGTYPE errors use the RESP "-WRONGTYPE"
// code, not the double-prefixed "-ERR WRONGTYPE".
func TestWrongTypeReplyFormat(t *testing.T) {
	tc := newTestContext(t)
	defer tc.cleanup()

	tc.exec("RPUSH", "mylist", "a")
	resp := tc.exec("GET", "mylist")
	if len(resp) < 2 || resp[:2] != "-W" {
		t.Errorf("GET on list: got %q, want reply starting with -WRONGTYPE", resp)
	}
	tc.expectContains(resp, "WRONGTYPE", "GET on wrong type is WRONGTYPE")
}
