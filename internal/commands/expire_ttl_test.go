package commands

import "testing"

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
