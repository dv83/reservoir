package protocol

import (
	"bufio"
	"strings"
	"testing"
)

func parse(t *testing.T, input string) (*ZeroCopyCommand, error) {
	t.Helper()
	p := NewZeroCopyParser()
	return p.ParseCommand(bufio.NewReader(strings.NewReader(input)))
}

func TestParseRESPArray(t *testing.T) {
	cmd, err := parse(t, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := strings.ToUpper(cmd.String()); got != "SET" {
		t.Fatalf("command = %q, want SET", got)
	}
	if cmd.ArgCount() != 2 {
		t.Fatalf("ArgCount = %d, want 2", cmd.ArgCount())
	}
	if cmd.ArgString(0) != "foo" || cmd.ArgString(1) != "bar" {
		t.Fatalf("args = %q,%q want foo,bar", cmd.ArgString(0), cmd.ArgString(1))
	}
}

func TestParseInlineCommand(t *testing.T) {
	cmd, err := parse(t, "PING\r\n")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if strings.ToUpper(cmd.String()) != "PING" {
		t.Fatalf("command = %q, want PING", cmd.String())
	}

	cmd, err = parse(t, "SET foo bar\r\n")
	if err != nil {
		t.Fatalf("parse inline args: %v", err)
	}
	if strings.ToUpper(cmd.String()) != "SET" || cmd.ArgCount() != 2 {
		t.Fatalf("inline SET parsed as %q with %d args", cmd.String(), cmd.ArgCount())
	}
}

func TestParseBinarySafeValue(t *testing.T) {
	// A bulk value that itself contains \r\n must be read by its length, intact.
	cmd, err := parse(t, "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cmd.ArgString(1) != "a\r\nb" {
		t.Fatalf("binary value = %q, want \"a\\r\\nb\"", cmd.ArgString(1))
	}
}

func TestParseRejectsHugeMultibulk(t *testing.T) {
	// Attacker-controlled array count must not trigger a huge allocation.
	if _, err := parse(t, "*2000000000\r\n"); err == nil {
		t.Fatalf("huge multibulk count accepted (allocation DoS)")
	}
}

func TestParseRejectsNumberOverflow(t *testing.T) {
	// An over-long digit run must be rejected, not silently wrapped.
	if _, err := parse(t, "*999999999999999999999999\r\n"); err == nil {
		t.Fatalf("overflowing multibulk count accepted")
	}
	if _, err := parse(t, "*1\r\n$999999999999999999999999\r\n"); err == nil {
		t.Fatalf("overflowing bulk length accepted")
	}
}

func TestParseMalformed(t *testing.T) {
	cases := []string{
		"*1\r\n+notbulk\r\n",  // array element is not a bulk string
		"*2\r\n$3\r\nSET\r\n", // truncated: fewer elements than declared
		"*1\r\n$5\r\nab\r\n",  // bulk length longer than data
	}
	for _, in := range cases {
		if _, err := parse(t, in); err == nil {
			t.Errorf("malformed input %q was accepted", in)
		}
	}
}

// TestParseFuzzNoPanic feeds many malformed byte sequences and asserts the
// parser never panics (it may return an error).
func TestParseFuzzNoPanic(t *testing.T) {
	seeds := []string{
		"", "\r\n", "*", "*-1\r\n", "*0\r\n", "$", "$-1\r\n",
		"*1\r\n$", "*1\r\n$3\r\nab", "*abc\r\n", "*1\r\n$abc\r\n",
		"\x00\x01\x02", "*1\r\n:5\r\n", strings.Repeat("*", 100),
	}
	for _, s := range seeds {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("parser panicked on %q: %v", s, r)
				}
			}()
			_, _ = parse(t, s)
		}()
	}
}

// FuzzParseCommand runs the seed corpus under `go test` and supports extended
// fuzzing via `go test -fuzz=FuzzParseCommand`. The only invariant is that the
// parser must never panic on arbitrary input.
func FuzzParseCommand(f *testing.F) {
	for _, s := range []string{
		"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n",
		"PING\r\n", "SET a b\r\n", "*0\r\n", "*-1\r\n",
		"*2000000000\r\n", "$-1\r\n", "*1\r\n$abc\r\n",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		p := NewZeroCopyParser()
		_, _ = p.ParseCommand(bufio.NewReader(strings.NewReader(string(data))))
	})
}
