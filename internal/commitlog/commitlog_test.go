package commitlog

import (
	"strings"
	"testing"
)

func TestValidateEntrySizes(t *testing.T) {
	if err := validateEntrySizes("SET", []byte("k"), []byte("v")); err != nil {
		t.Fatalf("normal entry rejected: %v", err)
	}
	// Key beyond the uint16 length field must be rejected, not silently truncated.
	bigKey := make([]byte, MaxFieldLen+1)
	if err := validateEntrySizes("SET", bigKey, nil); err == nil {
		t.Fatalf("oversized key accepted (would silently truncate on disk)")
	}
	bigOp := strings.Repeat("x", MaxFieldLen+1)
	if err := validateEntrySizes(bigOp, []byte("k"), nil); err == nil {
		t.Fatalf("oversized operation accepted")
	}
	// Value pushing total past the reader's cap must be rejected.
	bigVal := make([]byte, MaxEntrySize+1)
	if err := validateEntrySizes("SET", []byte("k"), bigVal); err == nil {
		t.Fatalf("over-cap entry accepted (reader would reject it as corrupt)")
	}
}

// TestCompactionPreservesTimestamps verifies the compactor keeps each entry's
// original timestamp. Rewriting them to "now" would make old compacted values
// sort after newer live-segment writes during recovery and resurrect stale data.
func TestCompactionPreservesTimestamps(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, 1, []*Entry{wr(100, "SET", "a", "va")})
	writeSegment(t, dir, 2, []*Entry{wr(200, "SET", "b", "vb")})
	compactDir(t, dir)

	reader, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	got := map[string]int64{}
	if err := reader.ReadAll(func(e *Entry) error {
		got[string(e.Key)] = e.Timestamp
		return nil
	}); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if got["a"] != 100 {
		t.Errorf("timestamp for a = %d, want 100 (preserved)", got["a"])
	}
	if got["b"] != 200 {
		t.Errorf("timestamp for b = %d, want 200 (preserved)", got["b"])
	}
}
