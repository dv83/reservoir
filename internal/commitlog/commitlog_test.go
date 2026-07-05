package commitlog

import (
	"path/filepath"
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
	c := NewCompactor(dir)

	state := map[string]*Entry{
		"a": {Type: EntryTypeWrite, Timestamp: 100, Operation: "SET", Key: []byte("a"), Value: []byte("va")},
		"b": {Type: EntryTypeWrite, Timestamp: 200, Operation: "SET", Key: []byte("b"), Value: []byte("vb")},
	}
	for _, e := range state {
		e.CRC = e.CalculateCRC()
	}

	oldest := filepath.Join(dir, "0000000000000001"+SegmentFileExtension)
	if _, err := c.writeCompactedSegment(state, oldest); err != nil {
		t.Fatalf("writeCompactedSegment: %v", err)
	}

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
