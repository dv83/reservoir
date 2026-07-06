package commitlog

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// encodeEntry serializes an entry into the on-disk frame
// ([4B totalLen][body]) exactly as writeEntry does.
func encodeEntry(e *Entry) []byte {
	op := []byte(e.Operation)
	body := []byte{e.Type}
	body = binary.BigEndian.AppendUint64(body, uint64(e.Timestamp))
	body = binary.BigEndian.AppendUint16(body, uint16(len(op)))
	body = append(body, op...)
	body = binary.BigEndian.AppendUint16(body, uint16(len(e.Key)))
	body = append(body, e.Key...)
	body = binary.BigEndian.AppendUint32(body, uint32(len(e.Value)))
	body = append(body, e.Value...)
	body = binary.BigEndian.AppendUint32(body, e.CRC)
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), body...)
}

func validEntry(op, key, val string) *Entry {
	e := &Entry{Type: EntryTypeWrite, Timestamp: 1, Operation: op, Key: []byte(key), Value: []byte(val)}
	e.CRC = e.CalculateCRC()
	return e
}

// corruptFrame builds a frame whose declared body length is honored but whose
// internal keyLen field claims far more bytes than the body actually contains —
// the exact shape that made the un-bounds-checked reader panic.
func corruptFrame() []byte {
	body := []byte{EntryTypeWrite}
	body = binary.BigEndian.AppendUint64(body, 1)     // timestamp
	body = binary.BigEndian.AppendUint16(body, 0)     // operation length 0
	body = binary.BigEndian.AppendUint16(body, 60000) // key length lies
	// ...but no key/value/crc bytes follow.
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), body...)
}

// TestReaderSkipsCorruptEntriesWithoutPanic verifies that a corrupt entry with a
// bogus internal length field is skipped (not panicked on) and the surrounding
// valid entries are still recovered.
func TestReaderSkipsCorruptEntriesWithoutPanic(t *testing.T) {
	dir := t.TempDir()

	var buf []byte
	buf = append(buf, encodeEntry(validEntry("SET", "k1", "v1"))...)
	buf = append(buf, corruptFrame()...)
	buf = append(buf, encodeEntry(validEntry("SET", "k2", "v2"))...)

	segPath := filepath.Join(dir, "0000000000000001"+SegmentFileExtension)
	if err := os.WriteFile(segPath, buf, 0o644); err != nil {
		t.Fatalf("write segment: %v", err)
	}

	reader, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	var keys []string
	// Must not panic even though the middle entry is corrupt.
	if err := reader.ReadAll(func(e *Entry) error {
		keys = append(keys, string(e.Key))
		return nil
	}); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	// Both valid entries recovered; the corrupt one skipped.
	got := map[string]bool{}
	for _, k := range keys {
		got[k] = true
	}
	if !got["k1"] || !got["k2"] {
		t.Fatalf("recovered keys = %v, want k1 and k2", keys)
	}
}

// TestReaderTruncatedTailEntry verifies a truncated final entry (file cut mid
// write) does not panic and the earlier valid entry is recovered.
func TestReaderTruncatedTailEntry(t *testing.T) {
	dir := t.TempDir()

	buf := encodeEntry(validEntry("SET", "good", "v"))
	full := encodeEntry(validEntry("SET", "cut", "value"))
	buf = append(buf, full[:len(full)-6]...) // drop the tail of the last entry

	segPath := filepath.Join(dir, "0000000000000001"+SegmentFileExtension)
	if err := os.WriteFile(segPath, buf, 0o644); err != nil {
		t.Fatalf("write segment: %v", err)
	}

	reader, err := NewReader(dir)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	var keys []string
	if err := reader.ReadAll(func(e *Entry) error {
		keys = append(keys, string(e.Key))
		return nil
	}); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(keys) != 1 || keys[0] != "good" {
		t.Fatalf("recovered keys = %v, want [good]", keys)
	}
}
