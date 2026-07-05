package store

import (
	"testing"
	"time"
)

type recordedWrite struct {
	op    string
	key   string
	value string
}

// fakeCommitLog implements the CommitLogger interface and records writes.
type fakeCommitLog struct {
	writes []recordedWrite
}

func (f *fakeCommitLog) Write(op string, key, value []byte) error {
	f.writes = append(f.writes, recordedWrite{op: op, key: string(key), value: string(value)})
	return nil
}
func (f *fakeCommitLog) Delete(key []byte) {}
func (f *fakeCommitLog) Close() error      { return nil }

func (f *fakeCommitLog) has(op, key string) bool {
	for _, w := range f.writes {
		if w.op == op && w.key == key {
			return true
		}
	}
	return false
}

// TestExpiryPersistedToCommitLog verifies that SetExpiry and RemoveExpiry append
// EXPIRE / PERSIST records to the commit log, so TTLs survive a restart.
func TestExpiryPersistedToCommitLog(t *testing.T) {
	s := createTestStore()
	fake := &fakeCommitLog{}
	s.SetCommitLog(fake)

	if err := s.Set("k", "v"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if !s.SetExpiry("k", time.Now().Add(time.Hour)) {
		t.Fatalf("SetExpiry returned false for existing key")
	}
	if !fake.has("EXPIRE", "k") {
		t.Fatalf("SetExpiry did not write an EXPIRE entry to the commit log; writes=%+v", fake.writes)
	}

	if !s.RemoveExpiry("k") {
		t.Fatalf("RemoveExpiry returned false")
	}
	if !fake.has("PERSIST", "k") {
		t.Fatalf("RemoveExpiry did not write a PERSIST entry to the commit log; writes=%+v", fake.writes)
	}

	// SetExpiry on a missing key must not log anything.
	before := len(fake.writes)
	if s.SetExpiry("missing", time.Now().Add(time.Hour)) {
		t.Fatalf("SetExpiry on missing key returned true")
	}
	if len(fake.writes) != before {
		t.Fatalf("SetExpiry on a missing key wrote to the commit log")
	}
}
