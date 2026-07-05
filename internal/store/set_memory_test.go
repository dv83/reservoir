package store

import (
	"strconv"
	"testing"
)

// TestSAddMemoryAccounting verifies that repeated SAdd on an existing set does
// not corrupt memory accounting. Previously each SAdd on an existing set
// replaced the StoredValue with a fresh one whose cached memorySize was zero,
// so the next SAdd computed a negative old size and inflated totalMemory on
// every call. The invariant: after adding many members and then removing them
// all (which deletes the key), total memory returns to its starting value.
func TestSAddMemoryAccounting(t *testing.T) {
	s := createTestStore()

	base := s.GetMemoryUsage()

	members := make([]string, 0, 200)
	for i := 0; i < 200; i++ {
		m := "member-" + strconv.Itoa(i)
		members = append(members, m)
		// Add one at a time to exercise the existing-set branch repeatedly.
		if _, err := s.SAdd("myset", m); err != nil {
			t.Fatalf("SAdd: %v", err)
		}
	}

	// Remove every member; the set becomes empty and the key is deleted.
	if _, err := s.SRem("myset", members...); err != nil {
		t.Fatalf("SRem: %v", err)
	}

	if got := s.GetMemoryUsage(); got != base {
		t.Fatalf("memory accounting drift: after add+remove total memory = %d, want %d (delta %d)", got, base, got-base)
	}
}
