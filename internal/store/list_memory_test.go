package store

import (
	"strconv"
	"sync/atomic"
	"testing"
)

// TestListMemoryAccounting verifies that a list which is grown and shrunk in
// place and then deleted returns total memory exactly to baseline. The cached
// StoredValue.memorySize must be kept in sync on every in-place mutation, or the
// generic Delete (which frees the cached size) frees the wrong amount.
func TestListMemoryAccounting(t *testing.T) {
	s := createTestStore()
	base := atomic.LoadInt64(&s.totalMemory)

	// Create and grow a list well beyond its creation size.
	if _, err := s.LPush("mylist", "seed"); err != nil {
		t.Fatalf("LPush: %v", err)
	}
	for i := 0; i < 200; i++ {
		if _, err := s.RPush("mylist", "element-"+strconv.Itoa(i)); err != nil {
			t.Fatalf("RPush: %v", err)
		}
	}
	// Shrink it back some.
	if _, err := s.LPop("mylist", 50); err != nil {
		t.Fatalf("LPop: %v", err)
	}

	if s.Delete("mylist") != true {
		t.Fatalf("Delete returned false")
	}

	if got := atomic.LoadInt64(&s.totalMemory); got != base {
		t.Fatalf("list memory drift after grow/shrink/delete: totalMemory = %d, want %d (delta %d)", got, base, got-base)
	}
}

// TestContainerGenericDeleteMemory verifies that deleting grown list/set/hash
// keys through the generic Delete path (DEL, as opposed to SREM/LPOP-to-empty)
// returns total memory exactly to baseline for every container type.
func TestContainerGenericDeleteMemory(t *testing.T) {
	s := createTestStore()
	base := atomic.LoadInt64(&s.totalMemory)

	for i := 0; i < 100; i++ {
		si := strconv.Itoa(i)
		if _, err := s.RPush("L", "item-"+si); err != nil {
			t.Fatalf("RPush: %v", err)
		}
		if _, err := s.SAdd("S", "member-"+si); err != nil {
			t.Fatalf("SAdd: %v", err)
		}
		if _, err := s.HSet("H", "field-"+si, "value-"+si); err != nil {
			t.Fatalf("HSet: %v", err)
		}
	}

	for _, k := range []string{"L", "S", "H"} {
		if !s.Delete(k) {
			t.Fatalf("Delete(%q) returned false", k)
		}
	}

	if got := atomic.LoadInt64(&s.totalMemory); got != base {
		t.Fatalf("container memory drift after grow + generic DEL: totalMemory = %d, want %d (delta %d)", got, base, got-base)
	}
}
