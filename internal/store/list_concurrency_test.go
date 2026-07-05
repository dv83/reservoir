package store

import (
	"strconv"
	"sync"
	"testing"
)

// TestListPushSetNoLostUpdate stresses concurrent RPush and LSet on the same
// list. LSet used to copy the list and swap it back with a pointer-comparing
// CAS; because RPush mutates the list in place (same pointer), a push racing
// with LSet's copy-and-swap was silently discarded. After the fix both run
// under the shard write lock, so every push must be retained.
func TestListPushSetNoLostUpdate(t *testing.T) {
	s := createTestStore()

	// Seed the list so LSet(0, ...) always has a valid index to overwrite.
	if _, err := s.RPush("mylist", "seed"); err != nil {
		t.Fatalf("seed RPush: %v", err)
	}

	const pushes = 2000
	var wg sync.WaitGroup
	wg.Add(2)

	// Pusher: append `pushes` distinct elements.
	go func() {
		defer wg.Done()
		for i := 0; i < pushes; i++ {
			if _, err := s.RPush("mylist", "e"+strconv.Itoa(i)); err != nil {
				t.Errorf("RPush: %v", err)
				return
			}
		}
	}()

	// Setter: continuously rewrite index 0 to contend with the pusher.
	go func() {
		defer wg.Done()
		for i := 0; i < pushes; i++ {
			if err := s.LSet("mylist", 0, "x"+strconv.Itoa(i)); err != nil {
				t.Errorf("LSet: %v", err)
				return
			}
		}
	}()

	wg.Wait()

	length, err := s.LLen("mylist")
	if err != nil {
		t.Fatalf("LLen: %v", err)
	}
	if want := int64(1 + pushes); length != want {
		t.Fatalf("lost updates: list length = %d, want %d", length, want)
	}
}
