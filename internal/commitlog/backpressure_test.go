package commitlog

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestNoAcknowledgedWriteIsDropped (add-tiered-storage, task 2.2): when the
// writer falls behind and the write channel fills up, Write and Delete must wait
// rather than drop the entry, so every acknowledged entry reaches the log.
// Before the change Write dropped an entry after waiting 100ms and Delete
// dropped one at once.
func TestNoAcknowledgedWriteIsDropped(t *testing.T) {
	oldCap, oldHook := writeChanCapacity, flushHook
	writeChanCapacity = 8
	var once sync.Once
	flushHook = func() {
		// Stall the first flush well past the old 100ms drop timeout while
		// writers keep sending.
		once.Do(func() { time.Sleep(400 * time.Millisecond) })
	}
	defer func() { writeChanCapacity, flushHook = oldCap, oldHook }()

	dir := t.TempDir()
	cl, err := NewCommitLog(dir)
	if err != nil {
		t.Fatal(err)
	}

	const writers, perWriter = 8, 3000 // > BatchSize, so a flush happens mid-run
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				key := []byte(fmt.Sprintf("k%d-%d", w, i))
				if i%10 == 0 {
					cl.Delete(key)
					continue
				}
				if err := cl.Write("SET", key, []byte("v")); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if err := cl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reader, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got := 0
	if err := reader.ReadAll(func(*Entry) error { got++; return nil }); err != nil {
		t.Fatal(err)
	}
	if want := writers * perWriter; got != want {
		t.Fatalf("log holds %d entries, want %d: acknowledged entries were dropped", got, want)
	}
}

// TestWriteAfterCloseFails: once Close has returned, writes are refused rather
// than blocking on a writer that is gone.
func TestWriteAfterCloseFails(t *testing.T) {
	cl, err := NewCommitLog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()
	done := make(chan error, 1)
	go func() { done <- cl.Write("SET", []byte("k"), []byte("v")) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Write after Close succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Write after Close blocked")
	}
}
