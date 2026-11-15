package commitlog

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

// TestRecoveryStreamsOrderedLog (add-tiered-storage, task 3.1): a log in
// timestamp order is replayed without loading it into memory.
func TestRecoveryStreamsOrderedLog(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ~80MB of segments")
	}
	dir := t.TempDir()
	value := string(bytes.Repeat([]byte("x"), 512<<10))
	ts := int64(1)
	for seg := 1; seg <= 2; seg++ {
		var entries []*Entry
		for i := 0; i < 80; i++ {
			entries = append(entries, wr(ts, "SET", fmt.Sprintf("k%d", ts), value))
			ts++
		}
		writeSegment(t, dir, seg, entries)
	}
	count := 0
	growth := peakHeapGrowth(func() {
		r, err := NewReader(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		if err := r.ReadAll(func(*Entry) error { count++; return nil }); err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("replayed %d entries (80MB), peak heap growth %.1fMB", count, float64(growth)/(1<<20))
	if count != 160 {
		t.Fatalf("replayed %d entries, want 160", count)
	}
	// Half the log, as in TestCompactionMemoryIsBoundedByKeys: loading the log
	// takes all of it, a busy machine adds measurement noise.
	if growth > 80<<20/2 {
		t.Fatalf("recovery grew the heap by %dMB for an 80MB log", growth>>20)
	}
}

// TestRecoverySortsUnorderedLog: a log whose timestamps go backwards (older
// format) is still replayed in timestamp order, file order breaking ties.
func TestRecoverySortsUnorderedLog(t *testing.T) {
	dir := t.TempDir()
	writeSegment(t, dir, 1, []*Entry{wr(30, "SET", "c", ""), wr(10, "SET", "a", "")})
	writeSegment(t, dir, 2, []*Entry{wr(20, "SET", "b1", ""), wr(20, "SET", "b2", "")})
	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var got []string
	r.ReadAll(func(e *Entry) error { got = append(got, string(e.Key)); return nil })
	if fmt.Sprint(got) != "[a b1 b2 c]" {
		t.Fatalf("replay order = %v, want [a b1 b2 c]", got)
	}
}

// TestWriterStampsInFileOrder: entries written concurrently land in the log
// with strictly increasing timestamps in file order.
func TestWriterStampsInFileOrder(t *testing.T) {
	dir := t.TempDir()
	cl, err := NewCommitLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				cl.Write("SET", []byte(fmt.Sprintf("k%d-%d", w, i)), []byte("v"))
			}
		}(w)
	}
	wg.Wait()
	cl.Close()

	r, err := NewReader(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var last int64
	n := 0
	for _, seg := range r.segments {
		r.readSegment(seg, func(e *Entry) error {
			if e.Timestamp <= last {
				t.Fatalf("entry %d: timestamp %d not above previous %d in file order", n, e.Timestamp, last)
			}
			last = e.Timestamp
			n++
			return nil
		})
	}
	if n != 16000 {
		t.Fatalf("log holds %d entries, want 16000", n)
	}
}
