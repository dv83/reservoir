package commitlog

import (
	"bytes"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestCompactionMemoryIsBoundedByKeys (add-tiered-storage, task 2.3): compaction
// must not hold the log's values in memory. 80MB of values over a handful of
// keys are compacted while the heap is sampled; the peak growth has to stay far
// below the log size. The previous compactor loaded every entry of every
// segment, values included, before deciding what to keep.
func TestCompactionMemoryIsBoundedByKeys(t *testing.T) {
	if testing.Short() {
		t.Skip("writes ~80MB of segments")
	}
	dir := t.TempDir()
	value := string(bytes.Repeat([]byte("x"), 512<<10)) // 512KB
	const perSeg = 80
	ts := int64(1)
	for seg := 1; seg <= 2; seg++ {
		entries := make([]*Entry, 0, perSeg)
		for i := 0; i < perSeg; i++ {
			entries = append(entries, wr(ts, "SET", fmt.Sprintf("k%d", i%4), value))
			ts++
		}
		writeSegment(t, dir, seg, entries)
	}
	logBytes := int64(2*perSeg) * int64(len(value))

	growth := peakHeapGrowth(func() { compactDir(t, dir) })
	t.Logf("log %dMB, peak heap growth during compaction %.1fMB", logBytes>>20, float64(growth)/(1<<20))
	if growth > logBytes/3 {
		t.Fatalf("compaction grew the heap by %dMB for a %dMB log: values are held in memory", growth>>20, logBytes>>20)
	}

	got := replayDir(t, dir)
	if len(got) != 4 {
		t.Fatalf("compacted log holds %d keys, want 4", len(got))
	}
}

// peakHeapGrowth runs fn while sampling the live heap and returns how far it
// grew above its level before fn started.
func peakHeapGrowth(fn func()) int64 {
	runtime.GC()
	var base runtime.MemStats
	runtime.ReadMemStats(&base)
	var peak uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		var m runtime.MemStats
		for {
			// Collect first, so the reading is live memory: garbage the GC has
			// not caught up with (common when the machine is busy) is not what
			// these tests bound.
			runtime.GC()
			runtime.ReadMemStats(&m)
			for {
				p := atomic.LoadUint64(&peak)
				if m.HeapAlloc <= p || atomic.CompareAndSwapUint64(&peak, p, m.HeapAlloc) {
					break
				}
			}
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Millisecond):
			}
		}
	}()
	fn()
	close(stop)
	<-done
	return int64(atomic.LoadUint64(&peak)) - int64(base.HeapAlloc)
}
