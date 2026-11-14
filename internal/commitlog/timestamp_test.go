package commitlog

import (
	"sync"
	"testing"
)

// TestEntryTimestampsStrictlyIncrease (task 2.4): recovery orders entries by
// timestamp, so entries created back to back — even from several goroutines —
// must never share or reverse a timestamp.
func TestEntryTimestampsStrictlyIncrease(t *testing.T) {
	cl := &CommitLog{}
	const goroutines, perG = 8, 5000
	got := make([][]int64, goroutines)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				got[g] = append(got[g], cl.nextTimestamp())
			}
		}(g)
	}
	wg.Wait()

	seen := make(map[int64]bool, goroutines*perG)
	for g, ts := range got {
		for i, v := range ts {
			if i > 0 && v <= ts[i-1] {
				t.Fatalf("goroutine %d: timestamp %d not greater than previous %d", g, v, ts[i-1])
			}
			if seen[v] {
				t.Fatalf("timestamp %d handed out twice", v)
			}
			seen[v] = true
		}
	}
}
