package store

import (
	"fmt"
	"os"
	"runtime"
	"testing"
	"unsafe"
)

// TestMemoryFootprintPerKey (add-tiered-storage, tasks 1.1 and 1.3) measures
// the heap a string key costs: 1M keys of 16-byte keys and 16-byte values, the
// heap growth divided by the key count. It is a measurement, not a check, and
// allocates several hundred MB, so it runs only with RESERVOIR_MEMBENCH=1:
//
//	RESERVOIR_MEMBENCH=1 go test ./internal/store/ -run TestMemoryFootprintPerKey -v
func TestMemoryFootprintPerKey(t *testing.T) {
	if os.Getenv("RESERVOIR_MEMBENCH") == "" {
		t.Skip("set RESERVOIR_MEMBENCH=1 to measure the per-key footprint")
	}
	for _, cluster := range []bool{false, true} {
		name := "standalone"
		if cluster {
			name = "cluster"
		}
		t.Run(name, func(t *testing.T) {
			perKey := measurePerKey(t, cluster, 1_000_000)
			t.Logf("StoredValue struct: %d bytes", unsafe.Sizeof(StoredValue{}))
			t.Logf("heap per key (16B key + 16B value): %.1f bytes", perKey)
			t.Logf("keys per GB of heap: %.2fM", float64(1<<30)/perKey/1e6)
		})
	}
}

func measurePerKey(t *testing.T, cluster bool, n int) float64 {
	t.Helper()
	s := NewStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: 1 << 40})
	defer s.Stop()
	if cluster {
		s.SetLocalOrigin(1)
	}
	before := heapInUse()
	for i := 0; i < n; i++ {
		// Fresh key and value strings per write, as the network path produces.
		k := fmt.Sprintf("key:%012d", i)     // 16 bytes
		value := fmt.Sprintf("val:%012d", i) // 16 bytes
		if cluster {
			// The cluster write path: a stamped LWW SET.
			if _, err := s.SetLWW(k, value, int64(i+1), 0, 1); err != nil {
				t.Fatal(err)
			}
		} else if err := s.Set(k, value); err != nil {
			t.Fatal(err)
		}
	}
	after := heapInUse()
	runtime.KeepAlive(s)
	return float64(after-before) / float64(n)
}

func heapInUse() uint64 {
	var m runtime.MemStats
	for i := 0; i < 3; i++ {
		runtime.GC()
	}
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}
