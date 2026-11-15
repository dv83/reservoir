package store

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"

	"reservoir/internal/lsmengine"
)

// Store-level cost of the LSM storage mode (no network): SET of new keys and
// GET of random existing keys, memory mode against LSM mode with a cache that
// holds everything and one far smaller than the data.
//
//	go test ./internal/store/ -run x -bench BenchmarkStorage -benchtime 2s

func benchStore(b *testing.B, mode string) *LockFreeStore {
	budget := int64(1 << 30)
	if mode == "lsm-small-cache" {
		budget = 4 << 20
	}
	s := NewLockFreeStore(&Limits{MaxKeySize: 1024, MaxValueSize: 1 << 20, MaxMemoryUsage: budget})
	if mode != "memory" {
		if err := s.EnableLSM(b.TempDir(), lsmengine.Options{CacheBytes: 16 << 20}); err != nil {
			b.Fatal(err)
		}
	}
	return s
}

func BenchmarkStorageSet(b *testing.B) {
	for _, mode := range []string{"memory", "lsm", "lsm-small-cache"} {
		b.Run(mode, func(b *testing.B) {
			s := benchStore(b, mode)
			defer s.Stop()
			var n int64
			b.SetParallelism(4)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := atomic.AddInt64(&n, 1)
					s.Set(fmt.Sprintf("key:%012d", i), "val:0123456789ab")
				}
			})
		})
	}
}

func BenchmarkStorageGet(b *testing.B) {
	const keys = 200000
	for _, mode := range []string{"memory", "lsm", "lsm-small-cache"} {
		b.Run(mode, func(b *testing.B) {
			s := benchStore(b, mode)
			defer s.Stop()
			for i := 0; i < keys; i++ {
				s.Set(fmt.Sprintf("key:%012d", i), "val:0123456789ab")
			}
			b.SetParallelism(4)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				r := rand.New(rand.NewSource(rand.Int63()))
				for pb.Next() {
					if _, ok := s.Get(fmt.Sprintf("key:%012d", r.Intn(keys))); !ok {
						b.Fatal("missing key")
					}
				}
			})
		})
	}
}
