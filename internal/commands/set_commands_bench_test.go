package commands

import (
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"reservoir/internal/store"
)

func BenchmarkSetCommands(b *testing.B) {
	// Create store with reasonable limits
	limits := &store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024, // 100MB
	}
	kvStore := store.NewLockFreeStore(limits)
	defer kvStore.Stop()

	registry := NewZeroCopyRegistry()

	b.Run("SADD", func(b *testing.B) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		handler, _ := registry.GetHandler("SADD")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()

			// SADD benchmark_set member{i}
			cmd := createTestCommand([][]byte{
				[]byte("benchmark_set"),
				[]byte(fmt.Sprintf("member%d", i)),
			})

			err := handler(kvStore, cmd, writer)
			if err != nil {
				b.Fatalf("SADD failed: %v", err)
			}
			writer.Flush()
		}
	})

	b.Run("SISMEMBER", func(b *testing.B) {
		// Pre-populate set with data
		populateSetBenchmark(kvStore, registry, "ismember_set", 1000)

		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		handler, _ := registry.GetHandler("SISMEMBER")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()

			// SISMEMBER ismember_set member{i % 1000}
			cmd := createTestCommand([][]byte{
				[]byte("ismember_set"),
				[]byte(fmt.Sprintf("member%d", i%1000)),
			})

			err := handler(kvStore, cmd, writer)
			if err != nil {
				b.Fatalf("SISMEMBER failed: %v", err)
			}
			writer.Flush()
		}
	})

	b.Run("SCARD", func(b *testing.B) {
		// Pre-populate set with data
		populateSetBenchmark(kvStore, registry, "card_set", 500)

		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		handler, _ := registry.GetHandler("SCARD")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()

			cmd := createTestCommand([][]byte{[]byte("card_set")})

			err := handler(kvStore, cmd, writer)
			if err != nil {
				b.Fatalf("SCARD failed: %v", err)
			}
			writer.Flush()
		}
	})

	b.Run("SMEMBERS", func(b *testing.B) {
		// Pre-populate set with smaller data for this test
		populateSetBenchmark(kvStore, registry, "members_set", 100)

		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		handler, _ := registry.GetHandler("SMEMBERS")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()

			cmd := createTestCommand([][]byte{[]byte("members_set")})

			err := handler(kvStore, cmd, writer)
			if err != nil {
				b.Fatalf("SMEMBERS failed: %v", err)
			}
			writer.Flush()
		}
	})

	b.Run("SREM", func(b *testing.B) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		saddHandler, _ := registry.GetHandler("SADD")
		sremHandler, _ := registry.GetHandler("SREM")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()

			// First add an element
			addCmd := createTestCommand([][]byte{
				[]byte("rem_set"),
				[]byte(fmt.Sprintf("member%d", i)),
			})
			saddHandler(kvStore, addCmd, writer)

			// Then remove it
			remCmd := createTestCommand([][]byte{
				[]byte("rem_set"),
				[]byte(fmt.Sprintf("member%d", i)),
			})

			err := sremHandler(kvStore, remCmd, writer)
			if err != nil {
				b.Fatalf("SREM failed: %v", err)
			}
			writer.Flush()
		}
	})

	b.Run("SPOP", func(b *testing.B) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		saddHandler, _ := registry.GetHandler("SADD")
		spopHandler, _ := registry.GetHandler("SPOP")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()

			// First add an element
			addCmd := createTestCommand([][]byte{
				[]byte("pop_set"),
				[]byte(fmt.Sprintf("member%d", i)),
			})
			saddHandler(kvStore, addCmd, writer)

			// Then pop it
			popCmd := createTestCommand([][]byte{[]byte("pop_set")})

			err := spopHandler(kvStore, popCmd, writer)
			if err != nil {
				b.Fatalf("SPOP failed: %v", err)
			}
			writer.Flush()
		}
	})
}

func BenchmarkSetOperations(b *testing.B) {
	// Create store with reasonable limits
	limits := &store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024, // 100MB
	}
	kvStore := store.NewLockFreeStore(limits)
	defer kvStore.Stop()

	registry := NewZeroCopyRegistry()

	b.Run("SDIFF", func(b *testing.B) {
		// Setup sets for difference operation
		populateSetBenchmark(kvStore, registry, "diff_set1", 100)
		populateSetBenchmark(kvStore, registry, "diff_set2", 50)

		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		handler, _ := registry.GetHandler("SDIFF")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()

			cmd := createTestCommand([][]byte{
				[]byte("diff_set1"),
				[]byte("diff_set2"),
			})

			err := handler(kvStore, cmd, writer)
			if err != nil {
				b.Fatalf("SDIFF failed: %v", err)
			}
			writer.Flush()
		}
	})

	b.Run("SINTER", func(b *testing.B) {
		// Setup sets for intersection operation
		populateSetBenchmark(kvStore, registry, "inter_set1", 100)
		populateSetBenchmark(kvStore, registry, "inter_set2", 100)

		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		handler, _ := registry.GetHandler("SINTER")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()

			cmd := createTestCommand([][]byte{
				[]byte("inter_set1"),
				[]byte("inter_set2"),
			})

			err := handler(kvStore, cmd, writer)
			if err != nil {
				b.Fatalf("SINTER failed: %v", err)
			}
			writer.Flush()
		}
	})

	b.Run("SUNION", func(b *testing.B) {
		// Setup sets for union operation
		populateSetBenchmark(kvStore, registry, "union_set1", 50)
		populateSetBenchmark(kvStore, registry, "union_set2", 50)

		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		handler, _ := registry.GetHandler("SUNION")

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()

			cmd := createTestCommand([][]byte{
				[]byte("union_set1"),
				[]byte("union_set2"),
			})

			err := handler(kvStore, cmd, writer)
			if err != nil {
				b.Fatalf("SUNION failed: %v", err)
			}
			writer.Flush()
		}
	})
}

func BenchmarkSetMemoryUsage(b *testing.B) {
	// Create store with reasonable limits
	limits := &store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024, // 100MB
	}
	kvStore := store.NewLockFreeStore(limits)
	defer kvStore.Stop()

	registry := NewZeroCopyRegistry()

	b.Run("Memory efficiency", func(b *testing.B) {
		var buf bytes.Buffer
		writer := bufio.NewWriter(&buf)
		handler, _ := registry.GetHandler("SADD")

		initialMemory := kvStore.GetMemoryUsage()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			buf.Reset()

			cmd := createTestCommand([][]byte{
				[]byte("memory_test_set"),
				[]byte(fmt.Sprintf("member%d", i)),
			})

			err := handler(kvStore, cmd, writer)
			if err != nil {
				b.Fatalf("SADD failed: %v", err)
			}
			writer.Flush()
		}
		b.StopTimer()

		finalMemory := kvStore.GetMemoryUsage()
		memoryPerOp := float64(finalMemory-initialMemory) / float64(b.N)
		b.ReportMetric(memoryPerOp, "bytes/op")
	})
}

func BenchmarkSetConcurrency(b *testing.B) {
	// Create store with reasonable limits
	limits := &store.Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 100 * 1024 * 1024, // 100MB
	}
	kvStore := store.NewLockFreeStore(limits)
	defer kvStore.Stop()

	registry := NewZeroCopyRegistry()

	b.Run("Concurrent SADD", func(b *testing.B) {
		handler, _ := registry.GetHandler("SADD")

		b.RunParallel(func(pb *testing.PB) {
			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)
			i := 0

			for pb.Next() {
				buf.Reset()

				cmd := createTestCommand([][]byte{
					[]byte("concurrent_set"),
					[]byte(fmt.Sprintf("member%d", i)),
				})

				err := handler(kvStore, cmd, writer)
				if err != nil {
					b.Fatalf("Concurrent SADD failed: %v", err)
				}
				writer.Flush()
				i++
			}
		})
	})

	b.Run("Concurrent SISMEMBER", func(b *testing.B) {
		// Pre-populate set with data
		populateSetBenchmark(kvStore, registry, "concurrent_check_set", 1000)

		handler, _ := registry.GetHandler("SISMEMBER")

		b.RunParallel(func(pb *testing.PB) {
			var buf bytes.Buffer
			writer := bufio.NewWriter(&buf)
			i := 0

			for pb.Next() {
				buf.Reset()

				cmd := createTestCommand([][]byte{
					[]byte("concurrent_check_set"),
					[]byte(fmt.Sprintf("member%d", i%1000)),
				})

				err := handler(kvStore, cmd, writer)
				if err != nil {
					b.Fatalf("Concurrent SISMEMBER failed: %v", err)
				}
				writer.Flush()
				i++
			}
		})
	})
}

// Helper function to populate a set with test data
func populateSetBenchmark(kvStore store.KVStore, registry *ZeroCopyRegistry, setKey string, count int) {
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	handler, _ := registry.GetHandler("SADD")

	for i := 0; i < count; i++ {
		buf.Reset()

		cmd := createTestCommand([][]byte{
			[]byte(setKey),
			[]byte(fmt.Sprintf("member%d", i)),
		})

		handler(kvStore, cmd, writer)
		writer.Flush()
	}
}
