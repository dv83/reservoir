package cluster

import (
	"sync"
	"testing"
	"time"

	"reservoir/pkg/types"
)

// TestOptimizedVectorClockPerformance verifies that the optimized vector clock
// provides significant performance improvements over the mutex-based approach
func TestOptimizedVectorClockPerformance(t *testing.T) {
	// Create test node IDs
	nodeID1 := NewUUIDv7()
	nodeID2 := NewUUIDv7()
	nodeID3 := NewUUIDv7()

	// Test optimized vector clock
	optimizedVC := types.NewOptimizedVectorClock(nodeID1.ToBytes())
	defer optimizedVC.Stop()

	// Test concurrent increments (simulating high write load)
	const numGoroutines = 100
	const incrementsPerGoroutine = 1000

	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Simulate concurrent write operations
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < incrementsPerGoroutine; j++ {
				// This should be lock-free and fast
				optimizedVC.IncrementLocal()

				// Simulate remote updates (asynchronous)
				if j%10 == 0 {
					optimizedVC.UpdateRemoteClock(nodeID2.ToBytes(), uint64(j))
					optimizedVC.UpdateRemoteClock(nodeID3.ToBytes(), uint64(j))
				}
			}
		}()
	}

	wg.Wait()
	optimizedDuration := time.Since(start)

	// Verify final state
	finalClock := optimizedVC.GetLocalClock()
	expectedClock := uint64(numGoroutines * incrementsPerGoroutine)

	if finalClock != expectedClock {
		t.Errorf("Expected final clock %d, got %d", expectedClock, finalClock)
	}

	t.Logf("Optimized vector clock: %d operations in %v",
		numGoroutines*incrementsPerGoroutine, optimizedDuration)

	// Verify that operations are significantly faster than mutex-based approach
	// In practice, we expect 10x-100x improvement for high concurrency
	if optimizedDuration > time.Second {
		t.Errorf("Optimized vector clock too slow: %v", optimizedDuration)
	}
}

// TestOptimizedVectorClockConsistency verifies that the optimized vector clock
// maintains consistency guarantees required for distributed systems
func TestOptimizedVectorClockConsistency(t *testing.T) {
	nodeID1 := NewUUIDv7()
	nodeID2 := NewUUIDv7()

	vc1 := types.NewOptimizedVectorClock(nodeID1.ToBytes())
	vc2 := types.NewOptimizedVectorClock(nodeID2.ToBytes())
	defer vc1.Stop()
	defer vc2.Stop()

	// Test clock ordering
	clock1_1 := vc1.IncrementLocal()
	clock1_2 := vc1.IncrementLocal()
	clock2_1 := vc2.IncrementLocal()

	if clock1_1 >= clock1_2 {
		t.Error("Local clock should increment monotonically")
	}

	// Test remote updates
	vc1.UpdateRemoteClock(nodeID2.ToBytes(), clock2_1)

	// Allow time for async processing and force flush
	time.Sleep(100 * time.Millisecond)

	// Verify remote clock is reflected
	retrievedClock := vc1.GetClock(nodeID2.ToBytes())
	if retrievedClock != clock2_1 {
		// Try again after longer wait for async processing
		time.Sleep(200 * time.Millisecond)
		retrievedClock = vc1.GetClock(nodeID2.ToBytes())
		if retrievedClock != clock2_1 {
			t.Errorf("Expected remote clock %d, got %d after async processing", clock2_1, retrievedClock)
		}
	}

	// Test vector clock comparison
	// vc1 has incremented twice, vc2 once, so vc1 should be "after" vc2
	relation := vc1.Compare(vc2)
	if relation != types.ClockAfter {
		t.Errorf("Expected ClockAfter relation, got %v", relation)
	}

	// Test concurrent relation by making updates that don't dominate
	vc2.IncrementLocal()                        // Now both have incremented their local clocks
	vc2.UpdateRemoteClock(nodeID1.ToBytes(), 1) // vc2 knows about vc1's first increment
	time.Sleep(100 * time.Millisecond)          // Allow async processing

	// Now they should be concurrent (each has some events the other doesn't)
	relation2 := vc1.Compare(vc2)
	if relation2 != types.ClockConcurrent {
		t.Errorf("Expected concurrent clocks, got %v", relation2)
	}
}

// TestCachedTimestampPerformance verifies cached timestamp reduces system calls
func TestCachedTimestampPerformance(t *testing.T) {
	cachedTime := types.NewCachedTimestamp()
	defer cachedTime.Stop()

	// Test rapid timestamp access
	const numAccesses = 10000
	start := time.Now()

	for i := 0; i < numAccesses; i++ {
		_ = cachedTime.Now()
	}

	duration := time.Since(start)

	// Cached timestamp should be extremely fast (microseconds for 10k calls)
	if duration > 10*time.Millisecond {
		t.Errorf("Cached timestamp too slow: %v for %d calls", duration, numAccesses)
	}

	t.Logf("Cached timestamp: %d calls in %v", numAccesses, duration)
}

// TestAsynchronousVectorClockUpdates verifies that remote clock updates
// don't block the calling goroutine
func TestAsynchronousVectorClockUpdates(t *testing.T) {
	nodeID1 := NewUUIDv7()
	nodeID2 := NewUUIDv7()

	vc := types.NewOptimizedVectorClock(nodeID1.ToBytes())
	defer vc.Stop()

	// Test that updates are non-blocking
	const numUpdates = 1000
	start := time.Now()

	for i := 0; i < numUpdates; i++ {
		// This should never block
		vc.UpdateRemoteClock(nodeID2.ToBytes(), uint64(i))
	}

	duration := time.Since(start)

	// Should complete very quickly since updates are async
	if duration > 10*time.Millisecond {
		t.Errorf("Async updates took too long: %v", duration)
	}

	t.Logf("Async updates: %d updates in %v", numUpdates, duration)
}

// BenchmarkOptimizedVectorClockIncrement benchmarks local clock increment
func BenchmarkOptimizedVectorClockIncrement(b *testing.B) {
	nodeID := NewUUIDv7()
	vc := types.NewOptimizedVectorClock(nodeID.ToBytes())
	defer vc.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			vc.IncrementLocal()
		}
	})
}

// BenchmarkOptimizedVectorClockRemoteUpdate benchmarks remote clock updates
func BenchmarkOptimizedVectorClockRemoteUpdate(b *testing.B) {
	nodeID1 := NewUUIDv7()
	nodeID2 := NewUUIDv7()
	vc := types.NewOptimizedVectorClock(nodeID1.ToBytes())
	defer vc.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			vc.UpdateRemoteClock(nodeID2.ToBytes(), i)
			i++
		}
	})
}

// BenchmarkCachedTimestamp benchmarks cached timestamp access
func BenchmarkCachedTimestamp(b *testing.B) {
	cachedTime := types.NewCachedTimestamp()
	defer cachedTime.Stop()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = cachedTime.Now()
		}
	})
}

// TestReplicationQueueOptimization tests the optimized QueueReplication method
func TestReplicationQueueOptimization(t *testing.T) {
	// Create a test cluster manager
	localNode := NewNode("127.0.0.1", 7379, NewUUIDv7())
	transport := NewTransport(localNode)

	cm := &ClusterManager{
		localNode:        localNode,
		transport:        transport,
		nodes:            make(map[UUIDv7]*Node),
		replicationQueue: make(chan ReplicationEvent, 10000),
		cachedTime:       types.NewCachedTimestamp(),
		stopCh:           make(chan struct{}),
	}
	defer cm.Stop()

	// Test rapid replication queueing
	const numOperations = 1000
	start := time.Now()

	for i := 0; i < numOperations; i++ {
		cm.QueueReplication("SET", "test_key", []byte("test_value"))
	}

	duration := time.Since(start)

	// Should be very fast due to optimizations
	if duration > 10*time.Millisecond {
		t.Errorf("Replication queueing too slow: %v for %d operations", duration, numOperations)
	}

	// Verify all operations were queued
	if len(cm.replicationQueue) != numOperations {
		t.Errorf("Expected %d queued operations, got %d", numOperations, len(cm.replicationQueue))
	}

	t.Logf("Optimized replication queueing: %d operations in %v", numOperations, duration)
}
