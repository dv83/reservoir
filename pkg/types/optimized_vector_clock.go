package types

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// OptimizedVectorClock represents a high-performance lock-free vector clock
// designed for hot path operations in multi-master replication
type OptimizedVectorClock struct {
	// Lock-free node clock array for fast local increments
	// Using atomic operations for the local node's clock
	localClock  atomic.Uint64
	localNodeID [16]byte // UUIDv7 bytes for fast comparison

	// Remote node clocks stored in a lock-free slice
	// Updated asynchronously to avoid blocking write operations
	remoteClocks atomic.Pointer[[]NodeClock] // Points to []NodeClock

	// Asynchronous update channel for remote clock updates
	updateChan chan ClockUpdate

	// Background processor for batched updates
	processor *ClockProcessor
}

// NodeClock represents a single node's clock entry
type NodeClock struct {
	NodeID [16]byte // UUIDv7 as byte array for fast comparison
	Clock  uint64   // Clock value (read atomically)
	_      [48]byte // Cache line padding to prevent false sharing
}

// ClockUpdate represents a pending clock update
type ClockUpdate struct {
	NodeID [16]byte
	Clock  uint64
}

// ClockProcessor handles batched asynchronous vector clock updates
type ClockProcessor struct {
	updateChan  chan ClockUpdate
	batchBuffer []ClockUpdate
	ticker      *time.Timer
	stopCh      chan struct{}
	wg          sync.WaitGroup

	// Callback to apply batched updates
	applyUpdates func([]ClockUpdate)

	// Dropped update tracking for reconciliation
	droppedUpdates []ClockUpdate
	droppedMutex   sync.Mutex
	droppedCount   atomic.Uint64
}

// NewOptimizedVectorClock creates a new optimized vector clock
func NewOptimizedVectorClock(localNodeID [16]byte) *OptimizedVectorClock {
	// Increased buffer size and use larger channel to prevent dropping
	updateChan := make(chan ClockUpdate, 10000) // 10x larger buffer to prevent drops

	vc := &OptimizedVectorClock{
		localNodeID: localNodeID,
		updateChan:  updateChan,
	}

	// Initialize with empty remote clocks slice
	vc.remoteClocks.Store(&[]NodeClock{})

	// Create and start background processor
	vc.processor = NewClockProcessor(updateChan, vc.applyBatchedUpdates)
	vc.processor.Start()

	return vc
}

// NewClockProcessor creates a new clock processor for batched updates
func NewClockProcessor(updateChan chan ClockUpdate, applyFunc func([]ClockUpdate)) *ClockProcessor {
	return &ClockProcessor{
		updateChan:     updateChan,
		batchBuffer:    make([]ClockUpdate, 0, 100), // Pre-allocated batch buffer
		applyUpdates:   applyFunc,
		stopCh:         make(chan struct{}),
		droppedUpdates: make([]ClockUpdate, 0, 50), // Buffer for dropped updates
	}
}

// Start begins the background processing
func (cp *ClockProcessor) Start() {
	cp.wg.Add(1)
	go cp.processingLoop()
}

// Stop stops the background processing
func (cp *ClockProcessor) Stop() {
	close(cp.stopCh)
	cp.wg.Wait()
	if cp.ticker != nil {
		cp.ticker.Stop()
	}
}

// processingLoop handles batched updates in background
func (cp *ClockProcessor) processingLoop() {
	defer cp.wg.Done()

	const batchTimeout = 10 * time.Millisecond // Fast batching for low latency
	cp.ticker = time.NewTimer(batchTimeout)
	defer cp.ticker.Stop()

	for {
		select {
		case <-cp.stopCh:
			// Process remaining updates before shutdown
			if len(cp.batchBuffer) > 0 {
				cp.applyUpdates(cp.batchBuffer)
			}
			return

		case update := <-cp.updateChan:
			cp.batchBuffer = append(cp.batchBuffer, update)

			// Apply batch if buffer is full
			if len(cp.batchBuffer) >= cap(cp.batchBuffer) {
				cp.applyUpdates(cp.batchBuffer)
				cp.batchBuffer = cp.batchBuffer[:0] // Reset buffer
				cp.ticker.Reset(batchTimeout)
			}

		case <-cp.ticker.C:
			// Apply partial batch on timeout
			if len(cp.batchBuffer) > 0 {
				cp.applyUpdates(cp.batchBuffer)
				cp.batchBuffer = cp.batchBuffer[:0] // Reset buffer
			}
			cp.ticker.Reset(batchTimeout)
		}
	}
}

// IncrementLocal increments the local node's clock (lock-free, hot path optimized)
func (vc *OptimizedVectorClock) IncrementLocal() uint64 {
	// Atomic increment - no locks, no blocking
	return vc.localClock.Add(1)
}

// GetLocalClock returns the current local clock value (lock-free read)
func (vc *OptimizedVectorClock) GetLocalClock() uint64 {
	return vc.localClock.Load()
}

// UpdateRemoteClock queues a remote clock update with retry and overflow protection
func (vc *OptimizedVectorClock) UpdateRemoteClock(nodeID [16]byte, clock uint64) {
	update := ClockUpdate{
		NodeID: nodeID,
		Clock:  clock,
	}

	// Try multiple attempts with backoff to prevent dropping critical updates
	for attempt := 0; attempt < 3; attempt++ {
		select {
		case vc.updateChan <- update:
			// Update queued successfully
			return
		default:
			// Channel full, try again with exponential backoff
			if attempt < 2 {
				// Brief pause to allow processing, but keep it minimal for hot path
				time.Sleep(time.Microsecond * time.Duration(1<<attempt)) // 1μs, 2μs
				continue
			}
			// Final attempt failed - this is a critical issue
			// Log the drop for monitoring (will be tracked for reconciliation)
			vc.processor.recordDroppedUpdate(update)
		}
	}
}

// applyBatchedUpdates applies a batch of remote clock updates
func (vc *OptimizedVectorClock) applyBatchedUpdates(updates []ClockUpdate) {
	if len(updates) == 0 {
		return
	}

	// Load current remote clocks
	currentPtr := vc.remoteClocks.Load()
	if currentPtr == nil {
		return
	}
	current := *currentPtr

	// Create new slice with potential growth for new nodes
	updated := make([]NodeClock, len(current), len(current)+len(updates))
	copy(updated, current)

	// Apply all updates in batch
	for _, update := range updates {
		updated = vc.updateNodeInSlice(updated, update.NodeID, update.Clock)
	}

	// Atomic pointer swap
	vc.remoteClocks.Store(&updated)
}

// updateNodeInSlice updates a specific node in the slice, growing if needed
func (vc *OptimizedVectorClock) updateNodeInSlice(clocks []NodeClock, nodeID [16]byte, clock uint64) []NodeClock {
	// Find existing node
	for i := range clocks {
		if clocks[i].NodeID == nodeID {
			// Update existing node if clock is newer
			if clock > clocks[i].Clock {
				atomic.StoreUint64(&clocks[i].Clock, clock)
			}
			return clocks
		}
	}

	// Node not found, add new entry
	newEntry := NodeClock{
		NodeID: nodeID,
		Clock:  clock,
	}
	return append(clocks, newEntry)
}

// GetClock returns the clock value for a specific node (lock-free read)
func (vc *OptimizedVectorClock) GetClock(nodeID [16]byte) uint64 {
	// Check if it's the local node
	if nodeID == vc.localNodeID {
		return vc.localClock.Load()
	}

	// Read remote clocks atomically
	clocksPtr := vc.remoteClocks.Load()
	if clocksPtr == nil {
		return 0
	}

	clocks := *clocksPtr
	for i := range clocks {
		if clocks[i].NodeID == nodeID {
			// Read clock atomically (though it's rarely updated, we want consistency)
			return atomic.LoadUint64(&clocks[i].Clock)
		}
	}

	return 0
}

// GetSnapshot returns a consistent snapshot of all clocks (for heartbeats)
func (vc *OptimizedVectorClock) GetSnapshot() map[[16]byte]uint64 {
	snapshot := make(map[[16]byte]uint64)

	// Add local clock
	snapshot[vc.localNodeID] = vc.localClock.Load()

	// Add remote clocks
	clocksPtr := vc.remoteClocks.Load()
	if clocksPtr != nil {
		clocks := *clocksPtr
		for i := range clocks {
			snapshot[clocks[i].NodeID] = atomic.LoadUint64(&clocks[i].Clock)
		}
	}

	return snapshot
}

// ClockRelation describes the causal relationship between two vector clocks.
type ClockRelation int

const (
	ClockBefore ClockRelation = iota
	ClockAfter
	ClockConcurrent
	ClockEqual
)

// Compare compares this vector clock with another (for consistency checks)
func (vc *OptimizedVectorClock) Compare(other *OptimizedVectorClock) ClockRelation {
	// Get snapshots for consistent comparison
	thisSnapshot := vc.GetSnapshot()
	otherSnapshot := other.GetSnapshot()

	// Get all unique node IDs
	allNodes := make(map[[16]byte]struct{})
	for nodeID := range thisSnapshot {
		allNodes[nodeID] = struct{}{}
	}
	for nodeID := range otherSnapshot {
		allNodes[nodeID] = struct{}{}
	}

	allLessEqual := true
	allGreaterEqual := true
	equal := true

	for nodeID := range allNodes {
		thisValue := thisSnapshot[nodeID]   // 0 if not present
		otherValue := otherSnapshot[nodeID] // 0 if not present

		if thisValue < otherValue {
			allGreaterEqual = false
		}
		if thisValue > otherValue {
			allLessEqual = false
		}
		if thisValue != otherValue {
			equal = false
		}
	}

	if equal {
		return ClockEqual
	} else if allLessEqual {
		return ClockBefore
	} else if allGreaterEqual {
		return ClockAfter
	} else {
		return ClockConcurrent
	}
}

// recordDroppedUpdate records a dropped update for later reconciliation
func (cp *ClockProcessor) recordDroppedUpdate(update ClockUpdate) {
	cp.droppedMutex.Lock()
	defer cp.droppedMutex.Unlock()

	cp.droppedUpdates = append(cp.droppedUpdates, update)
	cp.droppedCount.Add(1)

	// Prevent unbounded growth - keep only last 100 dropped updates
	if len(cp.droppedUpdates) > 100 {
		cp.droppedUpdates = cp.droppedUpdates[len(cp.droppedUpdates)-100:]
	}
}

// GetDroppedUpdates returns and clears dropped updates for reconciliation
func (cp *ClockProcessor) GetDroppedUpdates() []ClockUpdate {
	cp.droppedMutex.Lock()
	defer cp.droppedMutex.Unlock()

	if len(cp.droppedUpdates) == 0 {
		return nil
	}

	dropped := make([]ClockUpdate, len(cp.droppedUpdates))
	copy(dropped, cp.droppedUpdates)
	cp.droppedUpdates = cp.droppedUpdates[:0] // Clear the slice

	return dropped
}

// GetDroppedCount returns the total number of dropped updates
func (cp *ClockProcessor) GetDroppedCount() uint64 {
	return cp.droppedCount.Load()
}

// GetDroppedCount returns the total number of dropped vector clock updates
func (vc *OptimizedVectorClock) GetDroppedCount() uint64 {
	if vc.processor != nil {
		return vc.processor.GetDroppedCount()
	}
	return 0
}

// Stop shuts down the vector clock processor
func (vc *OptimizedVectorClock) Stop() {
	if vc.processor != nil {
		vc.processor.Stop()
	}
}

// CachedTimestamp provides batched time.Now() calls to reduce system call overhead
type CachedTimestamp struct {
	timestamp atomic.Value // stores time.Time
	ticker    *time.Timer
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewCachedTimestamp creates a new cached timestamp that updates every 5ms
func NewCachedTimestamp() *CachedTimestamp {
	ct := &CachedTimestamp{
		stopCh: make(chan struct{}),
	}

	// Initialize with current time
	ct.timestamp.Store(time.Now())

	// Start background updater
	ct.wg.Add(1)
	go ct.updateLoop()

	return ct
}

// updateLoop updates the cached timestamp periodically
func (ct *CachedTimestamp) updateLoop() {
	defer ct.wg.Done()

	const updateInterval = 5 * time.Millisecond // 5ms cache window
	ct.ticker = time.NewTimer(updateInterval)
	defer ct.ticker.Stop()

	for {
		select {
		case <-ct.stopCh:
			return
		case <-ct.ticker.C:
			ct.timestamp.Store(time.Now())
			ct.ticker.Reset(updateInterval)
		}
	}
}

// Now returns the cached timestamp (no system call)
func (ct *CachedTimestamp) Now() time.Time {
	return ct.timestamp.Load().(time.Time)
}

// Stop stops the background updater
func (ct *CachedTimestamp) Stop() {
	close(ct.stopCh)
	ct.wg.Wait()
}

// UnsafeString converts []byte to string without allocation (zero-copy)
// Used for performance-critical path in node ID comparisons
func UnsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// UnsafeBytes converts string to []byte without allocation (zero-copy)
// Used for performance-critical path
func UnsafeBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(
		&struct {
			string
			Cap int
		}{s, len(s)},
	))
}
