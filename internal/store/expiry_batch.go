package store

import (
	"sync"
	"time"
)

// ExpiryOperation represents a batched expiry operation
type ExpiryOperation struct {
	Type  ExpiryOpType
	Shard *LockFreeShard
	Key   string
}

type ExpiryOpType int

const (
	ExpiryRemoval   ExpiryOpType = iota // Remove from expiry map only
	ExpiredDeletion                     // Delete expired key from store and expiry
)

// ExpiryBatchProcessor batches expiry operations for better performance
type ExpiryBatchProcessor struct {
	store  *LockFreeStore
	queue  chan ExpiryOperation
	batch  []ExpiryOperation
	mu     sync.Mutex
	stopCh chan struct{}
	doneCh chan struct{}

	// Configuration
	batchSize    int
	flushTimeout time.Duration
}

// NewExpiryBatchProcessor creates a new batch processor for expiry operations
func NewExpiryBatchProcessor() *ExpiryBatchProcessor {
	return &ExpiryBatchProcessor{
		queue:        make(chan ExpiryOperation, 1000), // Buffer for 1000 operations
		batch:        make([]ExpiryOperation, 0, 50),   // Batch up to 50 operations
		stopCh:       make(chan struct{}),
		doneCh:       make(chan struct{}),
		batchSize:    50,                    // Process in batches of 50
		flushTimeout: 10 * time.Millisecond, // Flush every 10ms if batch not full
	}
}

// SetStore sets the store reference
func (ebp *ExpiryBatchProcessor) SetStore(store *LockFreeStore) {
	ebp.store = store
}

// QueueExpiryRemoval queues an expiry removal operation (remove from expiry map only)
func (ebp *ExpiryBatchProcessor) QueueExpiryRemoval(shard *LockFreeShard, key string) {
	select {
	case ebp.queue <- ExpiryOperation{
		Type:  ExpiryRemoval,
		Shard: shard,
		Key:   key,
	}:
		// Queued successfully
	default:
		// Queue full, fallback to direct operation
		ebp.removeExpiryDirect(shard, key)
	}
}

// QueueExpiredDeletion queues an expired key deletion operation (delete from store and expiry)
func (ebp *ExpiryBatchProcessor) QueueExpiredDeletion(shard *LockFreeShard, key string) {
	select {
	case ebp.queue <- ExpiryOperation{
		Type:  ExpiredDeletion,
		Shard: shard,
		Key:   key,
	}:
		// Queued successfully
	default:
		// Queue full, fallback to direct operation
		ebp.deleteExpiredDirect(shard, key)
	}
}

// Start begins the batch processing loop
func (ebp *ExpiryBatchProcessor) Start() {
	defer close(ebp.doneCh)

	ticker := time.NewTicker(ebp.flushTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ebp.stopCh:
			// Process remaining operations before shutdown
			ebp.flushBatch()
			return

		case op := <-ebp.queue:
			ebp.mu.Lock()
			ebp.batch = append(ebp.batch, op)
			shouldFlush := len(ebp.batch) >= ebp.batchSize
			ebp.mu.Unlock()

			if shouldFlush {
				ebp.flushBatch()
			}

		case <-ticker.C:
			// Flush on timeout even if batch is not full
			ebp.flushBatch()
		}
	}
}

// flushBatch processes all operations in the current batch
func (ebp *ExpiryBatchProcessor) flushBatch() {
	ebp.mu.Lock()
	if len(ebp.batch) == 0 {
		ebp.mu.Unlock()
		return
	}

	// Copy batch to process
	currentBatch := make([]ExpiryOperation, len(ebp.batch))
	copy(currentBatch, ebp.batch)

	// Clear the batch
	ebp.batch = ebp.batch[:0]
	ebp.mu.Unlock()

	// Process operations by type for better cache locality
	var removals []ExpiryOperation
	var deletions []ExpiryOperation

	for _, op := range currentBatch {
		switch op.Type {
		case ExpiryRemoval:
			removals = append(removals, op)
		case ExpiredDeletion:
			deletions = append(deletions, op)
		}
	}

	// Process all removals first (lighter operations)
	for _, op := range removals {
		ebp.removeExpiryDirect(op.Shard, op.Key)
	}

	// Process all deletions (heavier operations)
	for _, op := range deletions {
		ebp.deleteExpiredDirect(op.Shard, op.Key)
	}
}

// removeExpiryDirect removes key from expiry map directly
func (ebp *ExpiryBatchProcessor) removeExpiryDirect(shard *LockFreeShard, key string) {
	shard.expiryMu.Lock()
	delete(shard.expiry, key)
	shard.expiryMu.Unlock()
}

// deleteExpiredDirect deletes expired key from store and expiry map directly.
//
// The deletion is queued by the cleaner and processed up to a flush interval
// later. In that window a SET may have rewritten the key and cleared or renewed
// its TTL, so we MUST re-verify the key is still present and still expired
// inside the critical section — otherwise we would destroy a fresh value and
// drop the new TTL.
func (ebp *ExpiryBatchProcessor) deleteExpiredDirect(shard *LockFreeShard, key string) {
	// Check the value and its TTL, and remove both, all under the single shard
	// lock. SET clears/renews the expiry under this same lock, so holding it
	// across the check-and-delete makes the two operations serialize: if the key
	// was rewritten we observe the cleared/renewed expiry and leave it alone.
	shard.mu.Lock()
	oldValue, existed := shard.data[key]

	stillExpired := false
	if existed {
		shard.expiryMu.RLock()
		exp, hasExp := shard.expiry[key]
		shard.expiryMu.RUnlock()
		stillExpired = hasExp && time.Now().After(exp)
	}

	if stillExpired {
		delete(shard.data, key)
		shard.expiryMu.Lock()
		delete(shard.expiry, key)
		shard.expiryMu.Unlock()
	}
	shard.mu.Unlock()

	// Only mutate accounting when we actually reaped the key. If the key was
	// renewed we leave both its value and its new TTL intact.
	if !stillExpired {
		return
	}

	if ebp.store != nil {
		keyLen := int64(len(key))
		shardIdx := int(FastHash(key) & ebp.store.shardMask)

		if oldValue != nil {
			memoryDelta := -(keyLen + oldValue.memorySize)
			ebp.store.asyncMetrics.AddMemoryUpdate(shardIdx, memoryDelta)
			oldValue.Release()
		}

		ebp.store.asyncMetrics.AddKeyUpdate(shardIdx, -1)
	}
}

// Stop stops the batch processor
func (ebp *ExpiryBatchProcessor) Stop() {
	close(ebp.stopCh)
	<-ebp.doneCh // Wait for processing to complete
}
