package store

import (
	"sync/atomic"
)

// getShardIndex returns the shard index for a key
func (s *LockFreeStore) getShardIndex(key string) int {
	hash := FastHash(key)
	return int(hash & s.shardMask)
}

// ShardOperation represents a shard operation result
type ShardOperation struct {
	Found      bool
	Value      *StoredValue
	OldSize    int64
	NewSize    int64
	KeyCreated bool
	KeyDeleted bool
}

// ShardReadFunc represents a read operation on a shard
type ShardReadFunc func(*LockFreeShard) ShardOperation

// ShardWriteFunc represents a write operation on a shard
type ShardWriteFunc func(*LockFreeShard) ShardOperation

// withShardRead performs a read operation on a shard with read lock
func (s *LockFreeStore) withShardRead(key string, fn ShardReadFunc) ShardOperation {
	shard := s.getShard(key)
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	return fn(shard)
}

// withShardWrite performs a write operation on a shard with write lock
func (s *LockFreeStore) withShardWrite(key string, fn ShardWriteFunc) ShardOperation {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	result := fn(shard)

	// Update metrics based on operation result
	if result.KeyCreated {
		s.asyncMetrics.AddKeyUpdate(s.getShardIndex(key), 1)
		atomic.AddInt64(&s.totalKeys, 1)
	}

	if result.KeyDeleted {
		s.asyncMetrics.AddKeyUpdate(s.getShardIndex(key), -1)
		atomic.AddInt64(&s.totalKeys, -1)
	}

	// Update memory metrics if size changed
	if result.NewSize != result.OldSize {
		delta := result.NewSize - result.OldSize
		s.asyncMetrics.AddMemoryUpdate(s.getShardIndex(key), delta)
		atomic.AddInt64(&s.totalMemory, delta)
	}

	return result
}

// getValue safely gets a value from shard data
func getValue(shard *LockFreeShard, key string) (value *StoredValue, found bool) {
	value, found = shard.data[key]
	return
}

// setValue safely sets a value in shard data
func setValue(shard *LockFreeShard, key string, value *StoredValue) {
	shard.data[key] = value
}

// deleteValue safely deletes a value from shard data
func deleteValue(shard *LockFreeShard, key string) (existed bool) {
	_, existed = shard.data[key]
	if existed {
		delete(shard.data, key)
	}
	return
}

// updateValue safely updates a value in shard data if it exists and matches expected
func updateValue(shard *LockFreeShard, key string, expected, new *StoredValue) (updated bool) {
	if current, exists := shard.data[key]; exists && current == expected {
		shard.data[key] = new
		return true
	}
	return false
}

// MemoryTracker helps track memory changes during operations
type MemoryTracker struct {
	store    *LockFreeStore
	shardIdx int
	keyLen   int64
}

// NewMemoryTracker creates a new memory tracker for a key
func (s *LockFreeStore) NewMemoryTracker(key string) *MemoryTracker {
	return &MemoryTracker{
		store:    s,
		shardIdx: s.getShardIndex(key),
		keyLen:   int64(len(key)),
	}
}

// TrackNewKey tracks memory for a new key creation
func (mt *MemoryTracker) TrackNewKey(valueSize int64) {
	totalSize := mt.keyLen + valueSize
	mt.store.asyncMetrics.AddKeyUpdate(mt.shardIdx, 1)
	mt.store.asyncMetrics.AddMemoryUpdate(mt.shardIdx, totalSize)
	atomic.AddInt64(&mt.store.totalKeys, 1)
	atomic.AddInt64(&mt.store.totalMemory, totalSize)
}

// TrackDeletedKey tracks memory for a deleted key
func (mt *MemoryTracker) TrackDeletedKey(oldValueSize int64) {
	totalSize := mt.keyLen + oldValueSize
	mt.store.asyncMetrics.AddKeyUpdate(mt.shardIdx, -1)
	mt.store.asyncMetrics.AddMemoryUpdate(mt.shardIdx, -totalSize)
	atomic.AddInt64(&mt.store.totalKeys, -1)
	atomic.AddInt64(&mt.store.totalMemory, -totalSize)
}

// TrackUpdatedValue tracks memory change for an updated value
func (mt *MemoryTracker) TrackUpdatedValue(oldSize, newSize int64) {
	if delta := newSize - oldSize; delta != 0 {
		mt.store.asyncMetrics.AddMemoryUpdate(mt.shardIdx, delta)
		atomic.AddInt64(&mt.store.totalMemory, delta)
	}
}
