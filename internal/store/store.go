package store

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	pkgErrors "reservoir/pkg/errors"
)

const numShards = 256

// Limits holds the memory and size constraints
type Limits struct {
	MaxKeySize     int64
	MaxValueSize   int64
	MaxMemoryUsage int64
}

// Store is now an alias for LockFreeStore - we unified to lock-free only
type Store = LockFreeStore

// NewStore creates a new lock-free store (unified interface)
func NewStore(limits *Limits) *Store {
	return NewLockFreeStore(limits)
}

// LockFreeShard представляє шард з RWMutex для стабільності під навантаженням
// Оптимізовано для cache line alignment та розділення hot/cold даних
type LockFreeShard struct {
	// HOT DATA: часто використовувані поля в першій cache line (64 bytes)
	mu          sync.RWMutex            // 24 bytes на x86-64
	data        map[string]*StoredValue // 8 bytes (pointer)
	memoryUsage int64                   // 8 bytes - atomic counter
	keyCount    int64                   // 8 bytes - atomic counter
	_           [16]byte                // padding до 64 bytes

	// SECOND CACHE LINE: середньо використовувані поля
	avgKeySize   int64    // 8 bytes - atomic - середній розмір ключа
	avgValueSize int64    // 8 bytes - atomic - середній розмір значення
	_            [48]byte // padding до наступної cache line

	// COLD DATA: рідко використовувані поля (expiry operations)
	expiryMu sync.RWMutex         // 24 bytes
	expiry   map[string]time.Time // 8 bytes (pointer)

	// tombstones records the LWW stamp of deleted keys (guarded by mu) so a
	// late-arriving older write cannot resurrect a key deleted more recently.
	// Only consulted by the LWW set/delete paths; GC'd by age.
	tombstones map[string]tombstone
}

// tombstone marks a key as deleted at a given LWW stamp, retained until GC.
type tombstone struct {
	stamp   hlcStamp
	deleted time.Time
}

// LockFreeStore - оптимізована версія Store з lock-free операціями
// Оптимізовано для cache line alignment
type LockFreeStore struct {
	// HOT DATA: найчастіше використовувані поля (перша cache line)
	shards          [numShards]*LockFreeShard // 8 bytes (pointer to array)
	totalMemory     int64                     // 8 bytes - atomic
	totalKeys       int64                     // 8 bytes - atomic
	shardMask       uint32                    // 4 bytes - для bit masking
	commitLogActive uint32                    // 4 bytes - atomic flag for fast check
	_               [32]byte                  // padding до 64 bytes

	// SECOND CACHE LINE: середньо використовувані поля
	limits         *Limits               // 8 bytes (pointer)
	asyncMetrics   *AsyncMetrics         // 8 bytes (pointer)
	latencyMetrics *LatencyMetrics       // 8 bytes (pointer)
	expiryBatch    *ExpiryBatchProcessor // 8 bytes (pointer) - batch expiry deletion
	_              [32]byte              // padding до наступної cache line

	// COLD DATA: рідко використовувані поля (control channels)
	stopCh            chan struct{}      // 8 bytes
	doneCh            chan struct{}      // 8 bytes
	deferredScheduler *DeferredScheduler // 8 bytes - deferred command scheduler
	commitLog         interface{}        // 8 bytes - commit log (interface to avoid import cycle)
	commitLogDir      string             // 8 bytes - commit log directory path
	recoveryActive    uint32             // 4 bytes - atomic flag for recovery mode
	localOrigin       uint64             // 8 bytes - atomic; this node's id for the counter CRDT (0 = non-cluster, plain INCR)
	_                 [24]byte           // padding до cache line boundary
}

// SetLocalOrigin records this node's origin id so INCR/DECR maintain the PN
// counter CRDT for convergent multi-master increments. A zero origin (the
// default) keeps the plain, non-CRDT increment path for single-node use.
func (s *LockFreeStore) SetLocalOrigin(origin uint64) {
	atomic.StoreUint64(&s.localOrigin, origin)
}

// NewLockFreeStore створює нову lock-free версію store
func NewLockFreeStore(limits *Limits) *LockFreeStore {
	store := &LockFreeStore{
		limits:         limits,
		shardMask:      numShards - 1, // 256 - 1 = 255 для bit masking
		asyncMetrics:   NewAsyncMetrics(),
		latencyMetrics: NewLatencyMetrics(),
		expiryBatch:    NewExpiryBatchProcessor(), // batch expiry processor
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
	}

	// Initialize deferred scheduler after store is created
	store.deferredScheduler = NewDeferredScheduler(store)

	// Start the deferred scheduler
	store.deferredScheduler.Start()

	for i := 0; i < numShards; i++ {
		store.shards[i] = &LockFreeShard{
			data:       make(map[string]*StoredValue),
			expiry:     make(map[string]time.Time),
			tombstones: make(map[string]tombstone),
		}
	}

	// Встановлюємо посилання на store в async metrics
	store.asyncMetrics.SetStore(store)

	// Запускаємо batch expiry processor
	store.expiryBatch.SetStore(store)
	go store.expiryBatch.Start()

	// Запускаємо background cleanup
	go store.startExpirationCleanup()

	return store
}

// SetCommitLog sets the commit log for the store
func (s *LockFreeStore) SetCommitLog(commitLog interface{}) {
	s.commitLog = commitLog
	if commitLog != nil {
		atomic.StoreUint32(&s.commitLogActive, 1)
	} else {
		atomic.StoreUint32(&s.commitLogActive, 0)
	}
}

// SetCommitLogDir sets the commit log directory path
func (s *LockFreeStore) SetCommitLogDir(dir string) {
	s.commitLogDir = dir
}

// GetCommitLog returns the commit log interface
func (s *LockFreeStore) GetCommitLog() interface{} {
	return s.commitLog
}

// GetCommitLogDir returns the commit log directory path
func (s *LockFreeStore) GetCommitLogDir() string {
	return s.commitLogDir
}

// GetCommitLogStats returns commit log statistics
func (s *LockFreeStore) GetCommitLogStats() map[string]interface{} {
	if s.commitLog == nil {
		return nil
	}

	// Use type assertion to call Stats() method
	type CommitLogStatsProvider interface {
		Stats() map[string]interface{}
	}

	if statsProvider, ok := s.commitLog.(CommitLogStatsProvider); ok {
		return statsProvider.Stats()
	}

	return nil
}

// TriggerCommitLogCompaction triggers manual compaction
func (s *LockFreeStore) TriggerCommitLogCompaction() error {
	if s.commitLog == nil {
		return fmt.Errorf("commit log not enabled")
	}

	// Use type assertion to trigger compaction
	type CommitLogCompactor interface {
		TriggerCompaction() error
	}

	if compactor, ok := s.commitLog.(CommitLogCompactor); ok {
		return compactor.TriggerCompaction()
	}

	return fmt.Errorf("commit log does not support manual compaction")
}

// SetRecoveryActive sets recovery mode (disables commit log writes during recovery)
func (s *LockFreeStore) SetRecoveryActive(active bool) {
	if active {
		atomic.StoreUint32(&s.recoveryActive, 1)
	} else {
		atomic.StoreUint32(&s.recoveryActive, 0)
	}
}

// IsRecoveryActive returns true if recovery is in progress
func (s *LockFreeStore) IsRecoveryActive() bool {
	return atomic.LoadUint32(&s.recoveryActive) == 1
}

// DeleteFast - lock-free версія Delete операції
func (s *LockFreeStore) DeleteFast(key string) bool {
	// Обчислюємо hash один раз using consistent hash function
	shard := s.getShardFast(key)
	hash := FastHash(key)
	shardIdx := int(hash & s.shardMask)

	// Deletion з RWMutex
	shard.mu.Lock()
	oldValue, existed := shard.data[key]
	if existed {
		delete(shard.data, key)
	}
	shard.mu.Unlock()
	if !existed {
		return false
	}

	// Асинхронне оновлення лічильників (консистентно з SetFast)
	keyLen := int64(len(key))

	if oldValue != nil {
		// Use MemoryUsage() rather than the raw memorySize field: for a list the
		// cached field is not maintained on in-place mutation, so it would be stale.
		memoryDelta := -(keyLen + oldValue.MemoryUsage())
		s.asyncMetrics.AddMemoryUpdate(shardIdx, memoryDelta)
		oldValue.Release() // Повертаємо до pool
	}

	// Асинхронно оновлюємо key count
	s.asyncMetrics.AddKeyUpdate(shardIdx, -1)

	// Batch видалення expiry для кращої продуктивності
	s.expiryBatch.QueueExpiryRemoval(shard, key)

	return true
}

// GetMemoryMap returns memory usage details for all keys
func (s *LockFreeStore) GetMemoryMap() ([]KeyMemoryInfo, error) {
	var result []KeyMemoryInfo

	// Pre-allocate slice to avoid frequent resizing (estimate usage)
	totalKeys := int(atomic.LoadInt64(&s.totalKeys))
	if totalKeys > 0 {
		result = make([]KeyMemoryInfo, 0, totalKeys)
	}

	// Track existing keys to avoid duplicates with pending commands
	existingKeys := make(map[string]bool)

	for i := 0; i < numShards; i++ {
		shard := s.shards[i]
		shard.mu.RLock()

		for key, val := range shard.data {
			// Check if expired (optimistic check)
			if s.isExpiredFast(shard, key) {
				continue
			}

			// Calculate size: value memory + key length
			size := val.MemoryUsage() + int64(len(key))

			// Check if key has TTL
			shard.expiryMu.RLock()
			_, hasTTL := shard.expiry[key]
			shard.expiryMu.RUnlock()

			info := KeyMemoryInfo{
				Key:      key,
				Type:     val.Type.String(),
				Size:     size,
				TTL:      hasTTL,
				Deferred: false, // Real data is NOT deferred/striped
			}
			result = append(result, info)
			existingKeys[key] = true
		}

		shard.mu.RUnlock()
	}

	// Add pending deferred commands as "phantom" striped items
	if s.deferredScheduler != nil {
		pendingCommands := s.deferredScheduler.ListDeferredCommands()
		for _, cmd := range pendingCommands {
			// Skip if key already exists (will be updated, not created)
			if existingKeys[cmd.Key] {
				continue
			}
			// Estimate size based on command args
			estimatedSize := int64(len(cmd.Key) + 50) // Base overhead
			for _, arg := range cmd.Args {
				estimatedSize += int64(len(arg))
			}

			info := KeyMemoryInfo{
				Key:      cmd.Key + " (pending)",
				Type:     "pending",
				Size:     estimatedSize,
				TTL:      false,
				Deferred: true, // Pending commands ARE striped
			}
			result = append(result, info)
		}
	}

	return result, nil
}

// getShardFast - оптимізована версія отримання шарду з SIMD
func (s *LockFreeStore) getShardFast(key string) *LockFreeShard {
	// Використовуємо SIMD-оптимізований hasher
	hash := FastHash(key)

	// Bit masking замість modulo (швидше)
	return s.shards[hash&s.shardMask]
}

// getShard - alias для getShardFast для сумісності
func (s *LockFreeStore) getShard(key string) *LockFreeShard {
	// Use the same hash function as getShardFast to ensure consistency
	return s.getShardFast(key)
}

// updateAverageSizes - оновлення середніх розмірів для оптимізації
func (s *LockFreeStore) updateAverageSizes(shard *LockFreeShard, keyLen, valueLen int64) {
	// Експоненціальне згладжування для середніх значень
	const alpha = 0.1 // коефіцієнт згладжування

	oldAvgKey := atomic.LoadInt64(&shard.avgKeySize)
	newAvgKey := int64(float64(oldAvgKey)*(1-alpha) + float64(keyLen)*alpha)
	atomic.StoreInt64(&shard.avgKeySize, newAvgKey)

	oldAvgValue := atomic.LoadInt64(&shard.avgValueSize)
	newAvgValue := int64(float64(oldAvgValue)*(1-alpha) + float64(valueLen)*alpha)
	atomic.StoreInt64(&shard.avgValueSize, newAvgValue)
}

// isExpiredFast - швидка перевірка expiry без блокувань
func (s *LockFreeStore) isExpiredFast(shard *LockFreeShard, key string) bool {
	shard.expiryMu.RLock()
	expireTime, hasExpiry := shard.expiry[key]
	shard.expiryMu.RUnlock()

	return hasExpiry && time.Now().After(expireTime)
}

// deleteExpiredAsync - DEPRECATED: replaced by batch processor
// Left for compatibility but not used in hot paths
func (s *LockFreeStore) deleteExpiredAsync(shard *LockFreeShard, key string) {
	// Fallback to batch processor
	if s.expiryBatch != nil {
		s.expiryBatch.QueueExpiredDeletion(shard, key)
		return
	}

	// Direct fallback if batch processor not available (should not happen)
	// Emergency fallback - process immediately
	shard.mu.Lock()
	oldValue, existed := shard.data[key]
	if existed {
		delete(shard.data, key)
	}
	shard.mu.Unlock()
	if existed {
		keyLen := int64(len(key))
		shardIdx := int(FastHash(key) & s.shardMask)
		if oldValue != nil {
			memoryDelta := -(keyLen + oldValue.MemoryUsage())
			s.asyncMetrics.AddMemoryUpdate(shardIdx, memoryDelta)
			oldValue.Release()
		}
		s.asyncMetrics.AddKeyUpdate(shardIdx, -1)
	}
	shard.expiryMu.Lock()
	delete(shard.expiry, key)
	shard.expiryMu.Unlock()
}

// removeExpiryAsync - DEPRECATED: replaced by batch processor
// Left for compatibility but not used in hot paths
func (s *LockFreeStore) removeExpiryAsync(shard *LockFreeShard, key string) {
	// Fallback to batch processor
	if s.expiryBatch != nil {
		s.expiryBatch.QueueExpiryRemoval(shard, key)
		return
	}

	// Direct fallback if batch processor not available
	shard.expiryMu.Lock()
	delete(shard.expiry, key)
	shard.expiryMu.Unlock()
}

// SetWithExpiry - встановлення значення з TTL
func (s *LockFreeStore) SetWithExpiry(key, value string, ttl time.Duration) error {
	// Спочатку звичайний set
	if err := s.Set(key, value); err != nil {
		return err
	}

	// Потім додаємо expiry
	shard := s.getShardFast(key)
	expireTime := time.Now().Add(ttl)

	shard.expiryMu.Lock()
	shard.expiry[key] = expireTime
	shard.expiryMu.Unlock()

	return nil
}

// logToCommitLog appends an operation to the commit log when it is active and we
// are not currently replaying recovery.
func (s *LockFreeStore) logToCommitLog(op string, key, value []byte) {
	if atomic.LoadUint32(&s.commitLogActive) == 1 && atomic.LoadUint32(&s.recoveryActive) == 0 {
		if cl, ok := s.commitLog.(CommitLogger); ok {
			cl.Write(op, key, value)
		}
	}
}

// SetExpiry - встановлення expiry для існуючого ключа
func (s *LockFreeStore) SetExpiry(key string, expireTime time.Time) bool {
	shard := s.getShard(key)

	// Check if key exists in the store
	shard.mu.RLock()
	_, exists := shard.data[key]
	shard.mu.RUnlock()

	if exists {
		shard.expiryMu.Lock()
		shard.expiry[key] = expireTime
		shard.expiryMu.Unlock()
		// Persist the TTL as an absolute deadline so it survives a restart.
		s.logToCommitLog("EXPIRE", []byte(key), []byte(strconv.FormatInt(expireTime.UnixNano(), 10)))
		return true
	}
	return false
}

// GetTTL - отримання TTL для ключа
func (s *LockFreeStore) GetTTL(key string) (time.Duration, bool) {
	shard := s.getShardFast(key)

	shard.expiryMu.RLock()
	expireTime, exists := shard.expiry[key]
	shard.expiryMu.RUnlock()

	if !exists {
		return 0, false
	}

	ttl := time.Until(expireTime)
	if ttl <= 0 {
		return 0, false
	}

	return ttl, true
}

// RemoveExpiry - видалення expiry для ключа
func (s *LockFreeStore) RemoveExpiry(key string) bool {
	shard := s.getShardFast(key)

	shard.expiryMu.Lock()
	defer shard.expiryMu.Unlock()

	// Check if key exists and has expiry
	if _, exists := shard.expiry[key]; exists {
		delete(shard.expiry, key)
		s.logToCommitLog("PERSIST", []byte(key), nil)
		return true
	}
	return false
}

// --- Canonical accounting helpers ---
//
// Every mutation must move the four counters together so they never drift:
// per-shard keyCount/memoryUsage and their global totals totalKeys/totalMemory.
// Historically different code paths updated different subsets (strings tracked
// totalKeys but not shard.keyCount; lists did the reverse; Delete forgot memory
// entirely), so counts and memory diverged over time. Route creates, deletes
// and in-place size changes through these three helpers.

// accountKeyAdded records a newly created key occupying memSize bytes.
func (s *LockFreeStore) accountKeyAdded(shard *LockFreeShard, memSize int64) {
	atomic.AddInt64(&shard.keyCount, 1)
	atomic.AddInt64(&s.totalKeys, 1)
	atomic.AddInt64(&shard.memoryUsage, memSize)
	atomic.AddInt64(&s.totalMemory, memSize)
}

// accountKeyRemoved records deletion of a key that occupied memSize bytes.
func (s *LockFreeStore) accountKeyRemoved(shard *LockFreeShard, memSize int64) {
	atomic.AddInt64(&shard.keyCount, -1)
	atomic.AddInt64(&s.totalKeys, -1)
	atomic.AddInt64(&shard.memoryUsage, -memSize)
	atomic.AddInt64(&s.totalMemory, -memSize)
}

// accountMemoryDelta records an in-place value size change (no key count change).
func (s *LockFreeStore) accountMemoryDelta(shard *LockFreeShard, delta int64) {
	if delta == 0 {
		return
	}
	atomic.AddInt64(&shard.memoryUsage, delta)
	atomic.AddInt64(&s.totalMemory, delta)
}

// wouldExceedMemory reports whether accepting a write that grows total memory by
// delta bytes would push it past the configured MaxMemoryUsage. This implements
// a "noeviction"-style policy: writes that would exceed the limit are rejected.
// A non-positive delta (overwrite that shrinks, or a delete) is always allowed.
func (s *LockFreeStore) wouldExceedMemory(delta int64) bool {
	if s.limits == nil || s.limits.MaxMemoryUsage <= 0 || delta <= 0 {
		return false
	}
	return atomic.LoadInt64(&s.totalMemory)+delta > s.limits.MaxMemoryUsage
}

// GetMemoryUsage - отримання загального використання пам'яті
func (s *LockFreeStore) GetMemoryUsage() int64 {
	return atomic.LoadInt64(&s.totalMemory)
}

// GetKeyCount - отримання загальної кількості ключів
func (s *LockFreeStore) GetKeyCount() int64 {
	return atomic.LoadInt64(&s.totalKeys)
}

// GetShardStats - отримання статистики всіх шардів
func (s *LockFreeStore) GetShardStats() []map[string]interface{} {
	stats := make([]map[string]interface{}, numShards)

	for i := 0; i < numShards; i++ {
		shard := s.shards[i]

		// Рахуємо кількість ключів з expiry
		shard.expiryMu.RLock()
		expiryCount := len(shard.expiry)
		shard.expiryMu.RUnlock()

		stats[i] = map[string]interface{}{
			"shard_id":       i,
			"key_count":      atomic.LoadInt64(&shard.keyCount),
			"memory_usage":   atomic.LoadInt64(&shard.memoryUsage),
			"avg_key_size":   atomic.LoadInt64(&shard.avgKeySize),
			"avg_value_size": atomic.LoadInt64(&shard.avgValueSize),
			"expiry_count":   expiryCount,
		}
	}

	return stats
}

// GetAllKeys - отримання всіх ключів (для debug/моніторингу)
func (s *LockFreeStore) GetAllKeys() []string {
	var keys []string

	for i := 0; i < numShards; i++ {
		shard := s.shards[i]
		shard.mu.RLock()
		for key := range shard.data {
			// Skip expired keys that the cleaner has not physically removed yet.
			if !s.isExpiredFast(shard, key) {
				keys = append(keys, key)
			}
		}
		shard.mu.RUnlock()
	}

	return keys
}

// ForceFlushMetrics примусово застосовує всі pending метрики
func (s *LockFreeStore) ForceFlushMetrics() {
	if s.asyncMetrics != nil {
		s.asyncMetrics.ForceFlush()
	}
}

// Stop зупиняє всі background процеси store
func (s *LockFreeStore) Stop() {
	// Stop expiration cleanup goroutine
	close(s.stopCh)
	<-s.doneCh // Wait for cleanup goroutine to finish

	// Stop expiry batch processor
	if s.expiryBatch != nil {
		s.expiryBatch.Stop()
	}

	// Stop async metrics
	if s.asyncMetrics != nil {
		s.asyncMetrics.Stop()
	}

	// Stop latency metrics
	if s.latencyMetrics != nil {
		s.latencyMetrics.Stop()
	}
}

// GetLatencyMetrics повертає поточні latency метрики
func (s *LockFreeStore) GetLatencyMetrics() map[string]map[string]time.Duration {
	if s.latencyMetrics == nil {
		return make(map[string]map[string]time.Duration)
	}
	return s.latencyMetrics.GetMetrics()
}

// startExpirationCleanup - background процес очищення прострочених ключів
func (s *LockFreeStore) startExpirationCleanup() {
	defer close(s.doneCh)
	ticker := time.NewTicker(time.Minute) // кожну хвилину
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.cleanupExpiredKeys()
			// Reap tombstones that have outlived any plausibly-delayed concurrent
			// write, bounding their memory. An hour is far beyond normal delivery.
			s.gcTombstones(time.Hour)
		}
	}
}

// cleanupExpiredKeys - очищення прострочених ключів
func (s *LockFreeStore) cleanupExpiredKeys() {
	now := time.Now()

	for i := 0; i < numShards; i++ {
		shard := s.shards[i]

		// Збираємо прострочені ключі
		var expiredKeys []string
		shard.expiryMu.RLock()
		for key, expireTime := range shard.expiry {
			if now.After(expireTime) {
				expiredKeys = append(expiredKeys, key)
			}
		}
		shard.expiryMu.RUnlock()

		// Видаляємо прострочені ключі через batch processor
		for _, key := range expiredKeys {
			s.deleteExpiredAsync(shard, key)
		}
	}
}

// Set - unified Set operation with inline optimization

// Clear removes all keys from the store
func (s *LockFreeStore) Clear() {
	// Write FLUSHALL to commit log BEFORE clearing data (if enabled AND not in recovery mode)
	if atomic.LoadUint32(&s.commitLogActive) == 1 && atomic.LoadUint32(&s.recoveryActive) == 0 {
		if cl, ok := s.commitLog.(CommitLogger); ok {
			cl.Write("FLUSHALL", []byte(""), []byte(""))
		}
	}

	for i := 0; i < numShards; i++ {
		shard := s.shards[i]

		// Clear the data map
		shard.mu.Lock()
		shard.data = make(map[string]*StoredValue)
		shard.mu.Unlock()

		// Clear expiry map
		shard.expiryMu.Lock()
		shard.expiry = make(map[string]time.Time)
		shard.expiryMu.Unlock()

		// Reset atomic counters
		atomic.StoreInt64(&shard.memoryUsage, 0)
		atomic.StoreInt64(&shard.keyCount, 0)
		atomic.StoreInt64(&shard.avgKeySize, 0)
		atomic.StoreInt64(&shard.avgValueSize, 0)
	}

	// Reset total counters
	atomic.StoreInt64(&s.totalMemory, 0)
	atomic.StoreInt64(&s.totalKeys, 0)
}

// BatchOperation для підвищення продуктивності batch операцій
type BatchOperation struct {
	Type  string // "SET", "DELETE"
	Key   string
	Value string
}

// ExecuteBatch - пакетне виконання операцій
func (s *LockFreeStore) ExecuteBatch(operations []BatchOperation) []error {
	errors := make([]error, len(operations))

	// Групуємо операції по шардам для кращої локальності
	shardOps := make(map[int][]int) // shard -> indices

	for i, op := range operations {
		shardIdx := int(FastHash(op.Key) & s.shardMask)
		shardOps[shardIdx] = append(shardOps[shardIdx], i)
	}

	// Виконуємо операції по шардам
	for _, indices := range shardOps {
		for _, i := range indices {
			op := operations[i]
			switch op.Type {
			case "SET":
				errors[i] = s.Set(op.Key, op.Value)
			case "DELETE":
				s.DeleteFast(op.Key)
				errors[i] = nil
			}
		}
	}

	return errors
}

// getShardHash - отримання хешу для шарду
func (s *LockFreeStore) getShardHash(key string) uint32 {
	// Use consistent hash function across all operations
	return FastHash(key)
}

// Atomic load/store для безпечного читання указівників
func atomicLoadPointer(ptr *unsafe.Pointer) unsafe.Pointer {
	return atomic.LoadPointer(ptr)
}

func atomicStorePointer(ptr *unsafe.Pointer, val unsafe.Pointer) {
	atomic.StorePointer(ptr, val)
}

// SetStoredValue stores a StoredValue directly (used internally)
func (s *LockFreeStore) SetStoredValue(key string, value *StoredValue) error {
	if int64(len(key)) > s.limits.MaxKeySize {
		return pkgErrors.NewKeyTooLarge(int64(len(key)), s.limits.MaxKeySize)
	}

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Calculate memory change
	var memoryDelta int64

	if existingValue, exists := shard.data[key]; exists {
		// Key exists, calculate the difference
		memoryDelta = value.MemoryUsage() - existingValue.MemoryUsage()
	} else {
		// New key
		memoryDelta = value.MemoryUsage() + int64(len(key))
		atomic.AddInt64(&shard.keyCount, 1)
		atomic.AddInt64(&s.totalKeys, 1)
	}

	// Check memory limits
	newMemory := atomic.LoadInt64(&s.totalMemory) + memoryDelta
	if newMemory > s.limits.MaxMemoryUsage {
		if !exists(shard.data, key) {
			atomic.AddInt64(&shard.keyCount, -1)
			atomic.AddInt64(&s.totalKeys, -1)
		}
		return pkgErrors.NewMemoryLimit(newMemory, s.limits.MaxMemoryUsage)
	}

	// Store the value
	shard.data[key] = value

	// Update memory usage
	atomic.AddInt64(&shard.memoryUsage, memoryDelta)
	atomic.AddInt64(&s.totalMemory, memoryDelta)

	return nil
}

// SET Operations Implementation

// SAdd adds one or more members to a set
func exists(m map[string]*StoredValue, key string) bool {
	_, ok := m[key]
	return ok
}

// Debug methods for internal structure inspection

// GetShardForDebug returns a shard by index for debug purposes
func (s *LockFreeStore) GetShardForDebug(shardIdx int) *LockFreeShardDebug {
	if shardIdx < 0 || shardIdx >= numShards {
		return nil
	}
	return &LockFreeShardDebug{shard: s.shards[shardIdx]}
}

// LockFreeShardDebug provides debug access to shard data
type LockFreeShardDebug struct {
	shard *LockFreeShard
}

// GetStoredValueForDebug returns the StoredValue for a key (for debug purposes)
func (sd *LockFreeShardDebug) GetStoredValueForDebug(key string) *StoredValue {
	if sd.shard == nil {
		return nil
	}

	sd.shard.mu.RLock()
	defer sd.shard.mu.RUnlock()

	return sd.shard.data[key]
}

// GetStoredValue returns the StoredValue for a key (for DEBUG STRUCTURE)
func (s *LockFreeStore) GetStoredValue(key string) (*StoredValue, bool) {
	hash := FastHash(key)
	shardIdx := hash & s.shardMask
	shard := s.shards[shardIdx]

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, exists := shard.data[key]
	if !exists {
		return nil, false
	}

	// Check if expired
	if s.isExpiredFast(shard, key) {
		return nil, false
	}

	return storedValue, true
}

// GetKeysByShards returns a map of shard index to keys in that shard
func (s *LockFreeStore) GetKeysByShards() map[int][]string {
	result := make(map[int][]string)

	for i := 0; i < numShards; i++ {
		shard := s.shards[i]
		shard.mu.RLock()

		var keys []string
		for key := range shard.data {
			// Check if not expired
			if !s.isExpiredFast(shard, key) {
				keys = append(keys, key)
			}
		}
		shard.mu.RUnlock()

		if len(keys) > 0 {
			result[i] = keys
		}
	}

	return result
}

// GetKeyMemoryUsage returns the memory usage of a specific key
func (s *LockFreeStore) GetKeyMemoryUsage(key string) int64 {
	storedValue, exists := s.GetStoredValue(key)
	if !exists {
		return 0
	}

	// Key memory + value memory
	keyMemory := int64(len(key))
	valueMemory := storedValue.MemoryUsage()

	return keyMemory + valueMemory
}

// GetKeyType returns the type of a stored value for a key
func (s *LockFreeStore) GetKeyType(key string) ValueType {
	storedValue, exists := s.GetStoredValue(key)
	if !exists {
		return StringType // Default
	}

	return storedValue.Type
}

// HASH Operations Implementation

// HSet sets field(s) in a hash
