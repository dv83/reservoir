package store

import (
	"sync/atomic"
	"time"
)

// AsyncMetrics управляє асинхронним оновленням метрик для покращення продуктивності
type AsyncMetrics struct {
	// Посилання на store для застосування оновлень
	store *LockFreeStore

	// Pending updates per shard
	shardUpdates [numShards]*ShardPendingUpdates

	// Control channels
	stopCh chan struct{}
	doneCh chan struct{}
}

// ShardPendingUpdates тримає pending оновлення для шарду
type ShardPendingUpdates struct {
	keyDelta    int64 // atomic
	memoryDelta int64 // atomic

	// Average size updates (батчимо для покращення продуктивності)
	totalKeySize   int64 // atomic
	totalValueSize int64 // atomic
	updateCount    int64 // atomic
}

// NewAsyncMetrics створює нову async metrics систему
func NewAsyncMetrics() *AsyncMetrics {
	am := &AsyncMetrics{
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	// Ініціалізуємо pending updates для всіх шардів
	for i := 0; i < numShards; i++ {
		am.shardUpdates[i] = &ShardPendingUpdates{}
	}

	return am
}

// SetStore встановлює посилання на store (після створення store)
func (am *AsyncMetrics) SetStore(store *LockFreeStore) {
	am.store = store
	// Запускаємо background процес тільки після встановлення store
	go am.batchUpdateLoop()
}

// AddKeyUpdate додає update для key count (non-blocking)
func (am *AsyncMetrics) AddKeyUpdate(shardIdx int, delta int64) {
	atomic.AddInt64(&am.shardUpdates[shardIdx].keyDelta, delta)
}

// AddMemoryUpdate додає update для memory usage (non-blocking)
func (am *AsyncMetrics) AddMemoryUpdate(shardIdx int, delta int64) {
	atomic.AddInt64(&am.shardUpdates[shardIdx].memoryDelta, delta)
}

// AddSizeUpdate додає update для average sizes (non-blocking)
func (am *AsyncMetrics) AddSizeUpdate(shardIdx int, keySize, valueSize int64) {
	shardUpdate := am.shardUpdates[shardIdx]
	atomic.AddInt64(&shardUpdate.totalKeySize, keySize)
	atomic.AddInt64(&shardUpdate.totalValueSize, valueSize)
	atomic.AddInt64(&shardUpdate.updateCount, 1)
}

// batchUpdateLoop основний loop для батчингу оновлень
func (am *AsyncMetrics) batchUpdateLoop() {
	defer close(am.doneCh)

	ticker := time.NewTicker(100 * time.Millisecond) // Оновлюємо кожні 100ms для балансу
	defer ticker.Stop()

	for {
		select {
		case <-am.stopCh:
			// Останній batch перед закриттям
			am.flushAllUpdates()
			return
		case <-ticker.C:
			am.flushAllUpdates()
		}
	}
}

// flushAllUpdates застосовує всі pending оновлення до шардів
func (am *AsyncMetrics) flushAllUpdates() {
	// Це виконується в окремому goroutine, тому не блокує SET/GET операції
	for i := 0; i < numShards; i++ {
		am.flushShardUpdates(i)
	}
}

// flushShardUpdates застосовує pending оновлення для конкретного шарду
func (am *AsyncMetrics) flushShardUpdates(shardIdx int) {
	if am.store == nil {
		return // Store ще не встановлено
	}

	shardUpdate := am.shardUpdates[shardIdx]
	shard := am.store.shards[shardIdx]

	// Атомарно забираємо pending values
	keyDelta := atomic.SwapInt64(&shardUpdate.keyDelta, 0)
	memoryDelta := atomic.SwapInt64(&shardUpdate.memoryDelta, 0)
	totalKeySize := atomic.SwapInt64(&shardUpdate.totalKeySize, 0)
	totalValueSize := atomic.SwapInt64(&shardUpdate.totalValueSize, 0)
	updateCount := atomic.SwapInt64(&shardUpdate.updateCount, 0)

	// Якщо немає оновлень, пропускаємо
	if keyDelta == 0 && memoryDelta == 0 && updateCount == 0 {
		return
	}

	// Застосовуємо key/memory оновлення
	if keyDelta != 0 {
		atomic.AddInt64(&shard.keyCount, keyDelta)
		atomic.AddInt64(&am.store.totalKeys, keyDelta)
	}

	if memoryDelta != 0 {
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&am.store.totalMemory, memoryDelta)
	}

	// Обчислюємо нові середні значення
	if updateCount > 0 {
		avgKeySize := totalKeySize / updateCount
		avgValueSize := totalValueSize / updateCount

		// Застосовуємо експоненціальне згладжування в async режимі
		am.updateAveragesAsync(shard, avgKeySize, avgValueSize)
	}
}

// updateAveragesAsync оновлює середні розміри асинхронно
func (am *AsyncMetrics) updateAveragesAsync(shard *LockFreeShard, newKeySize, newValueSize int64) {
	// Використовуємо цілочисельну арифметику замість float64 для швидкості
	const alphaFixed = 102 // 0.1 * 1024 для fixed-point арифметики

	// Оновлюємо avgKeySize
	oldAvgKey := atomic.LoadInt64(&shard.avgKeySize)
	newAvgKey := (oldAvgKey*(1024-alphaFixed) + newKeySize*alphaFixed) / 1024
	atomic.StoreInt64(&shard.avgKeySize, newAvgKey)

	// Оновлюємо avgValueSize
	oldAvgValue := atomic.LoadInt64(&shard.avgValueSize)
	newAvgValue := (oldAvgValue*(1024-alphaFixed) + newValueSize*alphaFixed) / 1024
	atomic.StoreInt64(&shard.avgValueSize, newAvgValue)
}

// ForceFlush примусово застосовує всі pending оновлення
func (am *AsyncMetrics) ForceFlush() {
	am.flushAllUpdates()
}

// Stop зупиняє async metrics систему
func (am *AsyncMetrics) Stop() {
	close(am.stopCh)
	<-am.doneCh // Чекаємо завершення
}

// GetPendingStats повертає поточні pending метрики (для debugging)
func (am *AsyncMetrics) GetPendingStats() map[string]interface{} {
	stats := make(map[string]interface{})

	var totalPendingKeys int64
	var totalPendingMemory int64

	for i := 0; i < numShards; i++ {
		shardUpdate := am.shardUpdates[i]
		totalPendingKeys += atomic.LoadInt64(&shardUpdate.keyDelta)
		totalPendingMemory += atomic.LoadInt64(&shardUpdate.memoryDelta)
	}

	stats["pending_key_updates"] = totalPendingKeys
	stats["pending_memory_updates"] = totalPendingMemory

	return stats
}
