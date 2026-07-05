package store

import (
	"unsafe"
)

// PrefetchMode визначає тип prefetch операції
type PrefetchMode int

const (
	// PrefetchT0 - prefetch в L1 cache (найвища пріоритетність)
	PrefetchT0 PrefetchMode = iota
	// PrefetchT1 - prefetch в L2 cache
	PrefetchT1
	// PrefetchT2 - prefetch в L3 cache
	PrefetchT2
	// PrefetchNTA - non-temporal prefetch (не забруднює cache)
	PrefetchNTA
)

// Prefetch виконує memory prefetching для заданої адреси
func Prefetch(ptr unsafe.Pointer, mode PrefetchMode) {
	// В реальній реалізації тут були б assembly інструкції
	// prefetchT0, prefetchT1, prefetchT2, prefetchNTA

	// Поки що це no-op, але компілятор може оптимізувати
	// на основі hints
	_ = ptr
	_ = mode
}

// PrefetchBytes виконує prefetch для byte slice
func PrefetchBytes(data []byte, mode PrefetchMode) {
	if len(data) == 0 {
		return
	}

	// Prefetch кожні 64 bytes (cache line size)
	const cacheLineSize = 64
	for i := 0; i < len(data); i += cacheLineSize {
		Prefetch(unsafe.Pointer(&data[i]), mode)
	}
}

// PrefetchString виконує prefetch для string
func PrefetchString(s string, mode PrefetchMode) {
	if len(s) == 0 {
		return
	}

	data := stringToBytes(s)
	PrefetchBytes(data, mode)
}

// SequentialPrefetcher автоматично prefetch для sequential access
type SequentialPrefetcher struct {
	lastAddr      uintptr
	stride        int64 // Крок між access
	prefetchAhead int   // Скільки cache lines префетчити вперед
}

// NewSequentialPrefetcher створює новий sequential prefetcher
func NewSequentialPrefetcher(prefetchAhead int) *SequentialPrefetcher {
	return &SequentialPrefetcher{
		prefetchAhead: prefetchAhead,
	}
}

// Access реєструє доступ до пам'яті та виконує prefetch
func (sp *SequentialPrefetcher) Access(ptr unsafe.Pointer) {
	currentAddr := uintptr(ptr)

	if sp.lastAddr != 0 {
		// Обчислюємо stride
		sp.stride = int64(currentAddr - sp.lastAddr)

		// Префетчимо наступні cache lines
		for i := 1; i <= sp.prefetchAhead; i++ {
			nextAddr := currentAddr + uintptr(int64(i)*sp.stride)
			Prefetch(unsafe.Pointer(nextAddr), PrefetchT0)
		}
	}

	sp.lastAddr = currentAddr
}

// ListPrefetcher оптимізує доступ до list elements
type ListPrefetcher struct {
	elements []string
	index    int
}

// NewListPrefetcher створює prefetcher для list operations
func NewListPrefetcher(elements []string) *ListPrefetcher {
	lp := &ListPrefetcher{
		elements: elements,
	}

	// Префетчимо перші кілька елементів
	lp.prefetchRange(0, min(len(elements), 4))

	return lp
}

// GetNext повертає наступний елемент з prefetch
func (lp *ListPrefetcher) GetNext() (string, bool) {
	if lp.index >= len(lp.elements) {
		return "", false
	}

	element := lp.elements[lp.index]

	// Префетчимо наступні елементи
	nextPrefetchStart := lp.index + 1
	nextPrefetchEnd := min(len(lp.elements), nextPrefetchStart+2)
	if nextPrefetchStart < len(lp.elements) {
		lp.prefetchRange(nextPrefetchStart, nextPrefetchEnd)
	}

	lp.index++
	return element, true
}

// prefetchRange префетчить діапазон елементів
func (lp *ListPrefetcher) prefetchRange(start, end int) {
	for i := start; i < end; i++ {
		if i < len(lp.elements) {
			PrefetchString(lp.elements[i], PrefetchT1)
		}
	}
}

// ShardPrefetcher оптимізує доступ до shards
type ShardPrefetcher struct {
	shards []*LockFreeShard
}

// NewShardPrefetcher створює prefetcher для shards
func NewShardPrefetcher(shards []*LockFreeShard) *ShardPrefetcher {
	return &ShardPrefetcher{
		shards: shards,
	}
}

// PrefetchShard префетчить shard data
func (sp *ShardPrefetcher) PrefetchShard(shardIdx int) {
	if shardIdx < 0 || shardIdx >= len(sp.shards) {
		return
	}

	shard := sp.shards[shardIdx]

	// Префетчимо hot data shard
	Prefetch(unsafe.Pointer(shard), PrefetchT0)

	// Префетчимо map header
	if shard.data != nil {
		Prefetch(unsafe.Pointer(&shard.data), PrefetchT1)
	}
}

// PrefetchAdjacentShards префетчить сусідні shards
func (sp *ShardPrefetcher) PrefetchAdjacentShards(centerIdx int, radius int) {
	for i := centerIdx - radius; i <= centerIdx+radius; i++ {
		if i >= 0 && i < len(sp.shards) && i != centerIdx {
			sp.PrefetchShard(i)
		}
	}
}

// MapIteratorPrefetcher оптимізує ітерацію по map
type MapIteratorPrefetcher struct {
	keys       []string
	prefetched map[string]bool
}

// NewMapIteratorPrefetcher створює prefetcher для map iteration
func NewMapIteratorPrefetcher(keys []string) *MapIteratorPrefetcher {
	mip := &MapIteratorPrefetcher{
		keys:       keys,
		prefetched: make(map[string]bool),
	}

	// Префетчимо перші кілька ключів
	for i := 0; i < min(len(keys), 8); i++ {
		PrefetchString(keys[i], PrefetchT0)
		mip.prefetched[keys[i]] = true
	}

	return mip
}

// PrefetchKey префетчить key якщо ще не зроблено
func (mip *MapIteratorPrefetcher) PrefetchKey(key string) {
	if !mip.prefetched[key] {
		PrefetchString(key, PrefetchT1)
		mip.prefetched[key] = true
	}
}

// min повертає мінімум з двох чисел
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// PrefetchConfig конфігурація для prefetching
type PrefetchConfig struct {
	EnableSequential bool
	EnableList       bool
	EnableShard      bool
	PrefetchAhead    int
}

// DefaultPrefetchConfig повертає стандартну конфігурацію
func DefaultPrefetchConfig() *PrefetchConfig {
	return &PrefetchConfig{
		EnableSequential: true,
		EnableList:       true,
		EnableShard:      true,
		PrefetchAhead:    4,
	}
}

// GlobalPrefetchConfig глобальна конфігурація prefetching
var GlobalPrefetchConfig = DefaultPrefetchConfig()

// SetPrefetchConfig встановлює глобальну конфігурацію
func SetPrefetchConfig(config *PrefetchConfig) {
	GlobalPrefetchConfig = config
}

// IsPrefetchEnabled перевіряє чи включений prefetching
func IsPrefetchEnabled() bool {
	return GlobalPrefetchConfig != nil
}
