package store

import "time"

// KeyMemoryInfo contains memory usage details for a key
type KeyMemoryInfo struct {
	Key      string `json:"key"`
	Type     string `json:"type"`
	Size     int64  `json:"size"`
	TTL      bool   `json:"ttl"`
	Deferred bool   `json:"deferred"`
}

// KVStore представляє інтерфейс для key-value сховища
type KVStore interface {
	// Memory visualization
	GetMemoryMap() ([]KeyMemoryInfo, error)

	// Основні операції
	Set(key, value string) error
	SetWithOptions(key, value string, opts SetOptions) (bool, error)
	SetLWW(key, value string, physical int64, logical uint32, origin uint64) (bool, error)
	DeleteLWW(key string, physical int64, logical uint32, origin uint64) bool
	GetLWW(key string) (physical int64, logical uint32, origin uint64, ok bool)
	ShardCount() int
	ShardLWWDigest(shardIdx int) []LWWShardEntry
	Get(key string) (string, bool)
	Delete(key string) bool
	Clear()
	Exists(keys ...string) int64

	// Numeric operations
	Incr(key string) (int64, error)
	Decr(key string) (int64, error)
	IncrBy(key string, increment int64) (int64, error)
	DecrBy(key string, decrement int64) (int64, error)

	// String operations
	Append(key, value string) (int64, error)
	StrLen(key string) (int64, error)
	GetSet(key, value string) (string, bool, error)

	// TTL операції
	SetExpiry(key string, expireTime time.Time) bool
	GetTTL(key string) (time.Duration, bool)
	RemoveExpiry(key string) bool

	// List операції
	LPush(key string, elements ...string) (int64, error)
	RPush(key string, elements ...string) (int64, error)
	LPushX(key string, elements ...string) (int64, error)      // Додає тільки якщо ключ існує
	RPushX(key string, elements ...string) (int64, error)      // Додає тільки якщо ключ існує
	LPushUnique(key string, elements ...string) (int64, error) // Додає тільки унікальні елементи
	RPushUnique(key string, elements ...string) (int64, error) // Додає тільки унікальні елементи
	LPop(key string, count int) ([]string, error)
	RPop(key string, count int) ([]string, error)
	LLen(key string) (int64, error)
	LIndex(key string, index int64) (string, bool, error)
	LRange(key string, start, stop int64) ([]string, error)
	LSet(key string, index int64, element string) error
	LRem(key string, count int64, element string) (int64, error)
	LTrim(key string, start, stop int64) error

	// Set операції
	SAdd(key string, members ...string) (int64, error)
	SRem(key string, members ...string) (int64, error)
	SAddLWW(key string, physical int64, logical uint32, origin uint64, members ...string) (int64, error)
	SRemLWW(key string, physical int64, logical uint32, origin uint64, members ...string) (int64, error)
	SIsMember(key string, member string) (bool, error)
	SCard(key string) (int64, error)
	SMembers(key string) ([]string, error)
	SPop(key string) (string, bool, error)
	SDiff(keys ...string) ([]string, error)
	SInter(keys ...string) ([]string, error)
	SUnion(keys ...string) ([]string, error)
	SDiffStore(destination string, keys ...string) (int64, error)

	// Hash операції
	HSet(key string, fieldValues ...string) (int64, error)
	HGet(key, field string) (string, bool, error)
	HMGet(key string, fields ...string) ([]string, []bool, error)
	HDel(key string, fields ...string) (int64, error)
	HExists(key, field string) (bool, error)
	HLen(key string) (int64, error)
	HKeys(key string) ([]string, error)
	HVals(key string) ([]string, error)
	HGetAll(key string) ([]string, error)
	HIncrBy(key, field string, increment int64) (int64, error)
	HIncrByFloat(key, field string, increment float64) (float64, error)

	// Внутрішні методи для роботи з StoredValue
	SetStoredValue(key string, value *StoredValue) error

	// Метрики та моніторинг
	GetMemoryUsage() int64
	GetKeyCount() int64
	GetAllKeys() []string

	// Детальна інформація про ключі (для DEBUG STRUCTURE)
	GetStoredValue(key string) (*StoredValue, bool)
	GetKeysByShards() map[int][]string
	GetKeyMemoryUsage(key string) int64
	GetKeyType(key string) ValueType

	// Шардінг (для моніторингу)
	GetShardStats() []map[string]interface{}

	// Async metrics
	ForceFlushMetrics()

	// Commit Log management
	GetCommitLogDir() string
	GetCommitLogStats() map[string]interface{}
	TriggerCommitLogCompaction() error
	SetCommitLog(commitLog interface{})

	// Recovery management
	SetRecoveryActive(active bool)
	IsRecoveryActive() bool

	// Lifecycle management
	Stop()
}

// Перевірка, що store реалізує інтерфейс
var (
	_ KVStore = (*Store)(nil)
)
