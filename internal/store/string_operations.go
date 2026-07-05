package store

import (
	"sync/atomic"
	"time"

	pkgErrors "reservoir/pkg/errors"
)

// OPTIMIZED String operations for maximum performance - direct implementation

// Set stores a string value at the given key - OPTIMIZED HOT PATH
func (s *LockFreeStore) Set(key, value string) error {
	keyLen := int64(len(key))
	valueLen := int64(len(value))

	// Fast limit checks
	if keyLen > s.limits.MaxKeySize {
		return pkgErrors.NewKeyTooLarge(keyLen, s.limits.MaxKeySize)
	}
	if valueLen > s.limits.MaxValueSize {
		return pkgErrors.NewValueTooLarge(valueLen, s.limits.MaxValueSize)
	}

	// Pre-create StoredValue OUTSIDE critical section
	// Make safe copies for long-term storage
	keyCopy := string([]byte(key))
	valueCopy := string([]byte(value))
	storedValue := &StoredValue{
		Type:       ValueTypeString,
		StringVal:  valueCopy,
		memorySize: keyLen + valueLen,
	}

	// Single hash, minimal operations
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	// ULTRA MINIMAL CRITICAL SECTION: Check existence and assign
	var memoryDelta int64
	shard.mu.Lock()
	oldValue, existed := shard.data[keyCopy]
	if existed && oldValue != nil {
		memoryDelta = storedValue.memorySize - oldValue.memorySize
	} else {
		memoryDelta = storedValue.memorySize
	}
	shard.data[keyCopy] = storedValue
	// A plain SET clears any previous TTL on the key (Redis semantics).
	// Clearing it under the same shard lock that guards the data keeps the
	// (value, expiry) pair consistent for the expiry cleaner, which likewise
	// checks both under shard.mu. Leaving a stale expiry attached would let the
	// cleaner later reap the freshly-written value (e.g. SETEX k / DEL k / SET k).
	if existed {
		shard.expiryMu.Lock()
		delete(shard.expiry, keyCopy)
		shard.expiryMu.Unlock()
	}
	shard.mu.Unlock()

	// Update counters
	if !existed {
		atomic.AddInt64(&s.totalKeys, 1)
	}
	atomic.AddInt64(&shard.memoryUsage, memoryDelta)
	atomic.AddInt64(&s.totalMemory, memoryDelta)

	// ASYNC: Commit log in background (if enabled AND not in recovery mode)
	if atomic.LoadUint32(&s.commitLogActive) == 1 && atomic.LoadUint32(&s.recoveryActive) == 0 {
		if cl, ok := s.commitLog.(CommitLogger); ok {
			cl.Write("SET", []byte(key), []byte(value))
		}
	}

	return nil
}

// Get retrieves a string value from the given key - OPTIMIZED HOT PATH
func (s *LockFreeStore) Get(key string) (string, bool) {
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	// MINIMAL CRITICAL SECTION: Just read pointer
	shard.mu.RLock()
	value := shard.data[key]
	shard.mu.RUnlock()

	// Process OUTSIDE critical section
	if value == nil {
		return "", false
	}

	// Honor TTL on read: an expired key must not be served even though the
	// background cleaner has not physically removed it yet.
	if s.isExpiredFast(shard, key) {
		return "", false
	}

	// GET only applies to string values; for any other type the caller must
	// surface a WRONGTYPE error rather than an empty string. Signal "no string
	// value here" so the handler can distinguish and report it.
	if value.Type != ValueTypeString {
		return "", false
	}

	return value.StringVal, true
}

// Delete removes a key - OPTIMIZED
func (s *LockFreeStore) Delete(key string) bool {
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	// MINIMAL CRITICAL SECTION: Check and delete atomically
	shard.mu.Lock()
	_, existed := shard.data[key]
	if existed {
		delete(shard.data, key)
	}
	shard.mu.Unlock()

	// ASYNC: Update counters and commit log only if deleted
	if existed {
		atomic.AddInt64(&s.totalKeys, -1)

		// Remove any TTL entry synchronously under the same logical delete.
		// Otherwise the expiry map leaks entries for deleted keys, and a
		// subsequent recreate of the key would inherit the stale expiry.
		shard.expiryMu.Lock()
		delete(shard.expiry, key)
		shard.expiryMu.Unlock()

		// ASYNC: Commit log in background (if enabled AND not in recovery mode)
		if atomic.LoadUint32(&s.commitLogActive) == 1 && atomic.LoadUint32(&s.recoveryActive) == 0 {
			if cl, ok := s.commitLog.(CommitLogger); ok {
				cl.Delete([]byte(key))
			}
		}
	}

	return existed
}

// Exists checks if keys exist - OPTIMIZED
func (s *LockFreeStore) Exists(keys ...string) int64 {
	var count int64
	for _, key := range keys {
		hash := FastHash(key)
		shard := s.shards[int(hash&s.shardMask)]

		shard.mu.RLock()
		_, exists := shard.data[key]
		shard.mu.RUnlock()

		// An expired-but-not-yet-collected key must count as absent.
		if exists && !s.isExpiredFast(shard, key) {
			count++
		}
	}
	return count
}

// IncrBy increments integer value - OPTIMIZED
func (s *LockFreeStore) IncrBy(key string, increment int64) (int64, error) {
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, exists := shard.data[key]
	var currentValue int64

	if exists && storedValue != nil {
		// Check if the key holds the wrong type
		if !storedValue.IsString() {
			return 0, pkgErrors.ErrWrongType
		}
		// Parse existing value
		if storedValue.StringVal != "" {
			parsed, err := parseIntegerStrict(storedValue.StringVal)
			if err != nil {
				return 0, pkgErrors.ErrNotInteger
			}
			currentValue = parsed
		}
	}

	// Calculate new value, detecting signed 64-bit overflow (Redis rejects an
	// INCR/DECR whose result would wrap) instead of silently wrapping negative.
	newValue := currentValue + increment
	if (increment > 0 && newValue < currentValue) || (increment < 0 && newValue > currentValue) {
		return 0, pkgErrors.ErrIncrDecrOverflow
	}
	newValueStr := formatInt64Fast(newValue)

	// Direct string storage - no complex objects
	shard.data[key] = &StoredValue{
		Type:       ValueTypeString,
		StringVal:  newValueStr,
		memorySize: int64(len(key) + len(newValueStr)),
	}

	// Write to commit log asynchronously (if not in recovery mode)
	if atomic.LoadUint32(&s.recoveryActive) == 0 {
		if cl, ok := s.commitLog.(CommitLogger); ok && cl != nil {
			cl.Write("INCRBY", []byte(key), []byte(newValueStr))
		}
	}

	return newValue, nil
}

// Incr increments by 1
func (s *LockFreeStore) Incr(key string) (int64, error) {
	return s.IncrBy(key, 1)
}

// Decr decrements by 1
func (s *LockFreeStore) Decr(key string) (int64, error) {
	return s.IncrBy(key, -1)
}

// DecrBy decrements by amount
func (s *LockFreeStore) DecrBy(key string, decrement int64) (int64, error) {
	return s.IncrBy(key, -decrement)
}

// Append appends value to string - OPTIMIZED
func (s *LockFreeStore) Append(key, value string) (int64, error) {
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, exists := shard.data[key]
	var newValue string

	if exists && storedValue != nil {
		// Check if the key holds the wrong type
		if !storedValue.IsString() {
			return 0, pkgErrors.ErrWrongType
		}
		newValue = storedValue.StringVal + value
	} else {
		newValue = value
	}

	// Direct storage - no complex objects
	shard.data[key] = &StoredValue{
		Type:       ValueTypeString,
		StringVal:  newValue,
		memorySize: int64(len(key) + len(newValue)),
	}

	return int64(len(newValue)), nil
}

// StrLen returns string length - OPTIMIZED
func (s *LockFreeStore) StrLen(key string) (int64, error) {
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	shard.mu.RLock()
	storedValue, exists := shard.data[key]
	shard.mu.RUnlock()

	if !exists || storedValue == nil {
		return 0, nil
	}

	// Check if the key holds the wrong type
	if !storedValue.IsString() {
		return 0, pkgErrors.ErrWrongType
	}

	return int64(len(storedValue.StringVal)), nil
}

// GetSet atomically sets value and returns old value - OPTIMIZED
func (s *LockFreeStore) GetSet(key, value string) (string, bool, error) {
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	shard.mu.Lock()
	oldStoredValue, existed := shard.data[key]

	// Check if the key holds the wrong type
	if existed && oldStoredValue != nil && !oldStoredValue.IsString() {
		shard.mu.Unlock()
		return "", false, pkgErrors.ErrWrongType
	}

	// Direct storage - no complex objects
	shard.data[key] = &StoredValue{
		Type:       ValueTypeString,
		StringVal:  value,
		memorySize: int64(len(key) + len(value)),
	}
	shard.mu.Unlock()

	if existed && oldStoredValue != nil {
		return oldStoredValue.StringVal, true, nil
	} else {
		return "", false, nil
	}
}

// parseIntegerStrict parses an integer string strictly, returning error for invalid input
func parseIntegerStrict(s string) (int64, error) {
	if s == "" {
		return 0, nil // empty string treated as 0
	}

	var result int64
	negative := false
	i := 0

	if s[0] == '-' {
		negative = true
		i = 1
		if len(s) == 1 {
			return 0, pkgErrors.ErrNotInteger // just "-" is invalid
		}
	} else if s[0] == '+' {
		i = 1
		if len(s) == 1 {
			return 0, pkgErrors.ErrNotInteger // just "+" is invalid
		}
	}

	if i >= len(s) {
		return 0, pkgErrors.ErrNotInteger // no digits
	}

	for i < len(s) {
		if s[i] >= '0' && s[i] <= '9' {
			result = result*10 + int64(s[i]-'0')
		} else {
			return 0, pkgErrors.ErrNotInteger // invalid character
		}
		i++
	}

	if negative {
		result = -result
	}
	return result, nil
}

// Fast integer parsing without error handling overhead (for internal use where validation not needed)
func parseInt64Fast(s string) int64 {
	if s == "" {
		return 0
	}
	// Simple implementation - could use unsafe for more speed
	var result int64
	negative := false
	i := 0

	if s[0] == '-' {
		negative = true
		i = 1
	}

	for i < len(s) {
		if s[i] >= '0' && s[i] <= '9' {
			result = result*10 + int64(s[i]-'0')
		} else {
			break // Invalid character - return what we have
		}
		i++
	}

	if negative {
		result = -result
	}
	return result
}

// Fast integer formatting without allocations
func formatInt64Fast(n int64) string {
	if n == 0 {
		return "0"
	}

	// Simple implementation - for ultra speed could use unsafe + buffer pool
	negative := n < 0
	if negative {
		n = -n
	}

	// Count digits
	temp := n
	digits := 0
	for temp > 0 {
		digits++
		temp /= 10
	}

	// Build string
	result := make([]byte, digits+int(boolToInt(negative)))
	pos := len(result) - 1

	for n > 0 {
		result[pos] = byte('0' + n%10)
		n /= 10
		pos--
	}

	if negative {
		result[0] = '-'
	}

	return string(result)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// AddDeferredCommand schedules a command to be executed after specified delay
func (s *LockFreeStore) AddDeferredCommand(commandType, key string, args []string, delaySeconds int) string {
	cmd := &DeferredCommand{
		ID:          generateDeferredCommandID(),
		ExecuteAt:   time.Now().Add(time.Duration(delaySeconds) * time.Second),
		CommandType: commandType,
		Key:         key,
		Args:        args,
		CreatedAt:   time.Now(),
	}

	s.deferredScheduler.AddDeferredCommand(cmd)
	return cmd.ID
}

// CancelDeferredCommand cancels a previously scheduled command
func (s *LockFreeStore) CancelDeferredCommand(commandID string) bool {
	return s.deferredScheduler.CancelDeferredCommand(commandID)
}

// ListDeferredCommands returns all currently scheduled commands
func (s *LockFreeStore) ListDeferredCommands() []*DeferredCommand {
	return s.deferredScheduler.ListDeferredCommands()
}

// GetDeferredStats returns statistics about deferred commands
func (s *LockFreeStore) GetDeferredStats() map[string]interface{} {
	return s.deferredScheduler.GetStats()
}

// AddDeferredCommandDirect adds a pre-constructed deferred command (used for replication)
func (s *LockFreeStore) AddDeferredCommandDirect(cmd *DeferredCommand) {
	s.deferredScheduler.AddDeferredCommand(cmd)
}
