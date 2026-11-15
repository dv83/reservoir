package store

import (
	"sync/atomic"
	"time"

	pkgErrors "reservoir/pkg/errors"
)

// OPTIMIZED String operations for maximum performance - direct implementation

// SetOptions carries the optional modifiers of the Redis SET command.
type SetOptions struct {
	NX      bool          // only set if the key does not already exist
	XX      bool          // only set if the key already exists
	SetTTL  bool          // apply TTL
	TTL     time.Duration // expiry to apply when SetTTL is true
	KeepTTL bool          // preserve the key's existing TTL instead of clearing it
}

// SetWithOptions performs a SET honoring NX/XX and TTL modifiers atomically
// under the shard lock. It returns whether the write was applied (false when an
// NX/XX condition was not met).
func (s *LockFreeStore) SetWithOptions(key, value string, opts SetOptions) (bool, error) {
	s.expireIfNeeded(key)
	keyLen := int64(len(key))
	valueLen := int64(len(value))
	if keyLen > s.limits.MaxKeySize {
		return false, pkgErrors.NewKeyTooLarge(keyLen, s.limits.MaxKeySize)
	}
	if valueLen > s.limits.MaxValueSize {
		return false, pkgErrors.NewValueTooLarge(valueLen, s.limits.MaxValueSize)
	}

	keyCopy := string([]byte(key))
	valueCopy := string([]byte(value))
	storedValue := &StoredValue{
		Type:       ValueTypeString,
		StringVal:  valueCopy,
		memorySize: keyLen + valueLen,
	}

	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	var memoryDelta int64
	shard.mu.Lock()
	oldValue, existed := shard.get(keyCopy)

	// Evaluate NX/XX preconditions inside the lock so the check and the write
	// are atomic.
	if (opts.NX && existed) || (opts.XX && !existed) {
		shard.mu.Unlock()
		return false, nil
	}

	if existed && oldValue != nil {
		memoryDelta = storedValue.memorySize - oldValue.memorySize
	} else {
		memoryDelta = storedValue.memorySize
	}
	if s.wouldExceedMemory(shard, memoryDelta) {
		shard.mu.Unlock()
		return false, pkgErrors.NewMemoryLimit(atomic.LoadInt64(&s.totalMemory)+memoryDelta, s.limits.MaxMemoryUsage)
	}
	shard.put(keyCopy, storedValue)

	// TTL handling, all under the shard lock (consistent with the expiry cleaner):
	//   - SetTTL   -> apply the new expiry
	//   - KeepTTL  -> leave any existing expiry untouched
	//   - neither  -> a plain SET clears any existing TTL
	if opts.SetTTL {
		shard.expiryMu.Lock()
		shard.expiry[keyCopy] = time.Now().Add(opts.TTL)
		shard.expiryMu.Unlock()
	} else if !opts.KeepTTL && existed {
		shard.expiryMu.Lock()
		delete(shard.expiry, keyCopy)
		shard.expiryMu.Unlock()
	}
	shard.mu.Unlock()

	if existed {
		s.accountMemoryDelta(shard, memoryDelta)
	} else {
		s.accountKeyAdded(shard, memoryDelta)
	}

	s.logStringWrite(shard, "SET", key, value)

	return true, nil
}

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
	oldValue, existed := shard.get(keyCopy)
	if existed && oldValue != nil {
		memoryDelta = storedValue.memorySize - oldValue.memorySize
	} else {
		memoryDelta = storedValue.memorySize
	}
	// Enforce the memory limit before committing the write (noeviction policy):
	// reject writes that would push total memory past MaxMemoryUsage.
	if s.wouldExceedMemory(shard, memoryDelta) {
		shard.mu.Unlock()
		return pkgErrors.NewMemoryLimit(atomic.LoadInt64(&s.totalMemory)+memoryDelta, s.limits.MaxMemoryUsage)
	}
	shard.put(keyCopy, storedValue)
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

	// Update counters through the canonical helpers so key count and memory stay
	// consistent for both new keys and overwrites.
	if existed {
		s.accountMemoryDelta(shard, memoryDelta)
	} else {
		s.accountKeyAdded(shard, memoryDelta) // memoryDelta == storedValue.memorySize for a new key
	}

	// Commit log (if enabled and not replaying recovery), restating any TTL.
	s.logStringWrite(shard, "SET", key, value)

	return nil
}

// Get retrieves a string value from the given key - OPTIMIZED HOT PATH
func (s *LockFreeStore) Get(key string) (string, bool) {
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	// MINIMAL CRITICAL SECTION: Just read pointer
	shard.mu.RLock()
	value := shard.getV(key)
	shard.mu.RUnlock()

	// Process OUTSIDE critical section
	if value == nil {
		return "", false
	}
	if s.engine != nil {
		s.fillAfterRead(shard, key, value)
		s.touch(value)
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

	if s.tier != nil {
		if value.cold != nil {
			v, found, err := s.GetString(key)
			return v, found && err == nil
		}
		atomic.AddInt64(&shard.hotReads, 1)
		s.touch(value)
	}
	return value.StringVal, true
}

// GetString returns key's string value, reading it from disk if it is cold. It
// returns found=false for a missing or expired key, ErrWrongType for a key of
// another type, and an error if a cold value cannot be read or fails its
// checksum (tiered storage never serves corrupt data).
func (s *LockFreeStore) GetString(key string) (string, bool, error) {
	shard := s.shards[int(FastHash(key)&s.shardMask)]
	shard.mu.RLock()
	sv := shard.getV(key)
	if sv == nil || s.isExpiredFast(shard, key) {
		shard.mu.RUnlock()
		return "", false, nil
	}
	if sv.Type != ValueTypeString {
		shard.mu.RUnlock()
		return "", false, pkgErrors.ErrWrongType
	}
	if sv.cold == nil {
		v := sv.StringVal
		shard.mu.RUnlock()
		if s.tier != nil {
			atomic.AddInt64(&shard.hotReads, 1)
			s.touch(sv)
		}
		if s.engine != nil {
			s.fillAfterRead(shard, key, sv)
			s.touch(sv)
		}
		return v, true, nil
	}
	// Read under the shard lock: value-log garbage collection relocates a
	// record only under this lock, so the file cannot disappear mid-read.
	v, err := s.loadString(shard, key, sv)
	shard.mu.RUnlock()
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// Delete removes a key - OPTIMIZED
func (s *LockFreeStore) Delete(key string) bool {
	s.expireIfNeeded(key)
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	// MINIMAL CRITICAL SECTION: Check and delete atomically
	shard.mu.Lock()
	oldValue, existed := shard.get(key)
	if existed {
		shard.del(key)
	}
	shard.mu.Unlock()

	// ASYNC: Update counters and commit log only if deleted
	if existed {
		var freed int64
		if oldValue != nil {
			freed = oldValue.MemoryUsage()
		}
		s.accountKeyRemoved(shard, freed)

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
	for _, k := range keys {
		s.expireIfNeeded(k)
	}
	var count int64
	for _, key := range keys {
		hash := FastHash(key)
		shard := s.shards[int(hash&s.shardMask)]

		shard.mu.RLock()
		_, exists := shard.get(key)
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
	s.expireIfNeeded(key)
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, exists := shard.get(key)
	var currentValue int64

	// Cluster mode: maintain a PN-counter CRDT so concurrent increments on
	// different nodes converge without losing any (see crdt_counter.go). The
	// plain path below is kept for single-node use (localOrigin == 0).
	if origin := atomic.LoadUint64(&s.localOrigin); origin != 0 {
		ctr, err := counterFor(storedValue, exists, s.resetPoint(shard, key))
		if err != nil {
			return 0, err
		}
		current := ctr.value()
		newValue := current + increment
		if (increment > 0 && newValue < current) || (increment < 0 && newValue > current) {
			return 0, pkgErrors.ErrIncrDecrOverflow
		}
		ctr.add(origin, increment)
		s.storeCounter(shard, key, ctr, exists, storedValue)
		return newValue, nil
	}

	if exists && storedValue != nil {
		// Check if the key holds the wrong type
		if !storedValue.IsString() {
			return 0, pkgErrors.ErrWrongType
		}
		// Parse existing value. A cold value is at least minSpillBytes long,
		// longer than any integer, so it cannot be one.
		if storedValue.cold != nil {
			return 0, pkgErrors.ErrNotInteger
		}
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

	var oldMem int64
	if exists && storedValue != nil {
		oldMem = storedValue.MemoryUsage()
	}

	// Direct string storage - no complex objects
	newSV := &StoredValue{
		Type:       ValueTypeString,
		StringVal:  newValueStr,
		memorySize: int64(len(key) + len(newValueStr)),
	}
	shard.put(key, newSV)

	if exists {
		s.accountMemoryDelta(shard, newSV.memorySize-oldMem)
	} else {
		s.accountKeyAdded(shard, newSV.memorySize)
	}

	// Log the absolute result; INCR keeps the TTL, so restate it.
	s.logStringWrite(shard, "INCRBY", key, newValueStr)

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
	s.expireIfNeeded(key)
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, exists := shard.get(key)
	var newValue string

	var oldMem int64
	if exists && storedValue != nil {
		// Check if the key holds the wrong type
		if !storedValue.IsString() {
			return 0, pkgErrors.ErrWrongType
		}
		oldMem = storedValue.MemoryUsage()
		old, err := s.loadString(shard, key, storedValue)
		if err != nil {
			return 0, err
		}
		newValue = old + value
	} else {
		newValue = value
	}

	// Direct storage - no complex objects
	newSV := &StoredValue{
		Type:       ValueTypeString,
		StringVal:  newValue,
		memorySize: int64(len(key) + len(newValue)),
	}
	shard.put(key, newSV)

	if exists {
		s.accountMemoryDelta(shard, newSV.memorySize-oldMem)
	} else {
		s.accountKeyAdded(shard, newSV.memorySize)
	}

	// Log the resulting value; APPEND keeps the TTL, so restate it.
	s.logStringWrite(shard, "SET", key, newValue)

	return int64(len(newValue)), nil
}

// StrLen returns string length - OPTIMIZED
func (s *LockFreeStore) StrLen(key string) (int64, error) {
	s.expireIfNeeded(key)
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	shard.mu.RLock()
	storedValue, exists := shard.get(key)
	shard.mu.RUnlock()

	if !exists || storedValue == nil {
		return 0, nil
	}

	// Check if the key holds the wrong type
	if !storedValue.IsString() {
		return 0, pkgErrors.ErrWrongType
	}

	return stringLen(storedValue), nil
}

// GetSet atomically sets value and returns old value - OPTIMIZED
func (s *LockFreeStore) GetSet(key, value string) (string, bool, error) {
	s.expireIfNeeded(key)
	hash := FastHash(key)
	shard := s.shards[int(hash&s.shardMask)]

	shard.mu.Lock()
	oldStoredValue, existed := shard.get(key)

	// Check if the key holds the wrong type
	if existed && oldStoredValue != nil && !oldStoredValue.IsString() {
		shard.mu.Unlock()
		return "", false, pkgErrors.ErrWrongType
	}

	var oldMem int64
	var oldValue string
	if existed && oldStoredValue != nil {
		oldMem = oldStoredValue.MemoryUsage()
		v, err := s.loadString(shard, key, oldStoredValue)
		if err != nil {
			shard.mu.Unlock()
			return "", false, err
		}
		oldValue = v
	}

	// Direct storage - no complex objects
	newSV := &StoredValue{
		Type:       ValueTypeString,
		StringVal:  value,
		memorySize: int64(len(key) + len(value)),
	}
	shard.put(key, newSV)
	// GETSET discards any TTL, as SET does (Redis semantics).
	shard.expiryMu.Lock()
	delete(shard.expiry, key)
	shard.expiryMu.Unlock()
	shard.mu.Unlock()

	s.logStringWrite(shard, "SET", key, value)

	if existed && oldStoredValue != nil {
		s.accountMemoryDelta(shard, newSV.memorySize-oldMem)
		return oldValue, true, nil
	}
	s.accountKeyAdded(shard, newSV.memorySize)
	return "", false, nil
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
