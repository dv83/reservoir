package store

import (
	"errors"
	"strconv"
	"sync/atomic"
	"unsafe"

	pkgErrors "reservoir/pkg/errors"
)

// Hash operations for the LockFreeStore

func (s *LockFreeStore) HSet(key string, fieldValues ...string) (logN int64, logErr error) {
	s.expireIfNeeded(key)
	defer func() {
		if logErr == nil {
			s.logContainerOp("HSET", key, fieldValues...)
		}
	}()
	if len(fieldValues)%2 != 0 {
		return 0, errors.New("wrong number of arguments for hash set")
	}

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	var hashValue *HashValue
	var exists bool

	if storedValue, ok := shard.get(key); ok {
		if !storedValue.IsHash() {
			return 0, pkgErrors.ErrWrongType
		}
		hashValue, _ = storedValue.AsHash()
		exists = true
	} else {
		hashValue = NewHashValue()
		exists = false
	}

	// Add/update fields and count additions
	added, err := hashValue.SetMultiple(fieldValues...)
	if err != nil {
		return 0, err
	}

	// If it's a new hash, store it
	if !exists && added > 0 {
		storedHash := NewStoredHash()
		storedHash.HashVal = hashValue

		// Calculate memory usage
		memoryDelta := storedHash.MemoryUsage()
		newMemory := atomic.LoadInt64(&s.totalMemory) + memoryDelta

		if s.wouldExceedMemory(shard, memoryDelta) {
			return 0, pkgErrors.NewMemoryLimit(newMemory, s.limits.MaxMemoryUsage)
		}

		shard.put(key, storedHash)

		atomic.AddInt64(&shard.keyCount, 1)
		atomic.AddInt64(&s.totalKeys, 1)
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)

	} else if exists {
		// Update existing hash - update memory usage
		if oldStoredValue, ok := shard.get(key); ok {
			oldSize := oldStoredValue.memorySize - int64(unsafe.Sizeof(*oldStoredValue))
			newSize := hashValue.MemoryUsage()
			memoryDelta := newSize - oldSize

			// Update memory tracking
			atomic.AddInt64(&shard.memoryUsage, memoryDelta)
			atomic.AddInt64(&s.totalMemory, memoryDelta)

			// Update the memory size in the stored value
			oldStoredValue.memorySize = int64(unsafe.Sizeof(*oldStoredValue)) + newSize
		}
	}

	return added, nil
}

// HGet gets a field value from a hash
func (s *LockFreeStore) HGet(key, field string) (string, bool, error) {
	s.expireIfNeeded(key)
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, ok := shard.get(key)
	if !ok {
		return "", false, nil // Key doesn't exist
	}

	if !storedValue.IsHash() {
		return "", false, pkgErrors.ErrWrongType
	}

	hashValue, _ := storedValue.AsHash()
	value, exists := hashValue.Get(field)
	return value, exists, nil
}

// HMGet gets multiple field values from a hash
func (s *LockFreeStore) HMGet(key string, fields ...string) ([]string, []bool, error) {
	s.expireIfNeeded(key)
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, ok := shard.get(key)
	if !ok {
		// Key doesn't exist, return nil for all fields
		result := make([]string, len(fields))
		exists := make([]bool, len(fields))
		return result, exists, nil
	}

	if !storedValue.IsHash() {
		return nil, nil, pkgErrors.ErrWrongType
	}

	hashValue, _ := storedValue.AsHash()
	result := make([]string, len(fields))
	existsFlags := make([]bool, len(fields))

	for i, field := range fields {
		if value, exists := hashValue.Get(field); exists {
			result[i] = value
			existsFlags[i] = true
		} else {
			result[i] = ""
			existsFlags[i] = false
		}
	}

	return result, existsFlags, nil
}

// HDel deletes field(s) from a hash
func (s *LockFreeStore) HDel(key string, fields ...string) (logN int64, logErr error) {
	s.expireIfNeeded(key)
	defer func() {
		if logErr == nil && logN > 0 {
			s.logContainerOp("HDEL", key, fields...)
		}
	}()
	if len(fields) == 0 {
		return 0, nil
	}

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, ok := shard.get(key)
	if !ok {
		return 0, nil // Key doesn't exist
	}

	if !storedValue.IsHash() {
		return 0, pkgErrors.ErrWrongType
	}

	hashValue, _ := storedValue.AsHash()
	removed := hashValue.Delete(fields...)

	// If hash becomes empty, delete the key
	if hashValue.IsEmpty() {
		shard.del(key)
		atomic.AddInt64(&shard.keyCount, -1)
		atomic.AddInt64(&s.totalKeys, -1)
		atomic.AddInt64(&shard.memoryUsage, -storedValue.MemoryUsage())
		atomic.AddInt64(&s.totalMemory, -storedValue.MemoryUsage())
	} else if removed > 0 {
		// Update memory usage
		newSize := hashValue.MemoryUsage()
		oldSize := storedValue.memorySize - int64(unsafe.Sizeof(*storedValue))
		memoryDelta := newSize - oldSize

		storedValue.memorySize = int64(unsafe.Sizeof(*storedValue)) + newSize
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)
	}

	return removed, nil
}

// HExists checks if field exists in a hash
func (s *LockFreeStore) HExists(key, field string) (bool, error) {
	s.expireIfNeeded(key)
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, ok := shard.get(key)
	if !ok {
		return false, nil // Key doesn't exist
	}

	if !storedValue.IsHash() {
		return false, pkgErrors.ErrWrongType
	}

	hashValue, _ := storedValue.AsHash()
	return hashValue.Exists(field), nil
}

// HLen gets number of fields in a hash
func (s *LockFreeStore) HLen(key string) (int64, error) {
	s.expireIfNeeded(key)
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, ok := shard.get(key)
	if !ok {
		return 0, nil // Key doesn't exist
	}

	if !storedValue.IsHash() {
		return 0, pkgErrors.ErrWrongType
	}

	hashValue, _ := storedValue.AsHash()
	return hashValue.Len(), nil
}

// HKeys gets all field names in a hash
func (s *LockFreeStore) HKeys(key string) ([]string, error) {
	s.expireIfNeeded(key)
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, ok := shard.get(key)
	if !ok {
		return []string{}, nil // Key doesn't exist, return empty slice
	}

	if !storedValue.IsHash() {
		return nil, pkgErrors.ErrWrongType
	}

	hashValue, _ := storedValue.AsHash()
	return hashValue.Keys(), nil
}

// HVals gets all values in a hash
func (s *LockFreeStore) HVals(key string) ([]string, error) {
	s.expireIfNeeded(key)
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, ok := shard.get(key)
	if !ok {
		return []string{}, nil // Key doesn't exist, return empty slice
	}

	if !storedValue.IsHash() {
		return nil, pkgErrors.ErrWrongType
	}

	hashValue, _ := storedValue.AsHash()
	return hashValue.Values(), nil
}

// HGetAll gets all field-value pairs in a hash
func (s *LockFreeStore) HGetAll(key string) ([]string, error) {
	s.expireIfNeeded(key)
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, ok := shard.get(key)
	if !ok {
		return []string{}, nil // Key doesn't exist, return empty slice
	}

	if !storedValue.IsHash() {
		return nil, pkgErrors.ErrWrongType
	}

	hashValue, _ := storedValue.AsHash()
	return hashValue.GetAll(), nil
}

// HIncrBy increments a field's integer value
func (s *LockFreeStore) HIncrBy(key, field string, increment int64) (logN int64, logErr error) {
	s.expireIfNeeded(key)
	defer func() {
		if logErr == nil {
			s.logContainerOp("HSET", key, field, strconv.FormatInt(logN, 10))
		}
	}()
	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	var hashValue *HashValue
	var exists bool

	if storedValue, ok := shard.get(key); ok {
		if !storedValue.IsHash() {
			return 0, pkgErrors.ErrWrongType
		}
		hashValue, _ = storedValue.AsHash()
		exists = true
	} else {
		hashValue = NewHashValue()
		exists = false
	}

	// Increment the field
	newValue, err := hashValue.IncrBy(field, increment)
	if err != nil {
		return 0, err
	}

	// If it's a new hash, store it
	if !exists {
		storedHash := NewStoredHash()
		storedHash.HashVal = hashValue

		// Calculate memory usage
		memoryDelta := storedHash.MemoryUsage()
		newMemory := atomic.LoadInt64(&s.totalMemory) + memoryDelta

		if s.wouldExceedMemory(shard, memoryDelta) {
			return 0, pkgErrors.NewMemoryLimit(newMemory, s.limits.MaxMemoryUsage)
		}

		shard.put(key, storedHash)

		atomic.AddInt64(&shard.keyCount, 1)
		atomic.AddInt64(&s.totalKeys, 1)
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)
	} else {
		// Update existing hash memory usage
		if oldStoredValue, ok := shard.get(key); ok {
			oldSize := oldStoredValue.memorySize - int64(unsafe.Sizeof(*oldStoredValue))
			newSize := hashValue.MemoryUsage()
			memoryDelta := newSize - oldSize

			atomic.AddInt64(&shard.memoryUsage, memoryDelta)
			atomic.AddInt64(&s.totalMemory, memoryDelta)
			oldStoredValue.memorySize = int64(unsafe.Sizeof(*oldStoredValue)) + newSize
		}
	}

	return newValue, nil
}

// HIncrByFloat increments a field's float value
func (s *LockFreeStore) HIncrByFloat(key, field string, increment float64) (logF float64, logErr error) {
	s.expireIfNeeded(key)
	defer func() {
		// Log the field as stored, so replay reproduces its exact formatting.
		if logErr == nil && atomic.LoadUint64(&s.localOrigin) == 0 {
			if v, ok, _ := s.HGet(key, field); ok {
				s.logContainerOp("HSET", key, field, v)
			}
		}
	}()
	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	var hashValue *HashValue
	var exists bool

	if storedValue, ok := shard.get(key); ok {
		if !storedValue.IsHash() {
			return 0, pkgErrors.ErrWrongType
		}
		hashValue, _ = storedValue.AsHash()
		exists = true
	} else {
		hashValue = NewHashValue()
		exists = false
	}

	// Increment the field
	newValue, err := hashValue.IncrByFloat(field, increment)
	if err != nil {
		return 0, err
	}

	// If it's a new hash, store it
	if !exists {
		storedHash := NewStoredHash()
		storedHash.HashVal = hashValue

		// Calculate memory usage
		memoryDelta := storedHash.MemoryUsage()
		newMemory := atomic.LoadInt64(&s.totalMemory) + memoryDelta

		if s.wouldExceedMemory(shard, memoryDelta) {
			return 0, pkgErrors.NewMemoryLimit(newMemory, s.limits.MaxMemoryUsage)
		}

		shard.put(key, storedHash)

		atomic.AddInt64(&shard.keyCount, 1)
		atomic.AddInt64(&s.totalKeys, 1)
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)
	} else {
		// Update existing hash memory usage
		if oldStoredValue, ok := shard.get(key); ok {
			oldSize := oldStoredValue.memorySize - int64(unsafe.Sizeof(*oldStoredValue))
			newSize := hashValue.MemoryUsage()
			memoryDelta := newSize - oldSize

			atomic.AddInt64(&shard.memoryUsage, memoryDelta)
			atomic.AddInt64(&s.totalMemory, memoryDelta)
			oldStoredValue.memorySize = int64(unsafe.Sizeof(*oldStoredValue)) + newSize
		}
	}

	return newValue, nil
}
