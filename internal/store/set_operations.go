package store

import (
	"errors"
	"sync/atomic"
	"unsafe"

	pkgErrors "reservoir/pkg/errors"
)

// Set operations for the LockFreeStore

func (s *LockFreeStore) SAdd(key string, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	var setValue *SetValue
	var exists bool

	if storedValue, ok := shard.data[key]; ok {
		if !storedValue.IsSet() {
			return 0, pkgErrors.ErrWrongType
		}
		setValue, _ = storedValue.AsSet()
		exists = true
	} else {
		setValue = NewSetValue()
		exists = false
	}

	// Add members and count additions
	added := setValue.Add(members...)

	// If it's a new set, store it
	if !exists && added > 0 {
		storedSet := NewStoredSet()
		storedSet.SetVal = setValue

		// Calculate memory usage
		memoryDelta := storedSet.MemoryUsage() + int64(len(key))
		newMemory := atomic.LoadInt64(&s.totalMemory) + memoryDelta

		if newMemory > s.limits.MaxMemoryUsage {
			return 0, pkgErrors.NewMemoryLimit(newMemory, s.limits.MaxMemoryUsage)
		}

		shard.data[key] = storedSet

		atomic.AddInt64(&shard.keyCount, 1)
		atomic.AddInt64(&s.totalKeys, 1)
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)

	} else if exists && added > 0 {
		// The set was modified in place (AsSet returns the live *SetValue held
		// by the stored value), so the map entry already reflects the additions.
		// Update its cached memory size and the counters in place, mirroring
		// SRem. Wrapping it in a fresh StoredValue (as before) reset memorySize
		// to zero, so the next SAdd computed a negative oldSize and inflated
		// totalMemory on every call.
		storedValue := shard.data[key]
		oldSize := storedValue.memorySize - int64(unsafe.Sizeof(*storedValue))
		newSize := setValue.MemoryUsage()
		memoryDelta := newSize - oldSize

		storedValue.memorySize = int64(unsafe.Sizeof(*storedValue)) + newSize
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)
	}

	return added, nil
}

// SRem removes one or more members from a set
func (s *LockFreeStore) SRem(key string, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, ok := shard.data[key]
	if !ok {
		return 0, nil // Key doesn't exist
	}

	if !storedValue.IsSet() {
		return 0, pkgErrors.ErrWrongType
	}

	setValue, _ := storedValue.AsSet()
	removed := setValue.Remove(members...)

	// If set becomes empty, delete the key
	if setValue.IsEmpty() {
		delete(shard.data, key)
		// Free the value and the key length, symmetric with SAdd's create path
		// (which charges MemoryUsage()+len(key)). Subtracting only
		// MemoryUsage() leaked len(key) bytes of accounted memory per set.
		freed := storedValue.MemoryUsage() + int64(len(key))
		atomic.AddInt64(&shard.keyCount, -1)
		atomic.AddInt64(&s.totalKeys, -1)
		atomic.AddInt64(&shard.memoryUsage, -freed)
		atomic.AddInt64(&s.totalMemory, -freed)
	} else if removed > 0 {
		// Update memory usage
		newSize := setValue.MemoryUsage()
		oldSize := storedValue.memorySize - int64(unsafe.Sizeof(*storedValue))
		memoryDelta := newSize - oldSize

		storedValue.memorySize = int64(unsafe.Sizeof(*storedValue)) + newSize
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)
	}

	return removed, nil
}

// SIsMember checks if member is in the set
func (s *LockFreeStore) SIsMember(key string, member string) (bool, error) {
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, ok := shard.data[key]
	if !ok {
		return false, nil // Key doesn't exist
	}

	if !storedValue.IsSet() {
		return false, pkgErrors.ErrWrongType
	}

	setValue, _ := storedValue.AsSet()
	return setValue.IsMember(member), nil
}

// SCard returns the cardinality (size) of the set
func (s *LockFreeStore) SCard(key string) (int64, error) {
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, ok := shard.data[key]

	if !ok {
		return 0, nil // Key doesn't exist
	}

	if !storedValue.IsSet() {
		return 0, pkgErrors.ErrWrongType
	}

	setValue, _ := storedValue.AsSet()
	return setValue.Card(), nil
}

// SMembers returns all members of the set
func (s *LockFreeStore) SMembers(key string) ([]string, error) {
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, ok := shard.data[key]
	if !ok {
		return []string{}, nil // Key doesn't exist, return empty slice
	}

	if !storedValue.IsSet() {
		return nil, pkgErrors.ErrWrongType
	}

	setValue, _ := storedValue.AsSet()
	return setValue.Members(), nil
}

// SPop removes and returns a random member from the set
func (s *LockFreeStore) SPop(key string) (string, bool, error) {
	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, ok := shard.data[key]
	if !ok {
		return "", false, nil // Key doesn't exist
	}

	if !storedValue.IsSet() {
		return "", false, pkgErrors.ErrWrongType
	}

	setValue, _ := storedValue.AsSet()
	element, exists := setValue.Pop()

	if !exists {
		return "", false, nil // Set is empty
	}

	// If set becomes empty, delete the key
	if setValue.IsEmpty() {
		delete(shard.data, key)
		atomic.AddInt64(&shard.keyCount, -1)
		atomic.AddInt64(&s.totalKeys, -1)
		atomic.AddInt64(&shard.memoryUsage, -storedValue.MemoryUsage())
		atomic.AddInt64(&s.totalMemory, -storedValue.MemoryUsage())
	} else {
		// Update memory usage
		newSize := setValue.MemoryUsage()
		oldSize := storedValue.memorySize - int64(unsafe.Sizeof(*storedValue))
		memoryDelta := newSize - oldSize

		storedValue.memorySize = int64(unsafe.Sizeof(*storedValue)) + newSize
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)
	}

	return element, true, nil
}

// SDiff returns members in the first set that are not in the other sets
func (s *LockFreeStore) SDiff(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}

	// Get first set
	firstShard := s.getShard(keys[0])
	firstShard.mu.RLock()

	storedValue, ok := firstShard.data[keys[0]]
	if !ok {
		firstShard.mu.RUnlock()
		return []string{}, nil // First set doesn't exist
	}

	if !storedValue.IsSet() {
		firstShard.mu.RUnlock()
		return nil, pkgErrors.ErrWrongType
	}

	firstSet, _ := storedValue.AsSet()
	result := firstSet.Copy()
	firstShard.mu.RUnlock()

	// If only one key, return all members of first set
	if len(keys) == 1 {
		return result.Members(), nil
	}

	// Remove elements that exist in other sets
	for i := 1; i < len(keys); i++ {
		otherShard := s.getShard(keys[i])
		otherShard.mu.RLock()

		if otherValue, exists := otherShard.data[keys[i]]; exists {
			if !otherValue.IsSet() {
				otherShard.mu.RUnlock()
				return nil, pkgErrors.ErrWrongType
			}

			otherSet, _ := otherValue.AsSet()
			otherMembers := otherSet.Members()
			result.Remove(otherMembers...)
		}
		// If other set doesn't exist, skip it
		otherShard.mu.RUnlock()
	}

	return result.Members(), nil
}

// SInter returns members that are in all sets
func (s *LockFreeStore) SInter(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}

	// Snapshot each set under its own shard lock, one key at a time. The
	// previous implementation held an RLock on every key's shard at once; when
	// two keys mapped to the same shard (guaranteed with 256 shards under any
	// real key set) that took a recursive RLock on one RWMutex, which deadlocks
	// if a writer queues between the two acquisitions. Copying each set and
	// releasing immediately avoids holding more than one lock at a time, at the
	// cost of cross-key atomicity (already the behavior of SUnion/SDiff).
	snapshots := make([]*SetValue, 0, len(keys))
	for _, key := range keys {
		shard := s.getShard(key)
		shard.mu.RLock()

		storedValue, exists := shard.data[key]
		if !exists {
			// If any set is missing, the intersection is empty.
			shard.mu.RUnlock()
			return []string{}, nil
		}
		if !storedValue.IsSet() {
			shard.mu.RUnlock()
			return nil, pkgErrors.ErrWrongType
		}

		setValue, _ := storedValue.AsSet()
		snapshots = append(snapshots, setValue.Copy())
		shard.mu.RUnlock()
	}

	// Calculate intersection outside any lock.
	result := snapshots[0]
	for i := 1; i < len(snapshots); i++ {
		intersection := result.Inter(snapshots[i])
		result = NewSetValue()
		result.Add(intersection...)
	}

	return result.Members(), nil
}

// SUnion returns all members from all sets
func (s *LockFreeStore) SUnion(keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return []string{}, nil
	}

	result := NewSetValue()

	for _, key := range keys {
		shard := s.getShard(key)
		shard.mu.RLock()

		if storedValue, exists := shard.data[key]; exists {
			if !storedValue.IsSet() {
				shard.mu.RUnlock()
				return nil, pkgErrors.ErrWrongType
			}

			setValue, _ := storedValue.AsSet()
			members := setValue.Members()
			result.Add(members...)
		}
		// If set doesn't exist, skip it
		shard.mu.RUnlock()
	}

	return result.Members(), nil
}

// SDiffStore computes the difference between sets and stores result in destination key
func (s *LockFreeStore) SDiffStore(destination string, keys ...string) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	// Compute difference using existing SDiff method
	diff, err := s.SDiff(keys...)
	if err != nil {
		return 0, err
	}

	// Create new set with the difference result
	resultSet := NewSetValue()
	resultSet.Add(diff...)

	// Store the result set in destination key
	storedValue := NewStoredSet()
	storedValue.SetVal = resultSet
	if err := s.SetStoredValue(destination, storedValue); err != nil {
		return 0, err
	}

	return int64(len(diff)), nil
}

// SStats returns detailed statistics about a set including LSM-tree status
func (s *LockFreeStore) SStats(key string) (map[string]interface{}, error) {
	shard := s.getShard(key)

	shard.mu.RLock()
	defer shard.mu.RUnlock()

	storedValue, ok := shard.data[key]
	if !ok {
		return nil, errors.New("Key not found")
	}

	if !storedValue.IsSet() {
		return nil, pkgErrors.ErrWrongType
	}

	setValue, _ := storedValue.AsSet()
	return setValue.Stats(), nil
}

// exists is a helper function to check if key exists in map
