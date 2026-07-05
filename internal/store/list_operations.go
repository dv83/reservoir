package store

import (
	"fmt"
	"sync/atomic"

	"reservoir/pkg/errors"
)

// --- Helper methods for reducing code duplication ---

// readValue reads a value from shard with proper locking
func (s *LockFreeStore) readValue(shard *LockFreeShard, key string) (interface{}, bool) {
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	value, exists := shard.data[key]
	return value, exists
}

// requireList type-checks value and returns list, or error if wrong type
func (s *LockFreeStore) requireList(raw interface{}) (*ListValue, *StoredValue, error) {
	storedValue, ok := raw.(*StoredValue)
	if !ok {
		return nil, nil, fmt.Errorf("invalid stored value type")
	}
	if !storedValue.IsList() {
		return nil, nil, errors.ErrWrongType
	}
	list, _ := storedValue.AsList()
	return list, storedValue, nil
}

// checkValueSize validates value against memory limits
func (s *LockFreeStore) checkValueSize(size int64) error {
	if s.limits.MaxMemoryUsage > 0 && size > s.limits.MaxValueSize {
		return errors.NewValueTooLarge(size, s.limits.MaxValueSize)
	}
	return nil
}

// trackNewKey updates counters when a new key is created
func (s *LockFreeStore) trackNewKey(shard *LockFreeShard, memSize int64) {
	atomic.AddInt64(&shard.keyCount, 1)
	atomic.AddInt64(&shard.memoryUsage, memSize)
	atomic.AddInt64(&s.totalMemory, memSize)
}

// trackDeletedKey updates counters when a key is deleted
func (s *LockFreeStore) trackDeletedKey(shard *LockFreeShard, memSize int64) {
	atomic.AddInt64(&shard.keyCount, -1)
	atomic.AddInt64(&shard.memoryUsage, -memSize)
	atomic.AddInt64(&s.totalMemory, -memSize)
}

// newStoredList creates a StoredValue containing a list
func newStoredList(list *ListValue) *StoredValue {
	sv := NewStoredList()
	sv.ListVal = list
	return sv
}

// --- List Operations ---

// LPush adds elements to the head of a list (O(1) amortized with in-place modification)
func (s *LockFreeStore) LPush(key string, elements ...string) (int64, error) {
	if len(elements) == 0 {
		return 0, fmt.Errorf("wrong number of arguments for 'lpush' command")
	}
	if int64(len(key)) > s.limits.MaxKeySize {
		return 0, errors.NewKeyTooLarge(int64(len(key)), s.limits.MaxKeySize)
	}

	shard := s.getShard(key)

	// In-place modification with shard lock - no CAS needed
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if storedVal, exists := shard.data[key]; exists {
		// Key exists - check type and modify in-place
		if storedVal.Type != ValueTypeList {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		list := storedVal.ListVal
		oldSize := list.MemoryUsage()

		// Modify list in-place
		newLength := list.LPush(elements...)

		// Check size after modification
		newSize := list.MemoryUsage()
		if err := s.checkValueSize(newSize); err != nil {
			// Rollback - pop the elements we just added
			list.LPop(len(elements))
			return 0, err
		}

		// Update memory stats
		diff := newSize - oldSize
		atomic.AddInt64(&shard.memoryUsage, diff)
		atomic.AddInt64(&s.totalMemory, diff)

		return newLength, nil
	}

	// Key doesn't exist - create new list
	newList := NewListValue()
	newLength := newList.LPush(elements...)

	if err := s.checkValueSize(newList.MemoryUsage()); err != nil {
		return 0, err
	}

	storedVal := newStoredList(newList)
	shard.data[key] = storedVal

	// Update stats
	atomic.AddInt64(&s.totalKeys, 1)
	memSize := storedVal.MemoryUsage()
	atomic.AddInt64(&shard.memoryUsage, memSize)
	atomic.AddInt64(&s.totalMemory, memSize)

	return newLength, nil
}

// RPush adds elements to the tail of a list (O(1) amortized with in-place modification)
func (s *LockFreeStore) RPush(key string, elements ...string) (int64, error) {
	if len(elements) == 0 {
		return 0, fmt.Errorf("wrong number of arguments for 'rpush' command")
	}
	if int64(len(key)) > s.limits.MaxKeySize {
		return 0, errors.NewKeyTooLarge(int64(len(key)), s.limits.MaxKeySize)
	}

	shard := s.getShard(key)

	// In-place modification with shard lock - no CAS needed
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if storedVal, exists := shard.data[key]; exists {
		// Key exists - check type and modify in-place
		if storedVal.Type != ValueTypeList {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		list := storedVal.ListVal
		oldSize := list.MemoryUsage()

		// Modify list in-place
		newLength := list.RPush(elements...)

		// Check size after modification
		newSize := list.MemoryUsage()
		if err := s.checkValueSize(newSize); err != nil {
			// Rollback - pop the elements we just added
			list.RPop(len(elements))
			return 0, err
		}

		// Update memory stats
		diff := newSize - oldSize
		atomic.AddInt64(&shard.memoryUsage, diff)
		atomic.AddInt64(&s.totalMemory, diff)

		return newLength, nil
	}

	// Key doesn't exist - create new list
	newList := NewListValue()
	newLength := newList.RPush(elements...)

	if err := s.checkValueSize(newList.MemoryUsage()); err != nil {
		return 0, err
	}

	storedVal := newStoredList(newList)
	shard.data[key] = storedVal

	// Update stats
	atomic.AddInt64(&s.totalKeys, 1)
	memSize := storedVal.MemoryUsage()
	atomic.AddInt64(&shard.memoryUsage, memSize)
	atomic.AddInt64(&s.totalMemory, memSize)

	return newLength, nil
}

// LLen returns the length of a list
func (s *LockFreeStore) LLen(key string) (int64, error) {
	shard := s.getShard(key)

	raw, exists := s.readValue(shard, key)
	if !exists {
		return 0, nil
	}

	list, _, err := s.requireList(raw)
	if err != nil {
		return 0, err
	}
	return list.LLen(), nil
}

// LPop removes and returns elements from the head of a list
func (s *LockFreeStore) LPop(key string, count int) ([]string, error) {
	if count < 0 {
		return nil, fmt.Errorf("value is out of range, must be positive")
	}
	if count == 0 {
		count = 1
	}

	shard := s.getShard(key)

	// In-place modification with shard lock - O(1) amortized
	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedVal, exists := shard.data[key]
	if !exists {
		return []string{}, nil
	}

	if storedVal.Type != ValueTypeList {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := storedVal.ListVal
	oldSize := list.MemoryUsage()

	// Modify list in-place
	result := list.LPop(count)
	if len(result) == 0 {
		return result, nil
	}

	// Check if list became empty
	if list.IsEmpty() {
		delete(shard.data, key)
		s.trackDeletedKey(shard, oldSize)
		return result, nil
	}

	// Update memory stats
	newSize := list.MemoryUsage()
	diff := newSize - oldSize
	atomic.AddInt64(&shard.memoryUsage, diff)
	atomic.AddInt64(&s.totalMemory, diff)

	return result, nil
}

// RPop removes and returns elements from the tail of a list
func (s *LockFreeStore) RPop(key string, count int) ([]string, error) {
	if count < 0 {
		return nil, fmt.Errorf("value is out of range, must be positive")
	}
	if count == 0 {
		count = 1
	}

	shard := s.getShard(key)

	// In-place modification with shard lock - O(1) amortized
	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedVal, exists := shard.data[key]
	if !exists {
		return []string{}, nil
	}

	if storedVal.Type != ValueTypeList {
		return nil, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := storedVal.ListVal
	oldSize := list.MemoryUsage()

	// Modify list in-place
	result := list.RPop(count)
	if len(result) == 0 {
		return result, nil
	}

	// Check if list became empty
	if list.IsEmpty() {
		delete(shard.data, key)
		s.trackDeletedKey(shard, oldSize)
		return result, nil
	}

	// Update memory stats
	newSize := list.MemoryUsage()
	diff := newSize - oldSize
	atomic.AddInt64(&shard.memoryUsage, diff)
	atomic.AddInt64(&s.totalMemory, diff)

	return result, nil
}

// LRange returns a range of elements from a list
func (s *LockFreeStore) LRange(key string, start, stop int64) ([]string, error) {
	shard := s.getShard(key)

	raw, exists := s.readValue(shard, key)
	if !exists {
		return []string{}, nil
	}

	list, _, err := s.requireList(raw)
	if err != nil {
		return nil, err
	}
	return list.Range(int(start), int(stop)), nil
}

// LIndex returns the element at index in a list
func (s *LockFreeStore) LIndex(key string, index int64) (string, bool, error) {
	shard := s.getShard(key)

	raw, exists := s.readValue(shard, key)
	if !exists {
		return "", false, nil
	}

	list, _, err := s.requireList(raw)
	if err != nil {
		return "", false, err
	}

	value, ok := list.Index(int(index))
	return value, ok, nil
}

// LSet sets the element at index in a list.
//
// Modifies the list in place under the shard write lock, consistent with the
// push/pop paths. A previous version copied the list and swapped it in with a
// pointer-comparing CAS, but the push paths mutate the list in place without
// changing the *StoredValue pointer, so a concurrent push would pass the CAS
// pointer check and be silently discarded by the swap.
func (s *LockFreeStore) LSet(key string, index int64, element string) error {
	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedVal, exists := shard.data[key]
	if !exists {
		return fmt.Errorf("no such key")
	}
	if storedVal.Type != ValueTypeList {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := storedVal.ListVal
	oldSize := list.MemoryUsage()
	if !list.Set(int(index), element) {
		return fmt.Errorf("index out of range")
	}

	diff := list.MemoryUsage() - oldSize
	atomic.AddInt64(&shard.memoryUsage, diff)
	atomic.AddInt64(&s.totalMemory, diff)
	return nil
}

// LRem removes elements from a list (in-place under the shard write lock).
func (s *LockFreeStore) LRem(key string, count int64, element string) (int64, error) {
	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedVal, exists := shard.data[key]
	if !exists {
		return 0, nil
	}
	if storedVal.Type != ValueTypeList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := storedVal.ListVal
	oldSize := list.MemoryUsage()
	removed := list.Remove(count, element)

	if list.IsEmpty() {
		delete(shard.data, key)
		s.trackDeletedKey(shard, oldSize)
		return removed, nil
	}

	diff := list.MemoryUsage() - oldSize
	atomic.AddInt64(&shard.memoryUsage, diff)
	atomic.AddInt64(&s.totalMemory, diff)
	return removed, nil
}

// LTrim trims a list to the specified range (in-place under the shard write lock).
func (s *LockFreeStore) LTrim(key string, start, stop int64) error {
	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedVal, exists := shard.data[key]
	if !exists {
		return nil
	}
	if storedVal.Type != ValueTypeList {
		return fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := storedVal.ListVal
	oldSize := list.MemoryUsage()
	list.Trim(start, stop)

	if list.IsEmpty() {
		delete(shard.data, key)
		s.trackDeletedKey(shard, oldSize)
		return nil
	}

	diff := list.MemoryUsage() - oldSize
	atomic.AddInt64(&shard.memoryUsage, diff)
	atomic.AddInt64(&s.totalMemory, diff)
	return nil
}

// LPushX pushes elements only if key exists (O(1) with in-place modification)
func (s *LockFreeStore) LPushX(key string, elements ...string) (int64, error) {
	if len(elements) == 0 {
		return 0, fmt.Errorf("wrong number of arguments for 'lpushx' command")
	}

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedVal, exists := shard.data[key]
	if !exists {
		return 0, nil // Key doesn't exist, return 0
	}

	if storedVal.Type != ValueTypeList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := storedVal.ListVal
	oldSize := list.MemoryUsage()
	newLength := list.LPush(elements...)

	newSize := list.MemoryUsage()
	if err := s.checkValueSize(newSize); err != nil {
		list.LPop(len(elements))
		return 0, err
	}

	diff := newSize - oldSize
	atomic.AddInt64(&shard.memoryUsage, diff)
	atomic.AddInt64(&s.totalMemory, diff)

	return newLength, nil
}

// RPushX pushes elements only if key exists (O(1) with in-place modification)
func (s *LockFreeStore) RPushX(key string, elements ...string) (int64, error) {
	if len(elements) == 0 {
		return 0, fmt.Errorf("wrong number of arguments for 'rpushx' command")
	}

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedVal, exists := shard.data[key]
	if !exists {
		return 0, nil // Key doesn't exist, return 0
	}

	if storedVal.Type != ValueTypeList {
		return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	list := storedVal.ListVal
	oldSize := list.MemoryUsage()
	newLength := list.RPush(elements...)

	newSize := list.MemoryUsage()
	if err := s.checkValueSize(newSize); err != nil {
		list.RPop(len(elements))
		return 0, err
	}

	diff := newSize - oldSize
	atomic.AddInt64(&shard.memoryUsage, diff)
	atomic.AddInt64(&s.totalMemory, diff)

	return newLength, nil
}

// LPushUnique pushes only unique elements to the head (in-place modification)
func (s *LockFreeStore) LPushUnique(key string, elements ...string) (int64, error) {
	if len(elements) == 0 {
		return 0, fmt.Errorf("wrong number of arguments for 'lpushunique' command")
	}
	if int64(len(key)) > s.limits.MaxKeySize {
		return 0, errors.NewKeyTooLarge(int64(len(key)), s.limits.MaxKeySize)
	}

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if storedVal, exists := shard.data[key]; exists {
		if storedVal.Type != ValueTypeList {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		list := storedVal.ListVal
		oldSize := list.MemoryUsage()
		newLength := list.LPushUnique(elements...)

		newSize := list.MemoryUsage()
		if err := s.checkValueSize(newSize); err != nil {
			return 0, err
		}

		diff := newSize - oldSize
		atomic.AddInt64(&shard.memoryUsage, diff)
		atomic.AddInt64(&s.totalMemory, diff)

		return newLength, nil
	}

	// Key doesn't exist - create new list with unique elements
	newList := NewListValue()
	length := newList.LPushUnique(elements...)

	if err := s.checkValueSize(newList.MemoryUsage()); err != nil {
		return 0, err
	}

	storedVal := newStoredList(newList)
	shard.data[key] = storedVal

	atomic.AddInt64(&s.totalKeys, 1)
	memSize := storedVal.MemoryUsage()
	atomic.AddInt64(&shard.memoryUsage, memSize)
	atomic.AddInt64(&s.totalMemory, memSize)

	return length, nil
}

// RPushUnique pushes only unique elements to the tail (in-place modification)
func (s *LockFreeStore) RPushUnique(key string, elements ...string) (int64, error) {
	if len(elements) == 0 {
		return 0, fmt.Errorf("wrong number of arguments for 'rpushunique' command")
	}
	if int64(len(key)) > s.limits.MaxKeySize {
		return 0, errors.NewKeyTooLarge(int64(len(key)), s.limits.MaxKeySize)
	}

	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if storedVal, exists := shard.data[key]; exists {
		if storedVal.Type != ValueTypeList {
			return 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
		}

		list := storedVal.ListVal
		oldSize := list.MemoryUsage()
		newLength := list.RPushUnique(elements...)

		newSize := list.MemoryUsage()
		if err := s.checkValueSize(newSize); err != nil {
			return 0, err
		}

		diff := newSize - oldSize
		atomic.AddInt64(&shard.memoryUsage, diff)
		atomic.AddInt64(&s.totalMemory, diff)

		return newLength, nil
	}

	// Key doesn't exist - create new list with unique elements
	newList := NewListValue()
	length := newList.RPushUnique(elements...)

	if err := s.checkValueSize(newList.MemoryUsage()); err != nil {
		return 0, err
	}

	storedVal := newStoredList(newList)
	shard.data[key] = storedVal

	atomic.AddInt64(&s.totalKeys, 1)
	memSize := storedVal.MemoryUsage()
	atomic.AddInt64(&shard.memoryUsage, memSize)
	atomic.AddInt64(&s.totalMemory, memSize)

	return length, nil
}
