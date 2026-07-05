package store

import (
	"errors"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

// ValueType represents the type of a stored value
type ValueType int

const (
	ValueTypeString ValueType = iota
	ValueTypeList
	ValueTypeSet
	ValueTypeHash
)

// String returns string representation of ValueType
func (vt ValueType) String() string {
	switch vt {
	case ValueTypeString:
		return "string"
	case ValueTypeList:
		return "list"
	case ValueTypeSet:
		return "set"
	case ValueTypeHash:
		return "hash"
	default:
		return "unknown"
	}
}

// Legacy type aliases for compatibility
const (
	StringType = ValueTypeString
	ListType   = ValueTypeList
	SetType    = ValueTypeSet
	HashType   = ValueTypeHash
)

// NewStoredString creates a new stored value with string type
func NewStoredString(value string) *StoredValue {
	return &StoredValue{
		Type:      ValueTypeString,
		StringVal: value,
	}
}

// NewStoredHash creates a new stored value with hash type
func NewStoredHash() *StoredValue {
	return &StoredValue{
		Type:    ValueTypeHash,
		HashVal: NewHashValue(),
	}
}

// NewStoredList creates a new stored value with list type
func NewStoredList() *StoredValue {
	return &StoredValue{
		Type:    ValueTypeList,
		ListVal: NewListValue(),
	}
}

// NewStoredSet creates a new stored value with set type
func NewStoredSet() *StoredValue {
	return &StoredValue{
		Type:   ValueTypeSet,
		SetVal: NewSetValue(),
	}
}

// SetValue represents a Redis-compatible set with simple in-memory storage
type SetValue struct {
	// Memory tier for all sets
	Elements map[string]struct{} // Using map for O(1) operations
	mu       sync.RWMutex        // For concurrent access safety

	totalSize int64 // Total number of elements
}

// NewSetValue creates a new set value
func NewSetValue() *SetValue {
	return &SetValue{
		Elements:  make(map[string]struct{}),
		totalSize: 0,
	}
}

// NewSetValueFromKey creates a new set value
func NewSetValueFromKey(key string) *SetValue {
	return &SetValue{
		Elements:  make(map[string]struct{}),
		totalSize: 0,
	}
}

// Add adds one or more elements to the set
// Returns the number of elements that were actually added (not already present)
func (sv *SetValue) Add(elements ...string) int64 {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	added := int64(0)
	for _, element := range elements {
		if _, exists := sv.Elements[element]; !exists {
			sv.Elements[element] = struct{}{}
			added++
			sv.totalSize++
		}
	}

	return added
}

// Remove removes one or more elements from the set
// Returns the number of elements that were actually removed
func (sv *SetValue) Remove(elements ...string) int64 {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	removed := int64(0)
	for _, element := range elements {
		if _, exists := sv.Elements[element]; exists {
			delete(sv.Elements, element)
			removed++
			sv.totalSize--
		}
	}

	return removed
}

// IsMember checks if an element is a member of the set
func (sv *SetValue) IsMember(element string) bool {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	_, exists := sv.Elements[element]
	return exists
}

// Card returns the cardinality (size) of the set
func (sv *SetValue) Card() int64 {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	return sv.totalSize
}

// Members returns all elements in the set as a slice
func (sv *SetValue) Members() []string {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	members := make([]string, 0, len(sv.Elements))
	for element := range sv.Elements {
		members = append(members, element)
	}
	return members
}

// Pop removes and returns a random element from the set
// Returns empty string and false if set is empty
func (sv *SetValue) Pop() (string, bool) {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	if len(sv.Elements) == 0 {
		return "", false
	}

	// Get first element (random enough for simple implementation)
	for element := range sv.Elements {
		delete(sv.Elements, element)
		sv.totalSize--
		return element, true
	}

	return "", false
}

// IsEmpty returns true if the set is empty
func (sv *SetValue) IsEmpty() bool {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	return sv.totalSize == 0
}

// Diff returns elements that are in this set but not in the other set
func (sv *SetValue) Diff(other *SetValue) []string {
	sv.mu.RLock()
	defer sv.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	var diff []string
	for element := range sv.Elements {
		if _, exists := other.Elements[element]; !exists {
			diff = append(diff, element)
		}
	}
	return diff
}

// Inter returns elements that are in both sets (intersection)
func (sv *SetValue) Inter(other *SetValue) []string {
	sv.mu.RLock()
	defer sv.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	var inter []string

	// Iterate over the smaller set for efficiency
	if sv.totalSize <= other.totalSize {
		for element := range sv.Elements {
			if _, exists := other.Elements[element]; exists {
				inter = append(inter, element)
			}
		}
	} else {
		for element := range other.Elements {
			if _, exists := sv.Elements[element]; exists {
				inter = append(inter, element)
			}
		}
	}

	return inter
}

// Union returns elements that are in either set
func (sv *SetValue) Union(other *SetValue) []string {
	sv.mu.RLock()
	defer sv.mu.RUnlock()
	other.mu.RLock()
	defer other.mu.RUnlock()

	unionSet := make(map[string]struct{})

	// Add all elements from both sets
	for element := range sv.Elements {
		unionSet[element] = struct{}{}
	}
	for element := range other.Elements {
		unionSet[element] = struct{}{}
	}

	// Convert to slice
	union := make([]string, 0, len(unionSet))
	for element := range unionSet {
		union = append(union, element)
	}

	return union
}

// MemoryUsage returns estimated memory usage of the set
func (sv *SetValue) MemoryUsage() int64 {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	size := int64(unsafe.Sizeof(*sv))
	for element := range sv.Elements {
		size += int64(len(element))
	}

	return size
}

// Stats returns detailed statistics about the set
func (sv *SetValue) Stats() map[string]interface{} {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	return map[string]interface{}{
		"total_size":      sv.totalSize,
		"storage_type":    "memory",
		"memory_elements": len(sv.Elements),
		"memory_usage":    sv.MemoryUsage(),
	}
}

// Copy creates a copy of the set
func (sv *SetValue) Copy() *SetValue {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	copySet := &SetValue{
		Elements:  make(map[string]struct{}, len(sv.Elements)),
		totalSize: sv.totalSize,
	}

	for elem := range sv.Elements {
		copySet.Elements[elem] = struct{}{}
	}

	return copySet
}

// HashValue represents a Redis-compatible hash
type HashValue struct {
	Fields map[string]string // field-value pairs
	mu     sync.RWMutex      // For concurrent access safety
}

// NewHashValue creates a new hash value
func NewHashValue() *HashValue {
	return &HashValue{
		Fields: make(map[string]string),
	}
}

// Set sets field to value in the hash
func (hv *HashValue) Set(field, value string) bool {
	hv.mu.Lock()
	defer hv.mu.Unlock()

	_, existed := hv.Fields[field]
	hv.Fields[field] = value
	return !existed // return true if field was new
}

// SetMultiple sets multiple field-value pairs in the hash
// Returns number of new fields added and any error
func (hv *HashValue) SetMultiple(fieldValues ...string) (int64, error) {
	if len(fieldValues)%2 != 0 {
		return 0, errors.New("field-value pairs must be even")
	}

	hv.mu.Lock()
	defer hv.mu.Unlock()

	added := int64(0)
	for i := 0; i < len(fieldValues); i += 2 {
		field := fieldValues[i]
		value := fieldValues[i+1]

		if _, existed := hv.Fields[field]; !existed {
			added++
		}
		hv.Fields[field] = value
	}

	return added, nil
}

// Get gets the value of field in the hash
func (hv *HashValue) Get(field string) (string, bool) {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	value, exists := hv.Fields[field]
	return value, exists
}

// Del deletes field from the hash
func (hv *HashValue) Del(fields ...string) int64 {
	hv.mu.Lock()
	defer hv.mu.Unlock()

	deleted := int64(0)
	for _, field := range fields {
		if _, exists := hv.Fields[field]; exists {
			delete(hv.Fields, field)
			deleted++
		}
	}
	return deleted
}

// Exists checks if field exists in the hash
func (hv *HashValue) Exists(field string) bool {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	_, exists := hv.Fields[field]
	return exists
}

// Len returns the number of fields in the hash
func (hv *HashValue) Len() int64 {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	return int64(len(hv.Fields))
}

// Keys returns all field names in the hash
func (hv *HashValue) Keys() []string {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	keys := make([]string, 0, len(hv.Fields))
	for field := range hv.Fields {
		keys = append(keys, field)
	}
	return keys
}

// Values returns all values in the hash
func (hv *HashValue) Values() []string {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	values := make([]string, 0, len(hv.Fields))
	for _, value := range hv.Fields {
		values = append(values, value)
	}
	return values
}

// GetAll returns all field-value pairs as a flat slice (Redis HGETALL format)
func (hv *HashValue) GetAll() []string {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	result := make([]string, 0, len(hv.Fields)*2)
	for field, value := range hv.Fields {
		result = append(result, field, value)
	}
	return result
}

// GetAllAsMap returns all field-value pairs as a map
func (hv *HashValue) GetAllAsMap() map[string]string {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	result := make(map[string]string, len(hv.Fields))
	for field, value := range hv.Fields {
		result[field] = value
	}
	return result
}

// Delete deletes fields from the hash (alias for Del for compatibility)
func (hv *HashValue) Delete(fields ...string) int64 {
	return hv.Del(fields...)
}

// IsEmpty returns true if the hash is empty
func (hv *HashValue) IsEmpty() bool {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	return len(hv.Fields) == 0
}

// MemoryUsage returns estimated memory usage of the hash
func (hv *HashValue) MemoryUsage() int64 {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	size := int64(unsafe.Sizeof(*hv))
	for field, value := range hv.Fields {
		size += int64(len(field) + len(value))
	}

	return size
}

// IncrBy increments the integer value of a field by increment
func (hv *HashValue) IncrBy(field string, increment int64) (int64, error) {
	hv.mu.Lock()
	defer hv.mu.Unlock()

	currentStr, exists := hv.Fields[field]
	var current int64 = 0

	if exists {
		var err error
		current, err = strconv.ParseInt(currentStr, 10, 64)
		if err != nil {
			return 0, errors.New("hash value is not an integer")
		}
	}

	newValue := current + increment
	hv.Fields[field] = strconv.FormatInt(newValue, 10)

	return newValue, nil
}

// IncrByFloat increments the float value of a field by increment
func (hv *HashValue) IncrByFloat(field string, increment float64) (float64, error) {
	hv.mu.Lock()
	defer hv.mu.Unlock()

	currentStr, exists := hv.Fields[field]
	var current float64 = 0.0

	if exists {
		var err error
		current, err = strconv.ParseFloat(currentStr, 64)
		if err != nil {
			return 0, errors.New("hash value is not a float")
		}
	}

	newValue := current + increment
	hv.Fields[field] = strconv.FormatFloat(newValue, 'f', -1, 64)

	return newValue, nil
}

// StoredValue represents a value that can be stored in the key-value store
type StoredValue struct {
	Type       ValueType  `json:"type"`
	StringVal  string     `json:"string_val,omitempty"`
	ListVal    *ListValue `json:"list_val,omitempty"`
	SetVal     *SetValue  `json:"set_val,omitempty"`
	HashVal    *HashValue `json:"hash_val,omitempty"`
	WriteCount int64      `json:"write_count"`
	ReadCount  int64      `json:"read_count"`
	LastWrite  time.Time  `json:"last_write"`
	LastRead   time.Time  `json:"last_read"`
	memorySize int64      // cached memory size

	// Replication tracking fields (simplified implementation)
	Replicated          bool      `json:"replicated,omitempty"`
	LastReplication     time.Time `json:"last_replication,omitempty"`
	ReplicatedToNodes   []string  `json:"replicated_to_nodes,omitempty"`
	ReplicationAttempts int       `json:"replication_attempts,omitempty"`

	// Legacy compatibility field
	Data interface{} `json:"data,omitempty"`
}

// Release is a no-op method for compatibility with object pools
func (sv *StoredValue) Release() {
	// No-op: we don't use object pools for simple implementation
}

// ListValue represents a Redis-compatible list
// ListValue represents a Redis-compatible list using a deque for O(1) push/pop
type ListValue struct {
	deque *Deque       `json:"-"`
	mu    sync.RWMutex `json:"-"`
}

// Elements returns the list elements as a slice (for JSON serialization)
func (lv *ListValue) Elements() []string {
	lv.mu.RLock()
	defer lv.mu.RUnlock()
	return lv.deque.ToSlice()
}

// MarshalJSON implements json.Marshaler
func (lv *ListValue) MarshalJSON() ([]byte, error) {
	lv.mu.RLock()
	defer lv.mu.RUnlock()
	elements := lv.deque.ToSlice()
	// Manual JSON encoding for performance
	if len(elements) == 0 {
		return []byte(`{"elements":[]}`), nil
	}
	// Simple approach - could be optimized further
	result := `{"elements":[`
	for i, e := range elements {
		if i > 0 {
			result += ","
		}
		result += `"` + e + `"` // Note: doesn't escape special chars
	}
	result += `]}`
	return []byte(result), nil
}

// NewListValue creates a new list value with O(1) operations
func NewListValue() *ListValue {
	return &ListValue{
		deque: NewDeque(),
	}
}

// NewListValueFromSlice creates a list from existing elements
func NewListValueFromSlice(elements []string) *ListValue {
	return &ListValue{
		deque: NewDequeWithElements(elements),
	}
}

// LPush adds elements to the beginning of the list - O(1) amortized
func (lv *ListValue) LPush(elements ...string) int64 {
	lv.mu.Lock()
	defer lv.mu.Unlock()

	if len(elements) == 0 {
		return int64(lv.deque.Len())
	}

	return int64(lv.deque.PushFront(elements...))
}

// RPush adds elements to the end of the list - O(1) amortized
func (lv *ListValue) RPush(elements ...string) int64 {
	lv.mu.Lock()
	defer lv.mu.Unlock()

	return int64(lv.deque.PushBack(elements...))
}

// LPop removes and returns elements from the front - O(n) where n is count
func (lv *ListValue) LPop(count int) []string {
	lv.mu.Lock()
	defer lv.mu.Unlock()

	if lv.deque.Len() == 0 {
		return nil
	}

	if count <= 0 {
		count = 1
	}

	return lv.deque.PopFront(count)
}

// RPop removes and returns elements from the back - O(n) where n is count
func (lv *ListValue) RPop(count int) []string {
	lv.mu.Lock()
	defer lv.mu.Unlock()

	if lv.deque.Len() == 0 {
		return nil
	}

	if count <= 0 {
		count = 1
	}

	return lv.deque.PopBack(count)
}

// LLen returns the length of the list - O(1)
func (lv *ListValue) LLen() int64 {
	lv.mu.RLock()
	defer lv.mu.RUnlock()

	return int64(lv.deque.Len())
}

// LRange returns elements in the range [start, stop] - O(n) where n is range size
func (lv *ListValue) LRange(start, stop int) []string {
	lv.mu.RLock()
	defer lv.mu.RUnlock()

	return lv.deque.Range(start, stop)
}

// Copy creates a copy of the list
func (lv *ListValue) Copy() *ListValue {
	lv.mu.RLock()
	defer lv.mu.RUnlock()

	return NewListValueFromSlice(lv.deque.ToSlice())
}

// CopyWithPrepend creates a copy with elements prepended
func (lv *ListValue) CopyWithPrepend(elements ...string) *ListValue {
	lv.mu.RLock()
	defer lv.mu.RUnlock()

	newList := NewListValueFromSlice(lv.deque.ToSlice())
	newList.deque.PushFront(elements...)
	return newList
}

// CopyWithAppend creates a copy with elements appended
func (lv *ListValue) CopyWithAppend(elements ...string) *ListValue {
	lv.mu.RLock()
	defer lv.mu.RUnlock()

	newList := NewListValueFromSlice(lv.deque.ToSlice())
	newList.deque.PushBack(elements...)
	return newList
}

// Len returns the length of the list (alias for LLen for compatibility)
func (lv *ListValue) Len() int64 {
	return lv.LLen()
}

// Index returns the element at the given index
func (lv *ListValue) Index(index int) (string, bool) {
	lv.mu.RLock()
	defer lv.mu.RUnlock()

	length := lv.deque.Len()
	if length == 0 {
		return "", false
	}

	// Handle negative indices
	if index < 0 {
		index = length + index
	}

	return lv.deque.Get(index)
}

// MemoryUsage returns estimated memory usage of the list
func (lv *ListValue) MemoryUsage() int64 {
	lv.mu.RLock()
	defer lv.mu.RUnlock()

	return lv.deque.MemoryUsage() + int64(unsafe.Sizeof(*lv))
}

// IsEmpty returns true if the list is empty
func (lv *ListValue) IsEmpty() bool {
	lv.mu.RLock()
	defer lv.mu.RUnlock()

	return lv.deque.Len() == 0
}

// Range is an alias for LRange for compatibility
func (lv *ListValue) Range(start, stop int) []string {
	return lv.LRange(start, stop)
}

// LPushUnique adds elements to the beginning of the list only if they don't already exist
func (lv *ListValue) LPushUnique(elements ...string) int64 {
	lv.mu.Lock()
	defer lv.mu.Unlock()

	// Create a set of existing elements for O(1) lookup
	existingSet := make(map[string]struct{})
	for i := 0; i < lv.deque.Len(); i++ {
		if elem, ok := lv.deque.Get(i); ok {
			existingSet[elem] = struct{}{}
		}
	}

	// Only add elements that don't exist (in reverse to maintain order)
	for i := len(elements) - 1; i >= 0; i-- {
		if _, exists := existingSet[elements[i]]; !exists {
			lv.deque.PushFront(elements[i])
			existingSet[elements[i]] = struct{}{}
		}
	}

	return int64(lv.deque.Len())
}

// RPushUnique adds elements to the end of the list only if they don't already exist
func (lv *ListValue) RPushUnique(elements ...string) int64 {
	lv.mu.Lock()
	defer lv.mu.Unlock()

	// Create a set of existing elements for O(1) lookup
	existingSet := make(map[string]struct{})
	for i := 0; i < lv.deque.Len(); i++ {
		if elem, ok := lv.deque.Get(i); ok {
			existingSet[elem] = struct{}{}
		}
	}

	// Only add elements that don't exist
	for _, element := range elements {
		if _, exists := existingSet[element]; !exists {
			lv.deque.PushBack(element)
			existingSet[element] = struct{}{}
		}
	}

	return int64(lv.deque.Len())
}

// Set sets the element at the given index
func (lv *ListValue) Set(index int, element string) bool {
	lv.mu.Lock()
	defer lv.mu.Unlock()

	length := lv.deque.Len()
	if length == 0 {
		return false
	}

	// Handle negative indices
	if index < 0 {
		index = length + index
	}

	// Check bounds
	if index < 0 || index >= length {
		return false
	}

	return lv.deque.Set(index, element)
}

// Remove removes the first count occurrences of element from the list
// If count > 0: remove from head to tail
// If count < 0: remove from tail to head
// If count = 0: remove all occurrences
func (lv *ListValue) Remove(count int64, element string) int64 {
	lv.mu.Lock()
	defer lv.mu.Unlock()

	return int64(lv.deque.Remove(int(count), element))
}

// Trim trims the list to only contain elements in the specified range
func (lv *ListValue) Trim(start, stop int64) {
	lv.mu.Lock()
	defer lv.mu.Unlock()

	lv.deque.Trim(int(start), int(stop))
}

// Reset clears the stored value and resets statistics
func (sv *StoredValue) Reset() {
	sv.StringVal = ""
	sv.ListVal = nil
	sv.SetVal = nil
	sv.HashVal = nil
	sv.WriteCount = 0
	sv.ReadCount = 0
	sv.LastWrite = time.Time{}
	sv.LastRead = time.Time{}
}

// TrackRead increments read count and updates last read time
func (sv *StoredValue) TrackRead() {
	sv.ReadCount++
	sv.LastRead = time.Now()
}

// TrackWrite increments write count and updates last write time
func (sv *StoredValue) TrackWrite() {
	sv.WriteCount++
	sv.LastWrite = time.Now()
}

// TrackReplication tracks replication information (no-op for simple implementation)
func (sv *StoredValue) TrackReplication(nodes ...string) {
	// No-op: we don't track replication details in simple implementation
	sv.Replicated = true
	sv.LastReplication = time.Now()
	sv.ReplicatedToNodes = nodes
	sv.ReplicationAttempts++
}

// MemoryUsage returns the estimated memory usage of the stored value.
//
// Lists are mutated in place without maintaining the cached memorySize (unlike
// sets and hashes, which update it on every mutation), so for a list we always
// recompute the current size rather than trusting a stale cache. This keeps the
// delete paths — which free MemoryUsage() — accurate as a list grows and shrinks.
func (sv *StoredValue) MemoryUsage() int64 {
	if sv.Type != ValueTypeList && sv.memorySize > 0 {
		return sv.memorySize
	}

	size := int64(unsafe.Sizeof(*sv))

	switch sv.Type {
	case ValueTypeString:
		size += int64(len(sv.StringVal))
	case ValueTypeList:
		if sv.ListVal != nil {
			size += sv.ListVal.MemoryUsage()
		}
		// Do not cache: the list changes in place.
		return size
	case ValueTypeSet:
		if sv.SetVal != nil {
			size += sv.SetVal.MemoryUsage()
		}
	case ValueTypeHash:
		if sv.HashVal != nil {
			size += int64(unsafe.Sizeof(*sv.HashVal))
			for field, value := range sv.HashVal.Fields {
				size += int64(len(field) + len(value))
			}
		}
	}

	sv.memorySize = size
	return size
}

// GetDetailedStats returns detailed statistics about the stored value
func (sv *StoredValue) GetDetailedStats() map[string]interface{} {
	stats := map[string]interface{}{
		"type":        sv.Type.String(),
		"read_count":  sv.ReadCount,
		"write_count": sv.WriteCount,
		"last_read":   sv.LastRead,
		"last_write":  sv.LastWrite,
		"memory_size": sv.MemoryUsage(),
	}

	switch sv.Type {
	case ValueTypeString:
		stats["string_length"] = len(sv.StringVal)
	case ValueTypeList:
		if sv.ListVal != nil {
			stats["list_length"] = sv.ListVal.LLen()
		}
	case ValueTypeSet:
		if sv.SetVal != nil {
			setStats := sv.SetVal.Stats()
			for k, v := range setStats {
				stats["set_"+k] = v
			}
		}
	case ValueTypeHash:
		if sv.HashVal != nil {
			stats["hash_field_count"] = len(sv.HashVal.Fields)
		}
	}

	return stats
}

// Type checking methods

// IsSet returns true if the stored value is a set
func (sv *StoredValue) IsSet() bool {
	return sv.Type == ValueTypeSet
}

// AsSet returns the set value and a boolean indicating success
func (sv *StoredValue) AsSet() (*SetValue, bool) {
	if sv.Type == ValueTypeSet {
		return sv.SetVal, true
	}
	return nil, false
}

// IsHash returns true if the stored value is a hash
func (sv *StoredValue) IsHash() bool {
	return sv.Type == ValueTypeHash
}

// AsHash returns the hash value and a boolean indicating success
func (sv *StoredValue) AsHash() (*HashValue, bool) {
	if sv.Type == ValueTypeHash {
		return sv.HashVal, true
	}
	return nil, false
}

// IsList returns true if the stored value is a list
func (sv *StoredValue) IsList() bool {
	return sv.Type == ValueTypeList
}

// AsList returns the list value and a boolean indicating success
func (sv *StoredValue) AsList() (*ListValue, bool) {
	if sv.Type == ValueTypeList {
		return sv.ListVal, true
	}
	return nil, false
}

// IsString returns true if the stored value is a string
func (sv *StoredValue) IsString() bool {
	return sv.Type == ValueTypeString
}

// AsString returns the string value and a boolean indicating success
func (sv *StoredValue) AsString() (string, bool) {
	if sv.Type == ValueTypeString {
		return sv.StringVal, true
	}
	return "", false
}
