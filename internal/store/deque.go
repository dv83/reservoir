package store

// Deque implements a double-ended queue using a circular buffer.
// It provides O(1) amortized operations for push/pop at both ends.
type Deque struct {
	data        []string
	head        int   // index of first element
	tail        int   // index after last element
	count       int   // number of elements
	contentSize int64 // cached total string content size for O(1) memory tracking
}

const (
	dequeInitialCapacity = 8
	dequeGrowthFactor    = 2
)

// NewDeque creates a new empty deque
func NewDeque() *Deque {
	return &Deque{
		data:  make([]string, dequeInitialCapacity),
		head:  0,
		tail:  0,
		count: 0,
	}
}

// NewDequeWithElements creates a deque initialized with elements
func NewDequeWithElements(elements []string) *Deque {
	if len(elements) == 0 {
		return NewDeque()
	}

	// Find capacity that fits elements (power of 2 for efficient modulo)
	capacity := dequeInitialCapacity
	for capacity < len(elements) {
		capacity *= dequeGrowthFactor
	}

	// Calculate content size
	var contentSize int64
	for _, elem := range elements {
		contentSize += int64(len(elem))
	}

	d := &Deque{
		data:        make([]string, capacity),
		head:        0,
		tail:        len(elements),
		count:       len(elements),
		contentSize: contentSize,
	}
	copy(d.data, elements)
	return d
}

// Len returns the number of elements
func (d *Deque) Len() int {
	return d.count
}

// Cap returns the current capacity
func (d *Deque) Cap() int {
	return len(d.data)
}

// grow doubles the capacity of the deque
func (d *Deque) grow() {
	newCap := len(d.data) * dequeGrowthFactor
	if newCap == 0 {
		newCap = dequeInitialCapacity
	}

	newData := make([]string, newCap)

	// Copy elements in order
	if d.count > 0 {
		if d.head < d.tail {
			// Elements are contiguous
			copy(newData, d.data[d.head:d.tail])
		} else {
			// Elements wrap around
			n := copy(newData, d.data[d.head:])
			copy(newData[n:], d.data[:d.tail])
		}
	}

	d.data = newData
	d.head = 0
	d.tail = d.count
}

// PushFront adds elements to the front (left) of the deque
// Returns the new length
func (d *Deque) PushFront(elements ...string) int {
	for i := len(elements) - 1; i >= 0; i-- {
		if d.count == len(d.data) {
			d.grow()
		}
		d.head = (d.head - 1 + len(d.data)) % len(d.data)
		d.data[d.head] = elements[i]
		d.count++
		d.contentSize += int64(len(elements[i]))
	}
	return d.count
}

// PushBack adds elements to the back (right) of the deque
// Returns the new length
func (d *Deque) PushBack(elements ...string) int {
	for _, elem := range elements {
		if d.count == len(d.data) {
			d.grow()
		}
		d.data[d.tail] = elem
		d.tail = (d.tail + 1) % len(d.data)
		d.count++
		d.contentSize += int64(len(elem))
	}
	return d.count
}

// PopFront removes and returns elements from the front
func (d *Deque) PopFront(n int) []string {
	if n <= 0 || d.count == 0 {
		return nil
	}
	if n > d.count {
		n = d.count
	}

	result := make([]string, n)
	for i := 0; i < n; i++ {
		result[i] = d.data[d.head]
		d.contentSize -= int64(len(d.data[d.head]))
		d.data[d.head] = "" // Clear for GC
		d.head = (d.head + 1) % len(d.data)
		d.count--
	}
	return result
}

// PopBack removes and returns elements from the back
func (d *Deque) PopBack(n int) []string {
	if n <= 0 || d.count == 0 {
		return nil
	}
	if n > d.count {
		n = d.count
	}

	result := make([]string, n)
	for i := 0; i < n; i++ {
		d.tail = (d.tail - 1 + len(d.data)) % len(d.data)
		result[i] = d.data[d.tail]
		d.contentSize -= int64(len(d.data[d.tail]))
		d.data[d.tail] = "" // Clear for GC
		d.count--
	}
	return result
}

// Get returns the element at index (0-based from front)
func (d *Deque) Get(index int) (string, bool) {
	if index < 0 || index >= d.count {
		return "", false
	}
	actualIndex := (d.head + index) % len(d.data)
	return d.data[actualIndex], true
}

// Set sets the element at index
func (d *Deque) Set(index int, value string) bool {
	if index < 0 || index >= d.count {
		return false
	}
	actualIndex := (d.head + index) % len(d.data)
	// Update content size
	d.contentSize -= int64(len(d.data[actualIndex]))
	d.contentSize += int64(len(value))
	d.data[actualIndex] = value
	return true
}

// Range returns elements from start to stop (inclusive, supports negative indices)
func (d *Deque) Range(start, stop int) []string {
	if d.count == 0 {
		return []string{}
	}

	// Normalize negative indices
	if start < 0 {
		start = d.count + start
	}
	if stop < 0 {
		stop = d.count + stop
	}

	// Clamp to valid range
	if start < 0 {
		start = 0
	}
	if stop >= d.count {
		stop = d.count - 1
	}
	if start > stop {
		return []string{}
	}

	result := make([]string, stop-start+1)
	for i := 0; i < len(result); i++ {
		actualIndex := (d.head + start + i) % len(d.data)
		result[i] = d.data[actualIndex]
	}
	return result
}

// ToSlice returns all elements as a slice
func (d *Deque) ToSlice() []string {
	if d.count == 0 {
		return []string{}
	}
	return d.Range(0, d.count-1)
}

// Remove removes count occurrences of element
// count > 0: remove from head to tail
// count < 0: remove from tail to head
// count = 0: remove all occurrences
func (d *Deque) Remove(count int, element string) int {
	if d.count == 0 {
		return 0
	}

	removed := 0
	removedSize := int64(0)
	elementLen := int64(len(element))
	maxRemove := d.count
	if count > 0 {
		maxRemove = count
	} else if count < 0 {
		maxRemove = -count
	}

	// Collect elements to keep
	var keep []string
	if count >= 0 {
		// Forward scan
		for i := 0; i < d.count; i++ {
			idx := (d.head + i) % len(d.data)
			if d.data[idx] == element && (count == 0 || removed < maxRemove) {
				removed++
				removedSize += elementLen
			} else {
				keep = append(keep, d.data[idx])
			}
		}
	} else {
		// Backward scan for negative count
		temp := make([]string, d.count)
		removeFlags := make([]bool, d.count)

		for i := 0; i < d.count; i++ {
			idx := (d.head + i) % len(d.data)
			temp[i] = d.data[idx]
		}

		// Mark elements to remove from back
		for i := d.count - 1; i >= 0 && removed < maxRemove; i-- {
			if temp[i] == element {
				removeFlags[i] = true
				removed++
				removedSize += elementLen
			}
		}

		// Collect non-removed elements
		for i := 0; i < d.count; i++ {
			if !removeFlags[i] {
				keep = append(keep, temp[i])
			}
		}
	}

	// Rebuild deque with kept elements
	if removed > 0 {
		d.head = 0
		d.tail = len(keep)
		d.count = len(keep)
		d.contentSize -= removedSize

		// Reuse buffer if possible
		if len(keep) <= len(d.data) {
			for i := range d.data {
				if i < len(keep) {
					d.data[i] = keep[i]
				} else {
					d.data[i] = "" // Clear for GC
				}
			}
		} else {
			d.data = make([]string, len(keep)*2)
			copy(d.data, keep)
		}
	}

	return removed
}

// Trim keeps only elements from start to stop (inclusive)
func (d *Deque) Trim(start, stop int) {
	if d.count == 0 {
		return
	}

	// Normalize negative indices
	if start < 0 {
		start = d.count + start
	}
	if stop < 0 {
		stop = d.count + stop
	}

	// Handle out-of-range cases
	if start < 0 {
		start = 0
	}
	if stop >= d.count {
		stop = d.count - 1
	}
	if start > stop || start >= d.count {
		// Clear the deque
		d.head = 0
		d.tail = 0
		d.count = 0
		d.contentSize = 0
		// Clear data for GC
		for i := range d.data {
			d.data[i] = ""
		}
		return
	}

	// Extract elements to keep
	kept := d.Range(start, stop)

	// Calculate new content size
	var newContentSize int64
	for _, elem := range kept {
		newContentSize += int64(len(elem))
	}

	// Reset deque with kept elements
	d.head = 0
	d.tail = len(kept)
	d.count = len(kept)
	d.contentSize = newContentSize

	// Clear and copy
	for i := range d.data {
		if i < len(kept) {
			d.data[i] = kept[i]
		} else {
			d.data[i] = ""
		}
	}
}

// Contains checks if element exists in deque
func (d *Deque) Contains(element string) bool {
	for i := 0; i < d.count; i++ {
		idx := (d.head + i) % len(d.data)
		if d.data[idx] == element {
			return true
		}
	}
	return false
}

// Clear removes all elements
func (d *Deque) Clear() {
	for i := range d.data {
		d.data[i] = ""
	}
	d.head = 0
	d.tail = 0
	d.count = 0
	d.contentSize = 0
}

// MemoryUsage returns approximate memory usage in bytes (O(1) with cached content size)
func (d *Deque) MemoryUsage() int64 {
	// Base struct size + slice header + data
	var size int64 = 56 // approximate struct overhead (with contentSize field)

	// Capacity * pointer size for slice
	size += int64(len(d.data)) * 16 // string header size

	// Actual string content (cached for O(1) access)
	size += d.contentSize

	return size
}
