package store

import (
	"fmt"
	"sync/atomic"

	pkgErrors "reservoir/pkg/errors"
	"reservoir/pkg/types"
)

// ListElemDesc is the exported, gob-friendly descriptor of one list element
// exchanged over replication and anti-entropy. Deleted=true is a tombstone
// (only the id matters); otherwise it is an insert or a value update, applied
// through the same last-write-wins path.
type ListElemDesc struct {
	IDP     int64
	IDL     uint32
	IDO     uint64
	Pos     int64
	Value   string
	VP      int64
	VL      uint32
	VO      uint64
	Deleted bool
}

// ListDelta is a batch of element descriptors — the change a list op makes, or a
// full digest for anti-entropy.
type ListDelta struct {
	Elems []ListElemDesc
}

func descFromElem(e rgaElem) ListElemDesc {
	return ListElemDesc{
		IDP: e.id.TS.Physical, IDL: e.id.TS.Logical, IDO: e.id.Origin,
		Pos:   e.pos,
		Value: e.value,
		VP:    e.valStamp.TS.Physical, VL: e.valStamp.TS.Logical, VO: e.valStamp.Origin,
		Deleted: e.deleted,
	}
}

func (d ListElemDesc) id() hlcStamp {
	return hlcStamp{TS: types.HLCTimestamp{Physical: d.IDP, Logical: d.IDL}, Origin: d.IDO}
}

func (d ListElemDesc) valStamp() hlcStamp {
	return hlcStamp{TS: types.HLCTimestamp{Physical: d.VP, Logical: d.VL}, Origin: d.VO}
}

func (d ListElemDesc) elem() rgaElem {
	return rgaElem{id: d.id(), pos: d.Pos, value: d.Value, valStamp: d.valStamp(), deleted: d.Deleted}
}

// nextListStamp mints a unique, monotonically increasing id for a new list
// element on this node.
func (s *LockFreeStore) nextListStamp() hlcStamp {
	seq := atomic.AddUint64(&s.listSeq, 1)
	return hlcStamp{TS: types.HLCTimestamp{Physical: int64(seq)}, Origin: atomic.LoadUint64(&s.localOrigin)}
}

// isListCRDT reports whether the store maintains list CRDTs (cluster mode).
func (s *LockFreeStore) isListCRDT() bool {
	return atomic.LoadUint64(&s.localOrigin) != 0
}

// ensureListRGA returns the list's RGA, creating it (seeded from any existing
// plain deque elements at ascending positions) on first use. The caller holds
// the shard lock.
func (s *LockFreeStore) ensureListRGA(list *ListValue) *rgaList {
	if list.rga != nil {
		return list.rga
	}
	r := newRGAList()
	for _, v := range list.deque.ToSlice() {
		r.localAppend(s.nextListStamp(), v)
	}
	list.rga = r
	return r
}

// rematerialize rebuilds the deque read view from the RGA visible order and
// records the changed descriptors for replication.
func (list *ListValue) rematerialize(changed []rgaElem) {
	list.deque = NewDequeWithElements(list.rga.values())
	for _, e := range changed {
		list.pending = append(list.pending, descFromElem(e))
	}
}

// getListForWrite loads or creates the list at key for a CRDT write. Returns the
// list, its stored value (nil if new), and whether it already existed.
func (s *LockFreeStore) getListForWrite(shard *LockFreeShard, key string) (*ListValue, *StoredValue, bool, error) {
	if sv, ok := shard.data[key]; ok {
		if sv.Type != ValueTypeList {
			return nil, nil, false, pkgErrors.ErrWrongType
		}
		return sv.ListVal, sv, true, nil
	}
	return NewListValue(), nil, false, nil
}

// commitListWrite stores a new list or updates the memory accounting of an
// existing one after a CRDT mutation. The caller holds the shard lock.
func (s *LockFreeStore) commitListWrite(shard *LockFreeShard, key string, list *ListValue, sv *StoredValue, existed bool) {
	if !existed {
		newSV := newStoredList(list)
		mem := newSV.MemoryUsage()
		shard.data[key] = newSV
		s.accountKeyAdded(shard, mem)
		return
	}
	old := sv.memorySize
	sv.memorySize = 0
	delta := sv.MemoryUsage() - old
	atomic.AddInt64(&shard.memoryUsage, delta)
	atomic.AddInt64(&s.totalMemory, delta)
}

// pushCRDT is the shared body of LPush/RPush under the CRDT: prepend=true adds
// to the head, otherwise the tail.
func (s *LockFreeStore) pushCRDT(key string, prepend bool, elements ...string) (int64, error) {
	if len(elements) == 0 {
		return 0, fmt.Errorf("wrong number of arguments for push command")
	}
	if int64(len(key)) > s.limits.MaxKeySize {
		return 0, pkgErrors.NewKeyTooLarge(int64(len(key)), s.limits.MaxKeySize)
	}
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	list, sv, existed, err := s.getListForWrite(shard, key)
	if err != nil {
		return 0, err
	}
	r := s.ensureListRGA(list)

	changed := make([]rgaElem, 0, len(elements))
	for _, el := range elements {
		var e rgaElem
		if prepend {
			e = r.localPrepend(s.nextListStamp(), el)
		} else {
			e = r.localAppend(s.nextListStamp(), el)
		}
		changed = append(changed, e)
	}
	list.rematerialize(changed)
	s.commitListWrite(shard, key, list, sv, existed)
	return int64(r.length()), nil
}

// pushXCRDT pushes only if the list already exists and is non-empty.
func (s *LockFreeStore) pushXCRDT(key string, prepend bool, elements ...string) (int64, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	sv, ok := shard.data[key]
	if !ok {
		return 0, nil
	}
	if sv.Type != ValueTypeList {
		return 0, pkgErrors.ErrWrongType
	}
	list := sv.ListVal
	r := s.ensureListRGA(list)
	if r.length() == 0 {
		return 0, nil
	}
	changed := make([]rgaElem, 0, len(elements))
	for _, el := range elements {
		var e rgaElem
		if prepend {
			e = r.localPrepend(s.nextListStamp(), el)
		} else {
			e = r.localAppend(s.nextListStamp(), el)
		}
		changed = append(changed, e)
	}
	list.rematerialize(changed)
	s.commitListWrite(shard, key, list, sv, true)
	return int64(r.length()), nil
}

// pushUniqueCRDT pushes only the elements not already present (by value).
func (s *LockFreeStore) pushUniqueCRDT(key string, prepend bool, elements ...string) (int64, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	list, sv, existed, err := s.getListForWrite(shard, key)
	if err != nil {
		return 0, err
	}
	r := s.ensureListRGA(list)

	present := make(map[string]struct{})
	for _, v := range r.values() {
		present[v] = struct{}{}
	}
	changed := make([]rgaElem, 0, len(elements))
	for _, el := range elements {
		if _, dup := present[el]; dup {
			continue
		}
		present[el] = struct{}{}
		var e rgaElem
		if prepend {
			e = r.localPrepend(s.nextListStamp(), el)
		} else {
			e = r.localAppend(s.nextListStamp(), el)
		}
		changed = append(changed, e)
	}
	if len(changed) == 0 && !existed {
		// Nothing added and no prior list: don't create an empty key.
		return 0, nil
	}
	list.rematerialize(changed)
	s.commitListWrite(shard, key, list, sv, existed)
	return int64(r.length()), nil
}

// popCRDT removes up to count elements from the head (fromHead) or tail and
// returns their values, tombstoning them in the RGA.
func (s *LockFreeStore) popCRDT(key string, count int, fromHead bool) ([]string, error) {
	if count <= 0 {
		count = 1
	}
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	sv, ok := shard.data[key]
	if !ok {
		return nil, nil
	}
	if sv.Type != ValueTypeList {
		return nil, pkgErrors.ErrWrongType
	}
	list := sv.ListVal
	r := s.ensureListRGA(list)
	vis := r.visible()
	if len(vis) == 0 {
		return nil, nil
	}
	if count > len(vis) {
		count = len(vis)
	}

	popped := make([]string, 0, count)
	changed := make([]rgaElem, 0, count)
	for i := 0; i < count; i++ {
		var e rgaElem
		if fromHead {
			e = vis[i]
		} else {
			e = vis[len(vis)-1-i]
		}
		r.applyDelete(e.id)
		e.deleted = true
		popped = append(popped, e.value)
		changed = append(changed, e)
	}
	list.rematerialize(changed)
	s.finishListDelete(shard, key, list, sv, r)
	return popped, nil
}

// lsetCRDT replaces the value at index under last-write-wins.
func (s *LockFreeStore) lsetCRDT(key string, index int64, element string) error {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	sv, ok := shard.data[key]
	if !ok {
		return fmt.Errorf("no such key")
	}
	if sv.Type != ValueTypeList {
		return pkgErrors.ErrWrongType
	}
	list := sv.ListVal
	r := s.ensureListRGA(list)
	vis := r.visible()
	i := index
	if i < 0 {
		i += int64(len(vis))
	}
	if i < 0 || i >= int64(len(vis)) {
		return pkgErrors.ErrIndexOutOfRange
	}
	target := vis[i]
	stamp := s.nextListStamp()
	r.setValue(target.id, element, stamp)
	target.value, target.valStamp, target.deleted = element, stamp, false
	list.rematerialize([]rgaElem{target})
	s.commitListWrite(shard, key, list, sv, true)
	return nil
}

// lremCRDT removes elements equal to element, honoring the Redis count sign, and
// returns how many were removed.
func (s *LockFreeStore) lremCRDT(key string, count int64, element string) (int64, error) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	sv, ok := shard.data[key]
	if !ok {
		return 0, nil
	}
	if sv.Type != ValueTypeList {
		return 0, pkgErrors.ErrWrongType
	}
	list := sv.ListVal
	r := s.ensureListRGA(list)
	vis := r.visible()

	limit := count
	fromTail := false
	if count < 0 {
		limit = -count
		fromTail = true
	}

	changed := make([]rgaElem, 0)
	removeOne := func(e rgaElem) {
		r.applyDelete(e.id)
		e.deleted = true
		changed = append(changed, e)
	}
	if fromTail {
		for i := len(vis) - 1; i >= 0; i-- {
			if vis[i].value == element {
				removeOne(vis[i])
				if limit > 0 && int64(len(changed)) >= limit {
					break
				}
			}
		}
	} else {
		for i := 0; i < len(vis); i++ {
			if vis[i].value == element {
				removeOne(vis[i])
				if limit > 0 && int64(len(changed)) >= limit {
					break
				}
			}
		}
	}
	if len(changed) == 0 {
		return 0, nil
	}
	list.rematerialize(changed)
	s.finishListDelete(shard, key, list, sv, r)
	return int64(len(changed)), nil
}

// ltrimCRDT keeps only the visible range [start, stop] (Redis semantics),
// tombstoning everything outside it.
func (s *LockFreeStore) ltrimCRDT(key string, start, stop int64) error {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	sv, ok := shard.data[key]
	if !ok {
		return nil
	}
	if sv.Type != ValueTypeList {
		return pkgErrors.ErrWrongType
	}
	list := sv.ListVal
	r := s.ensureListRGA(list)
	vis := r.visible()
	n := int64(len(vis))
	if n == 0 {
		return nil
	}

	// Normalize negative indices.
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}

	changed := make([]rgaElem, 0)
	for i := int64(0); i < n; i++ {
		if start > stop || i < start || i > stop {
			e := vis[i]
			r.applyDelete(e.id)
			e.deleted = true
			changed = append(changed, e)
		}
	}
	if len(changed) == 0 {
		return nil
	}
	list.rematerialize(changed)
	s.finishListDelete(shard, key, list, sv, r)
	return nil
}

// finishListDelete updates accounting after a delete, reclaiming the key only if
// no live elements remain AND no tombstones are held (tombstones are retained so
// a concurrent older insert cannot resurrect an element; GC reclaims later).
func (s *LockFreeStore) finishListDelete(shard *LockFreeShard, key string, list *ListValue, sv *StoredValue, r *rgaList) {
	if r.length() == 0 && len(r.elems) == 0 {
		freed := sv.MemoryUsage()
		delete(shard.data, key)
		atomic.AddInt64(&shard.keyCount, -1)
		atomic.AddInt64(&s.totalKeys, -1)
		atomic.AddInt64(&shard.memoryUsage, -freed)
		atomic.AddInt64(&s.totalMemory, -freed)
		return
	}
	s.commitListWrite(shard, key, list, sv, true)
}

// fullDelta returns every RGA element (live and tombstoned) as a delta, for
// anti-entropy. It returns nil for a list with no CRDT state. The caller holds
// at least the shard read lock.
func (lv *ListValue) fullDelta() *ListDelta {
	if lv.rga == nil {
		return nil
	}
	d := &ListDelta{Elems: make([]ListElemDesc, 0, len(lv.rga.elems))}
	for _, e := range lv.rga.elems {
		d.Elems = append(d.Elems, descFromElem(*e))
	}
	return d
}

// DrainListDelta returns and clears the pending replication descriptors for a
// list key. ok is false if the key is not a CRDT list or has nothing pending.
func (s *LockFreeStore) DrainListDelta(key string) (ListDelta, bool) {
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()
	sv, ok := shard.data[key]
	if !ok || sv.Type != ValueTypeList || sv.ListVal == nil || len(sv.ListVal.pending) == 0 {
		return ListDelta{}, false
	}
	delta := ListDelta{Elems: sv.ListVal.pending}
	sv.ListVal.pending = nil
	return delta, true
}

// ApplyListDelta applies a replicated list delta to the local RGA, materializing
// the result. Inserts/updates go through the LWW insert path and deletes through
// the tombstone path, so application is idempotent and order-independent.
func (s *LockFreeStore) ApplyListDelta(key string, delta ListDelta) error {
	if len(delta.Elems) == 0 {
		return nil
	}
	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	sv, existed := shard.data[key]
	var list *ListValue
	if existed {
		if sv.Type != ValueTypeList {
			return pkgErrors.ErrWrongType
		}
		list = sv.ListVal
	} else {
		list = NewListValue()
	}
	if list.rga == nil {
		list.rga = newRGAList()
	}
	r := list.rga

	for _, d := range delta.Elems {
		if d.Deleted {
			r.applyDelete(d.id())
		} else {
			r.applyInsert(d.elem())
		}
	}
	// Rebuild the read view without re-queuing anything for replication.
	list.deque = NewDequeWithElements(r.values())

	if r.length() == 0 && len(r.elems) == 0 {
		if existed {
			freed := sv.MemoryUsage()
			delete(shard.data, key)
			atomic.AddInt64(&shard.keyCount, -1)
			atomic.AddInt64(&s.totalKeys, -1)
			atomic.AddInt64(&shard.memoryUsage, -freed)
			atomic.AddInt64(&s.totalMemory, -freed)
		}
		return nil
	}
	s.commitListWrite(shard, key, list, sv, existed)
	return nil
}
