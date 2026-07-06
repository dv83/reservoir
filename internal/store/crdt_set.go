package store

import (
	"sync/atomic"
	"unsafe"

	pkgErrors "reservoir/pkg/errors"
	"reservoir/pkg/types"
)

// lwwSet is a last-write-wins element set CRDT: each element carries the stamp
// of its most recent add and its most recent remove. An element is a member iff
// it has an add that is not superseded by a strictly-newer remove ("add-wins"
// bias on the exact-tie boundary). Applying the same adds/removes in any order
// on any node yields the same membership, so replicas converge without a
// coordinator.
type lwwSet struct {
	adds    map[string]hlcStamp
	removes map[string]hlcStamp
}

func newLWWSet() *lwwSet {
	return &lwwSet{
		adds:    make(map[string]hlcStamp),
		removes: make(map[string]hlcStamp),
	}
}

// add records an add of elem at stamp st, keeping only the newest add stamp.
// Returns whether elem became (or stayed) a member as a result.
func (s *lwwSet) add(elem string, st hlcStamp) {
	if cur, ok := s.adds[elem]; !ok || st.After(cur) {
		s.adds[elem] = st
	}
}

// remove records a remove of elem at stamp st, keeping only the newest remove.
func (s *lwwSet) remove(elem string, st hlcStamp) {
	if cur, ok := s.removes[elem]; !ok || st.After(cur) {
		s.removes[elem] = st
	}
}

// contains reports current membership under the add-wins rule: present iff an
// add exists and no remove is strictly newer than it.
func (s *lwwSet) contains(elem string) bool {
	a, ok := s.adds[elem]
	if !ok {
		return false
	}
	if r, rok := s.removes[elem]; rok && r.After(a) {
		return false
	}
	return true
}

// members returns the current member elements.
func (s *lwwSet) members() []string {
	out := make([]string, 0, len(s.adds))
	for elem := range s.adds {
		if s.contains(elem) {
			out = append(out, elem)
		}
	}
	return out
}

// card returns the number of current members.
func (s *lwwSet) card() int {
	n := 0
	for elem := range s.adds {
		if s.contains(elem) {
			n++
		}
	}
	return n
}

// --- SetValue integration: materialized membership backed by the CRDT ---

// ensureCRDT lazily creates the element-stamp CRDT, seeding any pre-existing
// plain members at the zero stamp so a later stamped op can supersede them.
// The caller must hold sv.mu.
func (sv *SetValue) ensureCRDT() {
	if sv.crdt != nil {
		return
	}
	sv.crdt = newLWWSet()
	for elem := range sv.Elements {
		sv.crdt.adds[elem] = hlcStamp{}
	}
}

// syncMember reconciles the materialized membership of elem with the CRDT,
// adjusting totalSize. It returns true if membership flipped. The caller must
// hold sv.mu.
func (sv *SetValue) syncMember(elem string) bool {
	_, present := sv.Elements[elem]
	want := sv.crdt.contains(elem)
	switch {
	case want && !present:
		sv.Elements[elem] = struct{}{}
		sv.totalSize++
		return true
	case !want && present:
		delete(sv.Elements, elem)
		sv.totalSize--
		return true
	}
	return false
}

// AddWithStamp records a stamped add of elem and returns whether it (newly)
// became a member. Applying the same stamped adds/removes in any order on any
// replica converges to the same membership.
func (sv *SetValue) AddWithStamp(elem string, st hlcStamp) bool {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	sv.ensureCRDT()
	sv.crdt.add(elem, st)
	return sv.syncMember(elem)
}

// RemoveWithStamp records a stamped remove of elem and returns whether it was a
// member and is now removed. The remove stamp is retained as a per-element
// tombstone so a concurrent older add cannot resurrect the element.
func (sv *SetValue) RemoveWithStamp(elem string, st hlcStamp) bool {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	sv.ensureCRDT()
	sv.crdt.remove(elem, st)
	return sv.syncMember(elem)
}

// appendLWWDigest appends this set's per-element LWW winners to out as digest
// entries under setKey: a present member carries its add stamp (Deleted=false),
// a tombstoned element carries its remove stamp (Deleted=true). Sending only the
// winner per element is enough for anti-entropy to converge, since each pull
// propagates the stamp that would win on the peer too. A set without a CRDT
// (only ever touched by the plain path) reports its members at the zero stamp.
func (sv *SetValue) appendLWWDigest(setKey string, out []LWWShardEntry) []LWWShardEntry {
	sv.mu.RLock()
	defer sv.mu.RUnlock()

	if sv.crdt == nil {
		for elem := range sv.Elements {
			out = append(out, LWWShardEntry{Key: setKey, Member: elem})
		}
		return out
	}

	emit := func(elem string) LWWShardEntry {
		if sv.crdt.contains(elem) {
			st := sv.crdt.adds[elem]
			return LWWShardEntry{Key: setKey, Member: elem, Physical: st.TS.Physical, Logical: st.TS.Logical, Origin: st.Origin}
		}
		st := sv.crdt.removes[elem]
		return LWWShardEntry{Key: setKey, Member: elem, Physical: st.TS.Physical, Logical: st.TS.Logical, Origin: st.Origin, Deleted: true}
	}

	for elem := range sv.crdt.adds {
		out = append(out, emit(elem))
	}
	for elem := range sv.crdt.removes {
		if _, isAdd := sv.crdt.adds[elem]; isAdd {
			continue // already emitted from the adds set
		}
		out = append(out, emit(elem))
	}
	return out
}

// gcElementTombstones drops remove tombstones whose stamp predates cutoffNanos,
// along with the matching (older, superseded) add, so a forgotten element cannot
// half-resurrect. Live members (add wins) are always kept. It returns how many
// tombstones were pruned and whether the set is now fully empty — no members and
// no stamps — so the caller can reclaim the key. The caller must hold the shard
// lock; this takes sv.mu.
func (sv *SetValue) gcElementTombstones(cutoffNanos int64) (pruned int, empty bool) {
	sv.mu.Lock()
	defer sv.mu.Unlock()

	if sv.crdt == nil {
		return 0, len(sv.Elements) == 0
	}

	for elem, rst := range sv.crdt.removes {
		if sv.crdt.contains(elem) {
			continue // add wins; element is live
		}
		if rst.TS.Physical < cutoffNanos {
			delete(sv.crdt.removes, elem)
			delete(sv.crdt.adds, elem)
			pruned++
		}
	}

	empty = len(sv.Elements) == 0 && len(sv.crdt.adds) == 0 && len(sv.crdt.removes) == 0
	return pruned, empty
}

// --- Store-level last-write-wins set operations ---

// SAddLWW is the convergent entry point for SADD, used by the cluster path
// (local writes stamped from the node's HLC, replicated writes stamped from the
// event). Each member is added at the given stamp; members already superseded by
// a newer remove stay absent. Returns the number of members that became present.
func (s *LockFreeStore) SAddLWW(key string, physical int64, logical uint32, origin uint64, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	stamp := hlcStamp{TS: types.HLCTimestamp{Physical: physical, Logical: logical}, Origin: origin}

	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, ok := shard.data[key]
	var setValue *SetValue
	existed := false
	if ok {
		if !storedValue.IsSet() {
			return 0, pkgErrors.ErrWrongType
		}
		setValue, _ = storedValue.AsSet()
		existed = true
	} else {
		setValue = NewSetValue()
	}

	added := int64(0)
	for _, m := range members {
		if setValue.AddWithStamp(m, stamp) {
			added++
		}
	}

	if !existed {
		storedSet := NewStoredSet()
		storedSet.SetVal = setValue
		memoryDelta := storedSet.MemoryUsage()
		if atomic.LoadInt64(&s.totalMemory)+memoryDelta > s.limits.MaxMemoryUsage {
			return 0, pkgErrors.NewMemoryLimit(atomic.LoadInt64(&s.totalMemory)+memoryDelta, s.limits.MaxMemoryUsage)
		}
		shard.data[key] = storedSet
		atomic.AddInt64(&shard.keyCount, 1)
		atomic.AddInt64(&s.totalKeys, 1)
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)
	} else {
		oldSize := storedValue.memorySize - int64(unsafe.Sizeof(*storedValue))
		newSize := setValue.MemoryUsage()
		memoryDelta := newSize - oldSize
		storedValue.memorySize = int64(unsafe.Sizeof(*storedValue)) + newSize
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)
	}

	s.writeSetCommitLog("SADD", key, members)
	return added, nil
}

// SRemLWW is the convergent entry point for SREM. Unlike the plain SRem, a
// remove of a member on a key the node has not seen still records a tombstone
// (creating an otherwise-empty set), so a later out-of-order add cannot win; and
// a set left materially empty is retained to keep its element tombstones.
// Returns the number of members that were present and are now removed.
func (s *LockFreeStore) SRemLWW(key string, physical int64, logical uint32, origin uint64, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	stamp := hlcStamp{TS: types.HLCTimestamp{Physical: physical, Logical: logical}, Origin: origin}

	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, ok := shard.data[key]
	var setValue *SetValue
	existed := false
	if ok {
		if !storedValue.IsSet() {
			return 0, pkgErrors.ErrWrongType
		}
		setValue, _ = storedValue.AsSet()
		existed = true
	} else {
		setValue = NewSetValue()
	}

	removed := int64(0)
	for _, m := range members {
		if setValue.RemoveWithStamp(m, stamp) {
			removed++
		}
	}

	if !existed {
		storedSet := NewStoredSet()
		storedSet.SetVal = setValue
		memoryDelta := storedSet.MemoryUsage()
		if atomic.LoadInt64(&s.totalMemory)+memoryDelta > s.limits.MaxMemoryUsage {
			return 0, pkgErrors.NewMemoryLimit(atomic.LoadInt64(&s.totalMemory)+memoryDelta, s.limits.MaxMemoryUsage)
		}
		shard.data[key] = storedSet
		atomic.AddInt64(&shard.keyCount, 1)
		atomic.AddInt64(&s.totalKeys, 1)
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)
	} else {
		oldSize := storedValue.memorySize - int64(unsafe.Sizeof(*storedValue))
		newSize := setValue.MemoryUsage()
		memoryDelta := newSize - oldSize
		storedValue.memorySize = int64(unsafe.Sizeof(*storedValue)) + newSize
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)
	}

	s.writeSetCommitLog("SREM", key, members)
	return removed, nil
}

// writeSetCommitLog appends a set mutation to the commit log using the same
// null-separated "key\x00member..." encoding the replication layer uses, when
// the log is active and recovery is not replaying.
func (s *LockFreeStore) writeSetCommitLog(op, key string, members []string) {
	if atomic.LoadUint32(&s.commitLogActive) != 1 || atomic.LoadUint32(&s.recoveryActive) != 0 {
		return
	}
	cl, ok := s.commitLog.(CommitLogger)
	if !ok {
		return
	}
	payload := key
	for _, m := range members {
		payload += "\x00" + m
	}
	cl.Write(op, []byte(key), []byte(payload))
}
