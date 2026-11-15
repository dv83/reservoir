package store

import (
	"math"
	"sync/atomic"
	"time"
	"unsafe"

	"reservoir/pkg/types"
)

// Key resets: DEL, FLUSHALL and expiry of keys of every type.
//
// Every key has a reset point — the newer of its key tombstone and the
// store-wide flush stamp. A replicated write is applied only if it belongs after
// the key's reset point, so a delayed or re-sent older write (anti-entropy
// re-sends whole shards) can never undo a delete:
//
//   - set elements and hash fields carry HLC stamps: an op is applied only if its
//     stamp is after the reset point, and a reset prunes elements at or before
//     it while keeping newer ones;
//   - list elements and PN-counter slots carry no comparable stamps, so each list
//     or counter incarnation records its generation — the reset point it was
//     created after. State of an older generation is dropped; state of a newer
//     generation replaces the local incarnation and teaches this node the delete
//     it missed (the generation is that delete's stamp).
//
// Conflict rule for writes concurrent with a reset: set and hash writes stamped
// after it survive; list pushes and counter increments made on a node that had
// not yet seen it belong to the old generation and are dropped (delete wins).

// resetPoint returns the stamp at or before which any write to key is obsolete.
// The caller holds shard.mu.
func (s *LockFreeStore) resetPoint(shard *LockFreeShard, key string) hlcStamp {
	shard.ensure(key)
	r := s.flushStampNow()
	if tomb, ok := shard.tombstones[key]; ok && tomb.stamp.After(r) {
		r = tomb.stamp
	}
	return r
}

// raiseTombstone records that key was reset at stamp, if that is newer than what
// this node knows. The caller holds shard.mu.
func raiseTombstone(shard *LockFreeShard, key string, stamp hlcStamp) {
	shard.ensure(key)
	if stamp.TS.IsZero() {
		return
	}
	if tomb, ok := shard.tombstones[key]; ok && !stamp.After(tomb.stamp) {
		return
	}
	shard.tombstones[key] = tombstone{stamp: stamp, deleted: time.Now()}
	shard.markDirty(key)
}

func (s *LockFreeStore) flushStampNow() hlcStamp {
	s.flushMu.RLock()
	defer s.flushMu.RUnlock()
	return s.flushStamp
}

// FlushStampLWW returns the newest stamped FLUSHALL this node has applied, for
// the anti-entropy digest; ok is false if there has been none.
func (s *LockFreeStore) FlushStampLWW() (physical int64, logical uint32, origin uint64, ok bool) {
	f := s.flushStampNow()
	return f.TS.Physical, f.TS.Logical, f.Origin, !f.TS.IsZero()
}

// deadlineStamp is the tombstone stamp for a container reclaimed by expiry: it
// covers every write stamped before the deadline and none after it. It derives
// from the replicated TTL deadline, so every node records the same one.
func deadlineStamp(deadline time.Time) hlcStamp {
	return hlcStamp{TS: types.HLCTimestamp{Physical: deadline.UnixNano() - 1, Logical: math.MaxUint32}, Origin: math.MaxUint64}
}

// isResettableContainer reports whether sv's writes do not clear its TTL and are
// reset by generation or element stamp rather than by a value stamp: sets,
// hashes, lists and PN-counters.
func isResettableContainer(sv *StoredValue) bool {
	if sv == nil {
		return false
	}
	switch sv.Type {
	case ValueTypeSet, ValueTypeHash, ValueTypeList:
		return true
	}
	return sv.counter != nil
}

// applyReset applies a reset at stamp r to the value stored under key: it
// removes the key, prunes it (sets and hashes keep elements stamped after r),
// or leaves it (a value written after r). It reports whether the key was
// removed and whether anything of it was kept. The caller holds shard.mu and
// handles commit logging; accounting is done here.
func (s *LockFreeStore) applyReset(shard *LockFreeShard, key string, r hlcStamp) (removed, kept bool) {
	sv, ok := shard.get(key)
	if !ok || sv == nil {
		return false, false
	}
	remove := false
	switch {
	case sv.counter != nil:
		remove = r.After(sv.counter.gen)
	case sv.Type == ValueTypeString:
		remove = !sv.hlc.After(r)
	case sv.Type == ValueTypeList:
		remove = sv.ListVal == nil || sv.ListVal.rga == nil || r.After(sv.ListVal.gen)
	case sv.Type == ValueTypeSet:
		if sv.SetVal == nil || sv.SetVal.pruneUpTo(r) {
			remove = true
		} else {
			s.reaccountContainer(shard, sv, sv.SetVal.MemoryUsage())
			return false, true
		}
	case sv.Type == ValueTypeHash:
		if sv.HashVal == nil || sv.HashVal.pruneUpTo(r) {
			remove = true
		} else {
			s.reaccountContainer(shard, sv, sv.HashVal.MemoryUsage())
			return false, true
		}
	default:
		remove = true
	}
	if !remove {
		return false, true
	}
	freed := sv.MemoryUsage()
	shard.del(key)
	shard.expiryMu.Lock()
	delete(shard.expiry, key)
	shard.expiryMu.Unlock()
	s.accountKeyRemoved(shard, freed)
	return true, false
}

// reaccountContainer updates memory accounting after a container shrank in
// place. The caller holds shard.mu.
func (s *LockFreeStore) reaccountContainer(shard *LockFreeShard, sv *StoredValue, payload int64) {
	header := int64(unsafe.Sizeof(*sv))
	delta := header + payload - sv.memorySize
	sv.memorySize = header + payload
	atomic.AddInt64(&shard.memoryUsage, delta)
	atomic.AddInt64(&s.totalMemory, delta)
}

// pruneUpTo drops every add and remove stamped at or before r and rebuilds the
// materialized members. It reports whether nothing is left (no members and no
// element tombstones), in which case the caller removes the key. A set without
// CRDT state has no stamps and is always fully reset.
func (sv *SetValue) pruneUpTo(r hlcStamp) (empty bool) {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	if sv.crdt == nil {
		return true
	}
	for e, st := range sv.crdt.adds {
		if !st.After(r) {
			sv.crdt.dropAdd(e)
		}
	}
	for e, st := range sv.crdt.removes {
		if !st.After(r) {
			sv.crdt.dropRemove(e)
		}
	}
	sv.resetElems(len(sv.crdt.adds))
	for e := range sv.crdt.adds {
		if sv.crdt.contains(e) {
			sv.putElem(e)
			sv.totalSize++
		}
	}
	return len(sv.crdt.adds) == 0 && len(sv.crdt.removes) == 0
}

// pruneUpTo drops every field write stamped at or before r and rebuilds the
// materialized fields; see SetValue.pruneUpTo.
func (hv *HashValue) pruneUpTo(r hlcStamp) (empty bool) {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	if hv.crdt == nil {
		return true
	}
	for f, fld := range hv.crdt.fields {
		if !fld.stamp.After(r) {
			hv.crdt.dropField(f)
		}
	}
	hv.resetFields(len(hv.crdt.fields))
	for f := range hv.crdt.fields {
		if v, ok := hv.crdt.get(f); ok {
			hv.putField(f, v)
		}
	}
	return len(hv.crdt.fields) == 0
}

// FlushLWW applies a stamped FLUSHALL: every key and element written at or
// before the stamp is removed, writes stamped after it are kept, and the stamp
// becomes every key's reset point, so stale re-deliveries of flushed data are
// ignored from now on. Applying an older or equal flush again is a no-op, so
// replicas converge whatever order flushes and writes arrive in.
func (s *LockFreeStore) FlushLWW(physical int64, logical uint32, origin uint64) {
	f := hlcStamp{TS: types.HLCTimestamp{Physical: physical, Logical: logical}, Origin: origin}
	s.flushMu.Lock()
	if !f.After(s.flushStamp) {
		s.flushMu.Unlock()
		return
	}
	s.flushStamp = f
	s.flushMu.Unlock()
	s.persistFlushStamp(f)

	var removedKeys []string
	anyKept := false
	for _, shard := range s.shards {
		shard.mu.Lock()
		// LSM mode: every key on disk is subject to the flush.
		shard.ensureWhere(nil)
		for key := range shard.data {
			removed, kept := s.applyReset(shard, key, f)
			if removed {
				removedKeys = append(removedKeys, key)
			}
			anyKept = anyKept || kept
		}
		// Tombstones and TTL records at or before the flush are covered by it.
		for key, tomb := range shard.tombstones {
			if !tomb.stamp.After(f) {
				shard.ensure(key)
				delete(shard.tombstones, key)
				shard.markDirty(key)
			}
		}
		for key, rec := range shard.ttlRecs {
			if !rec.stamp.After(f) {
				shard.ensure(key)
				delete(shard.ttlRecs, key)
				shard.markDirty(key)
				// A key kept by the flush was written after it; a TTL set
				// before the flush belonged to the flushed incarnation.
				shard.expiryMu.Lock()
				delete(shard.expiry, key)
				shard.expiryMu.Unlock()
			}
		}
		shard.mu.Unlock()
		s.maybeEvict()
	}

	if atomic.LoadUint32(&s.commitLogActive) != 1 || atomic.LoadUint32(&s.recoveryActive) != 0 {
		return
	}
	cl, ok := s.commitLog.(CommitLogger)
	if !ok || cl == nil {
		return
	}
	// One FLUSHALL entry when nothing survived; otherwise per-key deletes, so a
	// replay does not also wipe the writes stamped after the flush.
	if !anyKept {
		cl.Write("FLUSHALL", []byte(""), []byte(""))
		return
	}
	for _, key := range removedKeys {
		cl.Delete([]byte(key))
	}
}
