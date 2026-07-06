package store

import (
	"sync/atomic"
	"unsafe"

	pkgErrors "reservoir/pkg/errors"
	"reservoir/pkg/types"
)

// lwwHash is a last-write-wins map CRDT: every field independently keeps the
// stamp of its most recent write, which is either a set (a value) or a delete (a
// tombstone). A field is present iff its winning write is a set. Because each
// field resolves by the same total order used for string keys, applying the same
// field writes in any order on any node converges to the same map without a
// coordinator.
type lwwHash struct {
	fields map[string]hashField
}

// hashField is one field's winning write: its stamp, the value (when it is a
// set), and whether that winning write was a delete.
type hashField struct {
	stamp   hlcStamp
	value   string
	deleted bool
}

func newLWWHash() *lwwHash {
	return &lwwHash{fields: make(map[string]hashField)}
}

// set records a set of field=value at stamp st, winning only if st is strictly
// newer than the field's current stamp. Returns whether it was applied.
func (h *lwwHash) set(field, value string, st hlcStamp) bool {
	if cur, ok := h.fields[field]; !ok || st.After(cur.stamp) {
		h.fields[field] = hashField{stamp: st, value: value}
		return true
	}
	return false
}

// del records a delete of field at stamp st as a tombstone, winning only if st
// is strictly newer than the field's current stamp. A delete of an unseen field
// still records a tombstone, so a concurrent older set cannot resurrect it.
// Returns whether it was applied.
func (h *lwwHash) del(field string, st hlcStamp) bool {
	if cur, ok := h.fields[field]; !ok || st.After(cur.stamp) {
		h.fields[field] = hashField{stamp: st, deleted: true}
		return true
	}
	return false
}

// get returns the field's value if its winning write is a set.
func (h *lwwHash) get(field string) (string, bool) {
	f, ok := h.fields[field]
	if !ok || f.deleted {
		return "", false
	}
	return f.value, true
}

// --- HashValue integration: materialized fields backed by the CRDT ---

// ensureCRDT lazily creates the per-field CRDT, seeding any pre-existing plain
// fields at the zero stamp so a later stamped op can supersede them. The caller
// must hold hv.mu.
func (hv *HashValue) ensureCRDT() {
	if hv.crdt != nil {
		return
	}
	hv.crdt = newLWWHash()
	for field, value := range hv.Fields {
		hv.crdt.fields[field] = hashField{value: value}
	}
}

// syncField reconciles the materialized Fields[field] with the CRDT. It returns
// whether the field was present before and whether it is present after. The
// caller must hold hv.mu.
func (hv *HashValue) syncField(field string) (was, is bool) {
	_, was = hv.Fields[field]
	if val, ok := hv.crdt.get(field); ok {
		hv.Fields[field] = val
		return was, true
	}
	delete(hv.Fields, field)
	return was, false
}

// SetFieldWithStamp records a stamped set of field=value and returns whether the
// field was newly created (absent before, present after) — the count HSET
// reports.
func (hv *HashValue) SetFieldWithStamp(field, value string, st hlcStamp) bool {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	hv.ensureCRDT()
	hv.crdt.set(field, value, st)
	was, is := hv.syncField(field)
	return !was && is
}

// DelFieldWithStamp records a stamped delete of field (a tombstone retained so a
// concurrent older set cannot resurrect it) and returns whether the field was
// present and is now removed.
func (hv *HashValue) DelFieldWithStamp(field string, st hlcStamp) bool {
	hv.mu.Lock()
	defer hv.mu.Unlock()
	hv.ensureCRDT()
	hv.crdt.del(field, st)
	was, is := hv.syncField(field)
	return was && !is
}

// appendLWWDigest appends this hash's per-field LWW state to out as digest
// entries under key: each present field with its value and add stamp, each
// tombstoned field with its remove stamp (Deleted=true). A hash without a CRDT
// (only ever touched by the plain path) reports its fields at the zero stamp.
func (hv *HashValue) appendLWWDigest(key string, out []LWWShardEntry) []LWWShardEntry {
	hv.mu.RLock()
	defer hv.mu.RUnlock()

	if hv.crdt == nil {
		for f, val := range hv.Fields {
			out = append(out, LWWShardEntry{Key: key, Hash: true, Member: f, Value: val})
		}
		return out
	}

	for f, hf := range hv.crdt.fields {
		e := LWWShardEntry{
			Key: key, Hash: true, Member: f,
			Physical: hf.stamp.TS.Physical, Logical: hf.stamp.TS.Logical, Origin: hf.stamp.Origin,
			Deleted: hf.deleted,
		}
		if !hf.deleted {
			e.Value = hf.value
		}
		out = append(out, e)
	}
	return out
}

// gcFieldTombstones drops field tombstones whose stamp predates cutoffNanos.
// Live fields (a set is the winning write) are always kept. It returns how many
// tombstones were pruned and whether the hash is now fully empty — no live
// fields and no tombstones — so the caller can reclaim the key. The caller holds
// the shard lock; this takes hv.mu.
func (hv *HashValue) gcFieldTombstones(cutoffNanos int64) (pruned int, empty bool) {
	hv.mu.Lock()
	defer hv.mu.Unlock()

	if hv.crdt == nil {
		return 0, len(hv.Fields) == 0
	}

	for f, hf := range hv.crdt.fields {
		if !hf.deleted {
			continue // live field
		}
		if hf.stamp.TS.Physical < cutoffNanos {
			delete(hv.crdt.fields, f)
			pruned++
		}
	}

	empty = len(hv.Fields) == 0 && len(hv.crdt.fields) == 0
	return pruned, empty
}

// --- Store-level last-write-wins hash operations ---

// HSetLWW is the convergent entry point for HSET/HMSET, used by the cluster path
// (local writes stamped from the node's HLC, replicated writes stamped from the
// event). Each field=value pair is applied at the given stamp; a pair superseded
// by a newer write to the same field stays as-is. Returns the number of fields
// newly created.
func (s *LockFreeStore) HSetLWW(key string, physical int64, logical uint32, origin uint64, fieldValues ...string) (int64, error) {
	if len(fieldValues)%2 != 0 {
		return 0, pkgErrors.ErrWrongType
	}
	if len(fieldValues) == 0 {
		return 0, nil
	}
	stamp := hlcStamp{TS: types.HLCTimestamp{Physical: physical, Logical: logical}, Origin: origin}

	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, ok := shard.data[key]
	var hashValue *HashValue
	existed := false
	if ok {
		if !storedValue.IsHash() {
			return 0, pkgErrors.ErrWrongType
		}
		hashValue, _ = storedValue.AsHash()
		existed = true
	} else {
		hashValue = NewHashValue()
	}

	added := int64(0)
	for i := 0; i < len(fieldValues); i += 2 {
		if hashValue.SetFieldWithStamp(fieldValues[i], fieldValues[i+1], stamp) {
			added++
		}
	}

	s.storeHash(shard, key, storedValue, hashValue, existed)
	s.writeHashCommitLog("HSET", key, fieldValues)
	return added, nil
}

// HDelLWW is the convergent entry point for HDEL. Unlike the plain HDel, a delete
// of a field on a key the node has not seen still records a tombstone (creating
// an otherwise-empty hash), and a hash left materially empty is retained to keep
// its field tombstones. Returns the number of fields that were present and are
// now removed.
func (s *LockFreeStore) HDelLWW(key string, physical int64, logical uint32, origin uint64, fields ...string) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	stamp := hlcStamp{TS: types.HLCTimestamp{Physical: physical, Logical: logical}, Origin: origin}

	shard := s.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	storedValue, ok := shard.data[key]
	var hashValue *HashValue
	existed := false
	if ok {
		if !storedValue.IsHash() {
			return 0, pkgErrors.ErrWrongType
		}
		hashValue, _ = storedValue.AsHash()
		existed = true
	} else {
		hashValue = NewHashValue()
	}

	removed := int64(0)
	for _, f := range fields {
		if hashValue.DelFieldWithStamp(f, stamp) {
			removed++
		}
	}

	s.storeHash(shard, key, storedValue, hashValue, existed)
	s.writeHashCommitLog("HDEL", key, fields)
	return removed, nil
}

// storeHash inserts a newly-created hash (accounting for a new key) or updates
// the memory accounting of an existing one in place. The caller holds the shard
// lock.
func (s *LockFreeStore) storeHash(shard *LockFreeShard, key string, old *StoredValue, hashValue *HashValue, existed bool) {
	if !existed {
		storedHash := NewStoredHash()
		storedHash.HashVal = hashValue
		memoryDelta := storedHash.MemoryUsage()
		shard.data[key] = storedHash
		atomic.AddInt64(&shard.keyCount, 1)
		atomic.AddInt64(&s.totalKeys, 1)
		atomic.AddInt64(&shard.memoryUsage, memoryDelta)
		atomic.AddInt64(&s.totalMemory, memoryDelta)
		return
	}
	oldSize := old.memorySize - int64(unsafe.Sizeof(*old))
	newSize := hashValue.MemoryUsage()
	memoryDelta := newSize - oldSize
	old.memorySize = int64(unsafe.Sizeof(*old)) + newSize
	atomic.AddInt64(&shard.memoryUsage, memoryDelta)
	atomic.AddInt64(&s.totalMemory, memoryDelta)
}

// writeHashCommitLog appends a hash mutation to the commit log using the
// null-separated "key\x00a\x00b..." encoding the replication layer uses, when
// the log is active and recovery is not replaying.
func (s *LockFreeStore) writeHashCommitLog(op, key string, args []string) {
	if atomic.LoadUint32(&s.commitLogActive) != 1 || atomic.LoadUint32(&s.recoveryActive) != 0 {
		return
	}
	cl, ok := s.commitLog.(CommitLogger)
	if !ok {
		return
	}
	payload := key
	for _, a := range args {
		payload += "\x00" + a
	}
	cl.Write(op, []byte(key), []byte(payload))
}
