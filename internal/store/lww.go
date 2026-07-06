package store

import (
	"sync/atomic"
	"time"

	pkgErrors "reservoir/pkg/errors"
	"reservoir/pkg/types"
)

// hlcStamp is the last-write-wins version attached to a stored value: a hybrid
// logical clock timestamp plus the originating node's id. The node id breaks
// ties on an identical timestamp so every node picks the same winner for two
// concurrent writes — without it, an exact HLC tie could resolve differently on
// different nodes and diverge.
type hlcStamp struct {
	TS     types.HLCTimestamp
	Origin uint64
}

// After reports whether a wins over b under the LWW total order (timestamp,
// then origin node id).
func (a hlcStamp) After(b hlcStamp) bool {
	if c := a.TS.Compare(b.TS); c != 0 {
		return c > 0
	}
	return a.Origin > b.Origin
}

// SetWithHLC performs a last-write-wins string set: the value is applied only if
// its (timestamp, origin) stamp is strictly newer than the stamp already stored
// for the key. It returns whether the write was applied (false = a newer write
// already won, so this one is dropped). The same call serves a local write
// (stamp from the local HLC, which is always ahead so it applies) and a
// replicated write (stamp from the remote event, applied conditionally), so both
// nodes converge on the same value regardless of delivery order.
func (s *LockFreeStore) SetWithHLC(key, value string, stamp hlcStamp) (bool, error) {
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
	newSV := &StoredValue{
		Type:       ValueTypeString,
		StringVal:  valueCopy,
		memorySize: keyLen + valueLen,
		hlc:        stamp,
	}

	shard := s.shards[int(FastHash(key)&s.shardMask)]

	shard.mu.Lock()
	oldValue, existed := shard.data[keyCopy]

	// LWW guard: drop the write if the stored value's stamp is newer or equal.
	if existed && oldValue != nil && !stamp.After(oldValue.hlc) {
		shard.mu.Unlock()
		return false, nil
	}
	// Also drop it if the key was deleted more recently (a tombstone that is
	// newer than this write), so a late older create cannot resurrect the key.
	if tomb, ok := shard.tombstones[keyCopy]; ok && !stamp.After(tomb.stamp) {
		shard.mu.Unlock()
		return false, nil
	}

	var memoryDelta int64
	if existed && oldValue != nil {
		memoryDelta = newSV.memorySize - oldValue.memorySize
	} else {
		memoryDelta = newSV.memorySize
	}
	if s.wouldExceedMemory(memoryDelta) {
		shard.mu.Unlock()
		return false, pkgErrors.NewMemoryLimit(atomic.LoadInt64(&s.totalMemory)+memoryDelta, s.limits.MaxMemoryUsage)
	}
	shard.data[keyCopy] = newSV
	// This write supersedes any tombstone for the key.
	delete(shard.tombstones, keyCopy)

	// A plain SET clears any previous TTL (Redis semantics), under the same lock.
	if existed {
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

	if atomic.LoadUint32(&s.commitLogActive) == 1 && atomic.LoadUint32(&s.recoveryActive) == 0 {
		if cl, ok := s.commitLog.(CommitLogger); ok {
			cl.Write("SET", []byte(key), []byte(value))
		}
	}

	return true, nil
}

// SetLWW is the exported entry point for a last-write-wins string set, used by
// the replication layer (which cannot construct the unexported stamp). physical
// and logical are an HLC timestamp; origin is the writing node's id. Returns
// whether the write was applied.
func (s *LockFreeStore) SetLWW(key, value string, physical int64, logical uint32, origin uint64) (bool, error) {
	return s.SetWithHLC(key, value, hlcStamp{TS: types.HLCTimestamp{Physical: physical, Logical: logical}, Origin: origin})
}

// DeleteWithHLC performs a last-write-wins delete: it removes the key and records
// a tombstone at the given stamp, unless the stored value is strictly newer (in
// which case the delete is stale and dropped). The tombstone lets a later,
// older create be rejected instead of resurrecting the key. Returns whether the
// delete was applied.
func (s *LockFreeStore) DeleteWithHLC(key string, stamp hlcStamp) bool {
	shard := s.shards[int(FastHash(key)&s.shardMask)]

	shard.mu.Lock()
	oldValue, existed := shard.data[key]

	// Drop the delete if a newer write already won.
	if existed && oldValue != nil && !stamp.After(oldValue.hlc) {
		shard.mu.Unlock()
		return false
	}
	// Drop it if an equal/newer tombstone already exists.
	if tomb, ok := shard.tombstones[key]; ok && !stamp.After(tomb.stamp) {
		shard.mu.Unlock()
		return false
	}

	if existed {
		delete(shard.data, key)
		shard.expiryMu.Lock()
		delete(shard.expiry, key)
		shard.expiryMu.Unlock()
	}
	shard.tombstones[key] = tombstone{stamp: stamp, deleted: time.Now()}
	shard.mu.Unlock()

	if existed {
		var freed int64
		if oldValue != nil {
			freed = oldValue.MemoryUsage()
		}
		s.accountKeyRemoved(shard, freed)
		if atomic.LoadUint32(&s.commitLogActive) == 1 && atomic.LoadUint32(&s.recoveryActive) == 0 {
			if cl, ok := s.commitLog.(CommitLogger); ok {
				cl.Delete([]byte(key))
			}
		}
	}
	return true
}

// DeleteLWW is the exported entry point for a last-write-wins delete.
func (s *LockFreeStore) DeleteLWW(key string, physical int64, logical uint32, origin uint64) bool {
	return s.DeleteWithHLC(key, hlcStamp{TS: types.HLCTimestamp{Physical: physical, Logical: logical}, Origin: origin})
}

// gcTombstones removes tombstones older than the retention window. Tombstones
// only need to outlive the delivery of any concurrent older write; a generous
// window bounds memory without risking resurrection under normal delivery. It
// prunes string-key tombstones as well as set element, hash field, and list
// element tombstones, and reclaims a CRDT container once all its tombstones have
// aged out (so an emptied set, hash, or list eventually disappears from the
// keyspace).
func (s *LockFreeStore) gcTombstones(retain time.Duration) int {
	cutoff := time.Now().Add(-retain)
	cutoffNanos := cutoff.UnixNano()
	removed := 0
	for i := 0; i < numShards; i++ {
		shard := s.shards[i]
		shard.mu.Lock()
		for k, t := range shard.tombstones {
			if t.deleted.Before(cutoff) {
				delete(shard.tombstones, k)
				removed++
			}
		}
		for k, v := range shard.data {
			if v == nil {
				continue
			}
			var pruned int
			var empty bool
			switch v.Type {
			case ValueTypeSet:
				if v.SetVal == nil {
					continue
				}
				pruned, empty = v.SetVal.gcElementTombstones(cutoffNanos)
			case ValueTypeHash:
				if v.HashVal == nil {
					continue
				}
				pruned, empty = v.HashVal.gcFieldTombstones(cutoffNanos)
			case ValueTypeList:
				if v.ListVal == nil || v.ListVal.rga == nil {
					continue
				}
				pruned, empty = v.ListVal.rga.gcTombstones(cutoffNanos)
			default:
				continue
			}
			if empty {
				freed := v.MemoryUsage()
				delete(shard.data, k)
				atomic.AddInt64(&shard.keyCount, -1)
				atomic.AddInt64(&s.totalKeys, -1)
				atomic.AddInt64(&shard.memoryUsage, -freed)
				atomic.AddInt64(&s.totalMemory, -freed)
				removed++
			} else if pruned > 0 {
				old := v.memorySize
				v.memorySize = 0
				delta := v.MemoryUsage() - old
				atomic.AddInt64(&shard.memoryUsage, delta)
				atomic.AddInt64(&s.totalMemory, delta)
			}
		}
		shard.mu.Unlock()
	}
	return removed
}

// GetLWW returns the HLC stamp recorded for a key as raw fields, for anti-entropy
// digests. ok is false if the key does not exist.
func (s *LockFreeStore) GetLWW(key string) (physical int64, logical uint32, origin uint64, ok bool) {
	st, exists := s.GetHLC(key)
	if !exists {
		return 0, 0, 0, false
	}
	return st.TS.Physical, st.TS.Logical, st.Origin, true
}

// LWWShardEntry is one LWW-stamped fact exchanged during anti-entropy. It is
// either a string key's state (Member=="") — a value or a tombstone
// (Deleted=true) — or a single set element (Member!="") that is currently
// present (Deleted=false, stamp is its add) or tombstoned (Deleted=true, stamp
// is its remove).
type LWWShardEntry struct {
	Key      string
	Member   string
	Value    string
	Physical int64
	Logical  uint32
	Origin   uint64
	Deleted  bool
	// Counter, when non-nil, means this entry is a PN-counter key: its state is
	// reconciled by max-merge rather than by the LWW stamp.
	Counter *CounterState
	// Hash marks a hash field entry: Member is the field, Value its value
	// (empty when Deleted), reconciled by the per-field LWW stamp.
	Hash bool
	// List, when non-nil, means this entry is a list key: the full RGA element
	// set, reconciled by merging the delta (idempotent inserts and tombstones).
	List *ListDelta
}

// ShardCount returns the number of shards, so callers can iterate them.
func (s *LockFreeStore) ShardCount() int { return numShards }

// ShardLWWDigest returns the LWW state of one shard — string values and
// tombstones with their stamps — for anti-entropy reconciliation. Non-string
// values are skipped (LWW currently covers string keys).
func (s *LockFreeStore) ShardLWWDigest(shardIdx int) []LWWShardEntry {
	if shardIdx < 0 || shardIdx >= numShards {
		return nil
	}
	shard := s.shards[shardIdx]
	shard.mu.RLock()
	defer shard.mu.RUnlock()

	entries := make([]LWWShardEntry, 0, len(shard.data)+len(shard.tombstones))
	for k, v := range shard.data {
		if v == nil {
			continue
		}
		switch v.Type {
		case ValueTypeString:
			if v.counter != nil {
				// A counter key: its authoritative state is the PN-counter, so
				// emit that (max-merged on the peer) rather than the derived
				// string value.
				c := v.counter.clone()
				entries = append(entries, LWWShardEntry{
					Key:     k,
					Counter: &CounterState{Base: c.Base, P: c.P, N: c.N},
				})
			} else {
				entries = append(entries, LWWShardEntry{
					Key: k, Value: v.StringVal,
					Physical: v.hlc.TS.Physical, Logical: v.hlc.TS.Logical, Origin: v.hlc.Origin,
				})
			}
		case ValueTypeSet:
			if v.SetVal != nil {
				entries = v.SetVal.appendLWWDigest(k, entries)
			}
		case ValueTypeHash:
			if v.HashVal != nil {
				entries = v.HashVal.appendLWWDigest(k, entries)
			}
		case ValueTypeList:
			if v.ListVal != nil {
				if d := v.ListVal.fullDelta(); d != nil {
					entries = append(entries, LWWShardEntry{Key: k, List: d})
				}
			}
		}
	}
	for k, t := range shard.tombstones {
		entries = append(entries, LWWShardEntry{
			Key:      k,
			Physical: t.stamp.TS.Physical, Logical: t.stamp.TS.Logical, Origin: t.stamp.Origin,
			Deleted: true,
		})
	}
	return entries
}

// GetHLC returns the LWW stamp recorded for a key, if it exists.
func (s *LockFreeStore) GetHLC(key string) (hlcStamp, bool) {
	shard := s.shards[int(FastHash(key)&s.shardMask)]
	shard.mu.RLock()
	v, ok := shard.data[key]
	shard.mu.RUnlock()
	if !ok || v == nil {
		return hlcStamp{}, false
	}
	return v.hlc, true
}
