package store

import (
	"sync/atomic"

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
