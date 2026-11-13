package store

import (
	"strconv"
	"time"

	"reservoir/pkg/types"
)

// ExpireLWW applies a TTL change under last-write-wins. deadline is unix nanos;
// 0 means PERSIST (remove the TTL). The change is dropped if a newer TTL op is
// already recorded for the key, or if the key's current value was written after
// it (that SET cleared the TTL, per Redis semantics). Otherwise it is recorded in
// the key's TTL register — even when the key is absent, so it still takes effect
// when the write it follows arrives out of order — and applied to the key if
// present. Returns whether the key exists and took the TTL change.
func (s *LockFreeStore) ExpireLWW(key string, deadline int64, physical int64, logical uint32, origin uint64) bool {
	stamp := hlcStamp{TS: types.HLCTimestamp{Physical: physical, Logical: logical}, Origin: origin}
	shard := s.getShard(key)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	if rec, ok := shard.ttlRecs[key]; ok && !stamp.After(rec.stamp) {
		return false // a newer TTL op already won
	}
	v, present := shard.data[key]
	if present && v != nil && !v.hlc.TS.IsZero() && !stamp.After(v.hlc) {
		return false // the current value was written after this TTL op
	}

	shard.ttlRecs[key] = ttlRec{stamp: stamp, deadline: deadline, updated: time.Now()}
	if !present {
		return false
	}

	shard.expiryMu.Lock()
	if deadline == 0 {
		delete(shard.expiry, key)
	} else {
		shard.expiry[key] = time.Unix(0, deadline)
	}
	shard.expiryMu.Unlock()

	if deadline == 0 {
		s.logToCommitLog("PERSIST", []byte(key), nil)
	} else {
		s.logToCommitLog("EXPIRE", []byte(key), []byte(strconv.FormatInt(deadline, 10)))
	}
	return true
}

// applyTTLAfterSet resolves a key's TTL after an LWW SET at stamp. A TTL op newer
// than the SET stays in force and is re-applied to the new value; otherwise the
// SET clears the TTL (Redis semantics) and supersedes the record. The caller
// holds shard.mu.
func (s *LockFreeStore) applyTTLAfterSet(shard *LockFreeShard, key string, stamp hlcStamp) {
	shard.expiryMu.Lock()
	defer shard.expiryMu.Unlock()

	if rec, ok := shard.ttlRecs[key]; ok && rec.stamp.After(stamp) {
		if rec.deadline == 0 {
			delete(shard.expiry, key)
		} else {
			shard.expiry[key] = time.Unix(0, rec.deadline)
		}
		return
	}
	delete(shard.ttlRecs, key)
	delete(shard.expiry, key)
}

// recordExpiryTombstone leaves a tombstone when an LWW-stamped string value is
// reaped by expiry, at the value's own stamp. Without it, a peer that has not
// expired the key yet (clock skew, reaper lag, or a missed EXPIRE) would hand the
// same value back through anti-entropy and resurrect it — without its TTL. The
// tombstone blocks exactly that value and anything older, while any strictly
// newer write still wins. Values without a stamp (non-cluster writes, containers)
// are left alone. The caller holds shard.mu.
func recordExpiryTombstone(shard *LockFreeShard, key string, old *StoredValue) {
	if old == nil || old.hlc.TS.IsZero() {
		return
	}
	if tomb, ok := shard.tombstones[key]; ok && !old.hlc.After(tomb.stamp) {
		return
	}
	shard.tombstones[key] = tombstone{stamp: old.hlc, deleted: time.Now()}
}
