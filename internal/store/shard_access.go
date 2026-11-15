package store

import "sync/atomic"

// Shard map access (add-lsm-storage, tasks 1.3 and 2.1). Every read, write and
// delete of a shard's map goes through these helpers. In memory mode they are
// plain map operations. In LSM mode the map is a write-through cache over the
// engine: a miss loads the key from disk, and every key touched under the
// write lock is marked dirty and persisted when the lock is released
// (shardMutex.Unlock), so in-place container mutations are persisted too.

// get returns the value stored under key, comma-ok style. The caller holds the
// shard lock.
func (sh *LockFreeShard) get(key string) (*StoredValue, bool) {
	v, ok := sh.data[key]
	if sh.dirty == nil {
		return v, ok
	}
	if !ok {
		if sh.wlocked {
			if _, done := sh.loaded[key]; done {
				return nil, false
			}
			sh.loaded[key] = struct{}{}
		}
		v, ok = sh.load(key)
		if !ok {
			return nil, false
		}
	}
	if sh.wlocked {
		sh.dirty[key] = struct{}{}
	}
	return v, true
}

// getV returns the value stored under key, or nil. The caller holds the shard
// lock.
func (sh *LockFreeShard) getV(key string) *StoredValue {
	if sh.dirty == nil {
		return sh.data[key]
	}
	v, _ := sh.get(key)
	return v
}

// put stores v under key. The caller holds the shard write lock.
func (sh *LockFreeShard) put(key string, v *StoredValue) {
	sh.ensure(key)
	if sh.dirty != nil {
		if _, had := sh.data[key]; !had {
			atomic.AddInt64(&sh.owner.cachedEntries, 1)
		}
		sh.dirty[key] = struct{}{}
	}
	sh.data[key] = v
}

// del removes key. The caller holds the shard write lock.
func (sh *LockFreeShard) del(key string) {
	sh.ensure(key)
	if sh.dirty != nil {
		if _, had := sh.data[key]; had {
			atomic.AddInt64(&sh.owner.cachedEntries, -1)
		}
		sh.dirty[key] = struct{}{}
	}
	delete(sh.data, key)
}

// markDirty records that key's side state (tombstone, TTL register or TTL
// deadline) changed, so the next write-unlock persists the key's record. The
// key's record must have been loaded before the change (get, ensure or
// resetPoint): loading it afterwards would bring back what the change
// removed. The caller holds the shard write lock.
func (sh *LockFreeShard) markDirty(key string) {
	if sh.dirty != nil && sh.wlocked {
		sh.dirty[key] = struct{}{}
	}
}
