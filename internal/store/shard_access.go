package store

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
	sh.data[key] = v
	if sh.dirty != nil {
		sh.dirty[key] = struct{}{}
	}
}

// del removes key. The caller holds the shard write lock.
func (sh *LockFreeShard) del(key string) {
	delete(sh.data, key)
	if sh.dirty != nil {
		sh.dirty[key] = struct{}{}
	}
}
