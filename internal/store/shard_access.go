package store

// Shard map access (add-lsm-storage, task 1.3). Every read, write and delete of
// a shard's map goes through these helpers, so the LSM storage mode can load a
// missing key from disk and persist every change in one place. In memory mode
// they are plain map operations (and inline to them).

// get returns the value stored under key, comma-ok style. The caller holds the
// shard lock.
func (sh *LockFreeShard) get(key string) (*StoredValue, bool) {
	v, ok := sh.data[key]
	return v, ok
}

// getV returns the value stored under key, or nil. The caller holds the shard
// lock.
func (sh *LockFreeShard) getV(key string) *StoredValue {
	return sh.data[key]
}

// put stores v under key. The caller holds the shard write lock.
func (sh *LockFreeShard) put(key string, v *StoredValue) {
	sh.data[key] = v
}

// del removes key. The caller holds the shard write lock.
func (sh *LockFreeShard) del(key string) {
	delete(sh.data, key)
}
