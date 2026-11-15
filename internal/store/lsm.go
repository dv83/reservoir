package store

import (
	"fmt"
	"sync"
	"sync/atomic"

	"reservoir/internal/lsmengine"
	"reservoir/pkg/logger"
)

// LSM storage mode (add-lsm-storage): keys and values live in an embedded LSM
// engine; each shard map is a write-through cache over it.
//
//   - A command runs its usual logic on the in-memory StoredValue under the
//     shard lock. Every key it touches under the write lock is marked dirty
//     (shard_access.go), and before the lock is released the dirty keys' new
//     state — or their deletion — is committed to the engine in one batch. The
//     engine therefore sees a shard's writes in the order they happened, and
//     the cache never holds a change the engine lacks.
//   - A miss loads the key from the engine. Under the write lock the value is
//     cached; under a read lock it is returned without caching (the map cannot
//     be written under a read lock).
//
// Engine keys are kind | shard | user key; kind 'v' holds the encoded value.

const lsmKindValue = 'v'

func valueKey(shard byte, key string) []byte {
	b := make([]byte, 0, 2+len(key))
	b = append(b, lsmKindValue, shard)
	return append(b, key...)
}

// shardMutex is the shard lock. In LSM mode Unlock persists the dirty keys
// before releasing the lock.
type shardMutex struct {
	sync.RWMutex
	sh *LockFreeShard
}

func (m *shardMutex) Lock() {
	m.RWMutex.Lock()
	m.sh.wlocked = true
}

func (m *shardMutex) TryLock() bool {
	if !m.RWMutex.TryLock() {
		return false
	}
	m.sh.wlocked = true
	return true
}

func (m *shardMutex) Unlock() {
	sh := m.sh
	if len(sh.dirty) > 0 {
		sh.owner.flushShard(sh)
	}
	sh.wlocked = false
	m.RWMutex.Unlock()
}

// EnableLSM switches the store to LSM storage mode with the engine in dir. It
// must be called before the store holds data.
func (s *LockFreeStore) EnableLSM(dir string, opts lsmengine.Options) error {
	if s.engine != nil {
		return fmt.Errorf("LSM storage already enabled")
	}
	if s.tier != nil {
		return fmt.Errorf("LSM storage and tiered storage are exclusive")
	}
	eng, err := lsmengine.Open(dir, opts)
	if err != nil {
		return err
	}
	s.engine = eng
	for _, sh := range s.shards {
		sh.dirty = make(map[string]struct{})
	}
	// Key counts cover every key on disk, not only the cached ones.
	var total int64
	for _, sh := range s.shards {
		var n int64
		if err := eng.ScanPrefix([]byte{lsmKindValue, sh.idx}, func(_, _ []byte) bool { n++; return true }); err != nil {
			return err
		}
		sh.keyCount = n
		total += n
	}
	s.totalKeys = total
	logger.Info("LSM storage enabled in %s (%d keys on disk)", dir, total)
	return nil
}

// LSMEnabled reports whether the store runs in LSM storage mode.
func (s *LockFreeStore) LSMEnabled() bool { return s.engine != nil }

// load reads key from the engine. Under the write lock the value is cached and
// its memory accounted as cache memory (the key was already counted). The
// caller holds the shard lock.
func (sh *LockFreeShard) load(key string) (*StoredValue, bool) {
	s := sh.owner
	b, ok, err := s.engine.Get(valueKey(sh.idx, key))
	if err != nil {
		logger.Error("LSM storage: read of %q failed: %v", key, err)
		return nil, false
	}
	if !ok {
		return nil, false
	}
	v, err := decodeValue(b)
	if err != nil {
		logger.Error("LSM storage: value of %q is corrupt: %v", key, err)
		return nil, false
	}
	if sh.wlocked {
		sh.data[key] = v
		s.accountMemoryDelta(sh, v.MemoryUsage())
	} else {
		v.touched = touchedUncached
	}
	return v, true
}

// touchedUncached marks a value loaded under a read lock: a copy that is not in
// the cache. Hot read paths cache it afterwards (fillCache).
const touchedUncached = 2

// fillAfterRead caches key if v is an uncached copy loaded under a read lock.
// The caller has released the shard lock.
func (s *LockFreeStore) fillAfterRead(sh *LockFreeShard, key string, v *StoredValue) {
	if s.engine == nil || v == nil || atomic.LoadUint32(&v.touched) != touchedUncached {
		return
	}
	sh.mu.Lock()
	if _, ok := sh.data[key]; !ok {
		// Reload under the write lock rather than caching v: a write or delete
		// may have happened since v was read.
		if fresh, ok := sh.load(key); ok {
			fresh.touched = 1 // just read
		}
	}
	s.maybeEvict(sh)
	sh.mu.Unlock()
}

// maybeEvict shrinks the cache back under the memory budget. held is the shard
// the caller has write-locked.
func (s *LockFreeStore) maybeEvict(held *LockFreeShard) {
	if s.limits == nil || s.limits.MaxMemoryUsage <= 0 {
		return
	}
	if over := atomic.LoadInt64(&s.totalMemory) - s.limits.MaxMemoryUsage; over > 0 {
		s.evict(held, over+s.limits.MaxMemoryUsage/100)
	}
}

// evict removes clean cache entries, CLOCK order, until need bytes are freed
// or every shard has been visited twice. held is the shard the caller has
// write-locked; others are only try-locked, so writers never wait on each
// other. It returns the bytes freed.
func (s *LockFreeStore) evict(held *LockFreeShard, need int64) int64 {
	s.evictMu.Lock()
	defer s.evictMu.Unlock()
	var freed int64
	for pass := 0; pass < 2*numShards && freed < need; pass++ {
		sh := s.shards[s.evictHand]
		s.evictHand = (s.evictHand + 1) % numShards
		if sh != held {
			if !sh.mu.TryLock() {
				continue
			}
		}
		freed += s.evictShard(sh, need-freed)
		if sh != held {
			sh.mu.Unlock()
		}
	}
	return freed
}

// evictShard drops clean entries of one shard from the cache. Skipped: keys
// touched under the current lock (dirty — the caller may still be mutating
// them), lists whose deltas are not yet replicated, and recently read entries
// (their reference bit is cleared: a second chance). The caller holds the
// shard write lock.
func (s *LockFreeStore) evictShard(sh *LockFreeShard, need int64) int64 {
	var freed int64
	for key, v := range sh.data {
		if freed >= need {
			break
		}
		if _, dirty := sh.dirty[key]; dirty || v == nil {
			continue
		}
		if v.Type == ValueTypeList && v.ListVal != nil && len(v.ListVal.pending) > 0 {
			continue
		}
		if atomic.LoadUint32(&v.touched) != 0 {
			atomic.StoreUint32(&v.touched, 0)
			continue
		}
		mem := v.MemoryUsage()
		delete(sh.data, key)
		s.accountMemoryDelta(sh, -mem)
		freed += mem
		atomic.AddInt64(&s.evictions, 1)
	}
	return freed
}

// flushShard commits the dirty keys of a shard to the engine. The caller holds
// the shard write lock.
func (s *LockFreeStore) flushShard(sh *LockFreeShard) {
	b := s.engine.NewBatch()
	for key := range sh.dirty {
		if v, ok := sh.data[key]; ok && v != nil {
			enc, err := encodeValue(v)
			if err != nil {
				logger.Error("LSM storage: cannot encode %q: %v", key, err)
				continue
			}
			b.Set(valueKey(sh.idx, key), enc)
		} else {
			b.Delete(valueKey(sh.idx, key))
		}
	}
	if err := s.engine.Commit(b); err != nil {
		logger.Error("LSM storage: commit failed: %v", err)
	}
	clear(sh.dirty)
	if s.lsmCheck != nil {
		s.lsmCheck(sh)
	}
}

// clearEngine deletes every stored value (FLUSHALL in LSM mode).
func (s *LockFreeStore) clearEngine() {
	b := s.engine.NewBatch()
	b.DeletePrefix([]byte{lsmKindValue})
	if err := s.engine.Commit(b); err != nil {
		logger.Error("LSM storage: clear failed: %v", err)
	}
}

func (s *LockFreeStore) closeLSM() {
	if s.engine != nil {
		if err := s.engine.Close(); err != nil {
			logger.Error("LSM storage: close failed: %v", err)
		}
	}
}
