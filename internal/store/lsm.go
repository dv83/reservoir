package store

import (
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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

const (
	lsmKindValue  = 'v'
	lsmKindGlobal = 'g'
	lsmKindExpiry = 'x'
)

// expiryKey is an expiry-index entry: x | deadline (unix nanos, big endian, so
// entries sort by time) | shard | key. Entries may be stale — the key's TTL
// was changed or removed since — and the reaper checks the key's record
// before acting, so a flush only ever adds entries.
func expiryKey(deadline int64, shard byte, key string) []byte {
	b := make([]byte, 0, 10+len(key))
	b = append(b, lsmKindExpiry)
	b = binary.BigEndian.AppendUint64(b, uint64(deadline))
	b = append(b, shard)
	return append(b, key...)
}

// flushStampKey holds the newest stamped FLUSHALL, the reset point floor of
// every key.
var flushStampKey = []byte{lsmKindGlobal, 'f'}

// persistFlushStamp stores the flush stamp, so a restarted node still rejects
// stale re-deliveries of flushed data. A no-op in memory mode.
func (s *LockFreeStore) persistFlushStamp(f hlcStamp) {
	if s.engine == nil {
		return
	}
	e := &encoder{}
	e.stamp(f)
	b := s.engine.NewBatch()
	b.Set(flushStampKey, e.b)
	if err := s.engine.Commit(b); err != nil {
		logger.Error("LSM storage: cannot persist the flush stamp: %v", err)
	}
}

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
	if len(sh.dirty) > 0 || len(sh.loaded) > 0 {
		m.unlockFlush()
		return
	}
	sh.wlocked = false
	m.RWMutex.Unlock()
}

// unlockFlush persists the dirty keys, then releases the lock even if the
// flush panics, so one failure cannot leave the shard locked.
func (m *shardMutex) unlockFlush() {
	sh := m.sh
	defer func() {
		clear(sh.loaded)
		sh.wlocked = false
		m.RWMutex.Unlock()
	}()
	if len(sh.dirty) > 0 {
		sh.owner.flushShard(sh)
	}
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
		sh.loaded = make(map[string]struct{})
	}
	// Key counts cover every key on disk, not only the cached ones.
	var total int64
	for _, sh := range s.shards {
		var n int64
		if err := eng.ScanPrefix([]byte{lsmKindValue, sh.idx}, func(_, v []byte) bool {
			if len(v) > 1 && v[1]&recValue != 0 {
				n++
			}
			return true
		}); err != nil {
			return err
		}
		sh.keyCount = n
		total += n
	}
	s.totalKeys = total
	s.lsmStartedWarm = total > 0
	if b, ok, err := eng.Get(flushStampKey); err != nil {
		return err
	} else if ok {
		d := &decoder{b: b}
		f := d.stamp()
		if d.err != nil {
			return fmt.Errorf("LSM storage: corrupt flush stamp")
		}
		s.flushStamp = f
	}
	s.lsmStop = make(chan struct{})
	s.lsmDone = make(chan struct{})
	go s.lsmReaperLoop(s.lsmStop, s.lsmDone)
	logger.Info("LSM storage enabled in %s (%d keys on disk)", dir, total)
	return nil
}

// LSMEnabled reports whether the store runs in LSM storage mode.
func (s *LockFreeStore) LSMEnabled() bool { return s.engine != nil }

// keyRecord is what the engine stores per key: the value (if any) and the
// key's side state — tombstone, TTL register and TTL deadline — so all of it
// survives eviction and restart together.
type keyRecord struct {
	value    *StoredValue
	tomb     *tombstone
	ttl      *ttlRec
	deadline int64 // unix nanos; 0 = none
}

const (
	recValue    = 1 << 0
	recTomb     = 1 << 1
	recTTL      = 1 << 2
	recDeadline = 1 << 3
	recVersion  = 1
)

func encodeRecord(r keyRecord) ([]byte, error) {
	e := &encoder{}
	var flags byte
	if r.value != nil {
		flags |= recValue
	}
	if r.tomb != nil {
		flags |= recTomb
	}
	if r.ttl != nil {
		flags |= recTTL
	}
	if r.deadline != 0 {
		flags |= recDeadline
	}
	e.byte(recVersion)
	e.byte(flags)
	if r.value != nil {
		v, err := encodeValue(r.value)
		if err != nil {
			return nil, err
		}
		e.u(uint64(len(v)))
		e.b = append(e.b, v...)
	}
	if r.tomb != nil {
		e.stamp(r.tomb.stamp)
		e.i(r.tomb.deleted.UnixNano())
	}
	if r.ttl != nil {
		e.stamp(r.ttl.stamp)
		e.i(r.ttl.deadline)
		e.i(r.ttl.updated.UnixNano())
	}
	if r.deadline != 0 {
		e.i(r.deadline)
	}
	return e.b, nil
}

func decodeRecord(b []byte) (keyRecord, error) {
	var r keyRecord
	d := &decoder{b: b}
	if v := d.byte(); v != recVersion {
		return r, fmt.Errorf("LSM record: unsupported version %d", v)
	}
	flags := d.byte()
	if flags&recValue != 0 {
		n := d.u()
		if d.err != nil || n > uint64(len(d.b)) {
			return r, errCodec
		}
		v, err := decodeValue(d.b[:n])
		if err != nil {
			return r, err
		}
		d.b = d.b[n:]
		r.value = v
	}
	if flags&recTomb != 0 {
		st := d.stamp()
		r.tomb = &tombstone{stamp: st, deleted: time.Unix(0, d.i())}
	}
	if flags&recTTL != 0 {
		st := d.stamp()
		dl := d.i()
		r.ttl = &ttlRec{stamp: st, deadline: dl, updated: time.Unix(0, d.i())}
	}
	if flags&recDeadline != 0 {
		r.deadline = d.i()
	}
	if d.err == nil && len(d.b) != 0 {
		d.fail()
	}
	return r, d.err
}

// ensure loads key's record into the shard's maps, once per write-lock
// section, before anything about the key is read or changed: a flush rebuilds
// the record from the maps, so changing the side state of a key whose value
// is only on disk would otherwise drop the value. A no-op in memory mode,
// under a read lock, and for cached keys.
func (sh *LockFreeShard) ensure(key string) {
	if sh.dirty == nil || !sh.wlocked {
		return
	}
	if _, ok := sh.data[key]; ok {
		return
	}
	if _, ok := sh.loaded[key]; ok {
		return
	}
	sh.loaded[key] = struct{}{}
	sh.load(key)
}

// load reads key's record from the engine and returns its value.
//
// Under the write lock the whole record is cached: the value (its memory
// accounted as cache memory — the key itself was already counted), tombstone,
// TTL register and deadline. A value whose deadline has passed is reclaimed on
// the spot, exactly as the reaper would (expiry tombstone, key count), and
// reported missing.
//
// Under a read lock only the deadline is cached (the expiry map has its own
// lock), so TTL reads see it; the value is returned as an uncached copy, or as
// missing if it has expired. The caller holds the shard lock.
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
	rec, err := decodeRecord(b)
	if err != nil {
		logger.Error("LSM storage: record of %q is corrupt: %v", key, err)
		return nil, false
	}
	expired := rec.value != nil && rec.deadline != 0 && time.Now().UnixNano() > rec.deadline

	if !sh.wlocked {
		// Cache the deadline even when it has passed: the next expiry check
		// or reaper pass then reclaims the key under the write lock.
		if rec.deadline != 0 && rec.value != nil {
			sh.expiryMu.Lock()
			sh.expiry[key] = time.Unix(0, rec.deadline)
			sh.expiryMu.Unlock()
		}
		if rec.value == nil || expired {
			return nil, false
		}
		rec.value.touched = touchedUncached
		return rec.value, true
	}

	if rec.tomb != nil {
		sh.tombstones[key] = *rec.tomb
	}
	if rec.ttl != nil {
		sh.ttlRecs[key] = *rec.ttl
	}
	if expired {
		// Reclaim, as reapExpired does: the key was counted on disk.
		recordExpiryTombstone(sh, key, rec.value, time.Unix(0, rec.deadline))
		sh.dirty[key] = struct{}{}
		atomic.AddInt64(&sh.keyCount, -1)
		atomic.AddInt64(&s.totalKeys, -1)
		return nil, false
	}
	if rec.deadline != 0 {
		sh.expiryMu.Lock()
		sh.expiry[key] = time.Unix(0, rec.deadline)
		sh.expiryMu.Unlock()
	}
	if rec.value == nil {
		return nil, false
	}
	sh.data[key] = rec.value
	s.accountMemoryDelta(sh, rec.value.MemoryUsage())
	return rec.value, true
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
		// The record on disk holds the key's side state too.
		delete(sh.tombstones, key)
		delete(sh.ttlRecs, key)
		sh.expiryMu.Lock()
		delete(sh.expiry, key)
		sh.expiryMu.Unlock()
		s.accountMemoryDelta(sh, -mem)
		freed += mem
		atomic.AddInt64(&s.evictions, 1)
	}
	return freed
}

// flushShard commits the dirty keys' records to the engine — the value and
// side state as the shard's maps now hold them, or a delete when nothing is
// left. The caller holds the shard write lock.
func (s *LockFreeStore) flushShard(sh *LockFreeShard) {
	b := s.engine.NewBatch()
	sh.expiryMu.RLock()
	for key := range sh.dirty {
		var r keyRecord
		if v, ok := sh.data[key]; ok && v != nil {
			r.value = v
		}
		if t, ok := sh.tombstones[key]; ok {
			r.tomb = &t
		}
		if t, ok := sh.ttlRecs[key]; ok {
			r.ttl = &t
		}
		if d, ok := sh.expiry[key]; ok && r.value != nil {
			r.deadline = d.UnixNano()
		}
		if r.value == nil && r.tomb == nil && r.ttl == nil {
			b.Delete(valueKey(sh.idx, key))
			continue
		}
		enc, err := encodeRecord(r)
		if err != nil {
			logger.Error("LSM storage: cannot encode %q: %v", key, err)
			continue
		}
		b.Set(valueKey(sh.idx, key), enc)
		if r.deadline != 0 {
			b.Set(expiryKey(r.deadline, sh.idx, key), nil)
		}
	}
	sh.expiryMu.RUnlock()
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
	if s.lsmStop != nil {
		close(s.lsmStop)
		<-s.lsmDone
		s.lsmStop = nil
	}
	if s.engine != nil {
		if err := s.engine.Close(); err != nil {
			logger.Error("LSM storage: close failed: %v", err)
		}
	}
}

// lsmReapInterval is how often the expiry index is scanned.
const lsmReapInterval = time.Second

// lsmReapBatch bounds the keys reclaimed per pass, so one pass never holds up
// writers for long.
const lsmReapBatch = 10000

// lsmReaperLoop reclaims expired keys that are not in the cache — the
// in-memory reaper only sees cached deadlines.
func (s *LockFreeStore) lsmReaperLoop(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(lsmReapInterval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			for s.reapExpiryIndex(time.Now(), lsmReapBatch) == lsmReapBatch {
			}
		}
	}
}

// reapExpiryIndex reclaims up to max keys whose index entry is due by now and
// removes those entries. It returns how many entries it processed.
func (s *LockFreeStore) reapExpiryIndex(now time.Time, max int) int {
	type due struct {
		entry []byte
		shard byte
		key   string
	}
	var batch []due
	limit := uint64(now.UnixNano())
	err := s.engine.ScanPrefix([]byte{lsmKindExpiry}, func(k, _ []byte) bool {
		if len(k) < 10 || binary.BigEndian.Uint64(k[1:9]) > limit || len(batch) >= max {
			return false
		}
		batch = append(batch, due{entry: append([]byte(nil), k...), shard: k[9], key: string(k[10:])})
		return true
	})
	if err != nil {
		logger.Error("LSM storage: expiry index scan failed: %v", err)
		return 0
	}
	if len(batch) == 0 {
		return 0
	}
	for _, d := range batch {
		// reapExpired loads the key's record and acts only if its current
		// deadline has passed, so stale entries are harmless.
		reapExpired(s, s.shards[d.shard], d.key)
	}
	b := s.engine.NewBatch()
	for _, d := range batch {
		b.Delete(d.entry)
	}
	if err := s.engine.Commit(b); err != nil {
		logger.Error("LSM storage: expiry index cleanup failed: %v", err)
	}
	atomic.AddInt64(&s.lsmReaped, int64(len(batch)))
	return len(batch)
}

// shardView is everything a scan of one shard needs: values, tombstones, TTL
// registers and deadlines. In memory mode it is the shard's own maps. In LSM
// mode it merges the cache with the engine's records — cached state wins, as
// it is the same or newer — without filling the cache, so a scan does not push
// the whole shard through it. It holds one shard (1/256 of the data) while a
// scan runs; whole-shard digests are O(shard) anyway.
type shardView struct {
	data       map[string]*StoredValue
	tombstones map[string]tombstone
	ttlRecs    map[string]ttlRec
	deadlines  map[string]int64 // LSM mode only; memory mode uses the expiry map
}

// view returns the shard's view. The caller holds the shard lock and must not
// mutate the maps through it.
func (s *LockFreeStore) view(sh *LockFreeShard) shardView {
	if s.engine == nil {
		return shardView{data: sh.data, tombstones: sh.tombstones, ttlRecs: sh.ttlRecs}
	}
	v := shardView{
		data:       make(map[string]*StoredValue, len(sh.data)),
		tombstones: make(map[string]tombstone, len(sh.tombstones)),
		ttlRecs:    make(map[string]ttlRec, len(sh.ttlRecs)),
		deadlines:  make(map[string]int64),
	}
	for k, x := range sh.data {
		v.data[k] = x
	}
	for k, x := range sh.tombstones {
		v.tombstones[k] = x
	}
	for k, x := range sh.ttlRecs {
		v.ttlRecs[k] = x
	}
	sh.expiryMu.RLock()
	for k, d := range sh.expiry {
		v.deadlines[k] = d.UnixNano()
	}
	sh.expiryMu.RUnlock()
	err := s.engine.ScanPrefix([]byte{lsmKindValue, sh.idx}, func(ek, b []byte) bool {
		key := string(ek[2:])
		_, cached := sh.data[key]
		rec, err := decodeRecord(b)
		if err != nil {
			logger.Error("LSM storage: record of %q is corrupt: %v", key, err)
			return true
		}
		if rec.value != nil && !cached {
			v.data[key] = rec.value
		}
		if _, ok := v.tombstones[key]; !ok && rec.tomb != nil {
			v.tombstones[key] = *rec.tomb
		}
		if _, ok := v.ttlRecs[key]; !ok && rec.ttl != nil {
			v.ttlRecs[key] = *rec.ttl
		}
		if _, ok := v.deadlines[key]; !ok && rec.deadline != 0 && !cached {
			v.deadlines[key] = rec.deadline
		}
		return true
	})
	if err != nil {
		logger.Error("LSM storage: shard %d scan failed: %v", sh.idx, err)
	}
	return v
}

// expired reports whether key's deadline has passed.
func (v shardView) expired(s *LockFreeStore, sh *LockFreeShard, key string) bool {
	if v.deadlines == nil {
		return s.isExpiredFast(sh, key)
	}
	d := v.deadlines[key]
	return d != 0 && time.Now().UnixNano() > d
}

// hasDeadline reports whether key has a TTL.
func (v shardView) hasDeadline(sh *LockFreeShard, key string) bool {
	if v.deadlines == nil {
		sh.expiryMu.RLock()
		_, ok := sh.expiry[key]
		sh.expiryMu.RUnlock()
		return ok
	}
	return v.deadlines[key] != 0
}

// ensureWhere loads into the cache every record of the shard for which want
// returns true (all records if want is nil), for scans that change keys. The
// caller holds the shard write lock. A no-op in memory mode.
func (sh *LockFreeShard) ensureWhere(want func(record []byte) bool) {
	s := sh.owner
	if s.engine == nil || !sh.wlocked {
		return
	}
	var keys []string
	err := s.engine.ScanPrefix([]byte{lsmKindValue, sh.idx}, func(ek, b []byte) bool {
		if want == nil || want(b) {
			keys = append(keys, string(ek[2:]))
		}
		return true
	})
	if err != nil {
		logger.Error("LSM storage: shard %d scan failed: %v", sh.idx, err)
	}
	for _, k := range keys {
		sh.ensure(k)
	}
}

// recordHasGCState reports, from a record's header alone, whether tombstone GC
// may change it: it holds a key tombstone or TTL register, or a container
// (whose element tombstones age out).
func recordHasGCState(b []byte) bool {
	if len(b) < 2 {
		return false
	}
	flags := b[1]
	if flags&(recTomb|recTTL) != 0 {
		return true
	}
	if flags&recValue == 0 {
		return false
	}
	n, w := binary.Uvarint(b[2:])
	if w <= 0 || n < 2 || uint64(len(b)-2-w) < n {
		return false
	}
	switch ValueType(b[2+w+1]) { // value encoding: version, then type
	case ValueTypeSet, ValueTypeHash, ValueTypeList:
		return true
	}
	return false
}

// lsmGCShardsPerPass is how many shards one tombstone-GC pass covers in LSM
// mode, where each needs an engine scan: with GC every minute a full cycle
// takes about half an hour, so tombstones live one to one and a half hours.
const lsmGCShardsPerPass = 8
