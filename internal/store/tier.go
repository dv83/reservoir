package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"reservoir/internal/valuelog"
	"reservoir/pkg/logger"
)

// Tiered storage, Phase 1 (add-tiered-storage): string values move to the
// value log when RAM reaches the memory budget, instead of writes failing.
//
//   - A cold key keeps its StoredValue in the shard map — type, LWW stamp, TTL
//     and size stay in RAM, so it is the key index entry — with StringVal empty
//     and cold pointing at the value's record.
//   - Moving a value is not a write: the stamp, TTL and value are unchanged, no
//     commit-log entry, tombstone or replication event is produced.
//   - A spill replaces the key's StoredValue with a cold copy rather than
//     mutating it, because readers may hold the old pointer outside the lock.
//   - Cold values are read under the shard lock. Garbage collection relocates
//     a record only under the same lock, and deletes a file only after every
//     live record in it has moved, so a read never sees a deleted file.
//   - The commit log is the source of truth: on restart recovery replays it and
//     spills at the budget again (valuelog.Open discards old files).

const (
	// minSpillBytes: values shorter than this stay in RAM; the reference that
	// replaces them would save little or nothing.
	minSpillBytes = 64
	coldRefSize   = int64(unsafe.Sizeof(valuelog.Ref{}))
	// tierGCInterval is how often value-log garbage is checked; a file whose
	// live bytes are at most tierGCLiveRatio of its size is rewritten.
	tierGCInterval  = 10 * time.Second
	tierGCLiveRatio = 0.5
)

type tierState struct {
	log *valuelog.Log

	spillMu sync.Mutex // one spill pass at a time
	hand    int        // next shard to scan (guarded by spillMu)

	stop chan struct{}
	done chan struct{}

	spilledKeys   atomic.Int64 // values moved to disk
	spilledBytes  atomic.Int64
	coldReads     atomic.Int64
	coldReadFails atomic.Int64
	gcRuns        atomic.Int64
	gcMoved       atomic.Int64
	gcReclaimed   atomic.Int64
	rejectedFull  atomic.Int64 // writes refused because nothing could be spilled
}

// EnableTiering turns on tiered storage with value files in dir. It must be
// called before the store holds data (at startup, before recovery).
func (s *LockFreeStore) EnableTiering(dir string, maxFileSize int64) error {
	if s.tier != nil {
		return fmt.Errorf("tiering already enabled")
	}
	log, err := valuelog.Open(dir, maxFileSize)
	if err != nil {
		return err
	}
	t := &tierState{log: log, stop: make(chan struct{}), done: make(chan struct{})}
	s.tier = t
	go s.tierGCLoop(t)
	logger.Info("Tiered storage enabled: values beyond the %d-byte memory budget spill to %s", s.limits.MaxMemoryUsage, dir)
	return nil
}

// TieringEnabled reports whether tiered storage is on.
func (s *LockFreeStore) TieringEnabled() bool { return s.tier != nil }

func (s *LockFreeStore) stopTiering() {
	t := s.tier
	if t == nil {
		return
	}
	select {
	case <-t.stop:
		return
	default:
	}
	close(t.stop)
	<-t.done
	t.log.Close()
}

// loadString returns sv's string value, reading it from the value log if it is
// cold. The caller holds the key's shard lock (read or write).
func (s *LockFreeStore) loadString(shard *LockFreeShard, key string, sv *StoredValue) (string, error) {
	if sv.cold == nil {
		if s.tier != nil {
			// Per shard, so the read path does not contend on one counter.
			atomic.AddInt64(&shard.hotReads, 1)
		}
		return sv.StringVal, nil
	}
	t := s.tier
	if t == nil {
		return "", fmt.Errorf("cold value for %q but tiering is disabled", key)
	}
	v, err := t.log.Read(*sv.cold, []byte(key))
	if err != nil {
		t.coldReadFails.Add(1)
		logger.Error("Tiered storage: cannot read value of %q: %v", key, err)
		return "", fmt.Errorf("cannot read value of %q from disk: %w", key, err)
	}
	t.coldReads.Add(1)
	return string(v), nil
}

// stringLen returns the length of sv's string value without reading it.
func stringLen(sv *StoredValue) int64 {
	if sv.cold != nil {
		return int64(sv.cold.ValLen)
	}
	return int64(len(sv.StringVal))
}

// touch sets the CLOCK reference bit: a recently read value is skipped once by
// the spiller.
func (s *LockFreeStore) touch(sv *StoredValue) {
	if (s.tier != nil || s.engine != nil) && atomic.LoadUint32(&sv.touched) == 0 {
		atomic.StoreUint32(&sv.touched, 1)
	}
}

// wouldExceedMemory reports whether growing memory by delta must be refused.
// Without tiering that is the plain budget check (noeviction). With tiering it
// first spills values to make room and refuses only if not enough could be
// moved — the key index (and in-RAM containers) alone fill the budget. held is
// the shard the caller has locked, if any; others are only try-locked, so
// concurrent writers never wait for each other's shards.
func (s *LockFreeStore) wouldExceedMemory(held *LockFreeShard, delta int64) bool {
	if s.limits == nil || s.limits.MaxMemoryUsage <= 0 || delta <= 0 {
		return false
	}
	used := atomic.LoadInt64(&s.totalMemory)
	if s.engine != nil {
		used = s.cacheUsage()
	}
	over := used + delta - s.limits.MaxMemoryUsage
	if over <= 0 {
		return false
	}
	if s.engine != nil {
		// LSM mode: the cache shrinks by evicting clean entries; the write
		// itself always fits, since the data lives on disk.
		s.evict(held, over+s.limits.MaxMemoryUsage/100)
		return false
	}
	if s.tier == nil {
		return true
	}
	// Concurrent writers can take the room a spill frees before this writer
	// re-checks, so keep spilling while it makes progress. Refuse only when a
	// pass frees nothing: what is left in RAM cannot be spilled.
	for attempt := 0; attempt < 16; attempt++ {
		// Free a little more than needed, so the next writes do not spill
		// again at once.
		freed := s.spill(held, over+s.limits.MaxMemoryUsage/100)
		over = atomic.LoadInt64(&s.totalMemory) + delta - s.limits.MaxMemoryUsage
		if over <= 0 {
			return false
		}
		if freed == 0 {
			break
		}
	}
	s.tier.rejectedFull.Add(1)
	return true
}

// spill moves cold string values to the value log until at least need bytes
// of RAM are freed or every shard has been scanned twice (once to clear
// reference bits, once to spill). It returns the bytes freed.
func (s *LockFreeStore) spill(held *LockFreeShard, need int64) int64 {
	t := s.tier
	t.spillMu.Lock()
	defer t.spillMu.Unlock()

	var freed int64
	for pass := 0; pass < 2*numShards && freed < need; pass++ {
		shard := s.shards[t.hand]
		t.hand = (t.hand + 1) % numShards
		if shard != held {
			if !shard.mu.TryLock() {
				continue
			}
		}
		freed += s.spillShard(shard, need-freed)
		if shard != held {
			shard.mu.Unlock()
		}
	}
	return freed
}

// spillShard spills values of one shard. The caller holds shard.mu.
func (s *LockFreeStore) spillShard(shard *LockFreeShard, need int64) int64 {
	t := s.tier
	var freed int64
	for key, sv := range shard.data {
		if freed >= need {
			break
		}
		if sv == nil || sv.Type != ValueTypeString || sv.cold != nil || sv.counter != nil || len(sv.StringVal) < minSpillBytes {
			continue
		}
		if atomic.LoadUint32(&sv.touched) != 0 {
			atomic.StoreUint32(&sv.touched, 0) // second chance
			continue
		}
		ref, err := t.log.Append([]byte(key), []byte(sv.StringVal))
		if err != nil {
			logger.Error("Tiered storage: spill of %q failed: %v", key, err)
			return freed
		}
		cold := *sv
		cold.StringVal = ""
		cold.cold = &ref
		saved := int64(ref.ValLen) - coldRefSize
		cold.memorySize = sv.MemoryUsage() - saved
		shard.put(key, &cold)
		s.accountMemoryDelta(shard, -saved)
		freed += saved
		t.spilledKeys.Add(1)
		t.spilledBytes.Add(int64(ref.ValLen))
	}
	return freed
}

// tierGCLoop periodically rewrites mostly-dead value files.
func (s *LockFreeStore) tierGCLoop(t *tierState) {
	defer close(t.done)
	ticker := time.NewTicker(tierGCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-ticker.C:
			s.collectValueLog()
		}
	}
}

// collectValueLog measures each value file's live bytes from the key index
// (no per-write bookkeeping is needed) and rewrites files that are mostly dead.
func (s *LockFreeStore) collectValueLog() int64 {
	t := s.tier
	if t == nil {
		return 0
	}
	live := map[uint32]int64{}
	for _, shard := range s.shards {
		shard.mu.RLock()
		for key, sv := range shard.data {
			if sv != nil && sv.cold != nil {
				live[sv.cold.File] += 12 + int64(len(key)) + int64(sv.cold.ValLen)
			}
		}
		shard.mu.RUnlock()
	}
	var victims []uint32
	for id, size := range t.log.ClosedFileSizes() {
		if float64(live[id]) <= tierGCLiveRatio*float64(size) {
			victims = append(victims, id)
		}
	}
	if len(victims) == 0 {
		return 0
	}
	reclaimed, err := t.log.CollectFiles(victims, func(key []byte, old valuelog.Ref, value []byte) bool {
		k := string(key)
		shard := s.getShard(k)
		shard.mu.Lock()
		defer shard.mu.Unlock()
		sv, ok := shard.get(k)
		if !ok || sv == nil || sv.cold == nil || *sv.cold != old {
			return false // overwritten, deleted or already moved: dead
		}
		ref, err := t.log.Append(key, value)
		if err != nil {
			logger.Error("Tiered storage: relocating %q failed: %v", k, err)
			return false
		}
		moved := *sv
		moved.cold = &ref
		shard.put(k, &moved)
		t.gcMoved.Add(1)
		return true
	})
	if err != nil {
		logger.Error("Tiered storage: value-log collection failed: %v", err)
	}
	t.gcRuns.Add(1)
	t.gcReclaimed.Add(reclaimed)
	return reclaimed
}

// TierStats returns tiered-storage metrics, or nil when tiering is off.
func (s *LockFreeStore) TierStats() map[string]interface{} {
	t := s.tier
	if t == nil {
		return nil
	}
	var coldKeys, coldBytes, hot int64
	for _, shard := range s.shards {
		hot += atomic.LoadInt64(&shard.hotReads)
		shard.mu.RLock()
		for _, sv := range shard.data {
			if sv != nil && sv.cold != nil {
				coldKeys++
				coldBytes += int64(sv.cold.ValLen)
			}
		}
		shard.mu.RUnlock()
	}
	ls := t.log.Stats()
	cold := t.coldReads.Load()
	hitRate := 1.0
	if hot+cold > 0 {
		hitRate = float64(hot) / float64(hot+cold)
	}
	return map[string]interface{}{
		"memory_budget":       s.limits.MaxMemoryUsage,
		"memory_used":         atomic.LoadInt64(&s.totalMemory),
		"cold_keys":           coldKeys,
		"cold_value_bytes":    coldBytes,
		"value_files":         ls.Files,
		"value_disk_bytes":    ls.DiskBytes,
		"spilled_keys_total":  t.spilledKeys.Load(),
		"spilled_bytes_total": t.spilledBytes.Load(),
		"hot_reads":           hot,
		"cold_reads":          cold,
		"cold_read_failures":  t.coldReadFails.Load(),
		"ram_hit_rate":        hitRate,
		"gc_runs":             t.gcRuns.Load(),
		"gc_records_moved":    t.gcMoved.Load(),
		"gc_bytes_reclaimed":  t.gcReclaimed.Load(),
		"writes_rejected":     t.rejectedFull.Load(),
	}
}
