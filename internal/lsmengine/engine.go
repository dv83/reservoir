// Package lsmengine wraps the embedded Pebble LSM engine for the store's LSM
// storage mode (add-lsm-storage). It exposes only what the store needs: point
// reads, atomic batches, prefix scans and durability control.
//
// Durability: batches are committed to Pebble's write-ahead log without fsync,
// and a background goroutine syncs the log every SyncInterval — the same window
// the commit log has in memory mode. With SyncWrites every commit is synced.
package lsmengine

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

// Options configures an Engine.
type Options struct {
	// CacheBytes sizes Pebble's block cache.
	CacheBytes int64
	// MemTableBytes sizes each memtable; zero keeps Pebble's default.
	MemTableBytes uint64
	// SyncWrites makes every commit wait for fsync.
	SyncWrites bool
	// SyncInterval is how often the write-ahead log is synced when SyncWrites
	// is off; zero means 100ms.
	SyncInterval time.Duration
}

// Engine is an open Pebble database. It is safe for concurrent use.
type Engine struct {
	db       *pebble.DB
	cache    *pebble.Cache
	syncMode *pebble.WriteOptions

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// ErrClosed reports use after Close.
var ErrClosed = errors.New("lsmengine: closed")

// Open opens (or creates) the database in dir.
func Open(dir string, opts Options) (*Engine, error) {
	if opts.CacheBytes <= 0 {
		opts.CacheBytes = 64 << 20
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = 100 * time.Millisecond
	}
	cache := pebble.NewCache(opts.CacheBytes)
	po := &pebble.Options{Cache: cache}
	// Bloom filters on every level (10 bits per key, ~1% false positives):
	// a store write first checks whether its key exists, and for a new key —
	// most writes in a growing keyspace — that lookup would otherwise search
	// every level. Pebble ships with filters off.
	po.Levels = make([]pebble.LevelOptions, 7)
	for i := range po.Levels {
		po.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
		po.Levels[i].FilterType = pebble.TableFilter
	}
	po.EnsureDefaults()
	if opts.MemTableBytes > 0 {
		po.MemTableSize = opts.MemTableBytes
	}
	db, err := pebble.Open(dir, po)
	if err != nil {
		cache.Unref()
		return nil, fmt.Errorf("lsmengine: open %s: %w", dir, err)
	}
	e := &Engine{db: db, cache: cache, syncMode: pebble.NoSync, stop: make(chan struct{}), done: make(chan struct{})}
	if opts.SyncWrites {
		e.syncMode = pebble.Sync
		close(e.done)
	} else {
		go e.syncLoop(opts.SyncInterval)
	}
	return e, nil
}

func (e *Engine) syncLoop(every time.Duration) {
	defer close(e.done)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-t.C:
			e.Sync()
		}
	}
}

// Sync forces the write-ahead log to stable storage.
func (e *Engine) Sync() error {
	// An empty LogData record with Sync flushes and fsyncs the WAL, and with it
	// every batch committed before it.
	return e.db.LogData(nil, pebble.Sync)
}

// Get returns a copy of the value stored under key.
func (e *Engine) Get(key []byte) ([]byte, bool, error) {
	v, closer, err := e.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	out := append([]byte(nil), v...)
	closer.Close()
	return out, true, nil
}

// Batch is a set of writes committed atomically.
type Batch struct {
	b *pebble.Batch
}

// NewBatch starts an empty batch.
func (e *Engine) NewBatch() *Batch { return &Batch{b: e.db.NewBatch()} }

// Set adds a write of value under key. The batch copies both.
func (b *Batch) Set(key, value []byte) { _ = b.b.Set(key, value, nil) }

// Delete adds a delete of key.
func (b *Batch) Delete(key []byte) { _ = b.b.Delete(key, nil) }

// DeleteRange adds a delete of every key in [start, end).
func (b *Batch) DeleteRange(start, end []byte) { _ = b.b.DeleteRange(start, end, nil) }

// DeletePrefix adds a delete of every key with the given prefix.
func (b *Batch) DeletePrefix(prefix []byte) {
	end := prefixEnd(prefix)
	if end == nil {
		end = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	}
	b.DeleteRange(prefix, end)
}

// Empty reports whether the batch holds no writes.
func (b *Batch) Empty() bool { return b.b.Empty() }

// Commit applies the batch atomically and releases it.
func (e *Engine) Commit(b *Batch) error {
	defer b.b.Close()
	if b.b.Empty() {
		return nil
	}
	return b.b.Commit(e.syncMode)
}

// Discard releases a batch without applying it.
func (b *Batch) Discard() { b.b.Close() }

// ScanPrefix calls fn for every key with the given prefix, in key order, until
// fn returns false. key and value are only valid during the call.
func (e *Engine) ScanPrefix(prefix []byte, fn func(key, value []byte) bool) error {
	it, err := e.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixEnd(prefix)})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		if !fn(it.Key(), it.Value()) {
			break
		}
	}
	if err := it.Error(); err != nil {
		it.Close()
		return err
	}
	return it.Close()
}

// prefixEnd returns the smallest key greater than every key with prefix, or nil
// (no upper bound) if the prefix is all 0xff.
func prefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// Stats is a summary of engine metrics.
type Stats struct {
	DiskBytes     uint64
	CacheBytes    int64
	CacheHits     int64
	CacheMisses   int64
	MemTableBytes uint64
	Compactions   int64
	WALBytes      uint64
}

// Stats returns current engine metrics.
func (e *Engine) Stats() Stats {
	m := e.db.Metrics()
	return Stats{
		DiskBytes:     m.DiskSpaceUsage(),
		CacheBytes:    m.BlockCache.Size,
		CacheHits:     m.BlockCache.Hits,
		CacheMisses:   m.BlockCache.Misses,
		MemTableBytes: m.MemTable.Size,
		Compactions:   m.Compact.Count,
		WALBytes:      m.WAL.Size,
	}
}

// Close syncs the write-ahead log and closes the database.
func (e *Engine) Close() error {
	var err error
	e.once.Do(func() {
		select {
		case <-e.done:
		default:
			close(e.stop)
			<-e.done
		}
		if serr := e.Sync(); serr != nil {
			err = serr
		}
		if cerr := e.db.Close(); cerr != nil && err == nil {
			err = cerr
		}
		e.cache.Unref()
	})
	return err
}
