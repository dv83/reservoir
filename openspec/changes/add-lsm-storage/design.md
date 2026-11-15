# Design

## Context

- The store is 256 shard maps of `*StoredValue`, guarded by per-shard RWMutexes.
  Commands, CRDT merges, TTL registers and tombstones all operate on these maps
  and on side maps (`tombstones`, `ttlRecs`, `expiry`).
- Several operations scan every key: anti-entropy digests (one shard at a time),
  KEYS, the expiry reaper, FLUSHALL, tombstone and TTL garbage collection, stats.
- Durability is the commit log plus recovery replay; convergence state (element
  stamps, generations, tombstones) is not persisted and is rebuilt by
  anti-entropy after a restart.
- The target workload is many small keys, where per-key RAM is the limit.

## Goals / Non-Goals

**Goals:**
- Number of keys bounded by disk; RAM bounded by `--max-memory`.
- Same commands, Redis semantics and multi-master convergence as memory mode.
- Restart without replaying a log; convergence state persisted.

**Non-Goals:**
- Containers larger than RAM (one container is still loaded whole; a per-element
  layout is a follow-up).
- Partitioning the keyspace across nodes.
- Migrating an existing memory-mode commit log (a node can be re-seeded by
  anti-entropy from its peers).

## Decisions

### Decision 1: Pebble as the engine

Pure Go (no cgo), production-proven in CockroachDB, with batches, snapshots,
range iteration and a configurable block cache. Alternatives: BadgerDB (value
log separation, but heavier GC tuning and less active), RocksDB via cgo (build
and deployment cost), bbolt (B+tree, write amplification for random small
writes).

### Decision 2: the shard maps become a write-through cache

Rewriting every command against an iterator API is the cost that made an LSM
engine the Phase 2 option. Instead, each shard map stays the working set: a
command loads the key on a miss, runs its existing logic on the in-memory
`StoredValue` under the shard lock, and persists the key's resulting state
before releasing the lock. All entries are therefore clean and can be evicted at
any time, using the CLOCK bit Phase 1 added.

- Map access goes through `shard.get`, `shard.put` and `shard.del`, so a miss
  loads and a change persists in one place; a test checker compares cache and
  engine after every operation in the whole store suite.
- Containers are encoded whole, which is fine for the target (small values);
  large containers are the non-goal above.

### Decision 3: key layout and codec

Pebble keys are `kind | shard | user key` with one-byte kind and shard prefixes:

| kind | value |
|---|---|
| `v` | the encoded `StoredValue`: type, LWW stamp, value or CRDT state, TTL deadline and TTL register, generation |
| `t` | key tombstone stamp and deletion time |
| `x` | expiry index: `x | deadline | shard | key`, for the reaper |
| `g` | global records: flush stamp, node origin |

The codec is a versioned binary encoding of every CRDT (LWW set adds and
removes, hash fields, RGA elements, PN-counter slots), with round-trip property
tests. A shard digest is one range scan over `v | shard` and `t | shard`.

### Decision 4: durability through Pebble's WAL

In LSM mode the commit log is off. Each command writes one Pebble batch; batches
are committed without fsync and the WAL is synced every 100ms, matching today's
commit-log window (an acknowledged write can be lost only in a crash within that
window). `--lsm-sync-writes` syncs every batch for stricter durability.

### Decision 5: memory budget

`--max-memory` is split between the key cache (shard maps) and Pebble's block
cache and memtables. The cache evicts by CLOCK when its share is full; nothing is
ever lost by eviction because entries are clean.

## Risks / Trade-offs

- Every write costs an encode and a Pebble batch: lower peak write throughput
  than memory mode. Benchmarks decide whether to batch across commands.
- Cache misses cost an LSM read; random reads over a cold keyspace are bound by
  the disk.
- Whole-container encoding makes a write to a large container O(size).
- A missed persist call would make the cache and the engine diverge until the
  entry is evicted; the cache-vs-engine checker over the full suite guards it.

## Store-level measurements after task 2.3

`go test ./internal/store/ -run x -bench BenchmarkStorage -benchtime 2s`
(4 goroutines on 4 cores, 16-byte keys and values, no network):

| | memory | LSM, cache holds all | LSM, 4MB cache |
|---|---|---|---|
| SET (new keys) | 0.8-1.0 us | 4.6 us | 4.6 us |
| GET (random of 200K) | 120-160 ns | 142 ns | 1.2-1.3 us |

- A GET from the cache costs what it costs in memory mode; a miss costs about
  1.2us (Pebble read from its block cache, then a cache fill).
- A SET costs about 3.6us more than in memory mode, a store-level ceiling of
  roughly 220K SETs per second on this machine, close to today's end-to-end
  rate (100-160K), so LSM mode will lower peak write throughput until task 5.2.
  Profile: about a third of the time is the existence read for a new key
  (Pebble's Get builds an iterator; bloom filters, now enabled, help only for
  data already in sstables), about 13% the commit, about 12% compaction.
