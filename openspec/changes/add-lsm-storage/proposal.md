# Proposal

## Why

The target workload is a very large number of small keys. Today every key lives
in RAM: about 170 bytes per key in memory mode and about 180 bytes per cold key
with tiering (add-tiered-storage, Phase 1), which also never spills values under
64 bytes. One GB of RAM therefore holds 5-6 million keys; a billion keys would
need close to 200 GB of RAM per node, on every node, because replication is full.

## What Changes

- A new storage mode, `--storage=lsm`, keeps keys and values in an embedded LSM
  engine (Pebble, the pure-Go engine used by CockroachDB) on local disk. RAM
  holds a bounded cache of recently used keys and Pebble's block cache, both
  sized from `--max-memory`, so the number of keys is bounded by disk, not RAM.
- The shard maps become a write-through cache over the engine: every command
  keeps its current logic, CRDT state and Redis semantics, operating on the
  in-memory `StoredValue` as today; each write persists the key's new state in
  the same step, and a miss loads it from disk. Clean entries are evicted with
  the existing CLOCK bit.
- Everything that today scans the shard maps — anti-entropy digests, KEYS, the
  expiry reaper, FLUSHALL, tombstone and TTL bookkeeping, statistics — reads the
  engine instead. Keys are stored under a one-byte shard prefix, so a shard
  digest is one range scan.
- Tombstones, TTL registers, the flush stamp, list and counter generations and
  set and hash element stamps are persisted with the key, so convergence state
  survives a restart (today it is rebuilt by anti-entropy).
- In LSM mode Pebble's write-ahead log replaces the commit log; a restart opens
  the engine instead of replaying a log.
- Memory mode stays the default and is unchanged.

## Capabilities

### New Capabilities
- `lsm-storage`: keys and values on disk behind a bounded RAM cache.

### Modified Capabilities

## Impact

- New dependency: `github.com/cockroachdb/pebble` (BSD-3-Clause).
- `internal/store/`: a binary codec for `StoredValue` and all CRDT state; shard
  map access goes through get/put/delete helpers; scans go through the engine.
- `internal/server/`: storage-mode flag, start-up without commit-log replay.
- Tests: the existing store test suite also runs in LSM mode with a checker
  that compares the cache with the engine after every operation.
