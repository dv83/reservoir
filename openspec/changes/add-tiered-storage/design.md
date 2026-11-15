# Design

## Context

- The store keeps every key and value in RAM in 256 sharded maps. The memory
  limit implements noeviction: writes over the limit fail.
- `unsafe.Sizeof(StoredValue{})` is 232 bytes per key, before the key bytes, the
  value and Go map overhead. Most of it is per-key read/write statistics,
  replication-tracking fields and a legacy `Data interface{}`.
- Replication is full: every node stores every key. More nodes add availability
  and read throughput, not capacity.
- The commit log is an append-only, segmented operation log with per-entry CRC.
  Writes go through a channel that drops entries when it is full. Compaction
  loads the entire state into a map, so it already needs RAM proportional to the
  dataset, and it currently loses data for multi-operation keys (see
  `fix-compaction-state-loss`).
- Conflict resolution relies on per-key and per-element LWW stamps and on
  tombstones, and anti-entropy periodically re-sends whole shards.

## Goals / Non-Goals

**Goals:**
- Serve datasets whose values exceed RAM, as long as the key index fits.
- Keep RAM-speed reads for hot keys and Redis semantics for all commands.
- Keep multi-master convergence unchanged.

**Non-Goals:**
- Datasets where the keys alone exceed RAM (Phase 2 option, below).
- Partitioning the keyspace across nodes (separate, larger change).
- Containers on disk in Phase 1.

## Decisions

### Decision 1: Spill, never drop, in a multi-master cluster

Redis-style eviction deletes keys to free memory. Here a locally deleted key is
handed back by anti-entropy from any peer that still has it, and an eviction that
created a tombstone would replicate as a real delete. So moving data out of RAM
must be invisible to replication: a cold value is still present, only stored on
disk. A pure cache mode (allkeys-lru) can be offered later for standalone nodes.

### Decision 2: Keys and hot values in RAM, values on disk (Bitcask model)

Each key gets a compact index entry (segment id, offset, length, LWW stamp,
TTL deadline, type), target about 48-64 bytes plus the key. Values live in the
append-only segments that the commit log already writes; a byte-budgeted cache
(CLOCK or LRU) keeps hot values in RAM. A cold read is one positioned read with a
CRC check. This reuses the existing log format and keeps writes sequential.

Capacity becomes: number of keys × (index entry + key) must fit in RAM; values
are bounded by disk. 100 million keys with 20-byte keys need roughly 7-10 GB of
index including map overhead, instead of more than 23 GB of headers alone today,
before any values.

Alternatives considered:
- *Embedded LSM engine now* (for example Pebble, a pure-Go LSM used by
  CockroachDB; or the Kvrocks approach of Redis on RocksDB with one row per hash
  field or set member). Handles keys larger than RAM and large containers, but
  replaces the storage engine and every command path at once, and moves CRDT
  metadata into a different encoding. Kept as the Phase 2 option.
- *Eviction only*: loses data and conflicts with anti-entropy (Decision 1).
- *OS swap or mmap of the Go heap*: unpredictable pauses, and the garbage
  collector still scans the whole heap.
- *Partitioning keys across nodes*: multiplies capacity at RAM speed, but needs
  client routing (MOVED/ASK), rebalancing and per-partition anti-entropy. It is
  complementary and larger; revisit when one node's disk-backed capacity is not
  enough.

### Decision 2a (Phase 1 revision): a separate value log, not pointers into the commit log

Pointing index entries into commit-log segments couples the store to the log in
two ways that Phase 1 does not need: log appends are asynchronous (the offset is
known only in the writer goroutine), and compaction moves entries, so every
pointer would need updating. Phase 1 therefore spills cold string values into
a separate append-only value log owned by the store:

- a spill writes the value synchronously and keeps `(file, offset, length)` in
  the key's `StoredValue`, which already carries the type, stamp and TTL — it is
  the key index entry;
- a cold read is one positioned read with a CRC check;
- dead records (overwritten or deleted keys) are reclaimed by rewriting the live
  records of mostly-dead files;
- the commit log stays the source of truth. On restart recovery replays it and
  spills at the budget again, so value files from before the restart are
  discarded. This needs recovery to stream: entries are now stamped in the
  writer goroutine, so file order is timestamp order and recovery no longer sorts
  the whole log in memory.

The cost is writing a spilled value twice (log and value log); only values
evicted from RAM pay it. Pointing into the log directly remains possible later.

### Decision 3: Phase 0 before any tiering

Tiering builds on the log as a value store, so the log must first be lossless
(backpressure instead of drops, correct compaction) and compaction must stream
(merge segments by key without loading all state). Shrinking `StoredValue` pays
off immediately, even without tiering.

### Decision 4: Strings first

Strings are fixed values with key-level stamps, so one index entry per key
describes them fully. Sets, hashes and lists carry per-element CRDT state; on
disk they need either a whole-container encoding (rewrite on every change) or one
row per element. That choice waits for Phase 1 metrics.

### Decision 5: Anti-entropy from the index

Digests use the stamps and TTLs stored in the index and fetch a value only when a
peer actually needs it, so reconciliation does not page every cold value into
RAM. A hash-first digest per shard (send shard hashes, then only differing keys)
is a natural extension.

## Risks / Trade-offs

- Cold reads cost a disk read: tail latency rises for keys outside the cache.
  Expose the cache hit rate so operators can size RAM.
- Compaction now rewrites value data and competes for disk I/O.
- Without the Phase 0 log fixes, tiering would turn today's log drops and
  compaction loss into permanent data loss. Phase 0 is mandatory.
- Large containers stay in RAM in Phase 1, so a few huge sets or lists can still
  exhaust memory.

## Phase 0 measurements

Measured with `RESERVOIR_MEMBENCH=1 go test ./internal/store/ -run TestMemoryFootprintPerKey -v`
(1M string keys, 16-byte keys and values, heap growth per key; standalone and
cluster write paths give the same figures).

| | `StoredValue` | heap per key | keys per GB |
|---|---|---|---|
| Baseline | 232 bytes | 314 bytes | 3.4M |
| Statistics, replication-tracking and legacy fields removed | 88 bytes | 170 bytes | 6.3M |

The removed fields (read/write counts and times, replication flags, node list,
attempt count, a legacy `Data` interface) were never updated by any code path;
the debug command that printed them always showed zeros.

Of the remaining 170 bytes, about 96 are the `StoredValue` allocation (88 bytes
rounded to its size class), 32 the key and value bytes, and about 40 the map
entry. Further gains need structural changes (values stored inline in the shard
map, or one container pointer instead of three) and are left for Phase 1, where
the key index replaces this layout for cold keys anyway.
