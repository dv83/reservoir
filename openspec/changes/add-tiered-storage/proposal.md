# Proposal

## Why

Every key and value must fit in one node's RAM. At the configured limit writes
are rejected (noeviction), and clustering adds no capacity: replication is full,
so every node holds the whole dataset. RAM is also spent inefficiently: each key
carries a 232-byte `StoredValue` header before its key bytes, value and map
overhead, so 100 million small keys need over 23 GB for headers alone.

Datasets larger than RAM need a storage tier on disk, with RAM holding the key
index and the hot values.

## What Changes

Phased, each phase shippable on its own:

- **Phase 0 — prerequisites (no new feature).** Shrink the per-key header by
  moving statistics, replication-tracking and legacy fields out of `StoredValue`.
  Make the commit log apply backpressure instead of dropping entries when its
  write channel is full. Make compaction stream in bounded memory (depends on
  `fix-compaction-state-loss`). Persist lists, and sets and hashes on standalone
  nodes.
- **Phase 1 — values on disk for strings (opt-in).** Keep a compact in-RAM key
  index (key → segment, offset, length, LWW stamp, TTL) and a byte-budgeted
  cache of hot values; cold values are read from the append-only segments
  (Bitcask model). When the RAM budget is exceeded, values spill to disk instead
  of writes being rejected.
- **Phase 2 — decided after Phase 1 metrics.** Containers on disk, and an embedded
  LSM engine if the key index itself no longer fits in RAM. Not part of this
  change's tasks.

## Capabilities

### New Capabilities
- `tiered-storage`: serving a dataset larger than RAM by keeping cold values on
  disk behind an in-RAM key index and hot-value cache.

### Modified Capabilities
- `memory-management`: with tiering enabled, the memory limit bounds RAM use;
  reaching it spills values to disk instead of rejecting writes.

## Impact

- `internal/store` (value representation, read path, memory accounting),
  `internal/commitlog` (offset-returning appends, backpressure, streaming
  compaction), configuration (tiering flag and budgets), `/api/crdt` or a new
  stats endpoint for tier metrics.
- Reads of cold values go to disk: higher tail latency for those keys.
- Cluster: anti-entropy digests must be built from the key index, not by loading
  cold values.
