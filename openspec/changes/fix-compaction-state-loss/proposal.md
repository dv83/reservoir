# Proposal

## Why

Commit-log compaction keeps only the most recent entry per key, which is correct
only when every entry is a full overwrite. Several logged operations are not:
EXPIRE and PERSIST modify a key without carrying its value, and SADD, SREM, HSET,
HMSET and HDEL each carry only a delta. After compaction:

- `SET k v` followed by `EXPIRE k t` compacts to just the EXPIRE, so recovery
  applies a TTL to a missing key and **the value is lost**.
- `SADD s a` then `SADD s b` compacts to the last SADD, so **member `a` is lost**
  (likewise for hash fields).

Reproduced against the real CommitLog and Compactor. Any key that ever had a TTL,
and any multi-member set or multi-field hash, can lose data on recovery once its
segments are compacted. This also blocks using the log as a storage tier.

## What Changes

- Compaction keeps, per key, every entry since the key's last full reset (SET,
  absolute counter write, DEL or FLUSHALL) instead of only the last entry, so
  replaying the compacted log rebuilds exactly the state the original log did.
- Entries made obsolete by a later reset are still dropped; a key whose last
  reset is a delete is dropped entirely, as today.
- Operations the compactor does not recognize are kept, never collapsed.
- Original entry timestamps stay preserved.

## Capabilities

### New Capabilities

### Modified Capabilities
- `persistence`: compaction must preserve recovered state for every key, not just
  the last entry per key.

## Impact

- `internal/commitlog/compaction.go` (state building and segment rewrite).
- Recovery code is unchanged; it already replays entries in order.
- Compacted logs grow for set and hash keys with long add/remove histories,
  since their deltas are kept until a DEL. Folding them into snapshots is a
  possible follow-up.
