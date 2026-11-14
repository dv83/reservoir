# Design

## Context

Recovery applies `SET`, `INCR`, `INCRBY`, `DECR` and `DECRBY` entries with the
store's `Set`, which overwrites the key and clears its TTL, and `EXPIRE` with
`SetExpiry`. Compaction (see the archived `fix-compaction-state-loss`) treats SET
and counter entries as resets and keeps EXPIRE entries that follow them.
Recovery orders entries by timestamp with an unstable sort; entries are stamped
with `time.Now()` when they are logged.

## Goals / Non-Goals

**Goals:**
- After a restart every string key has the value and TTL it had before.
- No change to recovery or compaction rules.

**Non-Goals:**
- Persisting lists, or sets and hashes on standalone nodes.
- Restoring PN-counter CRDT state on restart (counters recover as values).

## Decisions

- **Restate the TTL in the log rather than change replay semantics.** One helper,
  `logStringWrite`, logs the value entry and, if the key still has a TTL, an
  `EXPIRE` with its absolute deadline. All string writers use it: Set,
  SetWithOptions, SetWithHLC, Append, GetSet, IncrBy and the PN-counter path.
  Keeping "SET clears the TTL" on replay means compaction's reset rule stays
  valid, and the log is self-describing: a key's TTL is always an explicit entry.
  - *Alternative:* replay counter entries keeping the TTL, and teach compaction a
    third operation class. Fewer log entries, but couples both components to
    command semantics; rejected.
- **Read the deadline and log both entries under `expiryMu` (read lock).** TTL
  changes take the write lock, so a concurrent EXPIRE or PERSIST is applied and
  logged either before or after the pair, never between them. This also fixes a
  pre-existing race where a SET logged after a concurrent EXPIRE could replay as
  clearing a TTL that the live key still has.
- **Strictly increasing log timestamps.** `Write` and `Delete` take
  `max(now, last+1)` via an atomic compare-and-swap. A value and its EXPIRE then
  cannot share a timestamp, so the unstable recovery sort cannot swap them.
- **GETSET discards the TTL**, matching Redis and the cluster path.

## Risks / Trade-offs

- One extra entry per write to a key with a TTL, for example rate-limit
  counters. Compaction collapses them to the last value and its EXPIRE.
- Holding the expiry read lock while enqueueing log entries delays TTL changes in
  that shard if the log's write channel is full.
- GETSET clearing the TTL on standalone nodes is a visible behavior change.
