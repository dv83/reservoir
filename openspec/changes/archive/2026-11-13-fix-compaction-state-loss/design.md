# Design

## Context

Compaction builds an in-memory `map[key]*Entry`, overwriting the entry for a key
whenever a newer write appears and removing it on a newer delete, then rewrites
the closed segments from that map. Recovery replays entries with the same
operation names the store logged (SET, INCRBY, EXPIRE, PERSIST, SADD, SREM,
HSET, HMSET, HDEL, DEL, FLUSHALL).

## Goals / Non-Goals

**Goals:**
- Replaying a compacted log yields exactly the state the uncompacted segments do.
- Keep dropping entries that a later reset makes irrelevant.

**Non-Goals:**
- Streaming (bounded-memory) compaction; tracked under `add-tiered-storage`.
- Persisting lists, or sets and hashes on standalone nodes.

## Decisions

- **Per-key retention since the last reset.** Classify each operation:
  - *Reset*: SET, INCR/INCRBY/DECR/DECRBY (logged as absolute values), DEL.
  - *Incremental*: EXPIRE, PERSIST, SADD, SREM, HSET, HMSET, HDEL, and any
    operation the compactor does not recognize.
  A reset discards the key's earlier retained entries; an incremental entry is
  appended to them. A key whose retained list starts with DEL keeps nothing.
- **FLUSHALL** discards every key's retained entries logged before it. The
  FLUSHALL itself can then be dropped: compaction always covers the oldest
  segments contiguously and recovery starts from an empty store, so nothing
  before it survives that it would need to clear.
- **Ordering.** Rewrite each key's retained entries in timestamp order; recovery
  replays in log order, so per-key order is what matters.

Alternatives considered:
- *Interpret operations and emit snapshots* (for example fold SADDs into one
  member list): smaller logs, but duplicates the store's semantics in the log
  layer. Possible follow-up once correctness is in place.
- *Snapshot the live store instead of the log*: needs a consistent cut against
  concurrent writes; larger change.

## Risks / Trade-offs

- Logs for churny set and hash keys shrink less than before. Correctness over
  size; folding is a follow-up.
- An incorrect classification would reintroduce loss, so unknown operations
  default to incremental (kept) and a property test compares full replays.
