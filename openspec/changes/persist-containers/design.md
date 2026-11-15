# Design

## Context

Recovery replays SADD/SREM/HSET/HDEL through the plain store methods and knows
no list operation. On cluster nodes the stamped SAddLWW/SRemLWW/HSetLWW/HDelLWW
log their op; the plain methods (which a cluster client write also runs first)
log nothing. Cluster lists are an RGA whose element ids are minted locally;
replaying plain list ops would mint new ids.

## Goals / Non-Goals

**Goals:**
- After a restart every list, set and hash holds what it held before, on
  standalone and cluster nodes.

**Non-Goals:**
- Persisting CRDT stamps of sets and hashes, generations or tombstones
  (re-established by live writes and anti-entropy, as today).
- Making SDIFFSTORE deterministic across replicas.

## Decisions

- **Plain methods log only on standalone nodes** (`localOrigin == 0`). On a
  cluster node the stamped LWW path that follows each client write logs it, so
  logging the plain call too would write every op twice.
- **Standalone lists log operations, not state.** Each write is logged with its
  arguments only if it succeeded and changed the list, and replayed through the
  same method. Positional ops (LSET, LREM, LTRIM, pops) are deterministic given
  the same prior state, which in-order replay guarantees.
  - *Alternative:* log the full list after each write. Simple replay, but O(n)
    log bytes per push; rejected.
- **Cluster lists log CRDT deltas.** After each local list change (and each
  applied remote delta) the changed element descriptors and the list's
  generation are logged as `LDELTA`. Replay applies them with `ApplyListDelta`,
  which is idempotent and order-independent, so the restarted node holds the
  same element ids as its peers.
- **Logging via deferred named results.** Each list method logs from a `defer`
  placed after its CRDT branch, so every return path of the plain body is
  covered without restructuring it.
- **Compaction is unchanged.** All new ops are incremental and are kept; DEL and
  SET of the key still drop what precedes them.

## Risks / Trade-offs

- [gob overhead per LDELTA entry] → a few hundred bytes per list write on
  cluster nodes; compaction keeps them all until the key is deleted.
