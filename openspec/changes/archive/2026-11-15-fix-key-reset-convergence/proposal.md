# Proposal

## Why

The cluster-replication spec promises that deleting a key leaves a tombstone
that a delayed older write cannot get past. That holds only for string keys.
For containers, the key tombstone is never checked:

| Delete of | Stale redelivery (anti-entropy re-sends whole shards) | Result |
|---|---|---|
| set | SAddLWW at an older stamp | members come back |
| hash | HSetLWW at an older stamp | fields come back |
| counter | MergeCounter of the pre-delete state | old value comes back |
| list | ApplyListDelta of the pre-delete elements | elements come back |

FLUSHALL is worse: it is not stamped, `Clear()` leaves no tombstone at all, and
a replicated FLUSHALL is applied whenever it arrives. Any node that missed it,
or got it late, hands every flushed key back through anti-entropy. A FLUSHALL
that arrives after a newer write also wipes that write on that node only.

Expiry of a container has the same gap: the reaper's tombstone uses the
value's stamp, which containers never set, so no tombstone is recorded.

## What Changes

- Each key gets a **reset point**: the newer of its key tombstone and a
  store-wide **flush stamp**. A replicated container write is applied only if
  it belongs after the reset point:
  - set and hash element ops: their stamp is newer than the reset point;
  - lists and counters, whose elements carry no comparable stamps: each
    incarnation records the **generation** (the reset point it was created
    after). An incarnation from an older generation is dropped; a newer one
    replaces the local one and raises the local reset point.
- Deleting a container (DEL, FLUSHALL, expiry) keeps set and hash elements
  stamped after the delete, and drops older ones, instead of dropping all.
- FLUSHALL in cluster mode is stamped. It sets the flush stamp, removes every
  key older than it, and travels in the anti-entropy digest, so a node that
  missed it still flushes.
- Reclaiming an expired container leaves a tombstone at its deadline, so
  writes from the expired incarnation cannot come back and writes after the
  deadline survive.
- Wire formats gain optional fields (counter and list generation, a flush
  entry in the digest); older payloads decode with zero values.

## Capabilities

### New Capabilities

### Modified Capabilities
- `cluster-replication`: deletes, FLUSHALL and expiry of every key type
  converge and cannot be undone by stale writes.

## Impact

- `internal/store/`: lww.go, crdt_set.go, crdt_hash.go, crdt_list_store.go,
  crdt_counter.go, ttl_lww.go, store.go (`Clear`, flush stamp).
- `internal/server/`: replicateWrite and ApplyReplication for FLUSHALL,
  digest reconcile; `internal/server/cluster_e2e_test.go`.
- Semantics for concurrent writes: an INCR or list push made on a node that
  had not yet seen a concurrent DEL/FLUSHALL is dropped with the old
  generation (delete wins); set and hash writes stamped after the delete survive.
