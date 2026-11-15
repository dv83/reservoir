# Design

## Context

String keys are guarded by `shard.tombstones[key]` in SetWithHLC and
DeleteWithHLC. Container apply paths (SAddLWW, SRemLWW, HSetLWW, HDelLWW,
ApplyListDelta, MergeCounter) ignore it. Set elements and hash fields carry HLC
stamps; list elements carry synthetic sequence ids (not comparable with HLC);
PN-counters carry per-node totals with no stamps. FLUSHALL calls `Clear()` on
every node and is replicated unstamped.

## Goals / Non-Goals

**Goals:**
- No delete (DEL, FLUSHALL, expiry) of any key type can be undone by a delayed
  or re-sent older write, on any node.
- Replicas converge after any mix of deletes and concurrent writes.

**Non-Goals:**
- Persisting tombstones, generations or the flush stamp across a restart.
- Changing standalone FLUSHALL.

## Decisions

- **Reset point.** `resetPoint(shard, key) = max(tombstones[key], flushStamp)`.
  Every container apply path reads it under the shard lock.
- **Sets and hashes filter by element stamp.** An add/remove or field write is
  applied only if its stamp is after the reset point. Deleting the key prunes
  elements at or before the delete stamp and keeps newer ones.
  *Conflict rule:* a write stamped after the delete survives, an older one does
  not — the same rule strings already follow.
- **Lists and counters use generations.** Each incarnation records the reset
  point current when it was created (`Gen`). On merge: `Gen` older than the
  local reset point → drop; newer than the local incarnation's `Gen` → raise the
  local reset point to it and replace the local incarnation; equal → merge as
  today. `Gen` is carried in `CounterState` and `ListDelta`.
  *Conflict rule:* every node keeps the incarnation of the newest generation
  seen anywhere; the generation is a delete stamp, so learning a newer one also
  teaches the node about the delete.
  - *Alternative:* stamp every counter op and list element with an HLC. Finer
    (concurrent INCRs after a delete survive) but changes both CRDTs' state and
    wire format substantially; rejected for now.
- **FLUSHALL is a store-wide reset.** In cluster mode the issuer stamps it;
  `FlushLWW(stamp)` raises `flushStamp` and applies the reset to every key
  (strings with an older stamp removed, containers pruned or dropped by
  generation). Each shard digest carries the flush stamp, and reconcile calls
  `FlushLWW`, so a node that missed the event still flushes. An unstamped
  FLUSHALL (standalone, older senders) keeps today's `Clear()`.
- **The cluster FLUSHALL handler does not call `Clear()`.** Clearing locally
  would also remove writes stamped after the flush that arrived from a node with
  a faster clock, which every peer keeps. The handler goes through
  `replicateWrite`, which stamps the flush and applies `FlushLWW`.
- **Commit log.** `FlushLWW` logs one FLUSHALL entry when nothing survived, and
  per-key deletes otherwise, so a replay does not wipe the writes stamped after
  the flush.
- **Expiry tombstone for containers at the deadline.** Containers have no value
  stamp; reaping one records a tombstone at `(deadline, max logical, max
  origin)`, derived from the replicated TTL register and so identical on every
  node. Strings keep the value-stamp tombstone, since a SET after the EXPIRE
  clears the TTL and must survive.

## Risks / Trade-offs

- [A restarted node recovers counters with generation 0] → if the key was
  deleted and recreated within the tombstone retention window, peers replace
  its counter and increments it made before catching up are lost. Persisting
  generations is a follow-up.
- [Tombstone GC] → after the retention window an incarnation older than a
  forgotten delete can come back, as for strings today. The flush stamp is a
  single value and is never collected.
