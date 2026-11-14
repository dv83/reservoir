# Design

## Context

The reaper (`ExpiryBatchProcessor.deleteExpiredDirect`) re-checks the deadline
under `shard.mu`, records an expiry tombstone at the value's HLC stamp (so a
stale replica cannot resurrect the key), deletes the value and its TTL and
updates accounting. GET checks `isExpiredFast` and hides the key; other
commands only look at `shard.data`.

## Goals / Non-Goals

**Goals:**
- No client command observes or builds on an expired key.
- Expired keys are reclaimed on access.

**Non-Goals:**
- Changing replication apply paths or anti-entropy.
- Changing how often the reaper runs.

## Decisions

- **Reclaim on access, sharing the reaper's code.** `reapExpired(shard, key)`
  holds the reaper's body; the reaper calls it, and `expireIfNeeded(key)` calls
  it when `isExpiredFast` says the deadline passed. Each client-facing method
  calls `expireIfNeeded` for every key it addresses before taking any lock.
  *Conflict rule:* a lazy reclaim is the reaper running at that instant, which
  the replication design already tolerates at any moment; it adds no new state
  and the tombstone it records is the reaper's. A write racing with the reclaim
  is serialized by `shard.mu`, and the reclaim re-checks the deadline under it, so
  a key renewed in between is left alone.
  - *Alternative:* teach every method to treat an expired value as absent while
    leaving it in place. More code on every path and the stale value still
    leaks into writes that rebuild from it; rejected.
- **Container apply paths reclaim first too.** The issuing node reclaimed its
  expired copy before writing. A receiver that instead merged the write into its
  still-stored expired copy would later reap the new write together with the
  old key, while the issuer keeps it — a divergence the multi-node test
  reproduced (and which the reaper alone could already cause, with worse luck).
  So SAddLWW, SRemLWW, HSetLWW, HDelLWW, ApplyListDelta and MergeCounter call
  `expireIfNeeded` before applying. *Conflict rule:* no write is merged into an
  expired copy on any node; the reclaim tombstone sits at the old value's stamp,
  below the new write's, so the new write survives everywhere.
- **ExpireLWW does not reclaim first.** A TTL extension sent by a node whose
  clock still considered the key live must not be dropped by a receiver whose
  clock runs slightly ahead.
- **No lazy reclaim during recovery.** Replay rebuilds the state as of each
  logged write; judging those writes by the current clock would drop a key
  before a later logged EXPIRE extends it.
- **Keep GET as it is.** It already hides expired keys and queues their removal,
  and it is the hot path.

## Risks / Trade-offs

- [Extra read lock per command] → one `expiryMu` RLock and map lookup; the
  write lock is only taken when a key actually expired.
- [Clock-skew between nodes] → unchanged from the reaper: each node expires by
  its own clock.
