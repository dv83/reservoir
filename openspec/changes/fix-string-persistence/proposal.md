# Proposal

## Why

On a node with the commit log enabled, string keys do not survive a restart
as they were:

| Command | Logged | After restart |
|---|---|---|
| `SET k v EX n` / `PX n` | `SET` only | TTL lost — the key never expires |
| `SET k v KEEPTTL` | `SET` | TTL removed |
| `INCR`/`INCRBY`/`DECR`/`DECRBY` on a key with a TTL | `INCRBY` (replayed as a SET) | TTL removed |
| `APPEND` | nothing | value rolls back |
| `GETSET` | nothing | value rolls back |

Recovery replays SET and counter entries as full overwrites that clear the TTL,
which is right for a plain SET but not for writes that set or keep a TTL; and
APPEND and GETSET were never logged at all. Separately, GETSET keeps the TTL on a
standalone node, while Redis — and Reservoir in cluster mode — discard it.

## What Changes

- Every string write is logged, including APPEND and GETSET (as a SET of the
  resulting value).
- When a string write leaves the key with a TTL, the log records an EXPIRE with
  the absolute deadline right after the value, so recovery restores it. This
  covers SET EX/PX/KEEPTTL, APPEND, counters, and LWW SETs that keep a newer TTL.
- Commit-log entries get strictly increasing timestamps, so a value and the
  EXPIRE logged right after it can never tie and be replayed out of order.
- GETSET discards the key's TTL, as SET does.
- Recovery and compaction are unchanged: they already handle a value followed
  by its EXPIRE correctly.

## Capabilities

### New Capabilities

### Modified Capabilities
- `persistence`: APPEND and GETSET are logged, and string keys keep their TTL
  across a restart.
- `string-commands`: GETSET discards the TTL.

## Impact

- `internal/store/string_operations.go`, `internal/store/lww.go`,
  `internal/store/crdt_counter.go` (logging), `internal/commitlog/commitlog.go`
  (timestamps).
- One extra log entry per write to a key that has a TTL.
- Behavior change: GETSET on a key with a TTL now removes the TTL on standalone
  nodes too.
