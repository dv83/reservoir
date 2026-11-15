# Proposal

## Why

With the commit log enabled, a restart loses data:

| Data | Standalone node | Cluster node |
|---|---|---|
| sets, hashes | lost (plain SADD/SREM/SPOP/HSET/HDEL/HINCRBY are not logged) | kept (the LWW paths log) |
| lists | lost (nothing is logged) | lost (nothing is logged) |
| SDIFFSTORE result | lost | lost |

The persistence spec states the gap ("Lists are not persisted, and sets and
hashes are persisted only in cluster mode"); this change closes it.

## What Changes

- Standalone: SADD, SREM, SPOP (as SREM of the popped member), HSET, HDEL,
  HINCRBY/HINCRBYFLOAT (as HSET of the resulting value) are logged in the same
  `key\x00arg...` encoding the cluster paths use.
- Standalone lists: every list write is logged with its arguments (LPUSH, RPUSH,
  LPUSHX, RPUSHX, LPUSHUNIQUE, RPUSHUNIQUE, LPOP/RPOP with the count, LSET,
  LREM, LTRIM) and replayed through the same store method. Replay runs in log
  order on the same state, so it reproduces the list exactly.
- Cluster lists: every list CRDT change is logged as an `LDELTA` entry (the
  changed elements with their ids, plus the list's generation) and replayed
  through `ApplyListDelta`, so a restarted node keeps the element ids its peers
  know and anti-entropy does not duplicate elements.
- SDIFFSTORE logs a DEL of the destination followed by an SADD of the result.
- Only successful writes that changed something are logged.

## Capabilities

### New Capabilities

### Modified Capabilities
- `persistence`: lists, sets and hashes survive a restart on every node.

## Impact

- `internal/store/`: set_operations.go, hash_operations.go, list_operations.go,
  crdt_list_store.go (logging); `internal/server/server.go` (replay of list ops
  and LDELTA).
- One log entry per container write on standalone nodes; one gob-encoded delta
  per list write on cluster nodes.
