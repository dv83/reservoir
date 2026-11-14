# Proposal

## Why

Between a key's deadline and the moment the background reaper reclaims it (up
to a minute), only GET, MGET, EXISTS and KEYS treat the key as gone. Every other
command still sees the old value:

| Command family | Behavior on an expired, not yet reaped key |
|---|---|
| SMEMBERS, SCARD, SISMEMBER, SUNION/SINTER/SDIFF, HGET, HGETALL, HLEN, LRANGE, LLEN, LINDEX, STRLEN | serve the expired data |
| SPOP, LPOP, RPOP, SREM, HDEL | remove and return expired data |
| SADD, HSET, RPUSH, LPUSHUNIQUE, APPEND, INCR, HINCRBY | build on the expired value, which keeps its past deadline |
| LPUSHX, RPUSHX, LSET | succeed as if the key existed |
| SET NX | refuses to write |
| EXPIRE | revives the key with a new deadline |
| DEL, GETSET | report the key as existing |

Redis treats an expired key as missing for every command and removes it on
access.

## What Changes

- Every client command that addresses a key first checks its deadline; if it
  has passed, the key is removed right then, exactly as the reaper would remove
  it (same re-check under the shard lock, same expiry tombstone in cluster mode,
  same memory and key-count accounting). The command then runs on a missing key.
- The reaper and the lazy path share one implementation.
- Container replication apply paths (SAddLWW, SRemLWW, HSetLWW, HDelLWW, list
  deltas, counter merges) also reclaim an expired copy before applying, as the
  issuing node did, so no replica merges a new write into the expired key.
- Recovery replay does not reclaim lazily.

## Capabilities

### New Capabilities

### Modified Capabilities
- `key-expiry`: expired keys are not served by any command, and an access
  reclaims them.

## Impact

- `internal/store/store.go` (shared reclaim helper), every client-facing method
  in `set_operations.go`, `hash_operations.go`, `list_operations.go`,
  `string_operations.go` and the TTL methods.
- One read-locked map lookup per command for keys without an expired deadline;
  GET's hot path is unchanged.
- Behavior change: commands on expired keys now follow Redis semantics.
