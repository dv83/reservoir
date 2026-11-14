## 1. Reproduce

- [x] 1.1 Store test: every set, hash, list, string and TTL command on an expired, not yet reaped key behaves as on a missing key
- [x] 1.2 Store test: an access reclaims the expired key, and a live key keeps its value and TTL

## 2. Fix

- [x] 2.1 Extract the reaper's check-and-delete into `reapExpired` and add `expireIfNeeded`
- [x] 2.2 Call `expireIfNeeded` at the entry of every client-facing set, hash, list, string and TTL method
- [x] 2.3 Multi-node test: SADD on an expired set on one node converges to the new set on every node
- [x] 2.4 Container apply paths reclaim an expired copy before applying; lazy reclaim is off during recovery (store test)

## 3. Verify

- [x] 3.1 Run `go test ./... -race` and confirm the store tests fail without the fix
