## 1. Reproduce

- [x] 1.1 Store tests: after DEL, a stale SAddLWW, HSetLWW, MergeCounter or ApplyListDelta does not bring the key back, and a newer one still applies
- [x] 1.2 Store test: after a stamped flush, stale writes for every key type are ignored and newer ones apply
- [x] 1.3 Multi-node tests: a DEL of every key type and a FLUSHALL, each applied on one node only, win on every node after anti-entropy; a write after the FLUSHALL survives

## 2. Fix

- [x] 2.1 Add the flush stamp and `resetPoint`
- [x] 2.2 Sets and hashes: filter element ops by the reset point; DEL prunes instead of dropping newer elements
- [x] 2.3 Lists and counters: add `Gen` to state and wire payloads; merge by generation
- [x] 2.4 `FlushLWW`, stamped FLUSHALL replication, flush stamp in the shard digest
- [x] 2.5 Container expiry tombstone at the deadline

## 3. Verify

- [x] 3.1 Run `go test ./... -race` and confirm the new tests fail without the fix
