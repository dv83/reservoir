## 1. Reproduce

- [ ] 1.1 Store tests: after DEL, a stale SAddLWW, HSetLWW, MergeCounter or ApplyListDelta does not bring the key back, and a newer one still applies
- [ ] 1.2 Store test: after a stamped flush, stale writes for every key type are ignored and newer ones apply
- [ ] 1.3 Multi-node test: FLUSHALL on one node with a peer that missed it converges to empty after anti-entropy

## 2. Fix

- [ ] 2.1 Add the flush stamp and `resetPoint`
- [ ] 2.2 Sets and hashes: filter element ops by the reset point; DEL prunes instead of dropping newer elements
- [ ] 2.3 Lists and counters: add `Gen` to state and wire payloads; merge by generation
- [ ] 2.4 `FlushLWW`, stamped FLUSHALL replication, flush stamp in the shard digest
- [ ] 2.5 Container expiry tombstone at the deadline

## 3. Verify

- [ ] 3.1 Run `go test ./... -race` and confirm the new tests fail without the fix
