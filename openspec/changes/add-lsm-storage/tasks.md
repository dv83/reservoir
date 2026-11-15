## 1. Foundation

- [x] 1.1 Add the Pebble dependency and an engine wrapper (open, batch, get, prefix scan, close) with tests
- [x] 1.2 Binary codec for StoredValue and every CRDT, with round-trip property tests
- [x] 1.3 Route all shard map access through get/put/del helpers (no behavior change in memory mode)

## 2. Write-through cache

- [x] 2.1 Persist each write in the same step and load on a miss in LSM mode
- [x] 2.2 Run the store test suite in LSM mode with a cache-versus-engine checker after every operation
- [x] 2.3 Evict clean entries by CLOCK at the cache budget

## 3. Engine-backed scans and side state

- [x] 3.1 Persist tombstones, TTL registers, the flush stamp and generations
- [x] 3.2 Expiry index and an engine-backed reaper
- [x] 3.3 Anti-entropy digests, KEYS, FLUSHALL, GC and stats from engine scans

## 4. Node integration

- [x] 4.1 `--storage=lsm` with Pebble's WAL and periodic sync; restart without log replay
- [x] 4.2 Restart and crash tests: every acknowledged write outside the sync window survives
- [x] 4.3 Cluster e2e test: three LSM nodes converge with a cache far smaller than the data

## 5. Measure and optimize

- [ ] 5.1 Benchmark many small keys (RSS, ops/s, p99) against memory mode and record the results
- [ ] 5.2 Cut the per-write cost: commit outside the shard lock with ordered group commit, and avoid the full existence read for new keys
- [ ] 5.3 Store large sets and hashes as one record per element, so a write to a large container is not O(size) (today each SADD to a 200K-member set re-encodes the whole set)
