# Tasks

## 1. Phase 0: measure and slim the per-key footprint

- [x] 1.1 Benchmark bytes per key for 1M string keys and record the baseline
- [x] 1.2 Move statistics, replication-tracking and legacy fields out of StoredValue
- [x] 1.3 Re-measure bytes per key and keys per GB

## 2. Phase 0: make the log a safe value store

- [x] 2.1 Complete fix-compaction-state-loss
- [x] 2.2 Replace dropping on a full write channel with backpressure, and test that no acknowledged write is lost
- [x] 2.3 Make compaction a streaming merge with bounded memory
- [x] 2.4 Persist lists, and sets and hashes on standalone nodes

## 3. Phase 1: values on disk for strings

- [ ] 3.1 Return segment and offset from log appends
- [ ] 3.2 Add the compact key index and the byte-budgeted hot-value cache
- [ ] 3.3 Serve cold reads from segments with CRC verification
- [ ] 3.4 Spill values to disk at the RAM budget instead of rejecting writes
- [ ] 3.5 Rebuild the key index from segments on startup
- [ ] 3.6 Build anti-entropy digests from the index without loading cold values
- [ ] 3.7 Add tiering flags and budgets, and tier metrics to the stats endpoint
- [ ] 3.8 Cluster e2e test: a dataset larger than the RAM budget converges and stays readable on every node

## 4. Decide Phase 2

- [ ] 4.1 Review Phase 1 hit rate, latency and index size, and decide on container tiering or an embedded LSM engine
