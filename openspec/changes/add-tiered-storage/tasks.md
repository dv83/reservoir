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

- [x] 3.1 Stamp commit-log entries in the writer so file order is replay order, and stream recovery instead of sorting the log in memory (sorted fallback for older logs)
- [x] 3.2 Add the value log: CRC-checked records, positioned reads, file rotation and garbage collection of dead records
- [x] 3.3 Cold string values in the store: tier-transparent reads that fail on a checksum error, and a CLOCK reference bit for victim selection
- [x] 3.4 Spill values at the RAM budget instead of rejecting writes, and reject new keys once the key index alone exceeds the budget
- [x] 3.5 Restart: recovery replays the log and spills at the budget again; value files from before the restart are discarded; tiering requires the commit log
- [x] 3.6 Anti-entropy digests read cold values from the value log one shard at a time, without caching them in RAM
- [x] 3.7 Add tiering flags and tier metrics to the stats endpoint
- [x] 3.8 Cluster e2e test: a dataset larger than the RAM budget converges and stays readable on every node

## 4. Decide Phase 2

- [ ] 4.1 Review Phase 1 hit rate, latency and index size, and decide on container tiering or an embedded LSM engine
