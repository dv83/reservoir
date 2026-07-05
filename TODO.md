# Reservoir TODO

Focus: Bug fixes, simplification, optimization, maintenance only. No new features.

---

## P0 - Bugs

- [x] **Fix unsafe.Sizeof() misuse** (6+ places)
  - Result: Analyzed 18 usages - most are correct (struct overhead calculation)
  - Fixed: Deleted dead `cache_alignment.go` (62 lines), removed useless line in `list_types.go`

- [x] **Fix retry queue overflow handling** (cluster.go)
  - Fixed: Events now persisted before cleanup in cleanupStaleFailedEvents()
  - Added: droppedEventCount tracking for all cleanup paths

- [x] **Fix Vector Clock display**
  - Fixed: Added nil check in GetVectorClock() with fallback to legacy clock
  - Verified: API now shows correct clock values (tested with cluster)

- [x] **Fix hardcoded command statistics** (getDocsData())
  - Fixed: Added GetGlobalCommandStats() to commands package
  - Stats now calculated dynamically from registry

---

## P1 - Cluster Stability

- [x] **Fix cluster node discovery**
  - Verified: All nodes see all other nodes correctly
  - Implementation: `broadcastNodeUpdate()` in handleJoinRequest already works
  - Tested: 3-node cluster shows 3/3 nodes on each node

- [x] **Implement node-to-node discovery propagation**
  - Already implemented: `handleNodeUpdate()` processes broadcasts correctly
  - All nodes in cluster have full mesh awareness

- [x] **Fix heartbeat mechanism**
  - Verified: Elections stable at term=1, heartbeats working (150ms interval)
  - Election timeout: 1-2s random, properly reset on heartbeat receipt

- [x] **Implement node health monitoring**
  - Fixed: All nodes now send heartbeats (not just leaders)
  - Fixed: healthCheckLoop re-enabled, detects failures within 5s
  - Test: Verified dead nodes are removed from cluster view

---

## P2 - Major Simplification

High-impact code reduction.

- [x] **Refactor list_operations.go** (1136 -> 667 lines, 41% reduction)
  - Extracted 10 helper methods: readValue, requireList, casSetValue, casDeleteKey, casCreateKey, etc.
  - Eliminated duplicate inline functions for locking
  - Code is cleaner and more maintainable

- [x] **Consolidate three expiry systems into one**
  - Removed: `LockFreeExpiryTracker` (275 lines of duplicate tracking)
  - Kept: Per-shard `expiry` map + `ExpiryBatchProcessor` for batching
  - Result: Simpler, no duplicate data, same functionality

- [ ] **Split server.go** (921 lines) - DEFERRED
  - Templates already extracted to templates.go
  - Would need: handlers_api.go, handlers_keys.go, stats.go
  - Lower priority - file is manageable as-is

- [ ] **Evaluate over-engineered optimizations** - DEFERRED
  - `memory_arena.go` (253 lines), `simd_hash.go` (237 lines), `memory_prefetch.go` (238 lines)
  - Total: 728 lines
  - Need benchmarks before removing - may have real performance benefits

- [x] **Remove Monitoring System & UI**
  - Reason: UI is not providing sufficient value and adds complexity.
  - Scope: Remove `internal/monitoring`, templates, server routes, and config flags.
  - Simplify `server.go` and `metrics.go` logic accordingly.

---

## P2.5 - Testing

- [x] **Add comprehensive integration test suite**
  - Created: `internal/commands/comprehensive_test.go` (~1200 lines)
  - Tests all String, List, Set commands with edge cases
  - WRONGTYPE error tests for all type combinations
  - Concurrent operation tests
  - Complex scenarios (queue, stack, counters)
  - Edge cases (empty keys, long values, binary data)
  - Argument validation tests

- [x] **Add advanced multi-client integration tests**
  - Created: `internal/commands/advanced_integration_test.go` (~600 lines)
  - Multi-client concurrent operations (shared counters, lists, sets)
  - Producer-consumer queue patterns
  - Mixed type operations with concurrent access
  - Complex workflows (sessions, leaderboards, rate limiters, distributed locks)
  - Data integrity tests (no data loss, atomic increments)
  - Stress tests (high concurrency, rapid create/delete)

- [x] **Fix WRONGTYPE errors for string commands**
  - Fixed: APPEND, STRLEN, INCR, GETSET now return WRONGTYPE on wrong type
  - Fixed: INCR on non-numeric string now returns "value is not an integer"
  - Fixed: All string ops now set Type field correctly

- [x] **Add DEFER command tests**
  - Created: `internal/commands/deferred_commands_test.go` (~475 lines)
  - Tests for DEFER with SET, INCR, INCRBY, DEL, APPEND
  - DEFER.CANCEL, DEFER.LIST, DEFER.STATS tests
  - Integration tests for command ordering and cancellation
  - Error handling tests for invalid arguments

---

## P3 - Code Cleanup ✅ COMPLETE

Lower-impact but cleaner codebase. Net reduction: ~133 lines.

- [x] **Remove 30+ redundant wrapper functions** in commands
  - Added `withNilReplication` adapter, removed wrappers from all command files
- [x] **Extract replication builder helper** (repeated 19+ times)
  - Added `buildReplicationData()` helper function
- [x] **Consolidate 63 identical error message patterns**
  - Created constants: `errNotInteger`, `errNotFloat`, `errDeferNotSupported`, `errCommitLogDisabled`
- [x] **Simplify buffer pooling** (4 pools -> 1)
  - Removed 3 unused pools (smallBufferPool, mediumBufferPool, largeBufferPool)
  - Kept stringBuilderPool (actually used for template expansion)
- [x] **Remove dual vector clock implementation**
  - Analyzed: Intentional design - simple VectorClock for serialization, OptimizedVectorClock for runtime
- [x] **Remove unnecessary case conversion wrappers** (commands.go:495-529)
  - Removed `lowerCase`/`upperCase` wrappers, simplified registration
- [x] **Remove hardcoded placeholder cluster nodes**
  - Removed 6 placeholder `clusterNodes := []string{"node1", "node2", "node3"}`
- [x] **Remove or simplify template expansion system**
  - Removed redundant `needsTemplateExpansion()` function
  - Simplified hot path in GET/SET commands
- [x] **Extract duplicate calculateCRC() function**
  - Created `Entry.CalculateCRC()` method, removed duplicates from reader.go and commitlog.go
- [x] **Fix PersistentQueue O(N) removal inefficiency**
  - Changed to in-place filtering with `map[string]struct{}` for O(1) lookups

- [ ] **Optimize Memory UI for large datasets** - P3
  - Issue: UI becomes sluggish with many keys (>1000)
  - Proposal: Return only top 200 keys (sorted by size desc) from API
  - Aggregate remaining keys into a single "Others" block
  - Will significantly reduce DOM elements and JSON payload size

---

## P4 - Scripts ✅ COMPLETE

- [x] **Fix cluster-test.sh** - `local` keyword error at line 105
  - Removed `local` keyword from all for-loop variables (bash only allows `local` inside functions)
- [x] **Fix cluster-test.sh** - false split-brain detection
  - Fixed grep pattern to check `is_current_node AND is_leader` instead of just `is_leader`
  - Old pattern matched any node listing the leader, new pattern only matches actual leader

---

## Completed

### P0 (Critical Bugs) - All Done
- [x] Fix leader election vote counting
- [x] Add max line length limit in protocol parser
- [x] Remove 'memory layout fix' hack
- [x] Make commit log reliable
- [x] Fix race condition in commitlog Close()
- [x] Fix unsafe pointer cast in GetLeader()
- [x] Fix leader election term bypass

### P1 (Important Fixes) - 7/12 Done
- [x] Fix memory leak in failedEvents map
- [x] Fix double-lock deadlock potential
- [x] Fix use-after-free in handleHeartbeat
- [x] Fix duplicate delete in list_operations.go
- [x] Add timeout/max retries to infinite retry loops
- [x] Fix buffer race condition in ZeroCopyCommand
- [x] Fix AsyncRecovery double-pass inefficiency

### Other
- [x] Fix cluster monitoring UI API (JSON encoding)
- [x] Fix split brain condition
- [x] Fix election timer after join
- [x] Include local node in GetNodes()
