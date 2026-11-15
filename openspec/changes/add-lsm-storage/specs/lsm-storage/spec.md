# Spec Delta

## Purpose
Lets a node hold more keys than fit in its RAM by keeping keys and values in an embedded LSM engine on disk behind a bounded in-memory cache.

## ADDED Requirements

### Requirement: Keys are bounded by disk, not RAM
In LSM mode the node SHALL accept and serve keys beyond what fits in RAM, keeping memory use within the configured budget.

#### Scenario: More keys than the cache holds
- **WHEN** an LSM-mode node with a small memory budget receives many times more keys than its cache holds
- **THEN** every write succeeds, every key is readable, and memory use stays within the budget

### Requirement: Same semantics as memory mode
In LSM mode every command SHALL return the same replies and leave the same state as in memory mode, including TTLs and multi-master convergence.

#### Scenario: Evicted key keeps its state
- **WHEN** a key with a TTL is evicted from the cache and read again
- **THEN** the reply and the remaining TTL are those it had before eviction

### Requirement: Restart without replay
In LSM mode a restarted node SHALL serve every write acknowledged before the last WAL sync, together with its convergence state (stamps, tombstones, generations), without replaying a log.

#### Scenario: Restart after writes
- **WHEN** an LSM-mode node restarts after writes and deletes
- **THEN** every key holds its value, and a deleted key is not resurrected by a stale write after the restart
