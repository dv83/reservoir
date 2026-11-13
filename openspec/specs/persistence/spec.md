# Persistence Specification

## Purpose
Defines the write-ahead commit log that lets a node rebuild its data after a restart. Lists are not persisted, and sets and hashes are persisted only in cluster mode.

## Requirements

### Requirement: Write-ahead logging
When the commit log is enabled, string, counter and TTL mutations SHALL be appended to a segmented log in which each entry carries a CRC32 checksum; set and hash mutations SHALL be appended when the node runs in cluster mode. Writes replayed during recovery SHALL NOT be logged again.

#### Scenario: Logged write
- **WHEN** the commit log is enabled and a client runs SET k v
- **THEN** a SET entry for k with a valid checksum is appended to the log

### Requirement: Crash-safe recovery
On startup the server SHALL replay the commit log asynchronously to rebuild logged data, and SHALL NOT crash on a truncated or corrupt entry.

#### Scenario: Restart after writes
- **WHEN** a node with the commit log enabled restarts after SET and INCR writes
- **THEN** after recovery the keys hold their logged values

#### Scenario: Corrupt tail
- **WHEN** the last log entry is truncated
- **THEN** recovery completes without crashing and earlier entries are applied

### Requirement: Compaction
Background compaction SHALL rewrite closed segments such that replaying the compacted log rebuilds exactly the same state as replaying the original segments. It SHALL drop only entries made obsolete by a later full overwrite or delete of the same key, keep every entry it cannot classify, and preserve each surviving entry's original timestamp.

#### Scenario: Superseded writes are dropped
- **WHEN** a key was SET several times and later compaction runs
- **THEN** the compacted log holds only the key's last SET

#### Scenario: TTL survives compaction
- **WHEN** a key was SET and then given a TTL with EXPIRE, and compaction runs
- **THEN** after recovery the key holds its value and its TTL

#### Scenario: Incremental container writes survive compaction
- **WHEN** a set received several SADDs and compaction runs
- **THEN** after recovery the set holds every added member
