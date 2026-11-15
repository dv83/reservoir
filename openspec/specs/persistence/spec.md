# Persistence Specification

## Purpose
Defines the write-ahead commit log that lets a node rebuild its data after a restart. Lists are not persisted, and sets and hashes are persisted only in cluster mode.

## Requirements

### Requirement: Write-ahead logging
When the commit log is enabled, string, counter, TTL, list, set and hash mutations — including APPEND and GETSET — SHALL be appended to a segmented log in which each entry carries a CRC32 checksum, on standalone and cluster nodes alike. Writes replayed during recovery SHALL NOT be logged again.

#### Scenario: Logged write
- **WHEN** the commit log is enabled and a client runs SET k v
- **THEN** a SET entry for k with a valid checksum is appended to the log

#### Scenario: APPEND is logged
- **WHEN** the commit log is enabled and a client runs APPEND k x
- **THEN** the log records the key's resulting value

### Requirement: Crash-safe recovery
On startup the server SHALL replay the commit log asynchronously to rebuild logged data, and SHALL NOT crash on a truncated or corrupt entry.

#### Scenario: Restart after writes
- **WHEN** a node with the commit log enabled restarts after SET and INCR writes
- **THEN** after recovery the keys hold their logged values

#### Scenario: Corrupt tail
- **WHEN** the last log entry is truncated
- **THEN** recovery completes without crashing and earlier entries are applied

### Requirement: String keys keep their TTL across a restart
After recovery, every string key SHALL have the value and TTL it had when the node stopped, including keys last written by SET with EX, PX or KEEPTTL, by APPEND, or by a counter command.

#### Scenario: SET with EX survives a restart
- **WHEN** a client runs SET k v EX 3600 and the node restarts
- **THEN** k holds v and still has a TTL of about one hour

#### Scenario: INCR keeps the TTL across a restart
- **WHEN** a key with a TTL is incremented with INCR and the node restarts
- **THEN** the key holds the incremented value and keeps its TTL

### Requirement: Containers survive a restart
After recovery, every list, set and hash SHALL hold exactly the elements, order and field values it held when the node stopped. On a cluster node a recovered list SHALL keep the element identities its peers know, so anti-entropy does not duplicate elements.

#### Scenario: Standalone list survives a restart
- **WHEN** a standalone node runs RPUSH l a b c, LPOP l, LSET l 0 x and restarts
- **THEN** LRANGE l 0 -1 replies x c

#### Scenario: Standalone set and hash survive a restart
- **WHEN** a standalone node runs SADD s a b, SREM s a, HSET h f 1, HINCRBY h f 2 and restarts
- **THEN** SMEMBERS s replies b and HGET h f replies 3

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
