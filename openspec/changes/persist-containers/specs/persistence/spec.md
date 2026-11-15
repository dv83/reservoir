# Spec Delta

## MODIFIED Requirements

### Requirement: Write-ahead logging
When the commit log is enabled, string, counter, TTL, list, set and hash mutations — including APPEND and GETSET — SHALL be appended to a segmented log in which each entry carries a CRC32 checksum, on standalone and cluster nodes alike. Writes replayed during recovery SHALL NOT be logged again.

#### Scenario: Logged write
- **WHEN** the commit log is enabled and a client runs SET k v
- **THEN** a SET entry for k with a valid checksum is appended to the log

#### Scenario: APPEND is logged
- **WHEN** the commit log is enabled and a client runs APPEND k x
- **THEN** the log records the key's resulting value

## ADDED Requirements

### Requirement: Containers survive a restart
After recovery, every list, set and hash SHALL hold exactly the elements, order and field values it held when the node stopped. On a cluster node a recovered list SHALL keep the element identities its peers know, so anti-entropy does not duplicate elements.

#### Scenario: Standalone list survives a restart
- **WHEN** a standalone node runs RPUSH l a b c, LPOP l, LSET l 0 x and restarts
- **THEN** LRANGE l 0 -1 replies x c

#### Scenario: Standalone set and hash survive a restart
- **WHEN** a standalone node runs SADD s a b, SREM s a, HSET h f 1, HINCRBY h f 2 and restarts
- **THEN** SMEMBERS s replies b and HGET h f replies 3
