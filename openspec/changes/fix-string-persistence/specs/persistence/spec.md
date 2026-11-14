# Spec Delta

## MODIFIED Requirements

### Requirement: Write-ahead logging
When the commit log is enabled, string, counter and TTL mutations — including APPEND and GETSET — SHALL be appended to a segmented log in which each entry carries a CRC32 checksum; set and hash mutations SHALL be appended when the node runs in cluster mode. Writes replayed during recovery SHALL NOT be logged again.

#### Scenario: Logged write
- **WHEN** the commit log is enabled and a client runs SET k v
- **THEN** a SET entry for k with a valid checksum is appended to the log

#### Scenario: APPEND is logged
- **WHEN** the commit log is enabled and a client runs APPEND k x
- **THEN** the log records the key's resulting value

## ADDED Requirements

### Requirement: String keys keep their TTL across a restart
After recovery, every string key SHALL have the value and TTL it had when the node stopped, including keys last written by SET with EX, PX or KEEPTTL, by APPEND, or by a counter command.

#### Scenario: SET with EX survives a restart
- **WHEN** a client runs SET k v EX 3600 and the node restarts
- **THEN** k holds v and still has a TTL of about one hour

#### Scenario: INCR keeps the TTL across a restart
- **WHEN** a key with a TTL is incremented with INCR and the node restarts
- **THEN** the key holds the incremented value and keeps its TTL
