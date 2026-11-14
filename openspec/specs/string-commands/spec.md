# String Commands Specification

## Purpose
Defines the Redis-compatible behavior of string keys: SET with options, reads, deletes, counters and in-place string edits.

## Requirements

### Requirement: SET with options
SET SHALL store the value and clear any TTL on the key. With NX it SHALL apply only if the key is absent, with XX only if present, replying null when the condition is not met. EX/PX SHALL set a TTL in seconds/milliseconds and KEEPTTL SHALL keep the existing TTL. Combining NX with XX, or EX/PX with KEEPTTL, SHALL fail with a syntax error.

#### Scenario: NX on an existing key
- **WHEN** a client runs SET k v NX and k already exists
- **THEN** the reply is null
- **AND** the stored value is unchanged

#### Scenario: SET with EX
- **WHEN** a client runs SET k v EX 60
- **THEN** k holds v with a TTL of about 60 seconds

### Requirement: Integer counters
INCR, DECR, INCRBY and DECRBY SHALL treat a missing key as 0, store and return the new integer value, reject a value that is not an integer with a not-an-integer error, and reject a result that would overflow a signed 64-bit integer without changing the stored value.

#### Scenario: Increment of a missing key
- **WHEN** a client runs INCR on a key that does not exist
- **THEN** the reply is 1 and the key holds "1"

#### Scenario: Overflow is rejected
- **WHEN** a key holds the maximum signed 64-bit integer and a client runs INCR on it
- **THEN** the command fails with an overflow error and the value is unchanged

### Requirement: Deletes and multi-key writes
DEL SHALL remove the given keys and reply with the number of keys removed. MSET SHALL set every given key to its paired value.

#### Scenario: Deleting existing and missing keys
- **WHEN** a client runs DEL a b and only a exists
- **THEN** the reply is 1 and a no longer exists

### Requirement: In-place string edits
APPEND SHALL append to the existing value (creating it if absent), keep the key's TTL and reply with the new length. GETSET SHALL reply with the previous value, store the new one and discard the key's TTL, as SET does.

#### Scenario: APPEND keeps the TTL
- **WHEN** a client runs APPEND on a key that has a TTL
- **THEN** the value is extended and the key keeps its TTL

#### Scenario: GETSET discards the TTL
- **WHEN** a client runs GETSET on a key that has a TTL
- **THEN** the reply is the previous value and the key no longer has a TTL
