# Spec Delta

## Purpose
Lets a node serve a dataset whose values exceed its RAM by keeping cold values on disk behind an in-RAM key index and hot-value cache.

## ADDED Requirements

### Requirement: Values spill to disk at the RAM budget
When tiering is enabled, a write that would exceed the RAM budget SHALL succeed by keeping values on disk rather than failing, as long as the disk budget and the key index budget allow it.

#### Scenario: Dataset larger than RAM
- **WHEN** tiering is enabled and clients write more value data than the RAM budget
- **THEN** every write succeeds and every key remains readable

### Requirement: Reads are tier-transparent
A read SHALL return the same value and type whether the value is cached in RAM or stored on disk, and a value read from disk SHALL be verified against its checksum.

#### Scenario: Reading a cold value
- **WHEN** a key's value has been moved out of RAM and a client runs GET on it
- **THEN** the reply is the key's current value

#### Scenario: Corrupt value on disk
- **WHEN** a cold value fails its checksum on read
- **THEN** the read fails with an error instead of returning corrupt data

### Requirement: Moving data between tiers is not a write
Moving a value between RAM and disk SHALL NOT change the key's value, TTL or conflict-resolution stamp, and SHALL NOT produce a tombstone or a replication event.

#### Scenario: Spill in a cluster
- **WHEN** a node moves a value from RAM to disk
- **THEN** no replication event is sent and peers' state is unchanged

### Requirement: Key index bound
With tiering enabled, the number of keys SHALL remain bounded by the key index budget in RAM, and a write that would create a key beyond it SHALL fail with a memory-limit error.

#### Scenario: Key index full
- **WHEN** the key index budget is exhausted and a client creates a new key
- **THEN** the write fails with a memory-limit error

### Requirement: Restart rebuilds the index
On startup with tiering enabled, the node SHALL rebuild its key index from the on-disk segments so that every key written before the restart is readable afterwards.

#### Scenario: Restart with cold data
- **WHEN** a node with values on disk restarts
- **THEN** all previously written keys are readable after startup
