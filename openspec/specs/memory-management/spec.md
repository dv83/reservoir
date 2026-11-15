# Memory Management Specification

## Purpose
Defines how the in-memory store bounds, validates and accounts for memory, and how type safety is enforced on keys.

## Requirements

### Requirement: Memory limit enforcement
With tiering disabled, the store SHALL reject any write that would grow total memory beyond the configured maximum with a memory-limit error, leaving existing data unchanged (noeviction). With tiering enabled, the maximum SHALL bound RAM use and reaching it SHALL move values to disk instead of rejecting writes. Writes that do not grow memory SHALL always be accepted.

#### Scenario: Write over the limit is rejected
- **WHEN** tiering is disabled and a write would raise total memory above the configured maximum
- **THEN** the write fails with a memory-limit error
- **AND** the previously stored value is unchanged

#### Scenario: Delete is accepted at the limit
- **WHEN** total memory is at the configured maximum and a client deletes a key
- **THEN** the delete succeeds and memory usage decreases

#### Scenario: Write over the limit spills with tiering
- **WHEN** tiering is enabled and a write would raise RAM use above the configured maximum
- **THEN** the write succeeds and colder values are moved to disk

### Requirement: Key and value size limits
The store SHALL reject a key longer than the configured maximum key size and a value larger than the configured maximum value size.

#### Scenario: Oversized value
- **WHEN** a client writes a value larger than the maximum value size
- **THEN** the write fails with a value-too-large error

### Requirement: Accurate memory and key accounting
The store SHALL maintain per-shard and global memory usage and key counts such that, after any sequence of writes and deletes, the totals equal the sum over the keys actually present.

#### Scenario: Create then delete returns to baseline
- **WHEN** a key of any type is created, modified and then deleted
- **THEN** total key count and memory usage return to their values before the key was created

### Requirement: Type safety
A command applied to a key holding a different data type SHALL fail with a WRONGTYPE error and SHALL NOT modify the key.

#### Scenario: List command on a string key
- **WHEN** a client runs LPUSH on a key that holds a string
- **THEN** the command fails with a WRONGTYPE error
- **AND** the string value is unchanged
