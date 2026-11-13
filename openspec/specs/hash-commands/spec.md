# Hash Commands Specification

## Purpose
Defines the Redis-compatible behavior of hash keys and their field operations.

## Requirements

### Requirement: Field writes and deletes
HSET and HMSET SHALL set the given field-value pairs and HSET SHALL reply with the number of fields newly created. HDEL SHALL remove the given fields and reply with the number removed.

#### Scenario: Updating an existing field
- **WHEN** a hash has field f and a client runs HSET k f v2 g v3
- **THEN** the reply is 1 and f holds v2

### Requirement: Field reads
HGET, HMGET, HEXISTS, HLEN, HKEYS, HVALS and HGETALL SHALL report field values, existence, count, names, values and all pairs; a missing key SHALL behave as an empty hash.

#### Scenario: Missing field
- **WHEN** a client runs HGET k missing
- **THEN** the reply is null

### Requirement: Numeric field increments
HINCRBY and HINCRBYFLOAT SHALL treat a missing field as 0, store and return the incremented value, and fail without change when the field does not hold a number of the required kind.

#### Scenario: Increment of a non-numeric field
- **WHEN** a field holds abc and a client runs HINCRBY k f 1
- **THEN** the command fails and the field still holds abc
