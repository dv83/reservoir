# Set Commands Specification

## Purpose
Defines the Redis-compatible behavior of set keys and set algebra.

## Requirements

### Requirement: Membership changes
SADD SHALL add members and reply with the number newly added; SREM SHALL remove members and reply with the number removed. SPOP SHALL remove and return one member, or reply null when the set is empty or missing.

#### Scenario: Adding a duplicate
- **WHEN** a set holds a and a client runs SADD k a b
- **THEN** the reply is 1

### Requirement: Membership queries
SISMEMBER, SCARD and SMEMBERS SHALL report membership, cardinality and all members; a missing key SHALL behave as an empty set.

#### Scenario: Cardinality of a missing key
- **WHEN** a client runs SCARD on a key that does not exist
- **THEN** the reply is 0

### Requirement: Set algebra
SDIFF, SINTER and SUNION SHALL return the difference, intersection and union of the given sets. SDIFFSTORE SHALL store the difference in the destination key and reply with its cardinality.

#### Scenario: Difference
- **WHEN** k1 holds a, b, c and k2 holds b
- **THEN** SDIFF k1 k2 returns a and c
