# List Commands Specification

## Purpose
Defines the Redis-compatible behavior of list keys, including the reservoir-specific unique-push commands.

## Requirements

### Requirement: Push and pop order
LPUSH SHALL insert each argument at the head in argument order and RPUSH SHALL append each at the tail, both replying with the new length. LPOP and RPOP SHALL remove and return up to count elements from the head or tail; a negative count SHALL fail with an out-of-range error.

#### Scenario: LPUSH ordering
- **WHEN** a client runs LPUSH k a b c on an empty key
- **THEN** LRANGE k 0 -1 returns c, b, a

#### Scenario: Pop with count
- **WHEN** a list holds a, b, c and a client runs LPOP k 2
- **THEN** the reply is a, b and the list holds c

### Requirement: Conditional pushes
LPUSHX and RPUSHX SHALL push only when the list already exists and SHALL NOT create a missing key, replying 0. LPUSHUNIQUE and RPUSHUNIQUE SHALL push only the arguments not already present in the list.

#### Scenario: LPUSHX on a missing key
- **WHEN** a client runs LPUSHX on a key that does not exist
- **THEN** the reply is 0 and the key is not created

#### Scenario: Unique push skips duplicates
- **WHEN** a list holds a, b and a client runs RPUSHUNIQUE k a c
- **THEN** only c is appended

### Requirement: Index-based access
LINDEX, LRANGE, LSET and LTRIM SHALL accept negative indices counted from the tail. LSET SHALL fail with a no-such-key error on a missing key and an index-out-of-range error for an index outside the list.

#### Scenario: Negative index
- **WHEN** a list holds a, b, c and a client runs LINDEX k -1
- **THEN** the reply is c

### Requirement: Removal by value
LREM SHALL remove elements equal to the given value: up to count from the head for a positive count, up to count from the tail for a negative count, and all of them for zero, replying with the number removed.

#### Scenario: Remove all occurrences
- **WHEN** a list holds a, b, a and a client runs LREM k 0 a
- **THEN** the reply is 2 and the list holds b
