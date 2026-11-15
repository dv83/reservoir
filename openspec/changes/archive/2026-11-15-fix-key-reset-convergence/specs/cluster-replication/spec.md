# Spec Delta

## MODIFIED Requirements

### Requirement: Deletes cannot be undone by stale writes
Deleting a key of any type, a set element, a hash field or a list element SHALL leave a tombstone so that a delayed older write cannot resurrect it, while any strictly newer write SHALL still apply. For lists and counters, whose elements carry no comparable stamps, a write SHALL count as older when it belongs to an incarnation created before the delete.

#### Scenario: Delayed older write
- **WHEN** a key is deleted and an older write to it arrives afterwards
- **THEN** the key stays deleted

#### Scenario: Stale set write after DEL
- **WHEN** a set is deleted and an SADD stamped before the delete arrives later
- **THEN** the set stays deleted

#### Scenario: Stale counter state after DEL
- **WHEN** a counter is deleted and a peer later re-sends its pre-delete counter state
- **THEN** the counter stays deleted on every node

#### Scenario: Newer write after DEL
- **WHEN** a set is deleted and an SADD stamped after the delete arrives
- **THEN** the set holds only the newer member

## ADDED Requirements

### Requirement: FLUSHALL converges
In cluster mode FLUSHALL SHALL remove, on every node, every key and element written before it, including on nodes that missed the replicated event, and SHALL NOT remove writes stamped after it.

#### Scenario: Peer missed the FLUSHALL
- **WHEN** a client runs FLUSHALL on one node and a peer never receives the event
- **THEN** after anti-entropy every node is empty and no flushed key returns

#### Scenario: Write after FLUSHALL survives
- **WHEN** a write stamped after a FLUSHALL reaches a node before the FLUSHALL does
- **THEN** after both arrive the node holds that write
