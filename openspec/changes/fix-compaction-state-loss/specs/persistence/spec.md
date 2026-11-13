# Spec Delta

## MODIFIED Requirements

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
