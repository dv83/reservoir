# Spec Delta

## ADDED Requirements

### Requirement: Acknowledged writes are never dropped from the log
When the commit log cannot keep up, a write SHALL wait until its entry is queued rather than drop it; every write acknowledged to a client SHALL be in the log once the log is closed. A write after the log is closed SHALL fail.

#### Scenario: Writer falls behind
- **WHEN** the log's writer stalls and the write queue fills while clients keep writing
- **THEN** the writes slow down and every acknowledged write and delete is in the log after it closes

### Requirement: Compaction memory is bounded by the number of keys
Compaction SHALL hold per-key metadata only, never the logged values, so its memory does not grow with the size of the log.

#### Scenario: Compacting a large log
- **WHEN** compaction rewrites segments holding 80MB of values for a few keys
- **THEN** the heap grows by a small fraction of the log size during compaction
