# Spec Delta

## MODIFIED Requirements

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
