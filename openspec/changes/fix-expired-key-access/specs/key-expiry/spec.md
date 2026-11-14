# Spec Delta

## RENAMED Requirements

- FROM: `### Requirement: Expired string keys are not served`
- TO: `### Requirement: Expired keys are not served`

## MODIFIED Requirements

### Requirement: Expired keys are not served
Once a key's deadline has passed, every command SHALL treat the key as missing, even before the background reaper has removed it: reads SHALL reply as for a missing key, conditional writes SHALL see the key as absent, and writes SHALL start from an empty key with no TTL. A command that addresses an expired key SHALL remove it.

#### Scenario: Read after the deadline
- **WHEN** a key's deadline has passed but it has not been reclaimed yet
- **THEN** GET replies as if the key does not exist

#### Scenario: Container read after the deadline
- **WHEN** a set's deadline has passed but it has not been reclaimed yet
- **THEN** SMEMBERS replies with an empty array and SCARD replies 0

#### Scenario: Write after the deadline starts fresh
- **WHEN** a counter's deadline has passed and a client runs INCR on it
- **THEN** the reply is 1 and the key has no TTL

#### Scenario: EXPIRE does not revive a key
- **WHEN** a key's deadline has passed and a client runs EXPIRE on it
- **THEN** the reply is 0 and the key does not exist

#### Scenario: Access reclaims the key
- **WHEN** a client runs any command on an expired key
- **THEN** the key no longer counts toward key count or memory usage
