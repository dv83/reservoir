# Key Expiry Specification

## Purpose
Defines how keys acquire, lose and honor time-to-live deadlines on a single node.

## Requirements

### Requirement: Setting and removing a TTL
EXPIRE SHALL set an absolute deadline on an existing key and reply 1, or reply 0 for a missing key. PERSIST SHALL remove a key's TTL. A plain SET SHALL clear the key's TTL; SET with KEEPTTL and APPEND SHALL keep it.

#### Scenario: EXPIRE on a missing key
- **WHEN** a client runs EXPIRE on a key that does not exist
- **THEN** the reply is 0

#### Scenario: SET clears the TTL
- **WHEN** a key has a TTL and a client runs SET on it without TTL options
- **THEN** the key no longer has a TTL

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

### Requirement: Background reclamation
The store SHALL physically remove expired keys and release their memory in the background.

#### Scenario: Expired key is reclaimed
- **WHEN** a key's deadline has passed and the background reaper has run
- **THEN** the key no longer counts toward key count or memory usage
