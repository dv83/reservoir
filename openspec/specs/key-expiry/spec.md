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

### Requirement: Expired string keys are not served
GET SHALL NOT return a key whose deadline has passed, even before the key is physically removed.

#### Scenario: Read after the deadline
- **WHEN** a key's deadline has passed but it has not been reclaimed yet
- **THEN** GET replies as if the key does not exist

### Requirement: Background reclamation
The store SHALL physically remove expired keys and release their memory in the background.

#### Scenario: Expired key is reclaimed
- **WHEN** a key's deadline has passed and the background reaper has run
- **THEN** the key no longer counts toward key count or memory usage
