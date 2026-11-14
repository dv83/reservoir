# Spec Delta

## MODIFIED Requirements

### Requirement: In-place string edits
APPEND SHALL append to the existing value (creating it if absent), keep the key's TTL and reply with the new length. GETSET SHALL reply with the previous value, store the new one and discard the key's TTL, as SET does.

#### Scenario: APPEND keeps the TTL
- **WHEN** a client runs APPEND on a key that has a TTL
- **THEN** the value is extended and the key keeps its TTL

#### Scenario: GETSET discards the TTL
- **WHEN** a client runs GETSET on a key that has a TTL
- **THEN** the reply is the previous value and the key no longer has a TTL
