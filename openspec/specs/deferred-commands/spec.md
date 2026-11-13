# Deferred Commands Specification

## Purpose
Defines DEFER, which schedules a write command to run after a delay.

## Requirements

### Requirement: Scheduling deferred writes
DEFER <seconds> <command> <key> [args] SHALL schedule SET, DEL, INCR, DECR, INCRBY, DECRBY or APPEND to run after the delay and reply with the scheduled command's id. Other commands and negative delays SHALL be rejected.

#### Scenario: Deferred SET
- **WHEN** a client runs DEFER 5 SET k v
- **THEN** the reply is an id and k holds v about 5 seconds later

#### Scenario: Read command cannot be deferred
- **WHEN** a client runs DEFER 5 GET k
- **THEN** the command fails

### Requirement: Cancellation
DEFER.CANCEL <id> SHALL cancel a pending deferred command so that it never runs.

#### Scenario: Cancel before execution
- **WHEN** a deferred command is cancelled before its delay elapses
- **THEN** its effect never happens

### Requirement: Exactly-once execution in a cluster
In cluster mode a deferred command SHALL run exactly once, on the node that accepted it, with its effect replicated like a client write and templates expanded once. The command SHALL be lost if that node fails before it is due.

#### Scenario: Deferred increment in a cluster
- **WHEN** a deferred INCR runs on a three-node cluster
- **THEN** every node converges to an increment of exactly 1
