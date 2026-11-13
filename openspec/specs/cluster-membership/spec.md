# Cluster Membership Specification

## Purpose
Defines how nodes form a cluster, authenticate each other, detect failures and elect a leader.

## Requirements

### Requirement: Joining through a seed
A node started with a seed address SHALL join that seed's cluster, and every node SHALL eventually know every other node.

#### Scenario: Three-node mesh
- **WHEN** two nodes join a third through it as their seed
- **THEN** each of the three nodes lists all three nodes

### Requirement: Inter-node authentication
When a cluster secret is configured, inter-node messages SHALL be authenticated with an HMAC and messages that fail authentication SHALL be rejected. Without a secret, traffic SHALL be unauthenticated and the node SHALL log a warning at startup.

#### Scenario: Wrong secret
- **WHEN** a node sends messages signed with a different secret
- **THEN** the receiving node rejects them

### Requirement: Failure detection
A node SHALL treat a peer it has not heard from within the node timeout as failed and remove it from its cluster view.

#### Scenario: Peer stops
- **WHEN** a peer stops sending heartbeats for longer than the node timeout
- **THEN** the remaining nodes no longer list it as healthy

### Requirement: Leader election
Nodes SHALL elect a leader by term-based voting in which a node grants at most one vote per term, persisting its term and vote before replying so a restart cannot cause a second vote in the same term. Accepting writes SHALL NOT depend on having a leader.

#### Scenario: Restart mid-term
- **WHEN** a node that voted in term T restarts and receives another vote request for term T
- **THEN** it does not grant a second vote
