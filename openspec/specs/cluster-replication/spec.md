# Cluster Replication Specification

## Purpose
Defines multi-master replication: every node accepts writes, and all replicas converge to the same state without coordination.

## Requirements

### Requirement: Multi-master writes
Every node SHALL accept writes and replicate them asynchronously to all other nodes.

#### Scenario: Write on any node
- **WHEN** a client writes a key on any node of a healthy cluster
- **THEN** the key becomes readable with the same value on every node

### Requirement: Order-independent convergence
Replicas that receive the same set of writes SHALL converge to the same state regardless of delivery order or duplicate delivery.

#### Scenario: Reordered delivery
- **WHEN** two replicas apply the same writes in different orders
- **THEN** they end in the same state

### Requirement: Per-type conflict resolution
Concurrent writes SHALL resolve as follows. Strings: the latest hybrid-logical-clock stamp wins, ties broken by node id. Set elements: present unless removed by a strictly newer stamp. Counters: increments from all nodes are preserved. Hash fields: latest write per field wins. Lists: all pushed elements survive in one deterministic order.

#### Scenario: Concurrent counter increments
- **WHEN** two nodes each increment the same counter concurrently
- **THEN** every node converges to the sum of both increments

#### Scenario: Concurrent list pushes
- **WHEN** two nodes push different elements to the same list concurrently
- **THEN** every node holds all elements in the same order

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

### Requirement: FLUSHALL converges
In cluster mode FLUSHALL SHALL remove, on every node, every key and element written before it, including on nodes that missed the replicated event, and SHALL NOT remove writes stamped after it.

#### Scenario: Peer missed the FLUSHALL
- **WHEN** a client runs FLUSHALL on one node and a peer never receives the event
- **THEN** after anti-entropy every node is empty and no flushed key returns

#### Scenario: Write after FLUSHALL survives
- **WHEN** a write stamped after a FLUSHALL reaches a node before the FLUSHALL does
- **THEN** after both arrive the node holds that write

### Requirement: TTL convergence
TTL changes SHALL replicate as a per-key last-write-wins register: a write newer than the last TTL change clears the TTL, an older one does not, and a key reclaimed by expiry SHALL NOT be resurrected by peers.

#### Scenario: SET with EX in a cluster
- **WHEN** a client runs SET k v EX 3600 on one node
- **THEN** k has a TTL of about one hour on every node

### Requirement: Anti-entropy
Nodes SHALL periodically exchange per-shard state digests with peers and reconcile them under the same conflict rules, repairing divergence caused by lost replication events.

#### Scenario: Missed replication event
- **WHEN** a node missed a replicated write
- **THEN** after reconciliation it holds the same state as its peers

### Requirement: Tombstone retention
Tombstones SHALL be retained for a retention window of at least one hour and then reclaimed. Until then an emptied set, hash or list MAY remain as an empty key that holds tombstones.

#### Scenario: Emptied container is reclaimed
- **WHEN** every element of a set was removed and the retention window has passed
- **THEN** the key is removed from the keyspace
