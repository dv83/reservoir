# Observability Specification

## Purpose
Defines the HTTP endpoints that expose store and cluster state for operators.

## Requirements

### Requirement: CRDT state endpoint
GET /api/crdt SHALL return the node's CRDT statistics: tombstone counts per data type, the number of keys holding CRDT state and live element counts. In cluster mode it SHALL also return anti-entropy activity counters and replication queue statistics.

#### Scenario: Standalone node
- **WHEN** an operator requests /api/crdt on a node without clustering
- **THEN** the response contains store statistics and no anti-entropy section

### Requirement: Cluster nodes endpoint
GET /api/nodes SHALL return the local node, all known nodes and replication statistics, and SHALL reply 503 when clustering is disabled.

#### Scenario: Clustering disabled
- **WHEN** an operator requests /api/nodes on a standalone node
- **THEN** the response status is 503

### Requirement: Memory map endpoint
GET /api/memory-map SHALL return keys with their type and size, ordered by size descending, listing at most 200 keys individually and aggregating the rest into one entry.

#### Scenario: Many keys
- **WHEN** the store holds more than 200 keys
- **THEN** the response lists the 200 largest keys and one aggregated entry for the rest
