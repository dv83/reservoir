package cluster

// ReplicationHandler is an interface that the store must implement to handle
// replication and anti-entropy reconciliation.
type ReplicationHandler interface {
	// ApplyReplication applies a replicated operation.
	ApplyReplication(event ReplicationEvent) error
	// ShardCount reports how many shards the store has, so anti-entropy can
	// rotate over them.
	ShardCount() int
	// LocalDigest returns the LWW state of one shard (values and tombstones with
	// their stamps) for anti-entropy reconciliation.
	LocalDigest(shardIdx int) []SyncEntry
}

// SyncEntry is one key's LWW state exchanged during anti-entropy: a value with
// its HLC stamp, or a tombstone (Deleted=true).
type SyncEntry struct {
	Key         string `json:"key"`
	Value       []byte `json:"value,omitempty"`
	HLCPhysical int64  `json:"hlc_p"`
	HLCLogical  uint32 `json:"hlc_l"`
	HLCOrigin   uint64 `json:"hlc_o"`
	Deleted     bool   `json:"deleted,omitempty"`
}

// SyncRequest asks a peer for the LWW digest of one shard.
type SyncRequest struct {
	NodeID   UUIDv7 `json:"node_id"`
	ShardIdx int    `json:"shard_idx"`
}

// SyncResponse carries a shard's LWW digest back to the requester.
type SyncResponse struct {
	NodeID   UUIDv7      `json:"node_id"`
	ShardIdx int         `json:"shard_idx"`
	Entries  []SyncEntry `json:"entries"`
}
