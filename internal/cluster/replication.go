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

// SyncEntry is one LWW-stamped fact exchanged during anti-entropy. With
// Member=="" it is a string key's state — a value with its HLC stamp, or a
// tombstone (Deleted=true). With Member!="" it is a single set element that is
// present (Deleted=false, stamp is its add) or tombstoned (Deleted=true, stamp
// is its remove).
type SyncEntry struct {
	Key         string `json:"key"`
	Member      string `json:"member,omitempty"`
	Value       []byte `json:"value,omitempty"`
	HLCPhysical int64  `json:"hlc_p"`
	HLCLogical  uint32 `json:"hlc_l"`
	HLCOrigin   uint64 `json:"hlc_o"`
	Deleted     bool   `json:"deleted,omitempty"`
	// Counter marks a PN-counter key: Value holds the encoded counter state,
	// reconciled by max-merge instead of the LWW stamp.
	Counter bool `json:"counter,omitempty"`
	// Hash marks a hash field entry: Member is the field, Value its value
	// (empty when Deleted), reconciled by the per-field LWW stamp.
	Hash bool `json:"hash,omitempty"`
	// List marks a list key: Value holds the encoded RGA element set, reconciled
	// by merging the delta.
	List bool `json:"list,omitempty"`
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
