package cluster

// ReplicationHandler is an interface that the store must implement to handle replication
type ReplicationHandler interface {
	// ApplyReplication applies a replicated operation
	ApplyReplication(event ReplicationEvent) error
}
