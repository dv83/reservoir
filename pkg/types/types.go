package types

import (
	"sync"
	"time"
)

// NodeStatus represents the status of a cluster node
type NodeStatus int

const (
	NodeStatusAlive NodeStatus = iota
	NodeStatusSuspected
	NodeStatusDead
	NodeStatusJoining
	NodeStatusLeaving
)

func (ns NodeStatus) String() string {
	switch ns {
	case NodeStatusAlive:
		return "alive"
	case NodeStatusSuspected:
		return "suspected"
	case NodeStatusDead:
		return "dead"
	case NodeStatusJoining:
		return "joining"
	case NodeStatusLeaving:
		return "leaving"
	default:
		return "unknown"
	}
}

// PeerNode represents a cluster peer
type PeerNode struct {
	ID       string
	Addr     string
	Port     int
	Status   NodeStatus
	LastSeen time.Time
	Version  uint64
}

// Operation represents a replication operation type
type Operation int

const (
	OpSet Operation = iota
	OpDelete
	OpExpire
	OpClear
	OpTombstone
)

func (op Operation) String() string {
	switch op {
	case OpSet:
		return "SET"
	case OpDelete:
		return "DELETE"
	case OpExpire:
		return "EXPIRE"
	case OpClear:
		return "CLEAR"
	case OpTombstone:
		return "TOMBSTONE"
	default:
		return "UNKNOWN"
	}
}

// VectorClock represents a logical timestamp for distributed systems
type VectorClock map[string]uint64

func (vc VectorClock) Update(nodeID string) VectorClock {
	newVC := make(VectorClock)
	for k, v := range vc {
		newVC[k] = v
	}
	newVC[nodeID]++
	return newVC
}

func (vc VectorClock) Copy() VectorClock {
	newVC := make(VectorClock)
	for k, v := range vc {
		newVC[k] = v
	}
	return newVC
}

// ClockRelation represents the relationship between two vector clocks
type ClockRelation int

const (
	ClockBefore ClockRelation = iota
	ClockAfter
	ClockConcurrent
	ClockEqual
)

func (vc VectorClock) Compare(other VectorClock) ClockRelation {
	allLessEqual := true
	allGreaterEqual := true
	equal := true

	// Get all node IDs from both clocks
	nodeIDs := make(map[string]struct{})
	for nodeID := range vc {
		nodeIDs[nodeID] = struct{}{}
	}
	for nodeID := range other {
		nodeIDs[nodeID] = struct{}{}
	}

	for nodeID := range nodeIDs {
		thisValue := vc[nodeID]
		otherValue := other[nodeID]

		if thisValue < otherValue {
			allGreaterEqual = false
		}
		if thisValue > otherValue {
			allLessEqual = false
		}
		if thisValue != otherValue {
			equal = false
		}
	}

	if equal {
		return ClockEqual
	} else if allLessEqual {
		return ClockBefore
	} else if allGreaterEqual {
		return ClockAfter
	} else {
		return ClockConcurrent
	}
}

// LogEntry represents a single entry in the replication log
type LogEntry struct {
	Index       uint64
	Timestamp   time.Time
	NodeID      string
	Operation   Operation
	Key         string
	Value       string
	VectorClock VectorClock
}

// Tombstone represents a deleted key with metadata for preventing resurrection
type Tombstone struct {
	Key         string        // The deleted key
	DeletedAt   time.Time     // When the key was deleted
	NodeID      string        // Node that performed the deletion
	VectorClock VectorClock   // Vector clock at time of deletion
	TTL         time.Duration // How long to keep the tombstone
	ExpiresAt   time.Time     // Calculated expiration time
}

// IsExpired checks if the tombstone has exceeded its TTL
func (t *Tombstone) IsExpired() bool {
	return time.Now().After(t.ExpiresAt)
}

// TombstoneManager handles the storage and cleanup of tombstones
type TombstoneEntry struct {
	Key       string
	Tombstone Tombstone
	CreatedAt time.Time
}

// MessageType defines the types of messages in the replication protocol
type MessageType int

const (
	MsgHeartbeat MessageType = iota
	MsgReplication
	MsgAntiEntropy
	MsgClusterJoin
	MsgClusterLeave
	MsgBatch
	MsgAck
	MsgMerkleSync
	MsgMerkleRequest
	MsgMerkleResponse
)

func (mt MessageType) String() string {
	switch mt {
	case MsgHeartbeat:
		return "HEARTBEAT"
	case MsgReplication:
		return "REPLICATION"
	case MsgAntiEntropy:
		return "ANTI_ENTROPY"
	case MsgClusterJoin:
		return "CLUSTER_JOIN"
	case MsgClusterLeave:
		return "CLUSTER_LEAVE"
	case MsgBatch:
		return "BATCH"
	case MsgAck:
		return "ACK"
	case MsgMerkleSync:
		return "MERKLE_SYNC"
	case MsgMerkleRequest:
		return "MERKLE_REQUEST"
	case MsgMerkleResponse:
		return "MERKLE_RESPONSE"
	default:
		return "UNKNOWN"
	}
}

// ReplicationMessage represents a message sent between cluster nodes
type ReplicationMessage struct {
	Type        MessageType
	NodeID      string
	VectorClock VectorClock
	Entries     []LogEntry
	Timestamp   time.Time
	BatchID     uint64
}

// ReplicationStats represents replication statistics
type ReplicationStats struct {
	QueueDepth      int              `json:"queue_depth"`
	QueueCapacity   int              `json:"queue_capacity"`
	SpilloverActive bool             `json:"spillover_active"`
	SpilloverSize   int              `json:"spillover_size"`
	ProcessingRate  float64          `json:"processing_rate"`
	AvgLatency      time.Duration    `json:"avg_latency"`
	LockContention  float64          `json:"lock_contention"`
	PeerLag         map[string]int64 `json:"peer_lag"`
	LastUpdate      time.Time        `json:"last_update"`
}

// Shard represents a single data shard
type Shard struct {
	Mu          sync.RWMutex
	Data        map[string]string
	Expiry      map[string]time.Time
	MemoryUsage int64
}
