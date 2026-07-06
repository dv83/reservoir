package cluster

import (
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"reservoir/pkg/logger"
	"reservoir/pkg/types"

	"github.com/google/uuid"
)

// NodeState represents the state of a node in the cluster
type NodeState int

const (
	StateFollower NodeState = iota
	StateCandidate
	StateLeader
	StateOffline
)

// Node represents a single node in the cluster
type Node struct {
	// Identity
	NodeID    UUIDv7 `json:"node_id"`
	ClusterID UUIDv7 `json:"cluster_id"`
	Address   string `json:"address"`
	Port      int    `json:"port"`

	// State
	State         atomic.Value // NodeState
	LastHeartbeat atomic.Value // time.Time
	IsAlive       atomic.Bool

	// Leadership (inspired by Raft)
	CurrentTerm atomic.Uint64
	VotedFor    atomic.Value // *UUIDv7 (may wrap a nil pointer meaning "no vote")
	LeaderID    atomic.Value // *UUIDv7

	// Replication state (optimized lock-free vector clock)
	OptimizedVectorClock *types.OptimizedVectorClock

	// Legacy vector clock for backward compatibility (will be removed)
	VectorClock   map[UUIDv7]uint64
	vectorClockMu sync.RWMutex

	// Stats
	MessagesReceived atomic.Uint64
	MessagesSent     atomic.Uint64
	BytesReceived    atomic.Uint64
	BytesSent        atomic.Uint64
}

// SetVotedFor records a vote for the given candidate.
func (n *Node) SetVotedFor(id UUIDv7) {
	n.VotedFor.Store(&id)
}

// ClearVotedFor resets the vote. It stores a typed nil pointer rather than an
// untyped nil (atomic.Value panics on a nil interface store).
func (n *Node) ClearVotedFor() {
	n.VotedFor.Store((*UUIDv7)(nil))
}

// VotedForID returns the candidate this node voted for, or nil if it has not
// voted (never set, or cleared).
func (n *Node) VotedForID() *UUIDv7 {
	v := n.VotedFor.Load()
	if v == nil {
		return nil
	}
	id, _ := v.(*UUIDv7)
	return id
}

// NewNode creates a new cluster node with a fresh ID
func NewNode(address string, port int, clusterID UUIDv7) *Node {
	return NewNodeWithID(NewUUIDv7(), address, port, clusterID)
}

// NewNodeWithID creates a new cluster node with a specific ID
func NewNodeWithID(nodeID UUIDv7, address string, port int, clusterID UUIDv7) *Node {
	// Initialize optimized vector clock with error handling
	optimizedVC := types.NewOptimizedVectorClock(nodeID.ToBytes())
	if optimizedVC == nil {
		logger.Warning("Failed to create OptimizedVectorClock for node %s, cluster operations may be degraded", nodeID)
	}

	node := &Node{
		NodeID:               nodeID,
		ClusterID:            clusterID,
		Address:              address,
		Port:                 port,
		VectorClock:          make(map[UUIDv7]uint64),
		OptimizedVectorClock: optimizedVC,
	}

	node.State.Store(StateFollower)
	node.LastHeartbeat.Store(time.Now())
	node.IsAlive.Store(true)

	logger.Info("Created/Initialized node: ID=%s, Cluster=%s, Address=%s:%d, OptimizedVC=%v",
		node.NodeID, clusterID, address, port, optimizedVC != nil)

	return node
}

// newRemoteNode creates a bookkeeping Node for a peer. Unlike the local node it
// does NOT start an OptimizedVectorClock background goroutine: only the local
// node increments a local clock, and creating one OVC per remote peer — on
// every join and every node-update broadcast — leaked a goroutine each time the
// peer object was discarded or evicted by the health checker. The nil-OVC path
// is handled by UpdateVectorClock/GetVectorClock (they fall back to the legacy
// map).
func newRemoteNode(nodeID UUIDv7, address string, port int, clusterID UUIDv7) *Node {
	node := &Node{
		NodeID:      nodeID,
		ClusterID:   clusterID,
		Address:     address,
		Port:        port,
		VectorClock: make(map[UUIDv7]uint64),
		// OptimizedVectorClock intentionally left nil.
	}
	node.State.Store(StateFollower)
	node.LastHeartbeat.Store(time.Now())
	node.IsAlive.Store(true)
	return node
}

// GetState returns the current node state
func (n *Node) GetState() NodeState {
	return n.State.Load().(NodeState)
}

// SetState updates the node state
func (n *Node) SetState(state NodeState) {
	oldState := n.GetState()
	n.State.Store(state)
	logger.Info("Node %s state changed: %s -> %s", n.NodeID, oldState, state)
}

// UpdateHeartbeat updates the last heartbeat time
func (n *Node) UpdateHeartbeat() {
	// Note: In production, this could also use cached time,
	// but heartbeat updates are less frequent than writes
	n.LastHeartbeat.Store(time.Now())
	n.IsAlive.Store(true)
}

// GetLastHeartbeat returns the last heartbeat time
func (n *Node) GetLastHeartbeat() time.Time {
	return n.LastHeartbeat.Load().(time.Time)
}

// IsHealthy checks if the node is healthy based on heartbeat
func (n *Node) IsHealthy(timeout time.Duration) bool {
	lastBeat := n.GetLastHeartbeat()
	return time.Since(lastBeat) < timeout && n.IsAlive.Load()
}

// UpdateVectorClock updates the vector clock for a given node (optimized)
func (n *Node) UpdateVectorClock(nodeID UUIDv7, clock uint64) {
	// Safety check for nil pointer before using optimized vector clock
	if n.OptimizedVectorClock != nil {
		// Use optimized vector clock for new updates (non-blocking)
		n.OptimizedVectorClock.UpdateRemoteClock(nodeID.ToBytes(), clock)
	} else {
		logger.Warning("OptimizedVectorClock is nil for node %s, falling back to legacy clock", n.NodeID)
	}

	// Keep legacy vector clock updated for backward compatibility
	// TODO: Remove this once all code is migrated
	n.vectorClockMu.Lock()
	if current, exists := n.VectorClock[nodeID]; !exists || clock > current {
		n.VectorClock[nodeID] = clock
	}
	n.vectorClockMu.Unlock()
}

// GetVectorClock returns a copy of the vector clock (optimized)
func (n *Node) GetVectorClock() map[UUIDv7]uint64 {
	// Safety check for nil
	if n.OptimizedVectorClock == nil {
		// Fallback to legacy vector clock
		n.vectorClockMu.RLock()
		defer n.vectorClockMu.RUnlock()
		clock := make(map[UUIDv7]uint64, len(n.VectorClock))
		for k, v := range n.VectorClock {
			clock[k] = v
		}
		return clock
	}

	// Use optimized vector clock for reads (lock-free)
	snapshot := n.OptimizedVectorClock.GetSnapshot()

	// Convert back to UUIDv7 map for compatibility
	clock := make(map[UUIDv7]uint64, len(snapshot))
	for nodeIDBytes, clockVal := range snapshot {
		nodeID := UUIDv7(nodeIDBytes)
		clock[nodeID] = clockVal
	}
	return clock
}

// String returns string representation of NodeState
func (s NodeState) String() string {
	switch s {
	case StateFollower:
		return "FOLLOWER"
	case StateCandidate:
		return "CANDIDATE"
	case StateLeader:
		return "LEADER"
	case StateOffline:
		return "OFFLINE"
	default:
		return "UNKNOWN"
	}
}

// NodeInfo provides information about a node
type NodeInfo struct {
	NodeID        UUIDv7    `json:"node_id"`
	ClusterID     UUIDv7    `json:"cluster_id"`
	Address       string    `json:"address"`
	Port          int       `json:"port"`
	State         string    `json:"state"`
	IsAlive       bool      `json:"is_alive"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	CurrentTerm   uint64    `json:"current_term"`
	IsLeader      bool      `json:"is_leader"`
}

// GetInfo returns node information
func (n *Node) GetInfo() NodeInfo {
	leaderID := n.LeaderID.Load()
	isLeader := false
	if leaderID != nil && *leaderID.(*UUIDv7) == n.NodeID {
		isLeader = true
	}

	return NodeInfo{
		NodeID:        n.NodeID,
		ClusterID:     n.ClusterID,
		Address:       n.Address,
		Port:          n.Port,
		State:         n.GetState().String(),
		IsAlive:       n.IsAlive.Load(),
		LastHeartbeat: n.GetLastHeartbeat(),
		CurrentTerm:   n.CurrentTerm.Load(),
		IsLeader:      isLeader,
	}
}

// UUIDv7 wrapper around google/uuid for RFC-compliant UUIDv7 generation
type UUIDv7 [16]byte

// NewUUIDv7 generates a new RFC-compliant UUIDv7 using google/uuid
func NewUUIDv7() UUIDv7 {
	// Generate UUIDv7 using google/uuid (RFC-compliant)
	googleUUID := uuid.Must(uuid.NewV7())

	var uuidv7 UUIDv7
	copy(uuidv7[:], googleUUID[:])
	return uuidv7
}

// String returns the canonical string representation of UUIDv7
func (u UUIDv7) String() string {
	// Convert to google UUID for canonical formatting
	googleUUID := uuid.UUID(u)
	return googleUUID.String()
}

// MarshalJSON implements json.Marshaler to ensure UUIDv7 is serialized as a string
func (u UUIDv7) MarshalJSON() ([]byte, error) {
	return []byte("\"" + u.String() + "\""), nil
}

// Timestamp returns the timestamp portion of the UUIDv7
func (u UUIDv7) Timestamp() time.Time {
	// Extract timestamp from first 48 bits (6 bytes)
	ms := binary.BigEndian.Uint64(append([]byte{0, 0}, u[0:6]...))
	return time.UnixMilli(int64(ms))
}

// Compare compares two UUIDv7s (useful for sorting)
func (u UUIDv7) Compare(other UUIDv7) int {
	for i := 0; i < 16; i++ {
		if u[i] < other[i] {
			return -1
		}
		if u[i] > other[i] {
			return 1
		}
	}
	return 0
}

// ToBytes converts UUIDv7 to byte array for optimized vector clock
func (u UUIDv7) ToBytes() [16]byte {
	return [16]byte(u)
}

// IsValid checks if the UUID is a valid UUIDv7
func (u UUIDv7) IsValid() bool {
	googleUUID := uuid.UUID(u)
	return googleUUID.Version() == 7 && googleUUID.Variant() == uuid.RFC4122
}

// ParseUUID parses a UUID string into UUIDv7
func ParseUUID(s string) (UUIDv7, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return UUIDv7{}, err
	}
	var u UUIDv7
	copy(u[:], id[:])
	return u, nil
}
