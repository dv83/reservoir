package cluster

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"reservoir/pkg/logger"
	"reservoir/pkg/types"
)

const (
	// Timing constants (inspired by etcd/raft defaults)
	HeartbeatInterval   = 150 * time.Millisecond
	ElectionTimeoutMin  = 1000 * time.Millisecond
	ElectionTimeoutMax  = 2000 * time.Millisecond
	NodeTimeoutDuration = 5 * time.Second

	// Replication constants
	ReplicationBatchSize = 100
	ReplicationInterval  = 50 * time.Millisecond

	// Failed events cleanup constants
	MaxFailedEventAge  = 1 * time.Hour // Remove failed events older than this
	MaxFailedEventsMap = 10000         // Maximum number of failed events to track
)

// ClusterManager manages the cluster state and operations
type ClusterManager struct {
	localNode *Node
	transport *Transport

	// Node registry
	nodes   map[UUIDv7]*Node
	nodesMu sync.RWMutex

	// Election state
	electionTimer    *time.Timer
	electionTimeout  time.Duration
	lastElectionTime atomic.Value // time.Time
	votesReceived    atomic.Uint32
	electionTerm     atomic.Uint64
	votesNeeded      atomic.Uint32
	// voters tracks the distinct nodes that granted a vote in the current
	// election, so duplicate VoteResponses (retries, duplicate connections)
	// cannot be double-counted. electionWon ensures becomeLeader runs at most
	// once per election even if several responses cross the majority threshold.
	voters      map[UUIDv7]struct{}
	votersMu    sync.Mutex
	electionWon atomic.Bool
	// leaderTerm is the term for which the currently-recorded leader was
	// accepted, used to reject a second node claiming leadership in the same
	// term (which would otherwise flap LeaderID).
	leaderTerm atomic.Uint64
	// statePath is where currentTerm/votedFor are persisted so a restarted node
	// does not vote twice in a term. Empty disables persistence.
	statePath string
	stateMu   sync.Mutex

	// Replication queue (inspired by DragonflyDB's approach)
	replicationQueue   chan ReplicationEvent
	replicationHandler ReplicationHandler

	// Reliable replication with overflow protection
	persistentQueue   *PersistentReplicationQueue
	failedEvents      map[string]*FailedReplicationEvent
	failedEventsMu    sync.RWMutex
	retryQueue        chan *FailedReplicationEvent
	droppedEventCount atomic.Uint64

	// Callbacks
	onBecomeLeader   func()
	onBecomeFollower func()
	onNodeJoin       func(*Node)
	onNodeLeave      func(*Node)

	// Performance optimizations
	cachedTime *types.CachedTimestamp

	// Control
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// ReplicationEvent represents a data change to be replicated
type ReplicationEvent struct {
	Operation string    `json:"operation"`
	Key       string    `json:"key"`
	Value     []byte    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	NodeID    UUIDv7    `json:"node_id"`
	Clock     uint64    `json:"clock"`
	EventID   string    `json:"event_id"` // Unique identifier for deduplication
}

// FailedReplicationEvent represents a failed replication attempt with retry logic.
//
// A single event is shared between the replication goroutine (which records new
// failures via trackReplicationFailure) and the retry goroutine (which reschedules
// and prunes it). All of its mutable fields — FailedNodes, LastAttempt, NextRetry,
// TotalFailures — must be accessed under mu. mu is never held together with
// ClusterManager.failedEventsMu, to avoid lock-ordering deadlocks.
type FailedReplicationEvent struct {
	mu            sync.Mutex
	Event         ReplicationEvent
	FailedNodes   map[UUIDv7]int // Node ID -> failure count
	LastAttempt   time.Time
	NextRetry     time.Time
	TotalFailures int
	MaxRetries    int
}

// PersistentReplicationQueue provides persistent storage for critical replication events
type PersistentReplicationQueue struct {
	events []ReplicationEvent
	mutex  sync.RWMutex
	// In a real implementation, this would use a persistent storage like file/database
	// For now, keeping it in memory with eventual consistency guarantees
}

// NewClusterManager creates a new cluster manager
func NewClusterManager(address string, port int, clusterID UUIDv7) *ClusterManager {
	localNode := NewNode(address, port, clusterID)
	transport := NewTransport(localNode)

	cm := &ClusterManager{
		localNode:        localNode,
		transport:        transport,
		nodes:            make(map[UUIDv7]*Node),
		replicationQueue: make(chan ReplicationEvent, 50000), // 5x larger buffer
		failedEvents:     make(map[string]*FailedReplicationEvent),
		retryQueue:       make(chan *FailedReplicationEvent, 10000),
		cachedTime:       types.NewCachedTimestamp(),
		voters:           make(map[UUIDv7]struct{}),
		stopCh:           make(chan struct{}),
	}

	// Don't add local node to nodes map - it's handled separately in GetNodes()

	// Register message handlers
	cm.registerHandlers()

	// Initialize election timeout
	cm.resetElectionTimer()

	return cm
}

// SetAuthKey enables HMAC authentication of inter-node traffic using the given
// shared secret. Must be called before Start. All nodes in a cluster must use
// the same secret; an empty secret leaves authentication disabled.
func (cm *ClusterManager) SetAuthKey(secret string) {
	cm.transport.SetAuthKey([]byte(secret))
}

// SetStatePath enables persistence of the Raft voting state (currentTerm and
// votedFor) to the given file. Must be called before Start. An empty path
// leaves state in memory only (state is then lost on restart).
func (cm *ClusterManager) SetStatePath(path string) {
	cm.statePath = path
}

// raftState is the on-disk persistent voting state.
type raftState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    []byte `json:"voted_for,omitempty"` // 16 raw UUIDv7 bytes, empty if none
}

// loadState restores currentTerm/votedFor from disk (best-effort). Called before
// any election activity so a restarted node resumes at its last durable term.
func (cm *ClusterManager) loadState() {
	if cm.statePath == "" {
		return
	}
	data, err := os.ReadFile(cm.statePath)
	if err != nil {
		return // no prior state
	}
	var st raftState
	if err := json.Unmarshal(data, &st); err != nil {
		logger.Warning("Ignoring unreadable raft state file %s: %v", cm.statePath, err)
		return
	}
	cm.localNode.CurrentTerm.Store(st.CurrentTerm)
	if len(st.VotedFor) == 16 {
		var id UUIDv7
		copy(id[:], st.VotedFor)
		cm.localNode.VotedFor.Store(&id)
	}
	logger.Info("Restored persisted raft state: term=%d", st.CurrentTerm)
}

// persistState durably writes currentTerm/votedFor via a temp file + rename so a
// crash mid-write cannot corrupt it. Must be called after mutating term or vote
// and before acting on that decision (e.g. before replying to a vote request).
func (cm *ClusterManager) persistState() {
	if cm.statePath == "" {
		return
	}
	st := raftState{CurrentTerm: cm.localNode.CurrentTerm.Load()}
	if v := cm.localNode.VotedFor.Load(); v != nil {
		if id, ok := v.(*UUIDv7); ok && id != nil {
			st.VotedFor = id[:]
		}
	}
	data, err := json.Marshal(st)
	if err != nil {
		logger.Error("Failed to marshal raft state: %v", err)
		return
	}

	cm.stateMu.Lock()
	defer cm.stateMu.Unlock()
	tmp := cm.statePath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		logger.Error("Failed to write raft state: %v", err)
		return
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		logger.Error("Failed to write raft state: %v", err)
		return
	}
	_ = f.Sync()
	_ = f.Close()
	if err := os.Rename(tmp, cm.statePath); err != nil {
		logger.Error("Failed to commit raft state: %v", err)
	}
}

// Start starts the cluster manager
func (cm *ClusterManager) Start() error {
	// Restore persisted voting state before any election activity so a restarted
	// node resumes at its last durable term and cannot vote twice in it.
	cm.loadState()

	// Start transport
	if err := cm.transport.Start(); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}

	// Initialize persistent queue for critical event recovery
	cm.persistentQueue = NewPersistentReplicationQueue()
	if err := cm.persistentQueue.Init(); err != nil {
		logger.Warning("Failed to initialize persistent queue: %v", err)
	}

	// Start background goroutines
	cm.wg.Add(6) // heartbeat, election, health, replication, retry, reconciliation
	go cm.heartbeatLoop()
	go cm.electionLoop()
	go cm.healthCheckLoop()
	go cm.replicationLoop()
	go cm.retryLoop()
	go cm.reconciliationLoop()

	logger.Info("Cluster manager started: NodeID=%s, ClusterID=%s",
		cm.localNode.NodeID, cm.localNode.ClusterID)

	return nil
}

// Stop stops the cluster manager
func (cm *ClusterManager) Stop() {
	close(cm.stopCh)
	cm.transport.Stop()

	// Stop performance optimization components
	if cm.cachedTime != nil {
		cm.cachedTime.Stop()
	}
	if cm.localNode.OptimizedVectorClock != nil {
		cm.localNode.OptimizedVectorClock.Stop()
	}

	// NOTE: deliberately do NOT close(cm.retryQueue). Multiple goroutines send
	// on it (trackReplicationFailure, handleFailedReplication, processRetryQueue);
	// closing it before wg.Wait() would panic on a concurrent send and could feed
	// a nil event into retryLoop. Loops exit via stopCh; the channel is GC'd.

	// Stop persistent queue
	if cm.persistentQueue != nil {
		cm.persistentQueue.Close()
	}

	cm.wg.Wait()
	logger.Info("Cluster manager stopped")
}

// registerHandlers registers all message handlers
func (cm *ClusterManager) registerHandlers() {
	cm.transport.RegisterHandler(MsgHeartbeat, cm.handleHeartbeat)
	cm.transport.RegisterHandler(MsgJoinRequest, cm.handleJoinRequest)
	cm.transport.RegisterHandler(MsgJoinResponse, cm.handleJoinResponse)
	cm.transport.RegisterHandler(MsgVoteRequest, cm.handleVoteRequest)
	cm.transport.RegisterHandler(MsgVoteResponse, cm.handleVoteResponse)
	cm.transport.RegisterHandler(MsgAppendEntries, cm.handleAppendEntries)
	cm.transport.RegisterHandler(MsgReplicationPush, cm.handleReplicationPush)
	cm.transport.RegisterHandler(MsgNodeUpdate, cm.handleNodeUpdate)
}

// JoinCluster attempts to join an existing cluster
func (cm *ClusterManager) JoinCluster(seedNode string, port int) error {
	// Create temporary connection to seed node
	tempNode := &Node{
		NodeID:  NewUUIDv7(), // Temporary ID for connection
		Address: seedNode,
		Port:    port,
	}

	if err := cm.transport.ConnectToNode(tempNode.NodeID, seedNode, port); err != nil {
		return fmt.Errorf("failed to connect to seed node: %w", err)
	}

	// Send join request
	joinReq := JoinRequest{
		NodeID:    cm.localNode.NodeID,
		ClusterID: cm.localNode.ClusterID,
		Address:   cm.localNode.Address,
		Port:      cm.localNode.Port,
	}

	if err := cm.transport.Send(tempNode.NodeID, MsgJoinRequest, joinReq); err != nil {
		return fmt.Errorf("failed to send join request: %w", err)
	}

	logger.Info("Sent join request to %s:%d", seedNode, port)
	return nil
}

// heartbeatLoop sends periodic heartbeats
// All nodes send heartbeats for health monitoring, but only leader heartbeats
// affect leadership (handled in handleHeartbeat)
func (cm *ClusterManager) heartbeatLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.sendHeartbeats()
		}
	}
}

// sendHeartbeats sends heartbeats to all nodes (optimized)
func (cm *ClusterManager) sendHeartbeats() {
	hb := Heartbeat{
		NodeID:      cm.localNode.NodeID,
		Term:        cm.localNode.CurrentTerm.Load(),
		State:       cm.localNode.GetState(),
		VectorClock: cm.localNode.GetVectorClock(), // Uses optimized vector clock
		Timestamp:   cm.cachedTime.Now(),           // OPTIMIZED: Use cached timestamp
	}

	if err := cm.transport.Broadcast(MsgHeartbeat, hb); err != nil {
		logger.Warning("Failed to broadcast heartbeat: %v", err)
	}
}

// electionLoop handles leader elections
func (cm *ClusterManager) electionLoop() {
	defer cm.wg.Done()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-cm.electionTimer.C:
			if cm.localNode.GetState() != StateLeader {
				cm.startElection()
			}
		}
	}
}

// startElection starts a new leader election
func (cm *ClusterManager) startElection() {
	cm.localNode.SetState(StateCandidate)
	cm.localNode.CurrentTerm.Add(1)
	currentTerm := cm.localNode.CurrentTerm.Load()

	// Vote for self
	cm.localNode.VotedFor.Store(&cm.localNode.NodeID)
	cm.persistState()         // durably record the new term + self-vote before campaigning
	cm.votesReceived.Store(1) // Self vote
	cm.electionTerm.Store(currentTerm)
	cm.electionWon.Store(false)
	cm.votersMu.Lock()
	cm.voters = map[UUIDv7]struct{}{cm.localNode.NodeID: {}}
	cm.votersMu.Unlock()

	// Reset election timer
	cm.resetElectionTimer()

	logger.Info("Starting election for term %d", currentTerm)

	// Request votes from all nodes
	voteReq := VoteRequest{
		CandidateID: cm.localNode.NodeID,
		Term:        currentTerm,
		LastLogTerm: cm.getLastLogTerm(),
	}

	cm.nodesMu.RLock()
	totalNodes := len(cm.nodes) + 1 // Include local node in count
	cm.nodesMu.RUnlock()

	// Calculate votes needed for majority
	cm.votesNeeded.Store(uint32(totalNodes/2 + 1))

	if totalNodes == 1 {
		// Single node cluster, become leader immediately
		cm.becomeLeader()
		return
	}

	// Send vote requests
	if err := cm.transport.Broadcast(MsgVoteRequest, voteReq); err != nil {
		logger.Error("Failed to send vote requests: %v", err)
		cm.localNode.SetState(StateFollower)
		return
	}

	// Vote responses are handled in handleVoteResponse()
	// which will call becomeLeader() when majority is reached
}

// becomeLeader transitions to leader state. It is idempotent within a single
// election: the first caller wins the CAS and proceeds, any concurrent or later
// caller for the same election returns immediately.
func (cm *ClusterManager) becomeLeader() {
	if !cm.electionWon.CompareAndSwap(false, true) {
		return
	}
	cm.localNode.SetState(StateLeader)
	cm.localNode.LeaderID.Store(&cm.localNode.NodeID)
	cm.leaderTerm.Store(cm.localNode.CurrentTerm.Load())

	logger.Info("Became leader for term %d", cm.localNode.CurrentTerm.Load())

	// Send initial heartbeat immediately
	cm.sendHeartbeats()

	// Call callback if set
	if cm.onBecomeLeader != nil {
		cm.onBecomeLeader()
	}
}

// resetElectionTimer resets the election timer with random timeout
func (cm *ClusterManager) resetElectionTimer() {
	timeout := ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))

	if cm.electionTimer != nil {
		cm.electionTimer.Stop()
	}

	cm.electionTimer = time.NewTimer(timeout)
	cm.electionTimeout = timeout
}

// healthCheckLoop monitors node health
func (cm *ClusterManager) healthCheckLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.checkNodeHealth()
		}
	}
}

// checkNodeHealth checks health of all nodes
func (cm *ClusterManager) checkNodeHealth() {
	cm.nodesMu.Lock()
	defer cm.nodesMu.Unlock()

	for nodeID, node := range cm.nodes {
		if !node.IsHealthy(NodeTimeoutDuration) {
			// Node is unhealthy
			if node.IsAlive.CompareAndSwap(true, false) {
				node.SetState(StateOffline)
				logger.Warning("Node %s marked as offline", nodeID)

				// Remove from active nodes
				delete(cm.nodes, nodeID)

				// Call callback
				if cm.onNodeLeave != nil {
					cm.onNodeLeave(node)
				}
			}
		}
	}
}

// replicationLoop handles asynchronous replication
func (cm *ClusterManager) replicationLoop() {
	defer cm.wg.Done()

	batch := make([]ReplicationEvent, 0, ReplicationBatchSize)
	ticker := time.NewTicker(ReplicationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return

		case event := <-cm.replicationQueue:
			batch = append(batch, event)

			// Send batch if full
			if len(batch) >= ReplicationBatchSize {
				cm.sendReplicationBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			// Send partial batch on timeout
			if len(batch) > 0 {
				cm.sendReplicationBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// sendReplicationBatch sends a batch of replication events with failure tracking
func (cm *ClusterManager) sendReplicationBatch(batch []ReplicationEvent) {
	if len(batch) == 0 {
		return
	}

	push := ReplicationPush{
		NodeID: cm.localNode.NodeID,
		Events: batch,
	}

	// Try to broadcast to all nodes
	if err := cm.transport.Broadcast(MsgReplicationPush, push); err != nil {
		logger.Warning("Broadcast replication batch failed, tracking individual failures: %v", err)

		// If broadcast fails, try to send to individual nodes and track failures
		cm.sendBatchToIndividualNodes(batch, push)
	}
}

// sendBatchToIndividualNodes sends batch to individual nodes and tracks failures
func (cm *ClusterManager) sendBatchToIndividualNodes(batch []ReplicationEvent, push ReplicationPush) {
	// Collect failures while holding nodesMu, then process them after releasing
	type failure struct {
		event  ReplicationEvent
		nodeID UUIDv7
		err    error
	}
	var failures []failure

	cm.nodesMu.RLock()
	for _, node := range cm.nodes {
		if err := cm.transport.Send(node.NodeID, MsgReplicationPush, push); err != nil {
			for _, event := range batch {
				failures = append(failures, failure{event, node.NodeID, err})
			}
		}
	}
	cm.nodesMu.RUnlock()

	// Now track failures without holding nodesMu (prevents deadlock)
	for _, f := range failures {
		cm.trackReplicationFailure(f.event, f.nodeID, f.err)
	}
}

// trackReplicationFailure tracks a replication failure for retry
func (cm *ClusterManager) trackReplicationFailure(event ReplicationEvent, nodeID UUIDv7, err error) {
	logger.Warning("Replication failed for event %s to node %s: %v", event.EventID, nodeID, err)

	// Map insertion under failedEventsMu; per-event field mutation under the
	// event's own lock. The two locks are never held simultaneously.
	cm.failedEventsMu.Lock()
	failedEvent, exists := cm.failedEvents[event.EventID]
	if !exists {
		failedEvent = &FailedReplicationEvent{
			Event:         event,
			FailedNodes:   make(map[UUIDv7]int),
			LastAttempt:   time.Now(),
			NextRetry:     time.Now().Add(2 * time.Second), // Initial retry in 2 seconds
			TotalFailures: 0,
			MaxRetries:    10, // Configurable max retries
		}
		cm.failedEvents[event.EventID] = failedEvent
	}
	cm.failedEventsMu.Unlock()

	failedEvent.mu.Lock()
	failedEvent.FailedNodes[nodeID]++
	failedEvent.TotalFailures++
	failedEvent.mu.Unlock()

	// Schedule for retry
	select {
	case cm.retryQueue <- failedEvent:
		// Successfully queued for retry
	default:
		// Retry queue full - will be picked up by periodic retry processor
		logger.Warning("Retry queue full, failed event will be retried in next cycle")
	}
}

// QueueReplication queues a replication event with reliable delivery guarantees
func (cm *ClusterManager) QueueReplication(op string, key string, value []byte) {
	// OPTIMIZED: Increment vector clock using lock-free atomic operation
	var clock uint64
	if cm.localNode.OptimizedVectorClock != nil {
		clock = cm.localNode.OptimizedVectorClock.IncrementLocal()
	} else {
		// Fallback to legacy vector clock if optimized is unavailable
		cm.localNode.vectorClockMu.Lock()
		cm.localNode.VectorClock[cm.localNode.NodeID]++
		clock = cm.localNode.VectorClock[cm.localNode.NodeID]
		cm.localNode.vectorClockMu.Unlock()
	}

	// OPTIMIZED: Use cached timestamp to avoid time.Now() system call
	timestamp := cm.cachedTime.Now()

	// Generate unique event ID for deduplication
	eventID := fmt.Sprintf("%s-%d-%d", cm.localNode.NodeID.String(), clock, timestamp.UnixNano())

	logger.Debug("Queueing replication: ID=%s, Op=%s, Key=%s, ValueSize=%d", eventID, op, key, len(value))
	event := ReplicationEvent{
		Operation: op,
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		NodeID:    cm.localNode.NodeID,
		Clock:     clock,
		EventID:   eventID,
	}

	// Try fast path first (non-blocking)
	select {
	case cm.replicationQueue <- event:
		// Successfully queued via fast path
		return
	default:
		// Queue full - use overflow protection with persistence
	}

	// Overflow protection: Try with brief retry and persistence fallback
	for attempt := 0; attempt < 3; attempt++ {
		select {
		case cm.replicationQueue <- event:
			// Successfully queued after retry
			return
		default:
			if attempt < 2 {
				// Brief backoff for hot path optimization
				time.Sleep(time.Microsecond * time.Duration(1<<attempt)) // 1μs, 2μs
				continue
			}
		}
	}

	// All attempts failed - persist event for eventual delivery
	cm.droppedEventCount.Add(1)
	if cm.persistentQueue != nil {
		if err := cm.persistentQueue.Add(event); err != nil {
			logger.Error("Failed to persist critical replication event: %v", err)
		} else {
			logger.Warning("Replication queue full, event persisted for eventual delivery: EventID=%s", eventID)
		}
	} else {
		logger.Error("Replication queue full and no persistent queue available - CRITICAL DATA LOSS RISK: EventID=%s", eventID)
	}
}

// GetNodes returns all known nodes including local node
func (cm *ClusterManager) GetNodes() []*Node {
	cm.nodesMu.RLock()
	defer cm.nodesMu.RUnlock()

	// Include local node first
	nodes := make([]*Node, 0, len(cm.nodes)+1)
	nodes = append(nodes, cm.localNode)

	// Include all remote nodes
	for _, node := range cm.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// GetLeader returns the current leader node
func (cm *ClusterManager) GetLeader() *Node {
	leaderIDVal := cm.localNode.LeaderID.Load()
	if leaderIDVal == nil {
		return nil
	}

	leaderIDPtr, ok := leaderIDVal.(*UUIDv7)
	if !ok || leaderIDPtr == nil {
		return nil
	}
	leaderID := *leaderIDPtr

	// Check if leader is the local node
	if leaderID == cm.localNode.NodeID {
		return cm.localNode
	}

	cm.nodesMu.RLock()
	defer cm.nodesMu.RUnlock()

	return cm.nodes[leaderID]
}

// GetLocalNode returns the local node
func (cm *ClusterManager) GetLocalNode() *Node {
	return cm.localNode
}

// IsLeader returns true if local node is the leader
func (cm *ClusterManager) IsLeader() bool {
	return cm.localNode.GetState() == StateLeader
}

// getLastLogTerm returns the term of the last log entry (simplified)
func (cm *ClusterManager) getLastLogTerm() uint64 {
	// In real implementation, this would check the actual log
	return cm.localNode.CurrentTerm.Load()
}

// SetReplicationHandler sets the replication handler
func (cm *ClusterManager) SetReplicationHandler(handler ReplicationHandler) {
	cm.replicationHandler = handler
}

// GetReplicationStats returns statistics about replication health
func (cm *ClusterManager) GetReplicationStats() ReplicationStats {
	cm.failedEventsMu.RLock()
	failedEventCount := len(cm.failedEvents)

	// Count total failed nodes across all events. FailedNodes is mutated under
	// each event's own lock, so guard the read with it.
	totalFailedNodes := 0
	for _, failedEvent := range cm.failedEvents {
		failedEvent.mu.Lock()
		totalFailedNodes += len(failedEvent.FailedNodes)
		failedEvent.mu.Unlock()
	}
	cm.failedEventsMu.RUnlock()

	var persistentEventCount int
	if cm.persistentQueue != nil {
		pending := cm.persistentQueue.GetPending()
		persistentEventCount = len(pending)
	}

	// Get queue sizes
	replicationQueueSize := len(cm.replicationQueue)
	retryQueueSize := len(cm.retryQueue)

	// Get vector clock dropped count
	var vectorClockDropped uint64
	if cm.localNode.OptimizedVectorClock != nil {
		vectorClockDropped = cm.localNode.OptimizedVectorClock.GetDroppedCount()
	}

	return ReplicationStats{
		QueueSize:            replicationQueueSize,
		RetryQueueSize:       retryQueueSize,
		FailedEventCount:     failedEventCount,
		TotalFailedNodes:     totalFailedNodes,
		PersistentEventCount: persistentEventCount,
		DroppedEventCount:    cm.droppedEventCount.Load(),
		VectorClockDropped:   vectorClockDropped,
		QueueCapacity:        cap(cm.replicationQueue),
		RetryQueueCapacity:   cap(cm.retryQueue),
	}
}

// ReplicationStats contains statistics about replication health
type ReplicationStats struct {
	QueueSize            int    `json:"queue_size"`
	RetryQueueSize       int    `json:"retry_queue_size"`
	FailedEventCount     int    `json:"failed_event_count"`
	TotalFailedNodes     int    `json:"total_failed_nodes"`
	PersistentEventCount int    `json:"persistent_event_count"`
	DroppedEventCount    uint64 `json:"dropped_event_count"`
	VectorClockDropped   uint64 `json:"vector_clock_dropped"`
	QueueCapacity        int    `json:"queue_capacity"`
	RetryQueueCapacity   int    `json:"retry_queue_capacity"`
}

// Message structures
type Heartbeat struct {
	NodeID      UUIDv7            `json:"node_id"`
	Term        uint64            `json:"term"`
	State       NodeState         `json:"state"`
	VectorClock map[UUIDv7]uint64 `json:"vector_clock"`
	Timestamp   time.Time         `json:"timestamp"`
}

type JoinRequest struct {
	NodeID    UUIDv7 `json:"node_id"`
	ClusterID UUIDv7 `json:"cluster_id"`
	Address   string `json:"address"`
	Port      int    `json:"port"`
}

type JoinResponse struct {
	Accepted bool       `json:"accepted"`
	LeaderID UUIDv7     `json:"leader_id"`
	Nodes    []NodeInfo `json:"nodes"`
	Term     uint64     `json:"term"`
}

type VoteRequest struct {
	CandidateID UUIDv7 `json:"candidate_id"`
	Term        uint64 `json:"term"`
	LastLogTerm uint64 `json:"last_log_term"`
}

type VoteResponse struct {
	NodeID      UUIDv7 `json:"node_id"`
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type ReplicationPush struct {
	NodeID UUIDv7             `json:"node_id"`
	Events []ReplicationEvent `json:"events"`
}

type NodeUpdate struct {
	Node   NodeInfo `json:"node"`
	Joined bool     `json:"joined"` // true = node joined, false = node left
	Term   uint64   `json:"term"`
}

// NewPersistentReplicationQueue creates a new persistent replication queue
func NewPersistentReplicationQueue() *PersistentReplicationQueue {
	return &PersistentReplicationQueue{
		events: make([]ReplicationEvent, 0, 1000),
	}
}

// Init initializes the persistent queue
func (pq *PersistentReplicationQueue) Init() error {
	// In a real implementation, this would initialize persistent storage
	// For now, we use in-memory storage with recovery capabilities
	return nil
}

// Add adds an event to the persistent queue
func (pq *PersistentReplicationQueue) Add(event ReplicationEvent) error {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	// Prevent unbounded growth - keep only last 10000 events
	if len(pq.events) >= 10000 {
		// Remove oldest 1000 events to make room
		copy(pq.events, pq.events[1000:])
		pq.events = pq.events[:len(pq.events)-1000]
	}

	pq.events = append(pq.events, event)
	return nil
}

// GetPending returns all pending events for reconciliation
func (pq *PersistentReplicationQueue) GetPending() []ReplicationEvent {
	pq.mutex.RLock()
	defer pq.mutex.RUnlock()

	if len(pq.events) == 0 {
		return nil
	}

	// Return a copy of all events
	pending := make([]ReplicationEvent, len(pq.events))
	copy(pending, pq.events)
	return pending
}

// Remove removes events that have been successfully processed
// Uses in-place filtering to avoid allocation
func (pq *PersistentReplicationQueue) Remove(eventIDs []string) {
	if len(eventIDs) == 0 {
		return
	}

	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	// Create a map for O(1) lookup
	toRemove := make(map[string]struct{}, len(eventIDs))
	for _, id := range eventIDs {
		toRemove[id] = struct{}{}
	}

	// In-place filtering (avoids allocation)
	writeIdx := 0
	for _, event := range pq.events {
		if _, shouldRemove := toRemove[event.EventID]; !shouldRemove {
			pq.events[writeIdx] = event
			writeIdx++
		}
	}
	pq.events = pq.events[:writeIdx]
}

// Close closes the persistent queue
func (pq *PersistentReplicationQueue) Close() {
	// In a real implementation, this would close persistent storage connections
}

// retryLoop handles failed replication event retries with exponential backoff
func (cm *ClusterManager) retryLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(5 * time.Second) // Check for retries every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return

		case failedEvent := <-cm.retryQueue:
			cm.handleFailedReplication(failedEvent)

		case <-ticker.C:
			cm.processRetryQueue()
		}
	}
}

// reconciliationLoop periodically reconciles persistent queue with active replication
func (cm *ClusterManager) reconciliationLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(30 * time.Second) // Reconcile every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return

		case <-ticker.C:
			cm.cleanupStaleFailedEvents()
			cm.performReconciliation()
		}
	}
}

// cleanupStaleFailedEvents removes old failed events to prevent memory leaks
func (cm *ClusterManager) cleanupStaleFailedEvents() {
	cm.failedEventsMu.Lock()
	defer cm.failedEventsMu.Unlock()

	now := time.Now()
	staleCount := 0

	// Remove events older than MaxFailedEventAge
	for eventID, failedEvent := range cm.failedEvents {
		if now.Sub(failedEvent.LastAttempt) > MaxFailedEventAge {
			// Persist before deleting to prevent data loss
			if cm.persistentQueue != nil {
				cm.persistentQueue.Add(failedEvent.Event)
			}
			cm.droppedEventCount.Add(1)
			delete(cm.failedEvents, eventID)
			staleCount++
		}
	}

	// If map is still too large, remove oldest events
	if len(cm.failedEvents) > MaxFailedEventsMap {
		// Find and remove the oldest events
		removeCount := len(cm.failedEvents) - MaxFailedEventsMap
		type eventAge struct {
			id  string
			age time.Time
		}
		events := make([]eventAge, 0, len(cm.failedEvents))
		for id, fe := range cm.failedEvents {
			events = append(events, eventAge{id, fe.LastAttempt})
		}
		// Sort by age (oldest first) - simple bubble for small counts
		for i := 0; i < removeCount && i < len(events); i++ {
			for j := i + 1; j < len(events); j++ {
				if events[j].age.Before(events[i].age) {
					events[i], events[j] = events[j], events[i]
				}
			}
			// Persist before deleting to prevent data loss
			if cm.persistentQueue != nil {
				if fe, ok := cm.failedEvents[events[i].id]; ok {
					cm.persistentQueue.Add(fe.Event)
				}
			}
			cm.droppedEventCount.Add(1)
			delete(cm.failedEvents, events[i].id)
			staleCount++
		}
	}

	if staleCount > 0 {
		logger.Info("Cleaned up %d stale failed replication events", staleCount)
	}
}

// handleFailedReplication processes a failed replication event
func (cm *ClusterManager) handleFailedReplication(failedEvent *FailedReplicationEvent) {
	// Check if it's time to retry
	failedEvent.mu.Lock()
	notReady := time.Now().Before(failedEvent.NextRetry)
	failedEvent.mu.Unlock()
	if notReady {
		// Not ready for retry yet, put it back in the queue for later
		select {
		case cm.retryQueue <- failedEvent:
		default:
			// Retry queue full, skip this attempt
			logger.Warning("Retry queue full, skipping retry for event %s", failedEvent.Event.EventID)
		}
		return
	}

	// Attempt to resend the event
	if cm.attemptEventRetry(failedEvent) {
		// Success - remove from failed events
		cm.failedEventsMu.Lock()
		delete(cm.failedEvents, failedEvent.Event.EventID)
		cm.failedEventsMu.Unlock()
		logger.Info("Successfully retried replication event: %s", failedEvent.Event.EventID)
		return
	}

	// Failed again - update retry schedule under the event lock.
	failedEvent.mu.Lock()
	failedEvent.TotalFailures++
	failedEvent.LastAttempt = time.Now()
	exceeded := failedEvent.TotalFailures >= failedEvent.MaxRetries
	if !exceeded {
		backoffDuration := time.Duration(1<<failedEvent.TotalFailures) * time.Second
		if backoffDuration > 5*time.Minute {
			backoffDuration = 5 * time.Minute // Cap at 5 minutes
		}
		failedEvent.NextRetry = time.Now().Add(backoffDuration)
	}
	failedEvent.mu.Unlock()

	if exceeded {
		// Max retries exceeded - log critical error and persist for manual intervention
		logger.Error("CRITICAL: Max retries exceeded for replication event %s, requiring manual intervention", failedEvent.Event.EventID)
		if cm.persistentQueue != nil {
			cm.persistentQueue.Add(failedEvent.Event)
		}
		cm.failedEventsMu.Lock()
		delete(cm.failedEvents, failedEvent.Event.EventID)
		cm.failedEventsMu.Unlock()
		return
	}

	// Put back in retry queue
	select {
	case cm.retryQueue <- failedEvent:
	default:
		logger.Warning("Retry queue full, failed event will be retried in next reconciliation cycle")
	}
}

// attemptEventRetry attempts to retry a failed replication event
func (cm *ClusterManager) attemptEventRetry(failedEvent *FailedReplicationEvent) bool {
	// Try to queue the event again
	select {
	case cm.replicationQueue <- failedEvent.Event:
		return true
	default:
		// Queue still full - try direct send to specific failed nodes
		return cm.directRetryToFailedNodes(failedEvent)
	}
}

// directRetryToFailedNodes directly retries sending to specific failed nodes
func (cm *ClusterManager) directRetryToFailedNodes(failedEvent *FailedReplicationEvent) bool {
	// Create a replication push with just this event
	push := ReplicationPush{
		NodeID: cm.localNode.NodeID,
		Events: []ReplicationEvent{failedEvent.Event},
	}

	// Snapshot the failed-node set under the event lock, then perform the
	// network sends without holding any lock, then remove the reached nodes.
	failedEvent.mu.Lock()
	nodes := make([]UUIDv7, 0, len(failedEvent.FailedNodes))
	for nodeID := range failedEvent.FailedNodes {
		nodes = append(nodes, nodeID)
	}
	failedEvent.mu.Unlock()

	successCount := 0
	for _, nodeID := range nodes {
		if err := cm.transport.Send(nodeID, MsgReplicationPush, push); err != nil {
			logger.Warning("Direct retry failed for node %s: %v", nodeID, err)
		} else {
			successCount++
			failedEvent.mu.Lock()
			delete(failedEvent.FailedNodes, nodeID)
			failedEvent.mu.Unlock()
		}
	}

	failedEvent.mu.Lock()
	remaining := len(failedEvent.FailedNodes)
	failedEvent.mu.Unlock()

	// Consider it successful if we reached all previously failed nodes.
	return successCount > 0 && remaining == 0
}

// processRetryQueue processes all pending retries
func (cm *ClusterManager) processRetryQueue() {
	cm.failedEventsMu.RLock()
	candidates := make([]*FailedReplicationEvent, 0, len(cm.failedEvents))
	for _, failedEvent := range cm.failedEvents {
		candidates = append(candidates, failedEvent)
	}
	cm.failedEventsMu.RUnlock()

	now := time.Now()
	pendingRetries := make([]*FailedReplicationEvent, 0, len(candidates))
	for _, failedEvent := range candidates {
		failedEvent.mu.Lock()
		due := now.After(failedEvent.NextRetry)
		failedEvent.mu.Unlock()
		if due {
			pendingRetries = append(pendingRetries, failedEvent)
		}
	}

	// Process pending retries
	for _, failedEvent := range pendingRetries {
		select {
		case cm.retryQueue <- failedEvent:
		default:
			// Retry queue full; stop enqueuing and let the next cycle continue.
			// (A bare `break` here would only break the select, not the loop.)
			return
		}
	}
}

// performReconciliation reconciles persistent queue with current cluster state
func (cm *ClusterManager) performReconciliation() {
	if cm.persistentQueue == nil {
		return
	}

	// Get pending events from persistent storage
	pendingEvents := cm.persistentQueue.GetPending()
	if len(pendingEvents) == 0 {
		return
	}

	logger.Info("Starting reconciliation with %d pending persistent events", len(pendingEvents))

	successfulEvents := make([]string, 0, len(pendingEvents))

	// Try to re-queue persistent events
	for _, event := range pendingEvents {
		select {
		case cm.replicationQueue <- event:
			successfulEvents = append(successfulEvents, event.EventID)
		default:
			// Queue still full, try next reconciliation cycle
			continue
		}
	}

	// Remove successfully re-queued events from persistent storage
	if len(successfulEvents) > 0 {
		cm.persistentQueue.Remove(successfulEvents)
		logger.Info("Reconciliation completed: %d events re-queued, %d remaining",
			len(successfulEvents), len(pendingEvents)-len(successfulEvents))
	}
}

// broadcastNodeUpdate broadcasts information about a new or removed node to all cluster members
func (cm *ClusterManager) broadcastNodeUpdate(node *Node, joined bool) {
	// Create node update message
	update := NodeUpdate{
		Node:   node.GetInfo(),
		Joined: joined,
		Term:   cm.localNode.CurrentTerm.Load(),
	}

	// Send to all known nodes except the one that just joined/left
	cm.nodesMu.RLock()
	nodeCount := len(cm.nodes)
	cm.nodesMu.RUnlock()

	logger.Info("Broadcasting node update about %s (joined=%v) to %d existing nodes",
		node.NodeID, joined, nodeCount)

	cm.nodesMu.RLock()
	defer cm.nodesMu.RUnlock()

	for _, n := range cm.nodes {
		if n.NodeID == node.NodeID {
			continue // Skip the node that triggered the update
		}

		if err := cm.transport.Send(n.NodeID, MsgNodeUpdate, update); err != nil {
			logger.Warning("Failed to send node update to %s: %v", n.NodeID, err)
		} else {
			logger.Info("Sent node update to %s about %s (joined=%v)", n.NodeID, node.NodeID, joined)
		}
	}
}

// Initialize gob encoding for custom types
func init() {
	gob.Register(Heartbeat{})
	gob.Register(JoinRequest{})
	gob.Register(JoinResponse{})
	gob.Register(VoteRequest{})
	gob.Register(VoteResponse{})
	gob.Register(ReplicationPush{})
	gob.Register(ReplicationEvent{})
	gob.Register(NodeUpdate{})
	gob.Register(FailedReplicationEvent{})
	gob.Register(ReplicationStats{})
}
