package cluster

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"reservoir/pkg/logger"
	"reservoir/pkg/types"
)

// handleHeartbeat processes heartbeat messages
func (cm *ClusterManager) handleHeartbeat(msg *Message) error {
	var hb Heartbeat
	if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&hb); err != nil {
		return fmt.Errorf("failed to decode heartbeat: %w", err)
	}

	// Hold RLock while accessing node to prevent use-after-free
	cm.nodesMu.RLock()
	node, exists := cm.nodes[hb.NodeID]
	if !exists {
		cm.nodesMu.RUnlock()
		return nil
	}

	// Update node state while still holding lock. Advance the peer's recorded
	// term monotonically — never regress it if a heartbeat carries a lower term.
	node.UpdateHeartbeat()
	for {
		cur := node.CurrentTerm.Load()
		if hb.Term <= cur || node.CurrentTerm.CompareAndSwap(cur, hb.Term) {
			break
		}
	}

	// OPTIMIZED: Update vector clocks asynchronously (non-blocking)
	for nodeID, clock := range hb.VectorClock {
		node.UpdateVectorClock(nodeID, clock) // Now uses async updates
	}
	cm.nodesMu.RUnlock() // Release lock after node operations are done

	// Handle term updates
	currentTerm := cm.localNode.CurrentTerm.Load()
	if hb.Term > currentTerm {
		cm.localNode.CurrentTerm.Store(hb.Term)
		cm.localNode.SetState(StateFollower)
		cm.localNode.ClearVotedFor()
		cm.persistState()
		cm.resetElectionTimer()
		currentTerm = hb.Term
	}

	// If from a leader with a valid term, accept it and reset the election timer.
	// Raft invariant: only accept a leader whose term >= our term. Additionally,
	// never let a second node claim leadership for a term we already assigned to
	// a different leader (that would flap LeaderID between two "leaders").
	if hb.State == StateLeader && hb.Term >= currentTerm {
		lt := cm.leaderTerm.Load()
		curLeader, haveLeader := cm.currentLeaderID()
		switch {
		case hb.Term > lt || !haveLeader:
			cm.localNode.LeaderID.Store(&hb.NodeID)
			cm.leaderTerm.Store(hb.Term)
			cm.resetElectionTimer()
		case hb.Term == lt && curLeader == hb.NodeID:
			// Same leader, same term: an ordinary heartbeat — keep the timer alive.
			cm.resetElectionTimer()
		default:
			logger.Warning("Ignoring conflicting leader claim from %s for term %d (current leader for that term is %s)",
				hb.NodeID, hb.Term, curLeader)
		}
	}

	return nil
}

// currentLeaderID returns the currently-recorded leader node ID, if any.
func (cm *ClusterManager) currentLeaderID() (UUIDv7, bool) {
	v := cm.localNode.LeaderID.Load()
	if v == nil {
		return UUIDv7{}, false
	}
	id, ok := v.(*UUIDv7)
	if !ok || id == nil {
		return UUIDv7{}, false
	}
	return *id, true
}

// handleJoinRequest processes join requests
func (cm *ClusterManager) handleJoinRequest(msg *Message) error {
	var req JoinRequest
	if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&req); err != nil {
		return fmt.Errorf("failed to decode join request: %w", err)
	}

	logger.Info("Received join request from %s", req.NodeID)

	// Verify cluster ID
	if req.ClusterID != cm.localNode.ClusterID {
		logger.Warning("Join request with mismatched cluster ID: %s", req.ClusterID)
		resp := JoinResponse{
			Accepted: false,
			Term:     cm.localNode.CurrentTerm.Load(),
		}
		return cm.transport.Send(req.NodeID, MsgJoinResponse, resp)
	}

	// Add new node using the remote-node helper (no per-peer OVC goroutine).
	newNode := newRemoteNode(req.NodeID, req.Address, req.Port, req.ClusterID)

	// Connect to new node
	if err := cm.transport.ConnectToNode(req.NodeID, req.Address, req.Port); err != nil {
		logger.Error("Failed to connect to new node: %v", err)
		resp := JoinResponse{
			Accepted: false,
			Term:     cm.localNode.CurrentTerm.Load(),
		}
		return cm.transport.Send(req.NodeID, MsgJoinResponse, resp)
	}

	// Add to node registry
	cm.nodesMu.Lock()
	cm.nodes[req.NodeID] = newNode
	cm.nodesMu.Unlock()

	// Send current cluster state - include all nodes including local node
	nodes := make([]NodeInfo, 0)

	// Include local node first
	nodes = append(nodes, cm.localNode.GetInfo())

	// Include all other nodes
	cm.nodesMu.RLock()
	for _, node := range cm.nodes {
		nodes = append(nodes, node.GetInfo())
	}
	cm.nodesMu.RUnlock()

	// Determine leader ID
	var leaderID UUIDv7
	if cm.IsLeader() {
		// This node is the leader
		leaderID = cm.localNode.NodeID
	} else if leader := cm.GetLeader(); leader != nil {
		// Another node is the leader
		leaderID = leader.NodeID
	} else {
		// No leader found, use this node as default
		leaderID = cm.localNode.NodeID
	}

	resp := JoinResponse{
		Accepted: true,
		LeaderID: leaderID,
		Nodes:    nodes,
		Term:     cm.localNode.CurrentTerm.Load(),
	}

	// Call callback
	if cm.onNodeJoin != nil {
		cm.onNodeJoin(newNode)
	}

	logger.Info("Node %s joined cluster", req.NodeID)

	// Send join response to the new node
	if err := cm.transport.Send(req.NodeID, MsgJoinResponse, resp); err != nil {
		return fmt.Errorf("failed to send join response: %w", err)
	}

	// Broadcast new node info to all existing nodes
	cm.broadcastNodeUpdate(newNode, true)

	return nil
}

// handleJoinResponse processes join responses
func (cm *ClusterManager) handleJoinResponse(msg *Message) error {
	var resp JoinResponse
	if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&resp); err != nil {
		return fmt.Errorf("failed to decode join response: %w", err)
	}

	if !resp.Accepted {
		logger.Error("Join request rejected")
		return fmt.Errorf("join request rejected")
	}

	logger.Info("Join request accepted, received %d nodes", len(resp.Nodes))

	// Update term and state when joining cluster
	cm.localNode.CurrentTerm.Store(resp.Term)
	cm.localNode.SetState(StateFollower)

	// Update leader
	cm.localNode.LeaderID.Store(&resp.LeaderID)

	// Reset election timer since we just joined and have a leader
	cm.resetElectionTimer()

	// Add all nodes
	for _, nodeInfo := range resp.Nodes {
		if nodeInfo.NodeID == cm.localNode.NodeID {
			continue // Skip self
		}

		// Create remote-node bookkeeping (no per-peer OVC goroutine).
		node := newRemoteNode(nodeInfo.NodeID, nodeInfo.Address, nodeInfo.Port, nodeInfo.ClusterID)
		node.LastHeartbeat.Store(nodeInfo.LastHeartbeat)
		node.IsAlive.Store(nodeInfo.IsAlive)
		node.CurrentTerm.Store(nodeInfo.CurrentTerm)

		// Connect to node
		if err := cm.transport.ConnectToNode(nodeInfo.NodeID, nodeInfo.Address, nodeInfo.Port); err != nil {
			logger.Warning("Failed to connect to node %s: %v", nodeInfo.NodeID, err)
			continue
		}

		cm.nodesMu.Lock()
		cm.nodes[nodeInfo.NodeID] = node
		cm.nodesMu.Unlock()
	}

	return nil
}

// handleVoteRequest processes vote requests
func (cm *ClusterManager) handleVoteRequest(msg *Message) error {
	var req VoteRequest
	if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&req); err != nil {
		return fmt.Errorf("failed to decode vote request: %w", err)
	}

	currentTerm := cm.localNode.CurrentTerm.Load()
	voteGranted := false

	// Check term
	if req.Term < currentTerm {
		// Reject old term
		logger.Debug("Rejecting vote request from %s: old term %d < %d",
			req.CandidateID, req.Term, currentTerm)
	} else {
		// Update term if newer
		if req.Term > currentTerm {
			cm.localNode.CurrentTerm.Store(req.Term)
			cm.localNode.SetState(StateFollower)
			cm.localNode.ClearVotedFor()
			currentTerm = req.Term
		}

		// Check if we can vote (no prior vote this term, or already voted for this
		// same candidate). VotedForID safely handles a cleared/typed-nil vote.
		votedFor := cm.localNode.VotedForID()
		if votedFor == nil || *votedFor == req.CandidateID {
			// Grant vote
			cm.localNode.SetVotedFor(req.CandidateID)
			voteGranted = true
			cm.resetElectionTimer()
			logger.Info("Granted vote to %s for term %d", req.CandidateID, req.Term)
		}
	}

	// Persist the term/vote decision durably BEFORE replying, so a crash cannot
	// let us grant a second vote in this term after restart.
	cm.persistState()

	resp := VoteResponse{
		NodeID:      cm.localNode.NodeID,
		Term:        currentTerm,
		VoteGranted: voteGranted,
	}

	return cm.transport.Send(req.CandidateID, MsgVoteResponse, resp)
}

// handleVoteResponse processes vote responses
func (cm *ClusterManager) handleVoteResponse(msg *Message) error {
	var resp VoteResponse
	if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&resp); err != nil {
		return fmt.Errorf("failed to decode vote response: %w", err)
	}

	// Check if still candidate
	if cm.localNode.GetState() != StateCandidate {
		return nil
	}

	// Ignore votes from old terms
	if resp.Term != cm.electionTerm.Load() {
		logger.Debug("Ignoring vote from old term %d (current election term: %d)", resp.Term, cm.electionTerm.Load())
		return nil
	}

	// Check term - if response has higher term, step down
	if resp.Term > cm.localNode.CurrentTerm.Load() {
		cm.localNode.CurrentTerm.Store(resp.Term)
		cm.localNode.SetState(StateFollower)
		cm.localNode.ClearVotedFor()
		cm.persistState()
		return nil
	}

	if resp.VoteGranted {
		// Count distinct voters so a duplicate VoteResponse from the same node
		// cannot be counted twice.
		cm.votersMu.Lock()
		cm.voters[resp.NodeID] = struct{}{}
		votes := uint32(len(cm.voters))
		cm.votersMu.Unlock()
		cm.votesReceived.Store(votes)

		logger.Debug("Received vote from %s (total: %d, needed: %d)", resp.NodeID, votes, cm.votesNeeded.Load())

		// Check if we have majority. becomeLeader is idempotent per election, so
		// it runs at most once even if several responses cross the threshold.
		if votes >= cm.votesNeeded.Load() && cm.localNode.GetState() == StateCandidate {
			cm.becomeLeader()
		}
	}

	return nil
}

// handleAppendEntries processes append entries (leader heartbeat)
func (cm *ClusterManager) handleAppendEntries(msg *Message) error {
	// Reset election timer
	cm.resetElectionTimer()

	// In full Raft implementation, this would handle log replication
	// For now, it serves as leader heartbeat

	return nil
}

// handleReplicationPush processes replication push messages
func (cm *ClusterManager) handleReplicationPush(msg *Message) error {
	var push ReplicationPush
	if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&push); err != nil {
		return fmt.Errorf("failed to decode replication push: %w", err)
	}

	// OPTIMIZED: Update vector clocks asynchronously (non-blocking)
	for _, event := range push.Events {
		cm.localNode.UpdateVectorClock(event.NodeID, event.Clock) // Now uses async updates
	}

	// Process events
	for _, event := range push.Events {
		logger.Debug("Received replication event: op=%s, key=%s, from=%s",
			event.Operation, event.Key, event.NodeID)

		// Advance the local hybrid logical clock past the event's stamp so this
		// node's future writes are ordered after everything it has observed.
		if event.HLCPhysical != 0 || event.HLCLogical != 0 {
			cm.hlc.Update(types.HLCTimestamp{Physical: event.HLCPhysical, Logical: event.HLCLogical})
		}

		// Apply replication if handler is set
		if cm.replicationHandler != nil {
			if err := cm.replicationHandler.ApplyReplication(event); err != nil {
				logger.Error("Failed to apply replication: %v", err)
			}
		}
	}

	return nil
}

// handleNodeUpdate processes node update messages
func (cm *ClusterManager) handleNodeUpdate(msg *Message) error {
	var update NodeUpdate
	if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&update); err != nil {
		return fmt.Errorf("failed to decode node update: %w", err)
	}

	// Update term if needed
	if update.Term > cm.localNode.CurrentTerm.Load() {
		cm.localNode.CurrentTerm.Store(update.Term)
	}

	if update.Joined {
		// New node joined - add remote-node bookkeeping (no per-peer OVC goroutine).
		node := newRemoteNode(update.Node.NodeID, update.Node.Address, update.Node.Port, update.Node.ClusterID)
		node.CurrentTerm.Store(update.Node.CurrentTerm)

		// Connect to new node
		if err := cm.transport.ConnectToNode(update.Node.NodeID, update.Node.Address, update.Node.Port); err != nil {
			logger.Warning("Failed to connect to updated node %s: %v", update.Node.NodeID, err)
		} else {
			// Add to node registry
			cm.nodesMu.Lock()
			cm.nodes[update.Node.NodeID] = node
			cm.nodesMu.Unlock()

			logger.Info("Added node %s to cluster via broadcast update", update.Node.NodeID)
		}
	} else {
		// Node left - remove it from our node list
		cm.nodesMu.Lock()
		delete(cm.nodes, update.Node.NodeID)
		cm.nodesMu.Unlock()

		// Disconnect from node
		cm.transport.DisconnectFromNode(update.Node.NodeID)

		logger.Info("Removed node %s from cluster via broadcast update", update.Node.NodeID)
	}

	return nil
}
