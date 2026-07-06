package cluster

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"time"

	"reservoir/pkg/logger"
)

// antiEntropyInterval is how often a node reconciles one shard with one random
// peer. Anti-entropy is a background safety net that heals divergence caused by
// dropped replication events; live replication handles the common case.
const antiEntropyInterval = 2 * time.Second

// antiEntropyLoop periodically asks a random peer for the LWW digest of the next
// shard (rotating), then reconciles via last-write-wins.
func (cm *ClusterManager) antiEntropyLoop() {
	defer cm.wg.Done()

	ticker := time.NewTicker(antiEntropyInterval)
	defer ticker.Stop()

	shardIdx := 0
	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			if cm.replicationHandler == nil {
				continue
			}
			peer, ok := cm.randomPeer()
			if !ok {
				continue
			}
			n := cm.replicationHandler.ShardCount()
			if n == 0 {
				continue
			}
			req := SyncRequest{NodeID: cm.localNode.NodeID, ShardIdx: shardIdx % n}
			if err := cm.transport.Send(peer, MsgSyncRequest, req); err != nil {
				logger.Debug("anti-entropy: failed to request digest from %s: %v", peer, err)
			}
			shardIdx = (shardIdx + 1) % n
		}
	}
}

// randomPeer returns a random known peer node id.
func (cm *ClusterManager) randomPeer() (UUIDv7, bool) {
	cm.nodesMu.RLock()
	defer cm.nodesMu.RUnlock()
	if len(cm.nodes) == 0 {
		return UUIDv7{}, false
	}
	idx := rand.Intn(len(cm.nodes))
	i := 0
	for id := range cm.nodes {
		if i == idx {
			return id, true
		}
		i++
	}
	return UUIDv7{}, false
}

// handleSyncRequest responds with the local LWW digest of the requested shard.
func (cm *ClusterManager) handleSyncRequest(msg *Message) error {
	var req SyncRequest
	if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&req); err != nil {
		return fmt.Errorf("failed to decode sync request: %w", err)
	}
	if cm.replicationHandler == nil {
		return nil
	}
	resp := SyncResponse{
		NodeID:   cm.localNode.NodeID,
		ShardIdx: req.ShardIdx,
		Entries:  cm.replicationHandler.LocalDigest(req.ShardIdx),
	}
	return cm.transport.Send(req.NodeID, MsgSyncResponse, resp)
}

// handleSyncResponse reconciles the peer's digest into the local store. Each
// entry is applied under last-write-wins (via the replication apply path), so
// only entries strictly newer than what we hold take effect.
func (cm *ClusterManager) handleSyncResponse(msg *Message) error {
	var resp SyncResponse
	if err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&resp); err != nil {
		return fmt.Errorf("failed to decode sync response: %w", err)
	}
	if cm.replicationHandler == nil {
		return nil
	}

	for _, e := range resp.Entries {
		op := "SET"
		if e.Deleted {
			op = "DEL"
		}
		event := ReplicationEvent{
			Operation:   op,
			Key:         e.Key,
			Value:       e.Value,
			HLCPhysical: e.HLCPhysical,
			HLCLogical:  e.HLCLogical,
			HLCOrigin:   e.HLCOrigin,
		}
		if err := cm.replicationHandler.ApplyReplication(event); err != nil {
			logger.Debug("anti-entropy: apply failed for key %s: %v", e.Key, err)
		}
	}
	logger.Debug("anti-entropy: reconciled shard %d with %d entries from %s",
		resp.ShardIdx, len(resp.Entries), resp.NodeID)
	return nil
}
