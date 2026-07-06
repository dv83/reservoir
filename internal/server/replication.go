package server

import (
	"bytes"
	"encoding/gob"
	"strconv"
	"strings"
	"time"

	"reservoir/internal/cluster"
	"reservoir/internal/store"
	"reservoir/pkg/logger"
)

// encodeCounterState serializes a PN-counter snapshot for the "COUNTER"
// replication payload.
func encodeCounterState(st store.CounterState) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(st); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeCounterState parses a "COUNTER" replication payload back into a snapshot.
func decodeCounterState(value []byte) (store.CounterState, error) {
	var st store.CounterState
	err := gob.NewDecoder(bytes.NewReader(value)).Decode(&st)
	return st, err
}

// encodeListDelta serializes a list CRDT delta for the "LDELTA" replication
// payload.
func encodeListDelta(d store.ListDelta) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(d); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// decodeListDelta parses an "LDELTA" replication payload back into a delta.
func decodeListDelta(value []byte) (store.ListDelta, error) {
	var d store.ListDelta
	err := gob.NewDecoder(bytes.NewReader(value)).Decode(&d)
	return d, err
}

// StoreReplicationHandler implements cluster.ReplicationHandler
type StoreReplicationHandler struct {
	Store store.KVStore
}

// membersFromReplData extracts the members from the null-separated set
// replication encoding "key\x00member1\x00member2\x00...". It returns nil when
// no members are present.
func membersFromReplData(value []byte) []string {
	parts := strings.Split(string(value), "\x00")
	if len(parts) < 2 {
		return nil
	}
	return parts[1:]
}

// hincrResult extracts the field and computed result from the HINCRBY/
// HINCRBYFLOAT replication encoding "key\x00field\x00increment\x00result".
func hincrResult(value []byte) (field, result string, ok bool) {
	parts := strings.Split(string(value), "\x00")
	if len(parts) < 4 {
		return "", "", false
	}
	return parts[1], parts[3], true
}

// ShardCount reports the store's shard count for anti-entropy rotation.
func (h *StoreReplicationHandler) ShardCount() int {
	return h.Store.ShardCount()
}

// LocalDigest returns the LWW digest of one shard as cluster SyncEntries.
func (h *StoreReplicationHandler) LocalDigest(shardIdx int) []cluster.SyncEntry {
	raw := h.Store.ShardLWWDigest(shardIdx)
	entries := make([]cluster.SyncEntry, 0, len(raw))
	for _, e := range raw {
		se := cluster.SyncEntry{
			Key:         e.Key,
			Member:      e.Member,
			Value:       []byte(e.Value),
			HLCPhysical: e.Physical,
			HLCLogical:  e.Logical,
			HLCOrigin:   e.Origin,
			Deleted:     e.Deleted,
			Hash:        e.Hash,
		}
		if e.Counter != nil {
			// Carry the counter state as the encoded value; peers max-merge it.
			if enc, err := encodeCounterState(*e.Counter); err == nil {
				se.Value = enc
				se.Counter = true
			} else {
				logger.Warning("failed to encode counter digest for %s: %v", e.Key, err)
				continue
			}
		}
		entries = append(entries, se)
	}
	return entries
}

// ApplyReplication applies a replication event to the local store
func (h *StoreReplicationHandler) ApplyReplication(event cluster.ReplicationEvent) error {
	logger.Debug("Applying replication event: Op=%s, Key=%s, EventID=%s", event.Operation, event.Key, event.EventID)
	switch event.Operation {
	case "SET":
		// Last-write-wins: apply the value only if the event's HLC stamp is newer
		// than what we hold. A stamped event (HLC set) goes through SetLWW so
		// concurrent writes converge; unstamped events (older senders) fall back
		// to a plain Set.
		if event.HLCPhysical != 0 || event.HLCLogical != 0 {
			_, err := h.Store.SetLWW(event.Key, string(event.Value), event.HLCPhysical, event.HLCLogical, event.HLCOrigin)
			return err
		}
		return h.Store.Set(event.Key, string(event.Value))
	case "DEL":
		// Last-write-wins delete: a stamped tombstone so a stale older write can't
		// resurrect the key. Unstamped events fall back to a plain delete.
		if event.HLCPhysical != 0 || event.HLCLogical != 0 {
			h.Store.DeleteLWW(event.Key, event.HLCPhysical, event.HLCLogical, event.HLCOrigin)
			return nil
		}
		h.Store.Delete(event.Key)
		return nil
	case "SADD":
		// Parse members from null-separated replication data: "key\x00member1\x00member2\x00..."
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid SADD replication data: %s", string(event.Value))
			return nil
		}
		members := parts[1:]
		// Element-level CRDT: a stamped event converges under last-write-wins;
		// unstamped events (older senders) fall back to a plain add.
		if event.HLCPhysical != 0 || event.HLCLogical != 0 {
			_, err := h.Store.SAddLWW(event.Key, event.HLCPhysical, event.HLCLogical, event.HLCOrigin, members...)
			return err
		}
		_, err := h.Store.SAdd(event.Key, members...)
		return err
	case "SREM":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid SREM replication data: %s", string(event.Value))
			return nil
		}
		members := parts[1:]
		if event.HLCPhysical != 0 || event.HLCLogical != 0 {
			_, err := h.Store.SRemLWW(event.Key, event.HLCPhysical, event.HLCLogical, event.HLCOrigin, members...)
			return err
		}
		_, err := h.Store.SRem(event.Key, members...)
		return err
	case "MSET":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 || len(parts)%2 != 0 {
			logger.Warning("Invalid MSET replication data: %s", string(event.Value))
			return nil
		}
		for i := 0; i < len(parts); i += 2 {
			key := parts[i]
			value := parts[i+1]
			if err := h.Store.Set(key, value); err != nil {
				return err
			}
		}
		return nil
	case "HMSET", "HSET":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 3 || len(parts)%2 == 0 {
			logger.Warning("Invalid HSET/HMSET replication data: %s", string(event.Value))
			return nil
		}
		// parts[0] is key, parts[1:] are field-value pairs. Per-field CRDT: a
		// stamped event converges under LWW; unstamped events fall back to plain.
		if event.HLCPhysical != 0 || event.HLCLogical != 0 {
			_, err := h.Store.HSetLWW(event.Key, event.HLCPhysical, event.HLCLogical, event.HLCOrigin, parts[1:]...)
			return err
		}
		_, err := h.Store.HSet(event.Key, parts[1:]...)
		return err
	case "HDEL":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid HDEL replication data: %s", string(event.Value))
			return nil
		}
		fields := parts[1:]
		if event.HLCPhysical != 0 || event.HLCLogical != 0 {
			_, err := h.Store.HDelLWW(event.Key, event.HLCPhysical, event.HLCLogical, event.HLCOrigin, fields...)
			return err
		}
		_, err := h.Store.HDel(event.Key, fields...)
		return err
	case "HINCRBY", "HINCRBYFLOAT":
		// Data: "key\x00field\x00increment\x00result". The result is applied as a
		// per-field set so it converges (last-write-wins on the field) and stays
		// consistent with the field CRDT; unstamped events fall back to plain.
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 4 {
			logger.Warning("Invalid %s replication data: %s", event.Operation, string(event.Value))
			return nil
		}
		field := parts[1]
		resultStr := parts[3]
		if event.HLCPhysical != 0 || event.HLCLogical != 0 {
			_, err := h.Store.HSetLWW(event.Key, event.HLCPhysical, event.HLCLogical, event.HLCOrigin, field, resultStr)
			return err
		}
		_, err := h.Store.HSet(event.Key, field, resultStr)
		return err
	case "SPOP":
		element := string(event.Value)
		// SPOP replicates as a removal of the specific popped element; a stamped
		// event records a converging tombstone, like SREM.
		if event.HLCPhysical != 0 || event.HLCLogical != 0 {
			_, err := h.Store.SRemLWW(event.Key, event.HLCPhysical, event.HLCLogical, event.HLCOrigin, element)
			return err
		}
		_, err := h.Store.SRem(event.Key, element)
		return err
	case "SDIFFSTORE":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid SDIFFSTORE replication data: %s", string(event.Value))
			return nil
		}
		sourceKeys := parts[1:]
		_, err := h.Store.SDiffStore(event.Key, sourceKeys...)
		return err
	case "LPUSH":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid LPUSH replication data: %s", string(event.Value))
			return nil
		}
		elements := parts[1:]
		_, err := h.Store.LPush(event.Key, elements...)
		return err
	case "RPUSH":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid RPUSH replication data: %s", string(event.Value))
			return nil
		}
		elements := parts[1:]
		_, err := h.Store.RPush(event.Key, elements...)
		return err
	case "LPUSHX":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid LPUSHX replication data: %s", string(event.Value))
			return nil
		}
		elements := parts[1:]
		_, err := h.Store.LPushX(event.Key, elements...)
		return err
	case "RPUSHX":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid RPUSHX replication data: %s", string(event.Value))
			return nil
		}
		elements := parts[1:]
		_, err := h.Store.RPushX(event.Key, elements...)
		return err
	case "LPUSHUNIQUE":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid LPUSHUNIQUE replication data: %s", string(event.Value))
			return nil
		}
		elements := parts[1:]
		_, err := h.Store.LPushUnique(event.Key, elements...)
		return err
	case "RPUSHUNIQUE":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid RPUSHUNIQUE replication data: %s", string(event.Value))
			return nil
		}
		elements := parts[1:]
		_, err := h.Store.RPushUnique(event.Key, elements...)
		return err
	case "LPOP":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid LPOP replication data: %s", string(event.Value))
			return nil
		}
		count, err := strconv.Atoi(parts[1])
		if err != nil {
			logger.Warning("Invalid LPOP count in replication data: %s", parts[1])
			return nil
		}
		_, err = h.Store.LPop(event.Key, count)
		return err
	case "RPOP":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid RPOP replication data: %s", string(event.Value))
			return nil
		}
		count, err := strconv.Atoi(parts[1])
		if err != nil {
			logger.Warning("Invalid RPOP count in replication data: %s", parts[1])
			return nil
		}
		_, err = h.Store.RPop(event.Key, count)
		return err
	case "LSET":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) != 3 {
			logger.Warning("Invalid LSET replication data: %s", string(event.Value))
			return nil
		}
		index, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			logger.Warning("Invalid LSET index in replication data: %s", parts[1])
			return nil
		}
		element := parts[2]
		return h.Store.LSet(event.Key, index, element)
	case "LREM":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) != 3 {
			logger.Warning("Invalid LREM replication data: %s", string(event.Value))
			return nil
		}
		count, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			logger.Warning("Invalid LREM count in replication data: %s", parts[1])
			return nil
		}
		element := parts[2]
		_, err = h.Store.LRem(event.Key, count, element)
		return err
	case "LTRIM":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) != 3 {
			logger.Warning("Invalid LTRIM replication data: %s", string(event.Value))
			return nil
		}
		start, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			logger.Warning("Invalid LTRIM start in replication data: %s", parts[1])
			return nil
		}
		stop, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			logger.Warning("Invalid LTRIM stop in replication data: %s", parts[2])
			return nil
		}
		return h.Store.LTrim(event.Key, start, stop)
	case "LDELTA":
		// Convergent list delta: RGA element inserts/updates and tombstones,
		// applied idempotently so lists converge regardless of order.
		d, err := decodeListDelta(event.Value)
		if err != nil {
			logger.Warning("Invalid LDELTA replication data for %s: %v", event.Key, err)
			return nil
		}
		return h.Store.ApplyListDelta(event.Key, d)
	case "COUNTER":
		// Convergent PN-counter state: max-merge into the local counter so
		// concurrent increments from any node are all preserved.
		st, err := decodeCounterState(event.Value)
		if err != nil {
			logger.Warning("Invalid COUNTER replication data for %s: %v", event.Key, err)
			return nil
		}
		_, err = h.Store.MergeCounter(event.Key, st)
		return err
	case "INCR", "DECR":
		// Legacy result-based path (pre-CRDT senders): overwrites the value.
		return h.Store.Set(event.Key, string(event.Value))
	case "INCRBY", "DECRBY":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) != 2 {
			logger.Warning("Invalid INCRBY/DECRBY replication data: %s", string(event.Value))
			return nil
		}
		resultStr := parts[1]
		return h.Store.Set(event.Key, resultStr)
	case "APPEND":
		return h.Store.Set(event.Key, string(event.Value))
	case "DEFER":
		deferData := string(event.Value)
		parts := strings.Split(deferData, ":")
		if len(parts) < 3 {
			logger.Warning("Invalid DEFER replication data: %s", deferData)
			return nil
		}
		delaySeconds, _ := strconv.Atoi(parts[0])
		commandType := parts[1]
		key := parts[2]
		args := parts[3:]
		if lockFreeStore, ok := h.Store.(*store.LockFreeStore); ok {
			cmd := &store.DeferredCommand{
				ID:          event.Key,
				ExecuteAt:   time.Now().Add(time.Duration(delaySeconds) * time.Second),
				CommandType: commandType,
				Key:         key,
				Args:        args,
				CreatedAt:   time.Now(),
			}
			lockFreeStore.AddDeferredCommandDirect(cmd)
		}
		return nil
	case "DEFER.CANCEL":
		if lockFreeStore, ok := h.Store.(*store.LockFreeStore); ok {
			lockFreeStore.CancelDeferredCommand(event.Key)
		}
		return nil
	case "EXPIRE":
		// Value is the absolute expiry time as unix nanoseconds, so replicas
		// converge on the same deadline rather than each adding a relative TTL.
		nano, err := strconv.ParseInt(string(event.Value), 10, 64)
		if err != nil {
			return err
		}
		h.Store.SetExpiry(event.Key, time.Unix(0, nano))
		return nil
	case "PERSIST":
		h.Store.RemoveExpiry(event.Key)
		return nil
	case "FLUSHALL", "FLUSHDB":
		h.Store.Clear()
		return nil
	default:
		logger.Warning("Unknown replication operation: %s", event.Operation)
		return nil
	}
}
