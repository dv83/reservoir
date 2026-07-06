package server

import (
	"strconv"
	"strings"
	"time"

	"reservoir/internal/cluster"
	"reservoir/internal/store"
	"reservoir/pkg/logger"
)

// StoreReplicationHandler implements cluster.ReplicationHandler
type StoreReplicationHandler struct {
	Store store.KVStore
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
		_, err := h.Store.SAdd(event.Key, members...)
		return err
	case "SREM":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid SREM replication data: %s", string(event.Value))
			return nil
		}
		members := parts[1:]
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
		// parts[0] is key, parts[1:] are field-value pairs
		_, err := h.Store.HSet(event.Key, parts[1:]...)
		return err
	case "HDEL":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 2 {
			logger.Warning("Invalid HDEL replication data: %s", string(event.Value))
			return nil
		}
		fields := parts[1:]
		_, err := h.Store.HDel(event.Key, fields...)
		return err
	case "HINCRBY":
		// Data: "key\x00field\x00increment\x00result"
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 4 {
			logger.Warning("Invalid HINCRBY replication data: %s", string(event.Value))
			return nil
		}
		field := parts[1]
		resultStr := parts[3]
		// We use HSet to apply the fixed result on followers to ensure consistency
		_, err := h.Store.HSet(event.Key, field, resultStr)
		return err
	case "HINCRBYFLOAT":
		parts := strings.Split(string(event.Value), "\x00")
		if len(parts) < 4 {
			logger.Warning("Invalid HINCRBYFLOAT replication data: %s", string(event.Value))
			return nil
		}
		field := parts[1]
		resultStr := parts[3]
		_, err := h.Store.HSet(event.Key, field, resultStr)
		return err
	case "SPOP":
		element := string(event.Value)
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
	case "INCR", "DECR":
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
