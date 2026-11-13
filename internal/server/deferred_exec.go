package server

import (
	"strconv"

	"reservoir/pkg/logger"
)

// executeDeferred runs a due deferred command in cluster mode. It is installed
// as the store's DeferredExecutor so that a deferred command executes exactly
// once — on the node that accepted it — and its effect replicates through
// replicateWrite like any client write. key and args arrive with templates
// already expanded, so every replica receives the same concrete values.
//
// Each case mirrors the command handler's cluster path:
//   - SET/DEL are applied by replicateWrite itself (stamped LWW + tombstone).
//   - INCR/DECR/INCRBY/DECRBY apply to this node's PN-counter slot, then
//     replicateWrite ships the counter state.
//   - APPEND applies locally, then replicates the resulting value as a SET.
func (s *Server) executeDeferred(cmdType, key string, args []string) {
	switch cmdType {
	case "SET":
		if len(args) >= 1 {
			s.replicateWrite("SET", key, []byte(args[0]))
		}
	case "DEL":
		s.replicateWrite("DEL", key, nil)
	case "INCR", "DECR", "INCRBY", "DECRBY":
		delta := int64(1)
		switch cmdType {
		case "DECR":
			delta = -1
		case "INCRBY", "DECRBY":
			if len(args) < 1 {
				return
			}
			n, err := strconv.ParseInt(args[0], 10, 64)
			if err != nil || n == 0 {
				return
			}
			delta = n
			if cmdType == "DECRBY" {
				delta = -n
			}
		}
		if _, err := s.kvStore.IncrBy(key, delta); err != nil {
			logger.Warning("deferred %s on %q failed: %v", cmdType, key, err)
			return
		}
		s.replicateWrite(cmdType, key, nil)
	case "APPEND":
		if len(args) < 1 {
			return
		}
		if _, err := s.kvStore.Append(key, args[0]); err != nil {
			logger.Warning("deferred APPEND on %q failed: %v", key, err)
			return
		}
		if v, ok := s.kvStore.Get(key); ok {
			s.replicateWrite("SET", key, []byte(v))
		}
	default:
		logger.Warning("deferred command type %q is not supported in cluster mode", cmdType)
	}
}
