package server

import (
	"bufio"
	"net"
	"strings"
	"time"

	"reservoir/internal/commands"
	"reservoir/internal/protocol"
	"reservoir/internal/store"
	"reservoir/pkg/logger"
)

// replicatingHandler is a command handler that also emits replication events
// for cluster mode via the supplied callback.
type replicatingHandler func(store.KVStore, *protocol.ZeroCopyCommand, *bufio.Writer, commands.ReplicationCallback) error

// replicatingHandlers is the single source of truth for which commands are
// writes and how each one replicates. A command present here is a write and is
// dispatched through its replication-aware handler; everything else is treated
// as a read and dispatched through the plain registry handler.
//
// This replaces an earlier three-way scheme (a writeOperations bool map, a
// hand-written switch, and a "commands starting with G are reads" heuristic)
// whose structures had drifted out of sync — INCR/DECR/APPEND/SPOP/LSET/LREM/
// LTRIM/LPUSHX/RPUSHX/SDIFFSTORE were flagged as writes but had no switch case
// (so they silently fell back to the non-replicating handler), and the hash
// commands were the mirror image (switch cases with no map entry, so they were
// never reached). Keeping one table eliminates that whole class of bug.
var replicatingHandlers = map[string]replicatingHandler{
	// Strings
	"SET":     commands.HandleZeroCopySetWithReplication,
	"DEL":     commands.HandleZeroCopyDelWithReplication,
	"MSET":    commands.HandleZeroCopyMSetWithReplication,
	"APPEND":  commands.HandleZeroCopyAppendWithReplication,
	"GETSET":  commands.HandleZeroCopyGetSetWithReplication,
	"INCR":    commands.HandleZeroCopyIncrWithReplication,
	"DECR":    commands.HandleZeroCopyDecrWithReplication,
	"EXPIRE":  commands.HandleZeroCopyExpireWithReplication,
	"PERSIST": commands.HandleZeroCopyPersistWithReplication,
	"INCRBY":  commands.HandleZeroCopyIncrByWithReplication,
	"DECRBY":  commands.HandleZeroCopyDecrByWithReplication,
	// Sets
	"SADD":       commands.HandleZeroCopySAddWithReplication,
	"SREM":       commands.HandleZeroCopySRemWithReplication,
	"SPOP":       commands.HandleZeroCopySPopWithReplication,
	"SDIFFSTORE": commands.HandleZeroCopySDiffStoreWithReplication,
	// Lists
	"LPUSH":       commands.HandleZeroCopyLPushWithReplication,
	"RPUSH":       commands.HandleZeroCopyRPushWithReplication,
	"LPUSHX":      commands.HandleZeroCopyLPushXWithReplication,
	"RPUSHX":      commands.HandleZeroCopyRPushXWithReplication,
	"LPUSHUNIQUE": commands.HandleZeroCopyLPushUniqueWithReplication,
	"RPUSHUNIQUE": commands.HandleZeroCopyRPushUniqueWithReplication,
	"LPOP":        commands.HandleZeroCopyLPopWithReplication,
	"RPOP":        commands.HandleZeroCopyRPopWithReplication,
	"LSET":        commands.HandleZeroCopyLSetWithReplication,
	"LREM":        commands.HandleZeroCopyLRemWithReplication,
	"LTRIM":       commands.HandleZeroCopyLTrimWithReplication,
	// Hashes
	"HSET":         commands.HandleZeroCopyHSetWithReplication,
	"HMSET":        commands.HandleZeroCopyHMSetWithReplication,
	"HDEL":         commands.HandleZeroCopyHDelWithReplication,
	"HINCRBY":      commands.HandleZeroCopyHIncrByWithReplication,
	"HINCRBYFLOAT": commands.HandleZeroCopyHIncrByFloatWithReplication,
	// Deferred + admin
	"DEFER":        commands.HandleZeroCopyDeferWithReplication,
	"DEFER.CANCEL": commands.HandleZeroCopyDeferCancelWithReplication,
	"FLUSHALL":     commands.HandleZeroCopyFlushAllWithReplication,
	"FLUSHDB":      commands.HandleZeroCopyFlushDBWithReplication,
}

func (s *Server) handleConnection(conn net.Conn) {
	clientAddr := conn.RemoteAddr().String()

	// Extract IP address from client address (remove port)
	clientIP := clientAddr
	if host, _, err := net.SplitHostPort(clientAddr); err == nil {
		clientIP = host
	}

	// Simplified panic recovery (essential for connection stability)
	defer func() {
		_ = conn.Close()
		if r := recover(); r != nil {
			logger.Error("Connection handler panic for %s: %v", clientAddr, r)
		}
	}()

	// Add connection to manager
	s.connectionManager.AddConnection(clientIP)
	defer s.connectionManager.RemoveConnection(clientIP)

	// Set TCP options for optimal performance
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)                     // Lower latency - disable Nagle
		_ = tcpConn.SetKeepAlive(true)                   // Enable keep-alive
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second) // Keep-alive probe interval
		// Long timeouts to prevent dropping active connections during load
		_ = tcpConn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		_ = tcpConn.SetWriteDeadline(time.Now().Add(5 * time.Minute))
	}

	// Adaptive buffer sizing for optimal performance
	// Small commands (GET/SET) use smaller buffers, large commands get bigger buffers
	reader := bufio.NewReaderSize(conn, 8192) // 8KB read buffer (sufficient for most commands)
	writer := bufio.NewWriterSize(conn, 4096) // 4KB write buffer (optimized for response sizes)
	defer func() { _ = writer.Flush() }()

	if s.cfg.Debug {
		logger.Debug("Client connected: %s", clientAddr)
	}

	// Create zero-copy parser for this connection
	parser := protocol.NewZeroCopyParser()
	defer parser.Release() // Return buffer to pool when done

	// Pre-create replication callback once per connection (optimization).
	// SET is resolved by last-write-wins: it is stamped with a single HLC used
	// for both the local apply and the replicated event, so all replicas
	// converge. Other operations keep the plain queue-only path (the handler has
	// already applied them locally).
	var replicationCallback commands.ReplicationCallback
	if s.clusterManager != nil {
		replicationCallback = func(operation, key string, value []byte) {
			switch operation {
			case "SET":
				p, l, o := s.clusterManager.NextStamp()
				_, _ = s.kvStore.SetLWW(key, string(value), p, l, o)
				s.clusterManager.QueueReplicationLWW(operation, key, value, p, l, o)
			case "DEL":
				p, l, o := s.clusterManager.NextStamp()
				s.kvStore.DeleteLWW(key, p, l, o)
				s.clusterManager.QueueReplicationLWW(operation, key, value, p, l, o)
			default:
				s.clusterManager.QueueReplication(operation, key, value)
			}
		}
	}

	for {
		// Parse command using zero-copy parser
		cmd, err := parser.ParseCommand(reader)
		if err != nil {
			if s.cfg.Debug {
				logger.Debug("Client %s disconnected: %v", clientAddr, err)
			}
			return
		}

		// ULTRA HOT PATH - Minimize allocations and system calls
		// Get command using zero-copy byte slice comparison (no string allocation)
		cmdBytes := cmd.Command
		cmdLen := len(cmdBytes)

		// Fast path for most common commands using byte comparison (no string allocation)
		var handlerErr error

		// GET command (most common read operation) - ultra fast path
		if cmdLen == 3 &&
			(cmdBytes[0] == 'G' || cmdBytes[0] == 'g') &&
			(cmdBytes[1] == 'E' || cmdBytes[1] == 'e') &&
			(cmdBytes[2] == 'T' || cmdBytes[2] == 't') {
			// Direct GET handling - bypass all overhead
			handlerErr = commands.HandleZeroCopyGet(s.kvStore, cmd, writer)
			// SET command (most common write operation) - ultra fast path
		} else if cmdLen == 3 &&
			(cmdBytes[0] == 'S' || cmdBytes[0] == 's') &&
			(cmdBytes[1] == 'E' || cmdBytes[1] == 'e') &&
			(cmdBytes[2] == 'T' || cmdBytes[2] == 't') {
			// Direct SET with replication - bypass overhead
			handlerErr = commands.HandleZeroCopySetWithReplication(s.kvStore, cmd, writer, replicationCallback)
		} else {
			// Fallback to standard handler lookup for other commands
			commandName := cmd.String() // Only allocate string when needed
			upperCmd := strings.ToUpper(commandName)

			if rh, isWrite := replicatingHandlers[upperCmd]; isWrite {
				// Write command: dispatch through its replication-aware handler
				// so the mutation is propagated to the cluster.
				handlerErr = rh(s.kvStore, cmd, writer, replicationCallback)
			} else if handler, exists := s.commandRegistry.GetHandler(commandName); exists {
				// Read (or non-replicated) command.
				handlerErr = handler(s.kvStore, cmd, writer)
			} else {
				// Unknown command error
				if _, writeErr := writer.Write([]byte("-ERR unknown command '" + commandName + "'\r\n")); writeErr != nil {
					return // Connection broken
				}
				goto skipMonitoring // Skip expensive monitoring for error case
			}
		}

	skipMonitoring:

		// Release command buffer back to pool after processing is complete
		// This must be after skipMonitoring label to ensure cleanup on all paths
		cmd.Release()

		// Handle errors with minimal overhead
		if handlerErr != nil {
			writer.Write([]byte("-ERR internal server error\r\n"))
		}

		// Flush after each command for consistency
		if flushErr := writer.Flush(); flushErr != nil {
			if s.cfg.Debug {
				logger.Debug("Flush error for %s: %v", clientAddr, flushErr)
			}
			return // Connection broken
		}
	}
}
