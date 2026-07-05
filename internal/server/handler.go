package server

import (
	"bufio"
	"net"
	"strings"
	"time"

	"reservoir/internal/commands"
	"reservoir/internal/protocol"
	"reservoir/pkg/logger"
)

// Pre-computed write operations map for better performance
var writeOperations = map[string]bool{
	"SET":          true,
	"DEL":          true,
	"MSET":         true,
	"SADD":         true,
	"SREM":         true,
	"SPOP":         true,
	"SDIFFSTORE":   true,
	"LPUSH":        true,
	"RPUSH":        true,
	"LPUSHX":       true,
	"RPUSHX":       true,
	"LPUSHUNIQUE":  true,
	"RPUSHUNIQUE":  true,
	"LPOP":         true,
	"RPOP":         true,
	"LSET":         true,
	"LREM":         true,
	"LTRIM":        true,
	"INCR":         true,
	"DECR":         true,
	"INCRBY":       true,
	"DECRBY":       true,
	"APPEND":       true,
	"DEFER":        true,
	"DEFER.CANCEL": true,
	"FLUSHALL":     true, // Only supported in cluster mode if propagated
	"FLUSHDB":      true,
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

	// Pre-create replication callback once per connection (optimization)
	var replicationCallback commands.ReplicationCallback
	if s.clusterManager != nil {
		replicationCallback = func(operation, key string, value []byte) {
			s.clusterManager.QueueReplication(operation, key, value)
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
		var isWriteOp bool

		// GET command (most common read operation) - ultra fast path
		if cmdLen == 3 &&
			(cmdBytes[0] == 'G' || cmdBytes[0] == 'g') &&
			(cmdBytes[1] == 'E' || cmdBytes[1] == 'e') &&
			(cmdBytes[2] == 'T' || cmdBytes[2] == 't') {
			// Direct GET handling - bypass all overhead
			handlerErr = commands.HandleZeroCopyGet(s.kvStore, cmd, writer)
			isWriteOp = false
			// SET command (most common write operation) - ultra fast path
		} else if cmdLen == 3 &&
			(cmdBytes[0] == 'S' || cmdBytes[0] == 's') &&
			(cmdBytes[1] == 'E' || cmdBytes[1] == 'e') &&
			(cmdBytes[2] == 'T' || cmdBytes[2] == 't') {
			// Direct SET with replication - bypass overhead
			handlerErr = commands.HandleZeroCopySetWithReplication(s.kvStore, cmd, writer, replicationCallback)
			isWriteOp = true
		} else {
			// Fallback to standard handler lookup for other commands
			commandName := cmd.String() // Only allocate string when needed
			handler, exists := s.commandRegistry.GetHandler(commandName)
			if !exists {
				// Unknown command error
				if _, writeErr := writer.Write([]byte("-ERR unknown command\r\n")); writeErr != nil {
					return // Connection broken
				}
				goto skipMonitoring // Skip expensive monitoring for error case
			}

			// Check if write operation (only when not GET/SET)
			firstChar := commandName[0]
			if firstChar == 'g' || firstChar == 'G' {
				// Most read operations start with 'g' - assume read
				isWriteOp = false
				handlerErr = handler(s.kvStore, cmd, writer)
			} else {
				// Check writeOperations map only when necessary
				upperCmd := strings.ToUpper(commandName)
				isWriteOp = writeOperations[upperCmd]

				if isWriteOp {
					// Use specific replication handlers
					switch upperCmd {
					case "DEL":
						handlerErr = commands.HandleZeroCopyDelWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "MSET":
						handlerErr = commands.HandleZeroCopyMSetWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "SADD":
						handlerErr = commands.HandleZeroCopySAddWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "SREM":
						handlerErr = commands.HandleZeroCopySRemWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "LPUSH":
						handlerErr = commands.HandleZeroCopyLPushWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "RPUSH":
						handlerErr = commands.HandleZeroCopyRPushWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "LPOP":
						handlerErr = commands.HandleZeroCopyLPopWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "RPOP":
						handlerErr = commands.HandleZeroCopyRPopWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "HSET":
						handlerErr = commands.HandleZeroCopyHSetWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "HMSET":
						handlerErr = commands.HandleZeroCopyHMSetWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "HDEL":
						handlerErr = commands.HandleZeroCopyHDelWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "HINCRBY":
						handlerErr = commands.HandleZeroCopyHIncrByWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "HINCRBYFLOAT":
						handlerErr = commands.HandleZeroCopyHIncrByFloatWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "LPUSHUNIQUE":
						handlerErr = commands.HandleZeroCopyLPushUniqueWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "RPUSHUNIQUE":
						handlerErr = commands.HandleZeroCopyRPushUniqueWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "DEFER":
						handlerErr = commands.HandleZeroCopyDeferWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "DEFER.CANCEL":
						handlerErr = commands.HandleZeroCopyDeferCancelWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "FLUSHALL":
						handlerErr = commands.HandleZeroCopyFlushAllWithReplication(s.kvStore, cmd, writer, replicationCallback)
					case "FLUSHDB":
						handlerErr = commands.HandleZeroCopyFlushDBWithReplication(s.kvStore, cmd, writer, replicationCallback)
					default:
						// Fallback for other write commands
						handlerErr = handler(s.kvStore, cmd, writer)
					}
				} else {
					// Read operation
					handlerErr = handler(s.kvStore, cmd, writer)
				}
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
