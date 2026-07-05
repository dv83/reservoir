package commands

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"time"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

// Server command handlers for Redis server/utility operations

// HandleZeroCopyPing processes PING command with zero-copy
func HandleZeroCopyPing(_ store.KVStore, _ *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	_, err := writer.Write(pongResponse)
	return err
}

// HandleZeroCopyExpire processes EXPIRE command with zero-copy
func HandleZeroCopyExpire(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 2 {
		return writeError(writer, "wrong number of arguments for 'expire' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	secondsStr := cmd.ArgString(1)

	seconds, err := strconv.ParseInt(secondsStr, 10, 64)
	if err != nil {
		return writeError(writer, errNotInteger)
	}

	expireTime := time.Now().Add(time.Duration(seconds) * time.Second)
	success := kvStore.SetExpiry(key, expireTime)

	if success {
		return writeInteger(writer, 1)
	} else {
		return writeInteger(writer, 0)
	}
}

// HandleZeroCopyPExpire processes PEXPIRE (TTL in milliseconds) with zero-copy
func HandleZeroCopyPExpire(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 2 {
		return writeError(writer, "wrong number of arguments for 'pexpire' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	millisStr := cmd.ArgString(1)

	millis, err := strconv.ParseInt(millisStr, 10, 64)
	if err != nil {
		return writeError(writer, errNotInteger)
	}

	expireTime := time.Now().Add(time.Duration(millis) * time.Millisecond)
	if kvStore.SetExpiry(key, expireTime) {
		return writeInteger(writer, 1)
	}
	return writeInteger(writer, 0)
}

// HandleZeroCopyTTL processes TTL command with zero-copy
func HandleZeroCopyTTL(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'ttl' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	return writeTTL(kvStore, writer, key, false)
}

// HandleZeroCopyPTTL processes PTTL (TTL in milliseconds) with zero-copy
func HandleZeroCopyPTTL(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'pttl' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	return writeTTL(kvStore, writer, key, true)
}

// writeTTL implements Redis TTL/PTTL semantics: -2 if the key does not exist,
// -1 if it exists but has no associated expiry, otherwise the remaining time
// (seconds for TTL, milliseconds for PTTL).
func writeTTL(kvStore store.KVStore, writer *bufio.Writer, key string, millis bool) error {
	ttl, hasExpiry := kvStore.GetTTL(key)
	if hasExpiry && ttl > 0 {
		if millis {
			return writeInteger(writer, int64(ttl.Milliseconds()))
		}
		return writeInteger(writer, int64(ttl.Seconds()))
	}

	// No live expiry: distinguish a missing key (-2) from a key with no TTL (-1).
	if kvStore.Exists(key) == 0 {
		return writeInteger(writer, -2)
	}
	return writeInteger(writer, -1)
}

// HandleZeroCopyPersist processes PERSIST command with zero-copy
func HandleZeroCopyPersist(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'persist' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	success := kvStore.RemoveExpiry(key)

	if success {
		return writeInteger(writer, 1)
	} else {
		return writeInteger(writer, 0)
	}
}

// HandleZeroCopyKeys processes KEYS command with zero-copy
func HandleZeroCopyKeys(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'keys' command")
	}

	pattern := cmd.ArgString(0)
	allKeys := kvStore.GetAllKeys()

	// Fast path for "*" (all keys); otherwise filter by the glob pattern.
	keys := allKeys
	if pattern != "*" {
		keys = keys[:0:0]
		for _, key := range allKeys {
			if globMatch(pattern, key) {
				keys = append(keys, key)
			}
		}
	}

	// Write array header
	if err := writeArrayStart(writer, len(keys)); err != nil {
		return err
	}

	// Write each key
	for _, key := range keys {
		if err := writeBulkString(writer, key); err != nil {
			return err
		}
	}

	return nil
}

// HandleZeroCopyInfo processes INFO command with zero-copy
func HandleZeroCopyInfo(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	var section string
	if cmd.ArgCount() > 0 {
		section = strings.ToLower(cmd.ArgString(0))
	}

	info := getInfoSection(kvStore, section)
	return writeBulkString(writer, info)
}

// HandleZeroCopyFlushDBWithReplication processes FLUSHDB command with replication
func HandleZeroCopyFlushDBWithReplication(kvStore store.KVStore, _ *protocol.ZeroCopyCommand, writer *bufio.Writer, replicationCallback ReplicationCallback) error {
	// Clear the store
	kvStore.Clear()

	// Replicate FLUSHDB operation to other nodes
	if replicationCallback != nil {
		replicationCallback("FLUSHDB", "", nil)
	}

	_, err := writer.Write(okResponse)
	return err
}

// HandleZeroCopyFlushAllWithReplication processes FLUSHALL command with replication
func HandleZeroCopyFlushAllWithReplication(kvStore store.KVStore, _ *protocol.ZeroCopyCommand, writer *bufio.Writer, replicationCallback ReplicationCallback) error {
	// Clear the store
	kvStore.Clear()

	// Replicate FLUSHALL operation to other nodes
	if replicationCallback != nil {
		replicationCallback("FLUSHALL", "", nil)
	}

	_, err := writer.Write(okResponse)
	return err
}

// HandleZeroCopySelect processes SELECT command with zero-copy
func HandleZeroCopySelect(_ store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'select' command")
	}

	dbIndex, err := strconv.Atoi(cmd.ArgString(0))
	if err != nil || dbIndex < 0 {
		return writeError(writer, "invalid DB index")
	}

	// Reservoir only supports database 0
	if dbIndex != 0 {
		return writeError(writer, "only database 0 is supported")
	}

	_, err = writer.Write(okResponse)
	return err
}

// HandleZeroCopyConn processes CLIENT command with zero-copy
func HandleZeroCopyConn(_ store.KVStore, _ *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	// Simple client info response
	clientInfo := "id=1 addr=127.0.0.1:6379 name= age=0 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client"
	return writeBulkString(writer, clientInfo)
}

// HandleZeroCopyConfig processes CONFIG command with zero-copy
func HandleZeroCopyConfig(_ store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() < 1 {
		return writeError(writer, "wrong number of arguments for 'config' command")
	}

	subcommand := cmd.ArgString(0)
	switch subcommand {
	case "GET", "get":
		if cmd.ArgCount() < 2 {
			return writeError(writer, "wrong number of arguments for 'config get' command")
		}

		parameter := cmd.ArgString(1)
		return handleConfigGet(writer, parameter)

	case "SET", "set":
		// CONFIG SET is read-only in Reservoir
		return writeError(writer, "CONFIG SET is not supported in Reservoir (read-only)")

	default:
		return writeError(writer, fmt.Sprintf("unknown CONFIG subcommand '%s'", subcommand))
	}
}

// handleConfigGet returns dummy values for CONFIG GET
func handleConfigGet(writer *bufio.Writer, parameter string) error {
	// Return dummy values for common parameters that redis-benchmark queries
	switch parameter {
	case "save":
		return writeArray(writer, []string{"save", ""})
	case "appendonly":
		return writeArray(writer, []string{"appendonly", "no"})
	case "timeout":
		return writeArray(writer, []string{"timeout", "0"})
	case "tcp-keepalive":
		return writeArray(writer, []string{"tcp-keepalive", "0"})
	case "maxmemory":
		return writeArray(writer, []string{"maxmemory", "0"})
	case "maxmemory-policy":
		return writeArray(writer, []string{"maxmemory-policy", "noeviction"})
	case "*":
		// Return minimal config for wildcard queries
		configs := []string{
			"save", "",
			"appendonly", "no",
			"timeout", "0",
			"tcp-keepalive", "0",
			"maxmemory", "0",
			"maxmemory-policy", "noeviction",
		}
		return writeArray(writer, configs)
	default:
		// Return empty array for unknown parameters
		return writeArray(writer, []string{})
	}
}

// Helper functions for INFO command

// getInfoSection returns specific info section
func getInfoSection(kvStore store.KVStore, section string) string {
	switch section {
	case "memory":
		return fmt.Sprintf("# Memory\r\nused_memory:%d\r\nused_memory_human:%s\r\n",
			kvStore.GetMemoryUsage(), formatBytes(kvStore.GetMemoryUsage()))
	case "keyspace":
		return fmt.Sprintf("# Keyspace\r\ndb0:keys=%d\r\n", kvStore.GetKeyCount())
	default:
		return getAllInfo(kvStore)
	}
}

// getAllInfo returns all info sections
func getAllInfo(kvStore store.KVStore) string {
	memUsage := kvStore.GetMemoryUsage()
	keyCount := kvStore.GetKeyCount()

	info := fmt.Sprintf("# Server\r\nreservoir_version:1.0.0\r\nuptime_in_seconds:3600\r\n"+
		"# Memory\r\nused_memory:%d\r\nused_memory_human:%s\r\n"+
		"# Keyspace\r\ndb0:keys=%d\r\n",
		memUsage, formatBytes(memUsage), keyCount)

	return info
}

// formatBytes formats bytes in human readable format
func formatBytes(bytes int64) string {
	if bytes == 0 {
		return "0B"
	}

	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%dB", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f%cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
