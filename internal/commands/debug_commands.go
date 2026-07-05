package commands

import (
	"bufio"
	"fmt"
	"strings"
	"time"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

// HandleZeroCopyDebugStructure processes DEBUG STRUCTURE command with zero-copy
func HandleZeroCopyDebugStructure(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() < 1 || cmd.ArgCount() > 2 {
		return writeError(writer, "wrong number of arguments for 'debug structure' command")
	}

	subcommand := cmd.ArgString(0)
	if !strings.EqualFold(subcommand, "STRUCTURE") {
		return writeError(writer, fmt.Sprintf("unknown DEBUG subcommand '%s'", subcommand))
	}

	var debugInfo string

	// Check if a specific key was provided
	if cmd.ArgCount() == 2 {
		// Individual key debug info
		key := expandRandomTemplates(cmd.ArgString(1))
		debugInfo = getKeyStructureInfo(kvStore, key)
	} else {
		// All keys overview
		debugInfo = getAllKeysStructureInfo(kvStore)
	}

	return writeBulkString(writer, debugInfo)
}

// getAllKeysStructureInfo returns overview information for all keys
func getAllKeysStructureInfo(kvStore store.KVStore) string {
	var builder strings.Builder

	// Get total stats
	totalKeys := kvStore.GetKeyCount()
	totalMemory := kvStore.GetMemoryUsage()

	builder.WriteString(fmt.Sprintf("Total keys: %d\r\n", totalKeys))
	builder.WriteString(fmt.Sprintf("Memory usage: %.2f MB\r\n\r\n", float64(totalMemory)/(1024*1024)))

	if totalKeys == 0 {
		builder.WriteString("No keys found.\r\n")
		return builder.String()
	}

	// Get keys by shards
	keysByShards := kvStore.GetKeysByShards()

	builder.WriteString("Keys by shard:\r\n")

	for shardIdx := 0; shardIdx < 256; shardIdx++ {
		keys, exists := keysByShards[shardIdx]
		if !exists || len(keys) == 0 {
			continue
		}

		builder.WriteString(fmt.Sprintf("Shard %d:\r\n", shardIdx))

		for _, key := range keys {
			keyType := kvStore.GetKeyType(key)
			keyMemory := kvStore.GetKeyMemoryUsage(key)

			// Get stored value for access stats
			storedValue, exists := kvStore.GetStoredValue(key)
			var accessInfo string
			if exists {
				accessInfo = fmt.Sprintf("accessed: %d times", storedValue.ReadCount+storedValue.WriteCount)
			} else {
				accessInfo = "accessed: 0 times"
			}

			// Format type-specific info
			var typeInfo string
			switch keyType {
			case store.StringType:
				typeInfo = "String"
			case store.ListType:
				if length, err := kvStore.LLen(key); err == nil {
					typeInfo = fmt.Sprintf("List[%d]", length)
				} else {
					typeInfo = "List"
				}
			case store.SetType:
				if card, err := kvStore.SCard(key); err == nil {
					typeInfo = fmt.Sprintf("Set[%d]", card)
				} else {
					typeInfo = "Set"
				}
			default:
				typeInfo = "Unknown"
			}

			builder.WriteString(fmt.Sprintf("  - %s (%s, %d bytes, %s)\r\n",
				key, typeInfo, keyMemory, accessInfo))
		}
		builder.WriteString("\r\n")
	}

	return builder.String()
}

// getKeyStructureInfo returns detailed internal structure information for a key
func getKeyStructureInfo(kvStore store.KVStore, key string) string {
	// Calculate which shard the key belongs to
	shardIndex := store.ShardIndex(key)
	hash := store.FastHash(key)

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("Key: %s\r\n", key))
	builder.WriteString(fmt.Sprintf("Shard: %d (of 256)\r\n", shardIndex))
	builder.WriteString(fmt.Sprintf("Hash: %d (0x%08X)\r\n", hash, hash))

	// Get stored value for detailed information
	storedValue, exists := kvStore.GetStoredValue(key)
	if !exists {
		builder.WriteString("Status: Key does not exist\r\n")
		return builder.String()
	}

	builder.WriteString("Status: Key exists\r\n")

	// Get exact memory usage
	keyMemory := kvStore.GetKeyMemoryUsage(key)
	builder.WriteString(fmt.Sprintf("Memory: %d bytes\r\n", keyMemory))

	// Get TTL information
	if ttl, hasTTL := kvStore.GetTTL(key); hasTTL {
		if ttl > 0 {
			builder.WriteString(fmt.Sprintf("TTL: %v (expires in %v)\r\n", ttl, ttl.Round(time.Second)))
		} else {
			builder.WriteString("TTL: Expired (should be cleaned up)\r\n")
		}
	} else {
		builder.WriteString("TTL: Not set\r\n")
	}

	// Add access statistics
	builder.WriteString("\r\nAccess Statistics:\r\n")
	builder.WriteString(fmt.Sprintf("  - Read count: %d\r\n", storedValue.ReadCount))
	builder.WriteString(fmt.Sprintf("  - Write count: %d\r\n", storedValue.WriteCount))
	if !storedValue.LastRead.IsZero() {
		builder.WriteString(fmt.Sprintf("  - Last read: %s\r\n", storedValue.LastRead.Format("2006-01-02 15:04:05")))
	} else {
		builder.WriteString("  - Last read: Never\r\n")
	}
	if !storedValue.LastWrite.IsZero() {
		builder.WriteString(fmt.Sprintf("  - Last write: %s\r\n", storedValue.LastWrite.Format("2006-01-02 15:04:05")))
	} else {
		builder.WriteString("  - Last write: Never\r\n")
	}

	// Add replication information
	builder.WriteString("\r\nReplication Info:\r\n")
	if storedValue.Replicated {
		builder.WriteString("  - Replicated: Yes\r\n")
		if !storedValue.LastReplication.IsZero() {
			builder.WriteString(fmt.Sprintf("  - Last replication: %s\r\n", storedValue.LastReplication.Format("2006-01-02 15:04:05")))
		}
		if len(storedValue.ReplicatedToNodes) > 0 {
			builder.WriteString(fmt.Sprintf("  - Replicated to nodes: %v\r\n", storedValue.ReplicatedToNodes))
		}
		builder.WriteString(fmt.Sprintf("  - Replication attempts: %d\r\n", storedValue.ReplicationAttempts))
	} else {
		builder.WriteString("  - Replicated: No\r\n")
	}

	// Type-specific information
	switch storedValue.Type {
	case store.ListType:
		builder.WriteString("\r\nType: List\r\n")
		if listLength, err := kvStore.LLen(key); err == nil {
			builder.WriteString("Internal Structure:\r\n")
			builder.WriteString(fmt.Sprintf("  - List length: %d\r\n", listLength))

			// Show first few elements
			if elements, err := kvStore.LRange(key, 0, 9); err == nil && len(elements) > 0 {
				builder.WriteString("  - Elements (first 10):\r\n")
				for i, element := range elements {
					builder.WriteString(fmt.Sprintf("    [%d] \"%s\" (%d bytes)\r\n", i, element, len(element)))
				}
				if listLength > 10 {
					builder.WriteString(fmt.Sprintf("    ... and %d more elements\r\n", listLength-10))
				}
			} else {
				builder.WriteString("  - Elements: (empty list)\r\n")
			}
		}

	case store.SetType:
		builder.WriteString("\r\nType: Set\r\n")
		if setCard, err := kvStore.SCard(key); err == nil {
			builder.WriteString("Internal Structure:\r\n")
			builder.WriteString(fmt.Sprintf("  - Set cardinality: %d\r\n", setCard))

			// Show first few members
			if members, err := kvStore.SMembers(key); err == nil && len(members) > 0 {
				builder.WriteString("  - Members (first 10):\r\n")
				maxShow := 10
				for i, member := range members {
					if i >= maxShow {
						builder.WriteString(fmt.Sprintf("    ... and %d more members\r\n", len(members)-maxShow))
						break
					}
					builder.WriteString(fmt.Sprintf("    \"%s\" (%d bytes)\r\n", member, len(member)))
				}
			} else {
				builder.WriteString("  - Members: (empty set)\r\n")
			}
		}

	case store.StringType:
		builder.WriteString("\r\nType: String\r\n")
		if value, exists := kvStore.Get(key); exists {
			builder.WriteString("Internal Structure:\r\n")
			builder.WriteString(fmt.Sprintf("  - String value: \"%s\" (%d bytes)\r\n", value, len(value)))
		}

	default:
		builder.WriteString("\r\nType: Unknown\r\n")
	}

	return builder.String()
}
