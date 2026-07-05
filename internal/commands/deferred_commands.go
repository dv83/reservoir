package commands

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

// Package-level map for faster lookup (avoids allocation on each call)
var deferableWriteCommands = map[string]bool{
	"SET":    true,
	"DEL":    true,
	"INCR":   true,
	"DECR":   true,
	"INCRBY": true,
	"DECRBY": true,
	"APPEND": true,
}

// HandleZeroCopyDefer processes DEFER command - schedules a command for future execution
// Syntax: DEFER <delay_seconds> <command> <args...>
// Example: DEFER 30 SET mykey myvalue
func HandleZeroCopyDefer(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() < 3 {
		return writeError(writer, "wrong number of arguments for 'defer' command")
	}

	// Parse delay seconds
	delayStr := cmd.ArgString(0)
	delaySeconds, err := strconv.Atoi(delayStr)
	if err != nil || delaySeconds < 0 {
		return writeError(writer, "invalid delay seconds - must be non-negative integer")
	}

	// Parse command type
	commandType := strings.ToUpper(cmd.ArgString(1))

	// Validate that it's a write command
	if !deferableWriteCommands[commandType] {
		return writeError(writer, "only write commands can be deferred")
	}

	// Parse key (required for all deferred commands)
	if cmd.ArgCount() < 3 {
		return writeError(writer, "key is required for deferred commands")
	}
	key := cmd.ArgString(2)

	// Parse additional arguments (command-specific)
	var args []string
	for i := 3; i < cmd.ArgCount(); i++ {
		args = append(args, cmd.ArgString(i))
	}

	// Validate command arguments
	if err := validateDeferredCommand(commandType, key, args); err != nil {
		return writeError(writer, err.Error())
	}

	// Schedule the command
	if lockFreeStore, ok := kvStore.(*store.LockFreeStore); ok {
		commandID := lockFreeStore.AddDeferredCommand(commandType, key, args, delaySeconds)
		return writeBulkString(writer, commandID)
	}

	return writeError(writer, errDeferNotSupported)
}

// HandleZeroCopyDeferCancel processes DEFER.CANCEL command - cancels a scheduled command
// Syntax: DEFER.CANCEL <command_id>
func HandleZeroCopyDeferCancel(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'defer.cancel' command")
	}

	commandID := cmd.ArgStringCopy(0)

	if lockFreeStore, ok := kvStore.(*store.LockFreeStore); ok {
		cancelled := lockFreeStore.CancelDeferredCommand(commandID)
		if cancelled {
			return writeInteger(writer, 1)
		} else {
			return writeInteger(writer, 0)
		}
	}

	return writeError(writer, errDeferNotSupported)
}

// HandleZeroCopyDeferList processes DEFER.LIST command - lists all scheduled commands
// Syntax: DEFER.LIST
func HandleZeroCopyDeferList(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 0 {
		return writeError(writer, "wrong number of arguments for 'defer.list' command")
	}

	if lockFreeStore, ok := kvStore.(*store.LockFreeStore); ok {
		commands := lockFreeStore.ListDeferredCommands()

		// Write array header
		if err := writeArrayStart(writer, len(commands)); err != nil {
			return err
		}

		// Write each command as an array of details
		for _, deferredCmd := range commands {
			// Each command is represented as an array: [id, execute_at, command_type, key, args...]
			itemCount := 4 + len(deferredCmd.Args) // id, execute_at, command_type, key, + args
			if err := writeArrayStart(writer, itemCount); err != nil {
				return err
			}

			// Command ID
			if err := writeBulkString(writer, deferredCmd.ID); err != nil {
				return err
			}

			// Execute at timestamp (Unix seconds)
			executeAtStr := strconv.FormatInt(deferredCmd.ExecuteAt.Unix(), 10)
			if err := writeBulkString(writer, executeAtStr); err != nil {
				return err
			}

			// Command type
			if err := writeBulkString(writer, deferredCmd.CommandType); err != nil {
				return err
			}

			// Key
			if err := writeBulkString(writer, deferredCmd.Key); err != nil {
				return err
			}

			// Arguments
			for _, arg := range deferredCmd.Args {
				if err := writeBulkString(writer, arg); err != nil {
					return err
				}
			}
		}

		return nil
	}

	return writeError(writer, errDeferNotSupported)
}

// HandleZeroCopyDeferStats processes DEFER.STATS command - returns deferred command statistics
// Syntax: DEFER.STATS
func HandleZeroCopyDeferStats(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 0 {
		return writeError(writer, "wrong number of arguments for 'defer.stats' command")
	}

	if lockFreeStore, ok := kvStore.(*store.LockFreeStore); ok {
		stats := lockFreeStore.GetDeferredStats()

		// Write as array of key-value pairs
		if err := writeArrayStart(writer, len(stats)*2); err != nil {
			return err
		}

		for key, value := range stats {
			if err := writeBulkString(writer, key); err != nil {
				return err
			}
			if err := writeBulkString(writer, formatStatsValue(value)); err != nil {
				return err
			}
		}

		return nil
	}

	return writeError(writer, errDeferNotSupported)
}

// isWriteCommand checks if a command type is a write command that can be deferred
func isWriteCommand(commandType string) bool {
	return deferableWriteCommands[commandType]
}

// validateDeferredCommand validates the arguments for a deferred command
func validateDeferredCommand(commandType, key string, args []string) error {
	if key == "" {
		return errors.New("key cannot be empty")
	}

	switch commandType {
	case "SET":
		if len(args) < 1 {
			return errors.New("SET command requires value argument")
		}
	case "INCRBY", "DECRBY":
		if len(args) < 1 {
			return errors.New(commandType + " command requires increment argument")
		}
		if _, err := strconv.ParseInt(args[0], 10, 64); err != nil {
			return errors.New("increment must be a valid integer")
		}
	case "APPEND":
		if len(args) < 1 {
			return errors.New("APPEND command requires value argument")
		}
	case "DEL", "INCR", "DECR":
		// These commands don't need additional arguments
	default:
		return errors.New("unsupported command type for deferral: " + commandType)
	}

	return nil
}

// formatStatsValue formats a statistics value for output
func formatStatsValue(value interface{}) string {
	switch v := value.(type) {
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case string:
		return v
	default:
		return "unknown"
	}
}

// HandleZeroCopyDeferWithReplication processes DEFER command with replication support
func HandleZeroCopyDeferWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 3 {
		return writeError(writer, "wrong number of arguments for 'defer' command")
	}

	// Parse delay seconds
	delayStr := cmd.ArgString(0)
	delaySeconds, err := strconv.Atoi(delayStr)
	if err != nil || delaySeconds < 0 {
		return writeError(writer, "invalid delay seconds - must be non-negative integer")
	}

	// Parse command type
	commandType := strings.ToUpper(cmd.ArgString(1))

	// Validate that it's a write command
	if !deferableWriteCommands[commandType] {
		return writeError(writer, "only write commands can be deferred")
	}

	// Parse key (required for all deferred commands)
	if cmd.ArgCount() < 3 {
		return writeError(writer, "key is required for deferred commands")
	}
	key := cmd.ArgString(2)

	// Parse additional arguments (command-specific)
	var args []string
	for i := 3; i < cmd.ArgCount(); i++ {
		args = append(args, cmd.ArgString(i))
	}

	// Validate command arguments
	if err := validateDeferredCommand(commandType, key, args); err != nil {
		return writeError(writer, err.Error())
	}

	// Schedule the command
	if lockFreeStore, ok := kvStore.(*store.LockFreeStore); ok {
		commandID := lockFreeStore.AddDeferredCommand(commandType, key, args, delaySeconds)

		// Replicate the DEFER command to ensure scheduled commands are synchronized across cluster
		if replicate != nil {
			// Serialize the deferred command data for replication
			deferData := []byte(fmt.Sprintf("%d:%s:%s:%s", delaySeconds, commandType, key, strings.Join(args, ":")))
			replicate("DEFER", commandID, deferData)
		}

		return writeBulkString(writer, commandID)
	}

	return writeError(writer, errDeferNotSupported)
}

// HandleZeroCopyDeferCancelWithReplication processes DEFER.CANCEL command with replication support
func HandleZeroCopyDeferCancelWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'defer.cancel' command")
	}

	commandID := cmd.ArgStringCopy(0)

	if lockFreeStore, ok := kvStore.(*store.LockFreeStore); ok {
		cancelled := lockFreeStore.CancelDeferredCommand(commandID)

		// Replicate the cancellation to ensure synchronization across cluster
		if replicate != nil && cancelled {
			replicate("DEFER.CANCEL", commandID, []byte{})
		}

		if cancelled {
			return writeInteger(writer, 1)
		} else {
			return writeInteger(writer, 0)
		}
	}

	return writeError(writer, errDeferNotSupported)
}
