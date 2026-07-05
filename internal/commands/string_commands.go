package commands

import (
	"bufio"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

// String command handlers for Redis string operations

// HandleZeroCopyGet processes GET command with zero-copy - ULTRA OPTIMIZED HOT PATH
func HandleZeroCopyGet(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	// ULTRA FAST PATH: Skip ParseGetCommand, work directly with raw command
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'get' command")
	}

	// OPTIMIZED: Direct key access with efficient template expansion
	key := expandRandomTemplates(cmd.ArgString(0))

	// Use GetStoredValue so we can honor TTL (expired keys are treated as
	// absent) and distinguish a wrong-type key (list/set/hash) from a missing
	// one, returning WRONGTYPE like every other typed command does instead of
	// silently replying with an empty string.
	sv, exists := kvStore.GetStoredValue(key)
	if !exists {
		_, err := writer.Write(nullResponse)
		return err
	}
	if sv.Type != store.ValueTypeString {
		return writeError(writer, "WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return writeBulkString(writer, sv.StringVal)
}

// HandleZeroCopySetWithReplication processes SET command - ULTRA OPTIMIZED HOT PATH
func HandleZeroCopySetWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	// ULTRA FAST PATH: Skip ParseSetCommand, work directly with raw command
	if cmd.ArgCount() != 2 {
		return writeError(writer, "wrong number of arguments for 'set' command")
	}

	// OPTIMIZED: Efficient template expansion
	key := expandRandomTemplates(cmd.ArgString(0))
	value := expandRandomTemplates(cmd.ArgString(1))

	if err := kvStore.Set(key, value); err != nil {
		return writeError(writer, err.Error())
	}

	// OPTIMIZED: Minimal replication tracking
	if replicate != nil {
		// REMOVED: Expensive GetStoredValue() and TrackReplication() calls
		// Just replicate the command - background process handles tracking
		replicate("SET", key, []byte(value))
	}

	_, err := writer.Write(okResponse)
	return err
}

// HandleZeroCopyDelWithReplication processes DEL command with zero-copy and replication support
func HandleZeroCopyDelWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() == 0 {
		return writeError(writer, "wrong number of arguments for 'del' command")
	}

	deleted := int64(0)
	for i := 0; i < cmd.ArgCount(); i++ {
		key := expandRandomTemplates(cmd.ArgString(i))
		if kvStore.Delete(key) {
			deleted++

			// Replicate the operation if callback is provided
			if replicate != nil {
				replicate("DEL", key, nil)
			}
		}
	}

	return writeInteger(writer, deleted)
}

// HandleZeroCopyMGet processes MGET command with zero-copy
func HandleZeroCopyMGet(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() == 0 {
		return writeError(writer, "wrong number of arguments for 'mget' command")
	}

	// Write array header
	if err := writeArrayStart(writer, cmd.ArgCount()); err != nil {
		return err
	}

	for i := 0; i < cmd.ArgCount(); i++ {
		key := expandRandomTemplates(cmd.ArgString(i))
		if val, exists := kvStore.Get(key); exists {
			if err := writeBulkString(writer, val); err != nil {
				return err
			}
		} else {
			if _, err := writer.Write(nullResponse); err != nil {
				return err
			}
		}
	}

	return nil
}

// HandleZeroCopyMSetWithReplication processes MSET command with zero-copy and replication support
func HandleZeroCopyMSetWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount()%2 != 0 {
		return writeError(writer, "wrong number of arguments for 'mset' command")
	}

	// Prepare key-value pairs with template expansion
	pairs := make([]struct{ key, value string }, cmd.ArgCount()/2)
	for i := 0; i < cmd.ArgCount(); i += 2 {
		pairs[i/2].key = expandRandomTemplates(cmd.ArgString(i))
		pairs[i/2].value = expandRandomTemplates(cmd.ArgString(i + 1))
	}

	// Set all pairs
	for _, pair := range pairs {
		if err := kvStore.Set(pair.key, pair.value); err != nil {
			return writeError(writer, err.Error())
		}

		// Replicate each SET operation if callback is provided
		if replicate != nil {
			replicate("SET", pair.key, []byte(pair.value))
		}
	}

	_, err := writer.Write(okResponse)
	return err
}

// HandleZeroCopyExists processes EXISTS command with zero-copy
func HandleZeroCopyExists(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() == 0 {
		return writeError(writer, "wrong number of arguments for 'exists' command")
	}

	keys := make([]string, cmd.ArgCount())
	for i := 0; i < cmd.ArgCount(); i++ {
		keys[i] = expandRandomTemplates(cmd.ArgString(i))
	}

	count := kvStore.Exists(keys...)
	return writeInteger(writer, count)
}

// HandleZeroCopyIncrWithReplication processes INCR command with zero-copy and replication support
func HandleZeroCopyIncrWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'incr' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	result, err := kvStore.Incr(key)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the operation if callback is provided
	if replicate != nil {
		replicate("INCR", key, nil)
	}

	return writeInteger(writer, result)
}

// HandleZeroCopyDecrWithReplication processes DECR command with zero-copy and replication support
func HandleZeroCopyDecrWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'decr' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	result, err := kvStore.Decr(key)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the operation if callback is provided
	if replicate != nil {
		replicate("DECR", key, nil)
	}

	return writeInteger(writer, result)
}

// HandleZeroCopyIncrByWithReplication processes INCRBY command with zero-copy and replication support
func HandleZeroCopyIncrByWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 2 {
		return writeError(writer, "wrong number of arguments for 'incrby' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	incrementStr := cmd.ArgString(1)

	increment, err := parseInteger(incrementStr)
	if err != nil {
		return writeError(writer, errNotInteger)
	}

	result, err := kvStore.IncrBy(key, increment)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the operation if callback is provided
	if replicate != nil {
		replicate("INCRBY", key, []byte(incrementStr))
	}

	return writeInteger(writer, result)
}

// HandleZeroCopyDecrByWithReplication processes DECRBY command with zero-copy and replication support
func HandleZeroCopyDecrByWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 2 {
		return writeError(writer, "wrong number of arguments for 'decrby' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	decrementStr := cmd.ArgString(1)

	decrement, err := parseInteger(decrementStr)
	if err != nil {
		return writeError(writer, errNotInteger)
	}

	result, err := kvStore.DecrBy(key, decrement)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the operation if callback is provided
	if replicate != nil {
		replicate("DECRBY", key, []byte(decrementStr))
	}

	return writeInteger(writer, result)
}

// HandleZeroCopyAppendWithReplication processes APPEND command with zero-copy and replication support
func HandleZeroCopyAppendWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 2 {
		return writeError(writer, "wrong number of arguments for 'append' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	value := expandRandomTemplates(cmd.ArgString(1))

	// Get existing value first to calculate the final result for replication
	var finalValue string
	if existingValue, exists := kvStore.Get(key); exists {
		finalValue = existingValue + value
	} else {
		finalValue = value
	}

	result, err := kvStore.Append(key, value)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the operation if callback is provided
	if replicate != nil {
		// Replicate the final SET operation rather than APPEND for consistency
		replicate("SET", key, []byte(finalValue))
	}

	return writeInteger(writer, result)
}

// HandleZeroCopyStrLen processes STRLEN command with zero-copy
func HandleZeroCopyStrLen(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'strlen' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	length, err := kvStore.StrLen(key)
	if err != nil {
		return writeError(writer, err.Error())
	}

	return writeInteger(writer, length)
}

// HandleZeroCopyGetSet processes GETSET command with zero-copy
func HandleZeroCopyGetSet(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 2 {
		return writeError(writer, "wrong number of arguments for 'getset' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	value := expandRandomTemplates(cmd.ArgString(1))

	oldValue, hadValue, err := kvStore.GetSet(key, value)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Write response - old value as bulk string or null
	if hadValue {
		return writeBulkString(writer, oldValue)
	} else {
		_, err = writer.Write(nullResponse)
		return err
	}
}
