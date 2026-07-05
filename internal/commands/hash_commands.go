package commands

import (
	"bufio"
	"fmt"
	"strconv"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

// HandleZeroCopyHSetWithReplication processes HSET command with zero-copy and replication support
func HandleZeroCopyHSetWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 3 || cmd.ArgCount()%2 == 0 {
		return writeError(writer, "wrong number of arguments for 'hset' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	fieldValues := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		fieldValues[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	added, err := kvStore.HSet(key, fieldValues...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the HSET operation if callback is provided
	if replicate != nil && len(fieldValues) > 0 {
		replicate("HSET", key, buildReplicationData(key, fieldValues...))
	}

	return writeInteger(writer, added)
}

// HandleZeroCopyHGet processes HGET command with zero-copy
func HandleZeroCopyHGet(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 2 {
		return writeError(writer, "wrong number of arguments for 'hget' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	field := expandRandomTemplates(cmd.ArgString(1))

	value, exists, err := kvStore.HGet(key, field)
	if err != nil {
		return writeError(writer, err.Error())
	}

	if exists {
		return writeBulkString(writer, value)
	}

	_, err = writer.Write(nullResponse)
	return err
}

// HandleZeroCopyHMGet processes HMGET command with zero-copy
func HandleZeroCopyHMGet(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() < 2 {
		return writeError(writer, "wrong number of arguments for 'hmget' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	fields := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		fields[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	values, existsFlags, err := kvStore.HMGet(key, fields...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Write array header
	arrayHeader := fmt.Sprintf("*%d\r\n", len(values))
	if _, err := writer.Write([]byte(arrayHeader)); err != nil {
		return err
	}

	// Write each value or null
	for i, value := range values {
		if existsFlags[i] {
			if err := writeBulkString(writer, value); err != nil {
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

// HandleZeroCopyHMSetWithReplication processes HMSET command with zero-copy and replication support
func HandleZeroCopyHMSetWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 3 || cmd.ArgCount()%2 == 0 {
		return writeError(writer, "wrong number of arguments for 'hmset' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	fieldValues := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		fieldValues[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	_, err := kvStore.HSet(key, fieldValues...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the HMSET operation if callback is provided
	if replicate != nil && len(fieldValues) > 0 {
		replicate("HMSET", key, buildReplicationData(key, fieldValues...))
	}

	_, err = writer.Write(okResponse)
	return err
}

// HandleZeroCopyHDelWithReplication processes HDEL command with zero-copy and replication support
func HandleZeroCopyHDelWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 2 {
		return writeError(writer, "wrong number of arguments for 'hdel' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	fields := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		fields[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	removed, err := kvStore.HDel(key, fields...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the HDEL operation if callback is provided (only if fields were actually removed)
	if replicate != nil && removed > 0 && len(fields) > 0 {
		replicate("HDEL", key, buildReplicationData(key, fields...))
	}

	return writeInteger(writer, removed)
}

// HandleZeroCopyHExists processes HEXISTS command with zero-copy
func HandleZeroCopyHExists(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 2 {
		return writeError(writer, "wrong number of arguments for 'hexists' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	field := expandRandomTemplates(cmd.ArgString(1))

	exists, err := kvStore.HExists(key, field)
	if err != nil {
		return writeError(writer, err.Error())
	}

	if exists {
		_, err = writer.Write([]byte(":1\r\n"))
	} else {
		_, err = writer.Write([]byte(":0\r\n"))
	}
	return err
}

// HandleZeroCopyHLen processes HLEN command with zero-copy
func HandleZeroCopyHLen(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'hlen' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))

	length, err := kvStore.HLen(key)
	if err != nil {
		return writeError(writer, err.Error())
	}

	return writeInteger(writer, length)
}

// HandleZeroCopyHKeys processes HKEYS command with zero-copy
func HandleZeroCopyHKeys(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'hkeys' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))

	keys, err := kvStore.HKeys(key)
	if err != nil {
		return writeError(writer, err.Error())
	}

	return writeArray(writer, keys)
}

// HandleZeroCopyHVals processes HVALS command with zero-copy
func HandleZeroCopyHVals(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'hvals' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))

	values, err := kvStore.HVals(key)
	if err != nil {
		return writeError(writer, err.Error())
	}

	return writeArray(writer, values)
}

// HandleZeroCopyHGetAll processes HGETALL command with zero-copy
func HandleZeroCopyHGetAll(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'hgetall' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))

	fieldValuePairs, err := kvStore.HGetAll(key)
	if err != nil {
		return writeError(writer, err.Error())
	}

	return writeArray(writer, fieldValuePairs)
}

// HandleZeroCopyHIncrByWithReplication processes HINCRBY command with zero-copy and replication support
func HandleZeroCopyHIncrByWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 3 {
		return writeError(writer, "wrong number of arguments for 'hincrby' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	field := expandRandomTemplates(cmd.ArgString(1))
	incrementStr := expandRandomTemplates(cmd.ArgString(2))

	// Parse increment
	increment, err := strconv.ParseInt(incrementStr, 10, 64)
	if err != nil {
		return writeError(writer, "ERR "+errNotInteger)
	}

	result, err := kvStore.HIncrBy(key, field, increment)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the operation if callback is provided
	if replicate != nil {
		// Include field, increment and result for replication consistency
		replicationData := fmt.Sprintf("%s %s %d %d", key, field, increment, result)
		replicate("HINCRBY", key, []byte(replicationData))
	}

	return writeInteger(writer, result)
}

// HandleZeroCopyHIncrByFloatWithReplication processes HINCRBYFLOAT command with zero-copy and replication support
func HandleZeroCopyHIncrByFloatWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 3 {
		return writeError(writer, "wrong number of arguments for 'hincrbyfloat' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	field := expandRandomTemplates(cmd.ArgString(1))
	incrementStr := expandRandomTemplates(cmd.ArgString(2))

	// Parse increment
	increment, err := strconv.ParseFloat(incrementStr, 64)
	if err != nil {
		return writeError(writer, errNotFloat)
	}

	result, err := kvStore.HIncrByFloat(key, field, increment)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the operation if callback is provided
	if replicate != nil {
		// Include field, increment and result for replication consistency
		replicationData := fmt.Sprintf("%s %s %f %f", key, field, increment, result)
		replicate("HINCRBYFLOAT", key, []byte(replicationData))
	}

	// Write float response as bulk string (Redis format)
	resultStr := strconv.FormatFloat(result, 'g', -1, 64)
	return writeBulkString(writer, resultStr)
}
