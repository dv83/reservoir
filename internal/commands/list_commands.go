package commands

import (
	"bufio"
	"strconv"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

// HandleZeroCopyLPushWithReplication processes LPUSH command with zero-copy and replication support
func HandleZeroCopyLPushWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 2 {
		return writeError(writer, "wrong number of arguments for 'lpush' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	elements := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		elements[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	length, err := kvStore.LPush(key, elements...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the LPUSH operation (always replicate, even if length is 0)
	if replicate != nil && len(elements) > 0 {
		replicate("LPUSH", key, buildReplicationData(key, elements...))
	}

	return writeInteger(writer, length)
}

// HandleZeroCopyRPushWithReplication processes RPUSH command with zero-copy and replication support
func HandleZeroCopyRPushWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 2 {
		return writeError(writer, "wrong number of arguments for 'rpush' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	elements := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		elements[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	length, err := kvStore.RPush(key, elements...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the RPUSH operation (always replicate, even if length is 0)
	if replicate != nil && len(elements) > 0 {
		replicate("RPUSH", key, buildReplicationData(key, elements...))
	}

	return writeInteger(writer, length)
}

// HandleZeroCopyLPopWithReplication processes LPOP command with zero-copy and replication support
func HandleZeroCopyLPopWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 1 || cmd.ArgCount() > 2 {
		return writeError(writer, "wrong number of arguments for 'lpop' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	count := 1

	if cmd.ArgCount() == 2 {
		var err error
		count, err = strconv.Atoi(cmd.ArgString(1))
		if err != nil || count < 0 {
			return writeError(writer, errNotInteger)
		}
	}

	elements, err := kvStore.LPop(key, count)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate only if elements were actually popped (len(elements) > 0)
	if replicate != nil && len(elements) > 0 {
		args := append([]string{strconv.Itoa(len(elements))}, elements...)
		replicate("LPOP", key, buildReplicationData(key, args...))
	}

	if len(elements) == 0 {
		_, err := writer.Write(nullResponse)
		return err
	}

	if cmd.ArgCount() == 1 {
		// Для одного елемента повертаємо bulk string
		return writeBulkString(writer, elements[0])
	}

	// Для множини елементів повертаємо array
	return writeArray(writer, elements)
}

// HandleZeroCopyRPopWithReplication processes RPOP command with zero-copy and replication support
func HandleZeroCopyRPopWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 1 || cmd.ArgCount() > 2 {
		return writeError(writer, "wrong number of arguments for 'rpop' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	count := 1

	if cmd.ArgCount() == 2 {
		var err error
		count, err = strconv.Atoi(cmd.ArgString(1))
		if err != nil || count < 0 {
			return writeError(writer, errNotInteger)
		}
	}

	elements, err := kvStore.RPop(key, count)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate only if elements were actually popped (len(elements) > 0)
	if replicate != nil && len(elements) > 0 {
		args := append([]string{strconv.Itoa(len(elements))}, elements...)
		replicate("RPOP", key, buildReplicationData(key, args...))
	}

	if len(elements) == 0 {
		_, err := writer.Write(nullResponse)
		return err
	}

	if cmd.ArgCount() == 1 {
		// Для одного елемента повертаємо bulk string
		return writeBulkString(writer, elements[0])
	}

	// Для множини елементів повертаємо array
	return writeArray(writer, elements)
}

// HandleZeroCopyLLen обробляє команду LLEN
func HandleZeroCopyLLen(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'llen' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))

	length, err := kvStore.LLen(key)
	if err != nil {
		return writeError(writer, err.Error())
	}

	return writeInteger(writer, length)
}

// HandleZeroCopyLRange обробляє команду LRANGE
func HandleZeroCopyLRange(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 3 {
		return writeError(writer, "wrong number of arguments for 'lrange' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))

	start, err := strconv.ParseInt(cmd.ArgString(1), 10, 64)
	if err != nil {
		return writeError(writer, errNotInteger)
	}

	stop, err := strconv.ParseInt(cmd.ArgString(2), 10, 64)
	if err != nil {
		return writeError(writer, errNotInteger)
	}

	elements, err := kvStore.LRange(key, start, stop)
	if err != nil {
		return writeError(writer, err.Error())
	}

	return writeArray(writer, elements)
}

// HandleZeroCopyLIndex обробляє команду LINDEX
func HandleZeroCopyLIndex(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 2 {
		return writeError(writer, "wrong number of arguments for 'lindex' command")
	}

	key := cmd.ArgString(0)

	index, err := strconv.ParseInt(cmd.ArgString(1), 10, 64)
	if err != nil {
		return writeError(writer, errNotInteger)
	}

	element, found, err := kvStore.LIndex(key, index)
	if err != nil {
		return writeError(writer, err.Error())
	}

	if !found {
		_, err := writer.Write(nullResponse)
		return err
	}

	return writeBulkString(writer, element)
}

// HandleZeroCopyLSetWithReplication processes LSET command with zero-copy and replication support
func HandleZeroCopyLSetWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 3 {
		return writeError(writer, "wrong number of arguments for 'lset' command")
	}

	key := cmd.ArgString(0)

	index, err := strconv.ParseInt(cmd.ArgString(1), 10, 64)
	if err != nil {
		return writeError(writer, errNotInteger)
	}

	element := cmd.ArgString(2)

	err = kvStore.LSet(key, index, element)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the LSET operation (always replicate successful operations)
	if replicate != nil {
		replicate("LSET", key, buildReplicationData(key, strconv.FormatInt(index, 10), element))
	}

	_, err = writer.Write(okResponse)
	return err
}

// HandleZeroCopyLRemWithReplication processes LREM command with zero-copy and replication support
func HandleZeroCopyLRemWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 3 {
		return writeError(writer, "wrong number of arguments for 'lrem' command")
	}

	key := cmd.ArgString(0)

	count, err := strconv.ParseInt(cmd.ArgString(1), 10, 64)
	if err != nil {
		return writeError(writer, errNotInteger)
	}

	element := cmd.ArgString(2)

	removed, err := kvStore.LRem(key, count, element)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate only if elements were actually removed (removed > 0)
	if replicate != nil && removed > 0 {
		replicate("LREM", key, buildReplicationData(key, strconv.FormatInt(count, 10), element))
	}

	return writeInteger(writer, removed)
}

// HandleZeroCopyLTrimWithReplication processes LTRIM command with zero-copy and replication support
func HandleZeroCopyLTrimWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 3 {
		return writeError(writer, "wrong number of arguments for 'ltrim' command")
	}

	key := cmd.ArgString(0)

	start, err := strconv.ParseInt(cmd.ArgString(1), 10, 64)
	if err != nil {
		return writeError(writer, errNotInteger)
	}

	stop, err := strconv.ParseInt(cmd.ArgString(2), 10, 64)
	if err != nil {
		return writeError(writer, errNotInteger)
	}

	err = kvStore.LTrim(key, start, stop)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the LTRIM operation (always replicate successful operations)
	if replicate != nil {
		replicate("LTRIM", key, buildReplicationData(key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10)))
	}

	_, err = writer.Write(okResponse)
	return err
}

// Helper functions для відповідей

// HandleZeroCopyLPushXWithReplication processes LPUSHX command with zero-copy and replication support
func HandleZeroCopyLPushXWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 2 {
		return writeError(writer, "wrong number of arguments for 'lpushx' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	elements := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		elements[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	length, err := kvStore.LPushX(key, elements...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate only if elements were actually added (length > 0 means key existed and elements were added)
	if replicate != nil && length > 0 {
		replicate("LPUSHX", key, buildReplicationData(key, elements...))
	}

	return writeInteger(writer, length)
}

// HandleZeroCopyRPushXWithReplication processes RPUSHX command with zero-copy and replication support
func HandleZeroCopyRPushXWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 2 {
		return writeError(writer, "wrong number of arguments for 'rpushx' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	elements := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		elements[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	length, err := kvStore.RPushX(key, elements...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate only if elements were actually added (length > 0 means key existed and elements were added)
	if replicate != nil && length > 0 {
		replicate("RPUSHX", key, buildReplicationData(key, elements...))
	}

	return writeInteger(writer, length)
}

// HandleZeroCopyLPushUniqueWithReplication processes LPUSHUNIQUE command with zero-copy and replication support
func HandleZeroCopyLPushUniqueWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 2 {
		return writeError(writer, "wrong number of arguments for 'lpushunique' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	elements := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		elements[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	addedCount, err := kvStore.LPushUnique(key, elements...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate only if elements were actually added (addedCount > 0)
	if replicate != nil && addedCount > 0 {
		replicate("LPUSHUNIQUE", key, buildReplicationData(key, elements...))
	}

	return writeInteger(writer, addedCount)
}

// HandleZeroCopyRPushUniqueWithReplication processes RPUSHUNIQUE command with zero-copy and replication support
func HandleZeroCopyRPushUniqueWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 2 {
		return writeError(writer, "wrong number of arguments for 'rpushunique' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	elements := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		elements[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	addedCount, err := kvStore.RPushUnique(key, elements...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate only if elements were actually added (addedCount > 0)
	if replicate != nil && addedCount > 0 {
		replicate("RPUSHUNIQUE", key, buildReplicationData(key, elements...))
	}

	return writeInteger(writer, addedCount)
}
