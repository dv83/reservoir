package commands

import (
	"bufio"
	"fmt"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

// HandleZeroCopySAddWithReplication processes SADD command with zero-copy and replication support
func HandleZeroCopySAddWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 2 {
		return writeError(writer, "wrong number of arguments for 'sadd' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))

	// Collect elements to add
	elements := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		elements[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	// Add elements using store interface
	added, err := kvStore.SAdd(key, elements...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate using the EXACT same key/elements that were stored (only if actually added)
	if added > 0 && replicate != nil {
		replicate("SADD", key, buildReplicationData(key, elements...))
	}

	// Write integer response with number of added elements
	response := fmt.Sprintf(":%d\r\n", added)
	_, err = writer.Write([]byte(response))
	return err
}

// HandleZeroCopySMembers processes SMEMBERS command with zero-copy
func HandleZeroCopySMembers(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'smembers' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))

	members, err := kvStore.SMembers(key)
	if err != nil {
		return writeError(writer, err.Error())
	}

	return writeArray(writer, members)
}

// HandleZeroCopySRemWithReplication processes SREM command with zero-copy and replication support
func HandleZeroCopySRemWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 2 {
		return writeError(writer, "wrong number of arguments for 'srem' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))

	// Collect elements to remove
	elements := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		elements[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	removed, err := kvStore.SRem(key, elements...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate using the EXACT same key/elements that were removed (only if actually removed)
	if removed > 0 && replicate != nil {
		replicate("SREM", key, buildReplicationData(key, elements...))
	}

	// Write integer response with number of removed elements
	response := fmt.Sprintf(":%d\r\n", removed)
	_, err = writer.Write([]byte(response))
	return err
}

// HandleZeroCopySCard processes SCARD command with zero-copy
func HandleZeroCopySCard(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'scard' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))

	count, err := kvStore.SCard(key)
	if err != nil {
		return writeError(writer, err.Error())
	}

	response := fmt.Sprintf(":%d\r\n", count)
	_, err = writer.Write([]byte(response))
	return err
}

// HandleZeroCopySIsMember processes SISMEMBER command with zero-copy
func HandleZeroCopySIsMember(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() != 2 {
		return writeError(writer, "wrong number of arguments for 'sismember' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))
	member := expandRandomTemplates(cmd.ArgString(1))

	isMember, err := kvStore.SIsMember(key, member)
	if err != nil {
		return writeError(writer, err.Error())
	}

	if isMember {
		_, err = writer.Write([]byte(":1\r\n"))
	} else {
		_, err = writer.Write([]byte(":0\r\n"))
	}
	return err
}

// HandleZeroCopySPopWithReplication processes SPOP command with zero-copy and replication support
func HandleZeroCopySPopWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() != 1 {
		return writeError(writer, "wrong number of arguments for 'spop' command")
	}

	key := expandRandomTemplates(cmd.ArgString(0))

	element, exists, err := kvStore.SPop(key)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the popped element (only if an element was actually popped)
	if exists && replicate != nil {
		// Replicate with just the popped element
		replicate("SPOP", key, []byte(element))
	}

	if exists {
		return writeBulkString(writer, element)
	} else {
		_, err = writer.Write(nullResponse)
		return err
	}
}

// HandleZeroCopySDiff processes SDIFF command with zero-copy
func HandleZeroCopySDiff(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() < 1 {
		return writeError(writer, "wrong number of arguments for 'sdiff' command")
	}

	// Collect all keys
	keys := make([]string, cmd.ArgCount())
	for i := 0; i < cmd.ArgCount(); i++ {
		keys[i] = expandRandomTemplates(cmd.ArgString(i))
	}

	members, err := kvStore.SDiff(keys...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	return writeArray(writer, members)
}

// HandleZeroCopySInter processes SINTER command with zero-copy
func HandleZeroCopySInter(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() < 1 {
		return writeError(writer, "wrong number of arguments for 'sinter' command")
	}

	// Collect all keys
	keys := make([]string, cmd.ArgCount())
	for i := 0; i < cmd.ArgCount(); i++ {
		keys[i] = expandRandomTemplates(cmd.ArgString(i))
	}

	members, err := kvStore.SInter(keys...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	return writeArray(writer, members)
}

// HandleZeroCopySUnion processes SUNION command with zero-copy
func HandleZeroCopySUnion(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if cmd.ArgCount() < 1 {
		return writeError(writer, "wrong number of arguments for 'sunion' command")
	}

	// Collect all keys
	keys := make([]string, cmd.ArgCount())
	for i := 0; i < cmd.ArgCount(); i++ {
		keys[i] = expandRandomTemplates(cmd.ArgString(i))
	}

	members, err := kvStore.SUnion(keys...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	return writeArray(writer, members)
}

// HandleZeroCopySDiffStoreWithReplication processes SDIFFSTORE command with zero-copy and replication support
func HandleZeroCopySDiffStoreWithReplication(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error {
	if cmd.ArgCount() < 2 {
		return writeError(writer, "wrong number of arguments for 'sdiffstore' command")
	}

	destination := expandRandomTemplates(cmd.ArgString(0))

	// Collect source keys
	keys := make([]string, cmd.ArgCount()-1)
	for i := 1; i < cmd.ArgCount(); i++ {
		keys[i-1] = expandRandomTemplates(cmd.ArgString(i))
	}

	count, err := kvStore.SDiffStore(destination, keys...)
	if err != nil {
		return writeError(writer, err.Error())
	}

	// Replicate the SDIFFSTORE operation (if any elements were stored)
	if count > 0 && replicate != nil {
		replicate("SDIFFSTORE", destination, buildReplicationData(destination, keys...))
	}

	// Write integer response with number of elements in the result set
	response := fmt.Sprintf(":%d\r\n", count)
	_, err = writer.Write([]byte(response))
	return err
}
