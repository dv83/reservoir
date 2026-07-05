package commands

import (
	"bufio"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

// Per-goroutine random number generator pool for template expansion
var templateRngPool = sync.Pool{
	New: func() interface{} {
		return rand.New(rand.NewSource(time.Now().UnixNano()))
	},
}

// Global flag to disable template expansion for performance (99.9% of use cases don't need it)
var disableTemplateExpansion = true

// Pre-computed length strings for common sizes (0-1000) to avoid fmt.Sprintf allocations
var lengthStrings [1001]string

// Pre-allocated response buffers to eliminate all allocations in hot path
var (
	// Common byte sequences
	crlfBytes = []byte{'\r', '\n'}

	// Buffer pool for response building
	responseBufferPool = sync.Pool{
		New: func() interface{} {
			buf := make([]byte, 0, 512) // 512 bytes capacity
			return &buf
		},
	}

	// Pre-computed integer responses for common values (-100 to 1000)
	integerResponses [1101][]byte

	// Pre-computed bulk string length responses for common sizes (0-1000)
	bulkLengthResponses [1001][]byte

	// Pre-computed array start responses for common sizes (0-100)
	arrayStartResponses [101][]byte
)

func init() {
	// Pre-compute length strings for numbers 0-1000
	for i := 0; i <= 1000; i++ {
		lengthStrings[i] = fmt.Sprintf("%d", i)
	}

	// Pre-compute integer responses for common values (-100 to 1000)
	for i := -100; i <= 1000; i++ {
		integerResponses[i+100] = []byte(fmt.Sprintf(":%d\r\n", i))
	}

	// Pre-compute bulk string length responses for common sizes (0-1000)
	for i := 0; i <= 1000; i++ {
		bulkLengthResponses[i] = []byte(fmt.Sprintf("$%d\r\n", i))
	}

	// Pre-compute array start responses for common sizes (0-100)
	for i := 0; i <= 100; i++ {
		arrayStartResponses[i] = []byte(fmt.Sprintf("*%d\r\n", i))
	}
}

// ReplicationCallback is called to replicate operations (only on leader)
type ReplicationCallback func(operation, key string, value []byte)

// ZeroCopyHandler represents a zero-copy command handler function
type ZeroCopyHandler func(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error

// ZeroCopyHandlerWithReplication represents a command handler with replication support
type ZeroCopyHandlerWithReplication func(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer, replicate ReplicationCallback) error

// withNilReplication adapts a WithReplication handler to a standard handler by passing nil for replication
func withNilReplication(handler ZeroCopyHandlerWithReplication) ZeroCopyHandler {
	return func(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
		return handler(kvStore, cmd, writer, nil)
	}
}

// buildReplicationData builds null-separated replication data: "key\x00arg1\x00arg2\x00..."
func buildReplicationData(key string, args ...string) []byte {
	var builder strings.Builder
	builder.WriteString(key)
	for _, arg := range args {
		builder.WriteByte('\x00') // Use null byte as delimiter to support spaces in values
		builder.WriteString(arg)
	}
	return []byte(builder.String())
}

// ZeroCopyRegistry holds all zero-copy command handlers
type ZeroCopyRegistry struct {
	handlers map[string]ZeroCopyHandler
}

// NewZeroCopyRegistry creates a new zero-copy command registry
func NewZeroCopyRegistry() *ZeroCopyRegistry {
	r := &ZeroCopyRegistry{
		handlers: make(map[string]ZeroCopyHandler),
	}
	r.registerCommands()
	return r
}

func (r *ZeroCopyRegistry) registerCommands() {
	// Register all zero-copy handlers
	// Commands with replication support use withNilReplication adapter
	commands := map[string]ZeroCopyHandler{
		"GET":      HandleZeroCopyGet,
		"SET":      withNilReplication(HandleZeroCopySetWithReplication),
		"DEL":      withNilReplication(HandleZeroCopyDelWithReplication),
		"PING":     HandleZeroCopyPing,
		"MGET":     HandleZeroCopyMGet,
		"MSET":     withNilReplication(HandleZeroCopyMSetWithReplication),
		"INFO":     HandleZeroCopyInfo,
		"TTL":      HandleZeroCopyTTL,
		"PTTL":     HandleZeroCopyPTTL,
		"EXPIRE":   HandleZeroCopyExpire,
		"PEXPIRE":  HandleZeroCopyPExpire,
		"TYPE":     HandleZeroCopyType,
		"PERSIST":  HandleZeroCopyPersist,
		"FLUSHDB":  withNilReplication(HandleZeroCopyFlushDBWithReplication),
		"FLUSHALL": withNilReplication(HandleZeroCopyFlushAllWithReplication),
		"SELECT":   HandleZeroCopySelect,
		"CONN":     HandleZeroCopyConn,
		"KEYS":     HandleZeroCopyKeys,
		"CONFIG":   HandleZeroCopyConfig,
		"EXISTS":   HandleZeroCopyExists,
		"INCR":     withNilReplication(HandleZeroCopyIncrWithReplication),
		"DECR":     withNilReplication(HandleZeroCopyDecrWithReplication),
		"INCRBY":   withNilReplication(HandleZeroCopyIncrByWithReplication),
		"DECRBY":   withNilReplication(HandleZeroCopyDecrByWithReplication),
		"APPEND":   withNilReplication(HandleZeroCopyAppendWithReplication),
		"STRLEN":   HandleZeroCopyStrLen,
		"GETSET":   HandleZeroCopyGetSet,
		// List commands
		"LPUSH":       withNilReplication(HandleZeroCopyLPushWithReplication),
		"RPUSH":       withNilReplication(HandleZeroCopyRPushWithReplication),
		"LPUSHX":      withNilReplication(HandleZeroCopyLPushXWithReplication),
		"RPUSHX":      withNilReplication(HandleZeroCopyRPushXWithReplication),
		"LPUSHUNIQUE": withNilReplication(HandleZeroCopyLPushUniqueWithReplication),
		"RPUSHUNIQUE": withNilReplication(HandleZeroCopyRPushUniqueWithReplication),
		"LPOP":        withNilReplication(HandleZeroCopyLPopWithReplication),
		"RPOP":        withNilReplication(HandleZeroCopyRPopWithReplication),
		"LLEN":        HandleZeroCopyLLen,
		"LRANGE":      HandleZeroCopyLRange,
		"LINDEX":      HandleZeroCopyLIndex,
		"LSET":        withNilReplication(HandleZeroCopyLSetWithReplication),
		"LREM":        withNilReplication(HandleZeroCopyLRemWithReplication),
		"LTRIM":       withNilReplication(HandleZeroCopyLTrimWithReplication),
		// Set commands
		"SADD":       withNilReplication(HandleZeroCopySAddWithReplication),
		"SMEMBERS":   HandleZeroCopySMembers,
		"SREM":       withNilReplication(HandleZeroCopySRemWithReplication),
		"SCARD":      HandleZeroCopySCard,
		"SISMEMBER":  HandleZeroCopySIsMember,
		"SPOP":       withNilReplication(HandleZeroCopySPopWithReplication),
		"SDIFF":      HandleZeroCopySDiff,
		"SINTER":     HandleZeroCopySInter,
		"SUNION":     HandleZeroCopySUnion,
		"SDIFFSTORE": withNilReplication(HandleZeroCopySDiffStoreWithReplication),
		// Hash commands
		"HSET":         withNilReplication(HandleZeroCopyHSetWithReplication),
		"HGET":         HandleZeroCopyHGet,
		"HMGET":        HandleZeroCopyHMGet,
		"HMSET":        withNilReplication(HandleZeroCopyHMSetWithReplication),
		"HDEL":         withNilReplication(HandleZeroCopyHDelWithReplication),
		"HEXISTS":      HandleZeroCopyHExists,
		"HLEN":         HandleZeroCopyHLen,
		"HKEYS":        HandleZeroCopyHKeys,
		"HVALS":        HandleZeroCopyHVals,
		"HGETALL":      HandleZeroCopyHGetAll,
		"HINCRBY":      withNilReplication(HandleZeroCopyHIncrByWithReplication),
		"HINCRBYFLOAT": withNilReplication(HandleZeroCopyHIncrByFloatWithReplication),
		// Debug commands
		"DEBUG": HandleZeroCopyDebugStructure,
		// Deferred command support
		"DEFER":        HandleZeroCopyDefer,
		"DEFER.CANCEL": HandleZeroCopyDeferCancel,
		"DEFER.LIST":   HandleZeroCopyDeferList,
		"DEFER.STATS":  HandleZeroCopyDeferStats,
		// Commit log monitoring commands
		"COMMITLOG.STATS":    HandleZeroCopyCommitLogStats,
		"COMMITLOG.SEGMENTS": HandleZeroCopyCommitLogSegments,
		"COMMITLOG.COMPACT":  HandleZeroCopyCommitLogCompact,
		"COMMITLOG.INFO":     HandleZeroCopyCommitLogInfo,
	}

	// Register all case variations (commands are uppercase by default)
	for cmd, handler := range commands {
		r.handlers[cmd] = handler                  // Original (uppercase)
		r.handlers[strings.ToLower(cmd)] = handler // Lowercase
		r.handlers[capitalizeFirst(cmd)] = handler // Title case
	}
}

// GetHandler returns the handler for a command
func (r *ZeroCopyRegistry) GetHandler(command string) (ZeroCopyHandler, bool) {
	handler, exists := r.handlers[command]
	return handler, exists
}

// CommandStats holds command statistics by category
type CommandStats struct {
	TotalCommands     int
	StringCommands    int
	ListCommands      int
	SetCommands       int
	HashCommands      int
	DeferredCommands  int
	CommitLogCommands int
	ServerCommands    int
}

// GetCommandStats returns statistics about registered commands
func (r *ZeroCopyRegistry) GetCommandStats() CommandStats {
	return getCommandStatsFromMap(r.handlers)
}

// GetGlobalCommandStats returns command statistics (can be called without registry instance)
func GetGlobalCommandStats() CommandStats {
	r := NewZeroCopyRegistry()
	return r.GetCommandStats()
}

func getCommandStatsFromMap(handlers map[string]ZeroCopyHandler) CommandStats {
	stats := CommandStats{}

	for cmd := range handlers {
		stats.TotalCommands++

		switch {
		case strings.HasPrefix(cmd, "L") && (cmd == "LPUSH" || cmd == "LPOP" || cmd == "LLEN" ||
			cmd == "LRANGE" || cmd == "LINDEX" || cmd == "LSET" || cmd == "LREM" || cmd == "LTRIM" ||
			cmd == "LPUSHX" || cmd == "LPUSHUNIQUE"):
			stats.ListCommands++
		case strings.HasPrefix(cmd, "R") && (cmd == "RPUSH" || cmd == "RPOP" || cmd == "RPUSHX" || cmd == "RPUSHUNIQUE"):
			stats.ListCommands++
		case strings.HasPrefix(cmd, "S") && (cmd == "SADD" || cmd == "SMEMBERS" || cmd == "SREM" ||
			cmd == "SCARD" || cmd == "SISMEMBER" || cmd == "SPOP" || cmd == "SDIFF" ||
			cmd == "SINTER" || cmd == "SUNION" || cmd == "SDIFFSTORE"):
			stats.SetCommands++
		case strings.HasPrefix(cmd, "H") && (cmd == "HSET" || cmd == "HGET" || cmd == "HMGET" ||
			cmd == "HMSET" || cmd == "HDEL" || cmd == "HEXISTS" || cmd == "HLEN" ||
			cmd == "HKEYS" || cmd == "HVALS" || cmd == "HGETALL" || cmd == "HINCRBY" || cmd == "HINCRBYFLOAT"):
			stats.HashCommands++
		case strings.HasPrefix(cmd, "DEFER"):
			stats.DeferredCommands++
		case strings.HasPrefix(cmd, "COMMITLOG"):
			stats.CommitLogCommands++
		case cmd == "GET" || cmd == "SET" || cmd == "DEL" || cmd == "MGET" || cmd == "MSET" ||
			cmd == "EXISTS" || cmd == "INCR" || cmd == "DECR" || cmd == "INCRBY" || cmd == "DECRBY" ||
			cmd == "APPEND" || cmd == "STRLEN" || cmd == "GETSET":
			stats.StringCommands++
		default:
			stats.ServerCommands++
		}
	}

	return stats
}

// expandRandomTemplates is an alias for internal use
var expandRandomTemplates = ExpandRandomTemplates

// ExpandRandomTemplates replaces __rand_int__ placeholders with random integers
func ExpandRandomTemplates(s string) string {
	// Fastest path: if templates disabled globally, return immediately
	if disableTemplateExpansion {
		return s
	}

	// Ultra-fast path: check for underscore first (most strings won't have any)
	if len(s) < 12 || !hasUnderscore(s) {
		return s
	}

	// Fast path: check if template exists at all
	if !strings.Contains(s, "__rand_int__") {
		return s
	}

	// Get random generator from pool (no mutex contention)
	rng := templateRngPool.Get().(*rand.Rand)
	defer templateRngPool.Put(rng)

	// Get string builder from pool
	builder := getStringBuilder()
	defer putStringBuilder(builder)

	builder.Grow(len(s) + 64) // Pre-allocate space for numbers

	remaining := s
	for {
		index := strings.Index(remaining, "__rand_int__")
		if index == -1 {
			builder.WriteString(remaining)
			break
		}

		// Write text before placeholder
		builder.WriteString(remaining[:index])

		// Generate and write random number (0-999999999)
		randNum := rng.Int63n(1000000000)
		builder.WriteString(fmt.Sprintf("%d", randNum))

		// Continue with text after placeholder
		remaining = remaining[index+12:] // len("__rand_int__") = 12
	}

	return builder.String()
}

// hasUnderscore quickly checks if string contains underscore character
// This is much faster than strings.Contains for simple character checks
func hasUnderscore(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '_' {
			return true
		}
	}
	return false
}

// EnableTemplateExpansion enables template expansion feature (disabled by default for performance)
func EnableTemplateExpansion() {
	disableTemplateExpansion = false
}

// DisableTemplateExpansion disables template expansion feature (default for performance)
func DisableTemplateExpansion() {
	disableTemplateExpansion = true
}

// fastLengthString returns pre-computed length string for common sizes, falls back to fmt.Sprintf for large sizes
func fastLengthString(length int) string {
	if length >= 0 && length <= 1000 {
		return lengthStrings[length]
	}
	// Fallback for large sizes (rare case)
	return fmt.Sprintf("%d", length)
}

// Pre-compiled responses for better performance
var (
	okResponse         = []byte("+OK\r\n")
	queuedResponse     = []byte("+QUEUED\r\n")
	pongResponse       = []byte("+PONG\r\n")
	nullResponse       = []byte("$-1\r\n")
	emptyArrayResponse = []byte("*0\r\n")
	errorPrefix        = []byte("-ERR ")
	errorDash          = []byte("-")
	errorSuffix        = []byte("\r\n")
)

// Common error messages
const (
	errNotInteger        = "value is not an integer or out of range"
	errNotFloat          = "value is not a valid float"
	errDeferNotSupported = "deferred commands not supported by this store type"
	errCommitLogDisabled = "commit log not enabled"
)

// String builder pool for template expansion
var stringBuilderPool = sync.Pool{
	New: func() interface{} {
		return &strings.Builder{}
	},
}

// getStringBuilder returns a string builder from pool
func getStringBuilder() *strings.Builder {
	sb := stringBuilderPool.Get().(*strings.Builder)
	sb.Reset()
	return sb
}

// putStringBuilder returns a string builder to pool
func putStringBuilder(sb *strings.Builder) {
	stringBuilderPool.Put(sb)
}

// Helper functions for commands

// parseInteger parses a string to int64
func parseInteger(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

// Response writing functions

// knownErrorCodes are RESP error codes we emit that are their own reply code
// (rendered as "-CODE message"), rather than the generic "-ERR message". Kept
// as an explicit allowlist so ordinary messages that merely start with an
// uppercase word (e.g. "SET command requires ...") are not mistaken for codes.
var knownErrorCodes = []string{"WRONGTYPE"}

// hasErrorCodePrefix reports whether a message already begins with one of the
// known RESP error codes followed by a space.
func hasErrorCodePrefix(message string) bool {
	for _, code := range knownErrorCodes {
		if strings.HasPrefix(message, code+" ") {
			return true
		}
	}
	return false
}

// writeError writes an error response in RESP format ("-<CODE> <message>").
func writeError(writer *bufio.Writer, message string) error {
	prefix := errorPrefix
	if hasErrorCodePrefix(message) {
		// Message carries its own error code (e.g. WRONGTYPE); don't double it
		// up as "-ERR WRONGTYPE ...".
		prefix = errorDash
	}
	if _, err := writer.Write(prefix); err != nil {
		return err
	}
	if _, err := writer.WriteString(message); err != nil {
		return err
	}
	_, err := writer.Write(errorSuffix)
	return err
}

// writeInteger writes an integer response
func writeInteger(writer *bufio.Writer, value int64) error {
	// Use pre-computed response for common values (-100 to 1000)
	if value >= -100 && value <= 1000 {
		_, err := writer.Write(integerResponses[value+100])
		return err
	}

	// For uncommon values, use buffer pool to avoid allocations
	bufPtr := responseBufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0] // Reset length but keep capacity

	buf = append(buf, ':')
	buf = strconv.AppendInt(buf, value, 10)
	buf = append(buf, '\r', '\n')

	_, err := writer.Write(buf)

	// Return buffer to pool
	*bufPtr = buf
	responseBufferPool.Put(bufPtr)

	return err
}

// writeBulkString writes a bulk string response
func writeBulkString(writer *bufio.Writer, value string) error {
	valueLen := len(value)

	// Use pre-computed length response for common sizes (0-1000)
	if valueLen <= 1000 {
		// Write pre-computed length header
		if _, err := writer.Write(bulkLengthResponses[valueLen]); err != nil {
			return err
		}
		// Write value and CRLF
		if _, err := writer.WriteString(value); err != nil {
			return err
		}
		_, err := writer.Write(crlfBytes)
		return err
	}

	// For uncommon sizes, use buffer pool to avoid allocations
	bufPtr := responseBufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0] // Reset length but keep capacity

	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(valueLen), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, value...)
	buf = append(buf, '\r', '\n')

	_, err := writer.Write(buf)

	// Return buffer to pool
	*bufPtr = buf
	responseBufferPool.Put(bufPtr)

	return err
}

// writeArrayStart writes array start marker
func writeArrayStart(writer *bufio.Writer, count int) error {
	// Use pre-computed response for common array sizes (0-100)
	if count >= 0 && count <= 100 {
		_, err := writer.Write(arrayStartResponses[count])
		return err
	}

	// For uncommon sizes, use buffer pool to avoid allocations
	bufPtr := responseBufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0] // Reset length but keep capacity

	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(count), 10)
	buf = append(buf, '\r', '\n')

	_, err := writer.Write(buf)

	// Return buffer to pool
	*bufPtr = buf
	responseBufferPool.Put(bufPtr)

	return err
}

// writeArray writes an array of strings
func writeArray(writer *bufio.Writer, values []string) error {
	if err := writeArrayStart(writer, len(values)); err != nil {
		return err
	}

	for _, value := range values {
		if err := writeBulkString(writer, value); err != nil {
			return err
		}
	}

	return nil
}

// Case conversion helpers

// capitalizeFirst capitalizes first letter
func capitalizeFirst(s string) string {
	if len(s) == 0 {
		return s
	}

	result := make([]byte, len(s))
	copy(result, s)

	if result[0] >= 'a' && result[0] <= 'z' {
		result[0] -= 32
	}

	for i := 1; i < len(result); i++ {
		c := result[i]
		if c >= 'A' && c <= 'Z' {
			result[i] = c + 32
		} else {
			result[i] = c
		}
	}
	return string(result)
}

// HandleZeroCopyType handles the TYPE command
func HandleZeroCopyType(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	if len(cmd.Args) < 1 {
		writer.WriteString("-ERR wrong number of arguments for 'type' command\r\n")
		return writer.Flush()
	}

	key := string(cmd.Args[0])
	val, exists := kvStore.GetStoredValue(key)
	if !exists {
		writer.WriteString("+none\r\n")
		return writer.Flush()
	}

	var typeStr string
	if val.IsString() {
		typeStr = "string"
	} else if val.IsHash() {
		typeStr = "hash"
	} else if val.IsList() {
		typeStr = "list"
	} else if val.IsSet() {
		typeStr = "set"
	} else {
		typeStr = "unknown"
	}

	writer.WriteString("+" + typeStr + "\r\n")
	return writer.Flush()
}
