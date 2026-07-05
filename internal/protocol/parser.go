package protocol

import (
	"bufio"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// ZeroCopyCommand represents a parsed Redis command without string allocations
type ZeroCopyCommand struct {
	Command []byte   // Command name (SET, GET, etc.)
	Args    [][]byte // Arguments as byte slices
	Buffer  []byte   // Original buffer that holds all data (owned by this command)
}

// Release returns the command's buffer to the pool. MUST be called after command processing is complete.
// After calling Release(), the command and its arguments become invalid and must not be used.
func (cmd *ZeroCopyCommand) Release() {
	if cmd.Buffer != nil {
		PutPooledBuffer(cmd.Buffer)
		cmd.Buffer = nil
		cmd.Command = nil
		cmd.Args = nil
	}
}

// ZeroCopyParser provides zero-copy parsing of Redis protocol
type ZeroCopyParser struct {
	buf    []byte
	bufPos int // Current position for buffer allocation
}

// Maximum buffer size to prevent OOM attacks (16MB)
const MaxBufferSize = 16 * 1024 * 1024

// Maximum command size (1MB)
const MaxCommandSize = 1024 * 1024

// Buffer pool for reusing parser buffers
var bufferPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 16384) // 16KB initial size
	},
}

// Pool statistics for debugging
var (
	poolGets    int64
	poolPuts    int64
	poolRejects int64
)

// Initialize periodic buffer pool cleanup to prevent unbounded growth
func init() {
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			// Force garbage collection of unused buffers
			// The sync.Pool will be cleared on each GC cycle
			bufferPool = sync.Pool{
				New: func() interface{} {
					return make([]byte, 16384)
				},
			}
		}
	}()
}

// GetPooledBuffer gets a buffer from the pool
func GetPooledBuffer() []byte {
	atomic.AddInt64(&poolGets, 1)
	return bufferPool.Get().([]byte)
}

// PutPooledBuffer returns a buffer to the pool
func PutPooledBuffer(buf []byte) {
	// Only pool buffers that aren't too large
	if len(buf) <= 256*1024 { // 256KB max pooled size
		bufferPool.Put(buf[:cap(buf)])
		atomic.AddInt64(&poolPuts, 1)
	} else {
		atomic.AddInt64(&poolRejects, 1)
	}
}

// NewZeroCopyParser creates a new zero-copy parser
func NewZeroCopyParser() *ZeroCopyParser {
	return &ZeroCopyParser{
		buf:    GetPooledBuffer(), // Get buffer from pool
		bufPos: 0,
	}
}

// Release returns the parser's buffer to the pool
func (p *ZeroCopyParser) Release() {
	if p.buf != nil {
		PutPooledBuffer(p.buf)
		p.buf = nil
	}
}

// ParseCommand parses a single Redis command using zero-copy approach
func (p *ZeroCopyParser) ParseCommand(reader *bufio.Reader) (*ZeroCopyCommand, error) {
	// Reset buffer position for this command
	p.bufPos = 0

	// Read the first line to determine format, skipping empty lines (extra CRLFs)
	var line []byte
	var err error
	for {
		line, err = p.readLine(reader)
		if err != nil {
			return nil, err
		}
		if len(line) > 0 {
			break
		}
	}

	// Simple format (not RESP array)
	if len(line) == 0 || line[0] != '*' {
		return p.parseSimpleCommand(line)
	}

	// RESP array format
	return p.parseRESPArray(reader, line)
}

// MaxLineSize is the maximum allowed line length to prevent DoS attacks
const MaxLineSize = 64 * 1024 // 64KB per line

// readLine reads a line and returns it as a byte slice
// This function allocates a small buffer for the line only
func (p *ZeroCopyParser) readLine(reader *bufio.Reader) ([]byte, error) {
	var line []byte

	for {
		b, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}

		line = append(line, b)

		// Prevent DoS: limit line length
		if len(line) > MaxLineSize {
			return nil, errors.New("line too long")
		}

		if b == '\n' {
			// Remove \r\n
			end := len(line) - 1
			if end > 0 && line[end-1] == '\r' {
				end--
			}
			return line[:end], nil
		}
	}
}

// parseSimpleCommand parses non-RESP format commands
func (p *ZeroCopyParser) parseSimpleCommand(line []byte) (*ZeroCopyCommand, error) {
	if len(line) == 0 {
		return nil, errors.New("empty command")
	}

	// Split by spaces - zero-copy way
	args := p.splitBySpace(line)
	if len(args) == 0 {
		return nil, errors.New("empty command")
	}

	// Transfer buffer ownership to command to prevent race conditions
	cmd := &ZeroCopyCommand{
		Command: args[0],
		Args:    args[1:],
		Buffer:  p.buf, // Command takes ownership of buffer
	}

	// Get new buffer for parser - prevents race condition when parser is reused
	p.buf = GetPooledBuffer()
	p.bufPos = 0

	return cmd, nil
}

// parseRESPArray parses RESP array format
func (p *ZeroCopyParser) parseRESPArray(reader *bufio.Reader, firstLine []byte) (*ZeroCopyCommand, error) {
	// Parse array length
	if len(firstLine) < 2 {
		return nil, errors.New("invalid array format")
	}

	argCount, err := p.parseNumber(firstLine[1:])
	if err != nil {
		return nil, err
	}

	if argCount <= 0 {
		return nil, errors.New("invalid argument count")
	}

	// Pre-allocate args slice
	args := make([][]byte, argCount)

	// Parse each argument
	for i := 0; i < argCount; i++ {
		arg, err := p.parseRESPString(reader)
		if err != nil {
			return nil, err
		}
		args[i] = arg
	}

	if len(args) == 0 {
		return nil, errors.New("empty command")
	}

	// Transfer buffer ownership to command to prevent race conditions
	cmd := &ZeroCopyCommand{
		Command: args[0],
		Args:    args[1:],
		Buffer:  p.buf, // Command takes ownership of buffer
	}

	// Get new buffer for parser - prevents race condition when parser is reused
	p.buf = GetPooledBuffer()
	p.bufPos = 0

	return cmd, nil
}

// parseRESPString parses RESP bulk string
func (p *ZeroCopyParser) parseRESPString(reader *bufio.Reader) ([]byte, error) {
	// Read length line
	lengthLine, err := p.readLine(reader)
	if err != nil {
		return nil, err
	}

	if len(lengthLine) == 0 || lengthLine[0] != '$' {
		return nil, errors.New("expected bulk string")
	}

	length, err := p.parseNumber(lengthLine[1:])
	if err != nil {
		return nil, err
	}

	if length == -1 {
		return nil, nil // NULL bulk string
	}

	if length < 0 {
		return nil, errors.New("invalid bulk string length")
	}

	// Reserve space in buffer at current position
	startPos := p.bufPos
	needed := length + 2 // +2 for \r\n

	// Check if command size exceeds maximum
	if length > MaxCommandSize {
		return nil, errors.New("command too large")
	}

	// Ensure buffer is large enough
	if startPos+needed > len(p.buf) {
		newSize := len(p.buf)
		for newSize < startPos+needed {
			newSize *= 2
		}
		// Prevent buffer from growing beyond maximum size
		if newSize > MaxBufferSize {
			return nil, errors.New("buffer size exceeded maximum")
		}
		newBuf := make([]byte, newSize)
		copy(newBuf, p.buf)
		p.buf = newBuf
	}

	// Read the bulk string data directly into buffer
	_, err = io.ReadFull(reader, p.buf[startPos:startPos+needed])
	if err != nil {
		return nil, err
	}

	// Extract the string part (without \r\n)
	result := p.buf[startPos : startPos+length]
	p.bufPos = startPos + needed

	return result, nil
}

// parseNumber parses a number from byte slice
func (p *ZeroCopyParser) parseNumber(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, errors.New("empty number")
	}

	// Fast path for single digit
	if len(data) == 1 && data[0] >= '0' && data[0] <= '9' {
		return int(data[0] - '0'), nil
	}

	// Handle negative numbers
	start := 0
	sign := 1
	if data[0] == '-' {
		sign = -1
		start = 1
	}

	result := 0
	for i := start; i < len(data); i++ {
		if data[i] < '0' || data[i] > '9' {
			return 0, errors.New("invalid number")
		}
		result = result*10 + int(data[i]-'0')
	}

	return result * sign, nil
}

// splitBySpace splits byte slice by spaces - zero-copy
func (p *ZeroCopyParser) splitBySpace(data []byte) [][]byte {
	var result [][]byte
	start := 0

	for i := 0; i < len(data); i++ {
		if data[i] == ' ' || data[i] == '\t' {
			if i > start {
				result = append(result, data[start:i])
			}
			// Skip multiple spaces
			for i < len(data) && (data[i] == ' ' || data[i] == '\t') {
				i++
			}
			start = i
			i-- // Adjust for loop increment
		}
	}

	// Add last part
	if start < len(data) {
		result = append(result, data[start:])
	}

	return result
}

// String returns string representation of command (with allocation)
func (cmd *ZeroCopyCommand) String() string {
	return unsafeString(cmd.Command)
}

// ArgString returns string representation of argument (with allocation)
func (cmd *ZeroCopyCommand) ArgString(index int) string {
	if index < 0 || index >= len(cmd.Args) {
		return ""
	}
	return unsafeString(cmd.Args[index])
}

// ArgStringCopy returns argument at index as string with safe copy (for storage)
func (cmd *ZeroCopyCommand) ArgStringCopy(index int) string {
	if index < 0 || index >= len(cmd.Args) {
		return ""
	}
	// Create safe copy for long-term storage to prevent buffer corruption
	return string(cmd.Args[index])
}

// ArgCount returns number of arguments
func (cmd *ZeroCopyCommand) ArgCount() int {
	return len(cmd.Args)
}

// IsCommand checks if this is a specific command (case-insensitive)
func (cmd *ZeroCopyCommand) IsCommand(name string) bool {
	return equalIgnoreCase(cmd.Command, []byte(name))
}

// equalIgnoreCase compares byte slices ignoring case
func equalIgnoreCase(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		ca := a[i]
		cb := b[i]

		// Convert to lowercase
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}

		if ca != cb {
			return false
		}
	}

	return true
}

// unsafeString converts byte slice to string without allocation
// WARNING: Only use when you know the byte slice won't be modified
func unsafeString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// Specific command parsers for zero-copy operations

// ZeroCopySetCommand represents a parsed SET command
type ZeroCopySetCommand struct {
	Key   []byte
	Value []byte
}

// ParseSetCommand parses SET command specifically with zero-copy
func ParseSetCommand(cmd *ZeroCopyCommand) (*ZeroCopySetCommand, error) {
	if !cmd.IsCommand("SET") {
		return nil, errors.New("not a SET command")
	}

	if len(cmd.Args) != 2 {
		return nil, errors.New("SET command requires exactly 2 arguments")
	}

	return &ZeroCopySetCommand{
		Key:   cmd.Args[0],
		Value: cmd.Args[1],
	}, nil
}

// KeyString returns key as string (with allocation)
func (sc *ZeroCopySetCommand) KeyString() string {
	return unsafeString(sc.Key)
}

// ValueString returns value as string (with allocation)
func (sc *ZeroCopySetCommand) ValueString() string {
	return unsafeString(sc.Value)
}

// ZeroCopyGetCommand represents a parsed GET command
type ZeroCopyGetCommand struct {
	Key []byte
}

// ParseGetCommand parses GET command specifically with zero-copy
func ParseGetCommand(cmd *ZeroCopyCommand) (*ZeroCopyGetCommand, error) {
	if !cmd.IsCommand("GET") {
		return nil, errors.New("not a GET command")
	}

	if len(cmd.Args) != 1 {
		return nil, errors.New("GET command requires exactly 1 argument")
	}

	return &ZeroCopyGetCommand{
		Key: cmd.Args[0],
	}, nil
}

// KeyString returns key as string (with allocation)
func (gc *ZeroCopyGetCommand) KeyString() string {
	return unsafeString(gc.Key)
}

// ZeroCopyDelCommand represents a parsed DEL command
type ZeroCopyDelCommand struct {
	Key []byte
}

// ParseDelCommand parses DEL command specifically with zero-copy
func ParseDelCommand(cmd *ZeroCopyCommand) (*ZeroCopyDelCommand, error) {
	if !cmd.IsCommand("DEL") {
		return nil, errors.New("not a DEL command")
	}

	if len(cmd.Args) != 1 {
		return nil, errors.New("DEL command requires exactly 1 argument")
	}

	return &ZeroCopyDelCommand{
		Key: cmd.Args[0],
	}, nil
}

// KeyString returns key as string (with allocation)
func (dc *ZeroCopyDelCommand) KeyString() string {
	return unsafeString(dc.Key)
}

// ZeroCopyMSetCommand represents a parsed MSET command
type ZeroCopyMSetCommand struct {
	Pairs [][]byte // [key1, value1, key2, value2, ...]
}

// ParseMSetCommand parses MSET command specifically with zero-copy
func ParseMSetCommand(cmd *ZeroCopyCommand) (*ZeroCopyMSetCommand, error) {
	if !cmd.IsCommand("MSET") {
		return nil, errors.New("not a MSET command")
	}

	if len(cmd.Args) == 0 || len(cmd.Args)%2 != 0 {
		return nil, errors.New("MSET command requires even number of arguments")
	}

	return &ZeroCopyMSetCommand{
		Pairs: cmd.Args,
	}, nil
}

// PairCount returns number of key-value pairs
func (mc *ZeroCopyMSetCommand) PairCount() int {
	return len(mc.Pairs) / 2
}

// GetPair returns key-value pair at index
func (mc *ZeroCopyMSetCommand) GetPair(index int) (key, value []byte) {
	if index < 0 || index*2+1 >= len(mc.Pairs) {
		return nil, nil
	}
	return mc.Pairs[index*2], mc.Pairs[index*2+1]
}

// GetPairStrings returns key-value pair as strings (with allocation)
func (mc *ZeroCopyMSetCommand) GetPairStrings(index int) (key, value string) {
	k, v := mc.GetPair(index)
	return unsafeString(k), unsafeString(v)
}

// ZeroCopyMGetCommand represents a parsed MGET command
type ZeroCopyMGetCommand struct {
	Keys [][]byte
}

// ParseMGetCommand parses MGET command specifically with zero-copy
func ParseMGetCommand(cmd *ZeroCopyCommand) (*ZeroCopyMGetCommand, error) {
	if !cmd.IsCommand("MGET") {
		return nil, errors.New("not a MGET command")
	}

	if len(cmd.Args) == 0 {
		return nil, errors.New("MGET command requires at least 1 argument")
	}

	return &ZeroCopyMGetCommand{
		Keys: cmd.Args,
	}, nil
}

// KeyCount returns number of keys
func (mc *ZeroCopyMGetCommand) KeyCount() int {
	return len(mc.Keys)
}

// GetKey returns key at index
func (mc *ZeroCopyMGetCommand) GetKey(index int) []byte {
	if index < 0 || index >= len(mc.Keys) {
		return nil
	}
	return mc.Keys[index]
}

// GetKeyString returns key as string (with allocation)
func (mc *ZeroCopyMGetCommand) GetKeyString(index int) string {
	return unsafeString(mc.GetKey(index))
}

// ZeroCopyExpireCommand represents a parsed EXPIRE command
type ZeroCopyExpireCommand struct {
	Key     []byte
	Seconds []byte
}

// ParseExpireCommand parses EXPIRE command specifically with zero-copy
func ParseExpireCommand(cmd *ZeroCopyCommand) (*ZeroCopyExpireCommand, error) {
	if !cmd.IsCommand("EXPIRE") {
		return nil, errors.New("not a EXPIRE command")
	}

	if len(cmd.Args) != 2 {
		return nil, errors.New("EXPIRE command requires exactly 2 arguments")
	}

	return &ZeroCopyExpireCommand{
		Key:     cmd.Args[0],
		Seconds: cmd.Args[1],
	}, nil
}

// KeyString returns key as string (with allocation)
func (ec *ZeroCopyExpireCommand) KeyString() string {
	return unsafeString(ec.Key)
}

// SecondsString returns seconds as string (with allocation)
func (ec *ZeroCopyExpireCommand) SecondsString() string {
	return unsafeString(ec.Seconds)
}

// SecondsInt parses seconds as integer
func (ec *ZeroCopyExpireCommand) SecondsInt() (int, error) {
	parser := &ZeroCopyParser{}
	return parser.parseNumber(ec.Seconds)
}

// ZeroCopyTTLCommand represents a parsed TTL command
type ZeroCopyTTLCommand struct {
	Key []byte
}

// ParseTTLCommand parses TTL command specifically with zero-copy
func ParseTTLCommand(cmd *ZeroCopyCommand) (*ZeroCopyTTLCommand, error) {
	if !cmd.IsCommand("TTL") {
		return nil, errors.New("not a TTL command")
	}

	if len(cmd.Args) != 1 {
		return nil, errors.New("TTL command requires exactly 1 argument")
	}

	return &ZeroCopyTTLCommand{
		Key: cmd.Args[0],
	}, nil
}

// KeyString returns key as string (with allocation)
func (tc *ZeroCopyTTLCommand) KeyString() string {
	return unsafeString(tc.Key)
}

// ZeroCopyInfoCommand represents a parsed INFO command
type ZeroCopyInfoCommand struct {
	Section []byte // optional section
}

// ParseInfoCommand parses INFO command specifically with zero-copy
func ParseInfoCommand(cmd *ZeroCopyCommand) (*ZeroCopyInfoCommand, error) {
	if !cmd.IsCommand("INFO") {
		return nil, errors.New("not a INFO command")
	}

	var section []byte
	if len(cmd.Args) > 0 {
		section = cmd.Args[0]
	}

	return &ZeroCopyInfoCommand{
		Section: section,
	}, nil
}

// SectionString returns section as string (with allocation)
func (ic *ZeroCopyInfoCommand) SectionString() string {
	return unsafeString(ic.Section)
}

// HasSection returns true if section is specified
func (ic *ZeroCopyInfoCommand) HasSection() bool {
	return len(ic.Section) > 0
}

// Universal parser dispatcher
func ParseSpecificCommand(cmd *ZeroCopyCommand) (interface{}, error) {
	switch {
	case cmd.IsCommand("SET"):
		return ParseSetCommand(cmd)
	case cmd.IsCommand("GET"):
		return ParseGetCommand(cmd)
	case cmd.IsCommand("DEL"):
		return ParseDelCommand(cmd)
	case cmd.IsCommand("MSET"):
		return ParseMSetCommand(cmd)
	case cmd.IsCommand("MGET"):
		return ParseMGetCommand(cmd)
	case cmd.IsCommand("EXPIRE"):
		return ParseExpireCommand(cmd)
	case cmd.IsCommand("TTL"):
		return ParseTTLCommand(cmd)
	case cmd.IsCommand("INFO"):
		return ParseInfoCommand(cmd)
	default:
		return nil, errors.New("unsupported command for zero-copy parsing")
	}
}
