package commitlog

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"reservoir/pkg/logger"
)

// Reader provides sequential access to commit log entries
type Reader struct {
	dataDir       string
	segments      []string
	currentIndex  int
	currentFile   *os.File
	currentReader *bufio.Reader
}

// NewReader creates a new commit log reader
func NewReader(dataDir string) (*Reader, error) {
	// Find all segment files
	pattern := filepath.Join(dataDir, "*"+SegmentFileExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list segments: %w", err)
	}

	// Sort segments by ID (filename)
	sort.Strings(files)

	return &Reader{
		dataDir:  dataDir,
		segments: files,
	}, nil
}

// ReadAll reads all entries from the commit log in chronological order
func (r *Reader) ReadAll(callback func(*Entry) error) error {
	// First pass: collect all entries with timestamps
	var allEntries []*Entry

	for _, segmentPath := range r.segments {
		if err := r.readSegment(segmentPath, func(entry *Entry) error {
			allEntries = append(allEntries, entry)
			return nil
		}); err != nil {
			return fmt.Errorf("failed to read segment %s: %w", segmentPath, err)
		}
	}

	// Sort all entries by timestamp to ensure chronological order
	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].Timestamp < allEntries[j].Timestamp
	})

	logger.Info("Sorted %d entries by timestamp for chronological recovery", len(allEntries))

	// Second pass: process entries in chronological order
	for _, entry := range allEntries {
		if err := callback(entry); err != nil {
			return fmt.Errorf("callback error: %w", err)
		}
	}

	return nil
}

// readSegment reads all entries from a single segment
func (r *Reader) readSegment(path string, callback func(*Entry) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, ReadBufferSize)
	entriesRead := 0
	corruptEntries := 0

	for {
		entry, err := r.readEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Log corruption but continue reading
			logger.Warning("Corrupt entry in segment %s: %v", path, err)
			corruptEntries++

			// Try to recover by finding next valid entry
			if err := r.seekToNextEntry(reader); err != nil {
				break
			}
			continue
		}

		// Verify CRC
		calculatedCRC := entry.CalculateCRC()
		if calculatedCRC != entry.CRC {
			logger.Warning("CRC mismatch in segment %s: expected %x, got %x",
				path, entry.CRC, calculatedCRC)
			corruptEntries++
			continue
		}

		// Process entry
		if err := callback(entry); err != nil {
			return fmt.Errorf("callback error: %w", err)
		}

		entriesRead++
	}

	logger.Info("Read segment %s: %d entries, %d corrupt",
		filepath.Base(path), entriesRead, corruptEntries)

	return nil
}

// readEntry reads a single entry from the reader
func (r *Reader) readEntry(reader *bufio.Reader) (*Entry, error) {
	// Read total length
	var totalLen uint32
	if err := binary.Read(reader, binary.BigEndian, &totalLen); err != nil {
		return nil, err
	}

	// Sanity check length
	if totalLen > 10*1024*1024 { // 10MB max entry size
		return nil, fmt.Errorf("entry too large: %d bytes", totalLen)
	}

	// Read entry data
	data := make([]byte, totalLen)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}

	// Parse entry. Every field read is bounds-checked against the buffer: a
	// corrupt or truncated entry (an internal length field larger than the data)
	// must return an error, not panic — a panic during recovery would crash the
	// whole server rather than being skipped as corruption by readSegment.
	entry := &Entry{}
	pos := 0

	// need reports whether n more bytes are available from pos.
	need := func(n int) bool { return pos+n <= len(data) }

	// Type (1) + Timestamp (8)
	if !need(1 + 8) {
		return nil, fmt.Errorf("truncated entry header")
	}
	entry.Type = data[pos]
	pos++
	entry.Timestamp = int64(binary.BigEndian.Uint64(data[pos : pos+8]))
	pos += 8

	// Operation length and operation
	if !need(2) {
		return nil, fmt.Errorf("truncated operation length")
	}
	operationLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if !need(operationLen) {
		return nil, fmt.Errorf("operation length %d exceeds entry", operationLen)
	}
	if operationLen > 0 {
		entry.Operation = string(data[pos : pos+operationLen])
		pos += operationLen
	}

	// Key length and key
	if !need(2) {
		return nil, fmt.Errorf("truncated key length")
	}
	keyLen := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2
	if !need(keyLen) {
		return nil, fmt.Errorf("key length %d exceeds entry", keyLen)
	}
	entry.Key = make([]byte, keyLen)
	copy(entry.Key, data[pos:pos+keyLen])
	pos += keyLen

	// Value length and value
	if !need(4) {
		return nil, fmt.Errorf("truncated value length")
	}
	valueLen := int(binary.BigEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if valueLen < 0 || !need(valueLen) {
		return nil, fmt.Errorf("value length %d exceeds entry", valueLen)
	}
	if valueLen > 0 {
		entry.Value = make([]byte, valueLen)
		copy(entry.Value, data[pos:pos+valueLen])
		pos += valueLen
	}

	// CRC
	if !need(4) {
		return nil, fmt.Errorf("truncated CRC")
	}
	entry.CRC = binary.BigEndian.Uint32(data[pos : pos+4])

	return entry, nil
}

// seekToNextEntry tries to find the next valid entry after corruption
func (r *Reader) seekToNextEntry(reader *bufio.Reader) error {
	// Look for a valid entry header by scanning for reasonable values
	for {
		// Try to read 4 bytes for length
		lengthBytes, err := reader.Peek(4)
		if err != nil {
			return err
		}

		length := binary.BigEndian.Uint32(lengthBytes)
		if length > 0 && length < 10*1024*1024 { // Reasonable length
			// This might be a valid entry, return and let normal reading continue
			return nil
		}

		// Skip one byte and try again
		if _, err := reader.ReadByte(); err != nil {
			return err
		}
	}
}

// Close closes the reader
func (r *Reader) Close() error {
	if r.currentFile != nil {
		return r.currentFile.Close()
	}
	return nil
}

// Recovery reads the commit log and rebuilds the current state
type Recovery struct {
	reader *Reader
}

// NewRecovery creates a new recovery instance
func NewRecovery(dataDir string) (*Recovery, error) {
	reader, err := NewReader(dataDir)
	if err != nil {
		return nil, err
	}

	return &Recovery{
		reader: reader,
	}, nil
}

// Recover reads all entries and applies them to rebuild state
func (r *Recovery) Recover(apply func(entry *Entry) error) error {
	logger.Info("Starting commit log recovery...")

	startTime := time.Now()
	totalEntries := 0
	appliedEntries := 0

	// Read all entries and apply them
	err := r.reader.ReadAll(func(entry *Entry) error {
		totalEntries++

		// Apply entry
		if err := apply(entry); err != nil {
			logger.Warning("Failed to apply entry: %v", err)
			return nil // Continue recovery
		}

		appliedEntries++

		// Log progress every 10000 entries
		if appliedEntries%10000 == 0 {
			logger.Info("Recovery progress: %d entries applied", appliedEntries)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}

	duration := time.Since(startTime)
	logger.Info("Recovery completed: %d/%d entries applied in %v",
		appliedEntries, totalEntries, duration)

	return nil
}

// GetLatestSnapshot finds the most recent complete state snapshot
func (r *Recovery) GetLatestSnapshot() (map[string][]byte, error) {
	snapshot := make(map[string][]byte)

	// Read all entries and build final state
	err := r.reader.ReadAll(func(entry *Entry) error {
		switch entry.Type {
		case EntryTypeWrite:
			snapshot[string(entry.Key)] = entry.Value
		case EntryTypeDelete:
			delete(snapshot, string(entry.Key))
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

const ReadBufferSize = 64 * 1024 // 64KB read buffer
