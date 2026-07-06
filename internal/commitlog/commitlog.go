package commitlog

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"reservoir/pkg/logger"
)

const (
	// File and segment configuration
	SegmentFileExtension = ".log"
	IndexFileExtension   = ".idx"
	MaxSegmentSize       = 1024 * 1024 * 1024 // 1GB per segment
	WriteBufferSize      = 1024 * 1024        // 1MB write buffer (increased)

	// Batch configuration for async writes
	BatchSize           = 10000                  // Max entries per batch (10x increase)
	BatchTimeout        = 100 * time.Millisecond // Max time to wait for batch (10x increase)
	CompactionInterval  = 5 * time.Minute        // How often to run compaction
	CompactionThreshold = 0.5                    // Compact when 50% is garbage

	// Entry types
	EntryTypeWrite  = byte(0x01)
	EntryTypeDelete = byte(0x02)
	EntryTypeMeta   = byte(0x03)

	// Format limits. Operation and key lengths are encoded as uint16 on disk, so
	// values beyond this would silently truncate. MaxEntrySize matches the
	// reader's cap so we never write an entry the reader would reject as corrupt.
	MaxFieldLen  = 65535
	MaxEntrySize = 10 * 1024 * 1024
)

// validateEntrySizes rejects an entry that cannot be encoded and recovered
// losslessly: an operation/key beyond the uint16 length field, or a total entry
// larger than the reader's maximum.
func validateEntrySizes(operation string, key, value []byte) error {
	if len(operation) > MaxFieldLen {
		return fmt.Errorf("commit log: operation too long (%d bytes, max %d)", len(operation), MaxFieldLen)
	}
	if len(key) > MaxFieldLen {
		return fmt.Errorf("commit log: key too long (%d bytes, max %d)", len(key), MaxFieldLen)
	}
	total := 1 + 8 + 2 + len(operation) + 2 + len(key) + 4 + len(value) + 4
	if total > MaxEntrySize {
		return fmt.Errorf("commit log: entry too large (%d bytes, max %d)", total, MaxEntrySize)
	}
	return nil
}

// Entry represents a single write operation in the commit log
type Entry struct {
	Type      byte   // Operation type (SET, DEL, etc.)
	Timestamp int64  // Unix nano timestamp
	Key       []byte // Key
	Value     []byte // Value (empty for DEL)
	Operation string // Original operation name
	CRC       uint32 // CRC32 checksum
}

// CalculateCRC calculates CRC32 checksum for an entry
func (e *Entry) CalculateCRC() uint32 {
	h := crc32.NewIEEE()
	h.Write([]byte{e.Type})
	binary.Write(h, binary.BigEndian, e.Timestamp)
	h.Write([]byte(e.Operation))
	h.Write(e.Key)
	h.Write(e.Value)
	return h.Sum32()
}

// Segment represents a single commit log file
type Segment struct {
	ID        uint64
	Path      string
	File      *os.File
	Writer    *bufio.Writer
	Size      int64
	StartTime time.Time
	EndTime   time.Time
	mu        sync.RWMutex
	closed    bool
}

// CommitLog is the main commit log structure
type CommitLog struct {
	// Configuration
	dataDir        string
	maxSegmentSize int64

	// Current segment
	currentSegment *Segment
	segmentID      uint64

	// Write batching
	writeChan   chan *Entry
	batchBuffer []*Entry
	flushTimer  *time.Timer

	// Compaction
	compactionChan chan struct{}
	lastCompaction time.Time

	// Shutdown
	shutdownChan chan struct{}

	// Statistics
	totalEntries   int64
	totalBytes     int64
	compactedBytes int64
	droppedEntries int64

	// Lifecycle
	mu     sync.RWMutex
	closed bool
	wg     sync.WaitGroup
}

// NewCommitLog creates a new commit log instance
func NewCommitLog(dataDir string) (*CommitLog, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	cl := &CommitLog{
		dataDir:        dataDir,
		maxSegmentSize: MaxSegmentSize,
		writeChan:      make(chan *Entry, 100000), // 100K buffer
		batchBuffer:    make([]*Entry, 0, BatchSize),
		compactionChan: make(chan struct{}, 1),
		shutdownChan:   make(chan struct{}),
	}

	// Load existing segments or create first one
	if err := cl.initializeSegments(); err != nil {
		return nil, fmt.Errorf("failed to initialize segments: %w", err)
	}

	// Start background workers
	cl.wg.Add(2)
	go cl.writeWorker()
	go cl.compactionWorker()

	logger.Info("Commit log initialized in %s", dataDir)
	return cl, nil
}

// WriteTimeout is the maximum time to wait for the write channel
const WriteTimeout = 100 * time.Millisecond

// Write writes an entry to the commit log with a short timeout
// Returns error if the entry could not be queued
func (cl *CommitLog) Write(operation string, key []byte, value []byte) error {
	cl.mu.RLock()
	if cl.closed {
		cl.mu.RUnlock()
		return errors.New("commit log is closed")
	}
	cl.mu.RUnlock()

	if err := validateEntrySizes(operation, key, value); err != nil {
		return err
	}

	entry := &Entry{
		Type:      EntryTypeWrite,
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Value:     value,
		Operation: operation,
	}

	// Calculate CRC
	entry.CRC = entry.CalculateCRC()

	// Send to write channel with timeout
	select {
	case cl.writeChan <- entry:
		atomic.AddInt64(&cl.totalEntries, 1)
		return nil
	case <-time.After(WriteTimeout):
		// Channel full after timeout - this indicates serious backpressure
		atomic.AddInt64(&cl.droppedEntries, 1)
		dropped := atomic.LoadInt64(&cl.droppedEntries)
		logger.Error("Commit log write channel full after %v, dropping entry (total dropped: %d)", WriteTimeout, dropped)
		return errors.New("commit log write channel full")
	}
}

// Delete asynchronously writes a delete entry to the commit log
func (cl *CommitLog) Delete(key []byte) {
	cl.mu.RLock()
	if cl.closed {
		cl.mu.RUnlock()
		return
	}
	cl.mu.RUnlock()

	entry := &Entry{
		Type:      EntryTypeDelete,
		Timestamp: time.Now().UnixNano(),
		Key:       key,
		Operation: "DEL",
	}

	// Calculate CRC
	entry.CRC = entry.CalculateCRC()

	// Send to write channel (non-blocking)
	select {
	case cl.writeChan <- entry:
		atomic.AddInt64(&cl.totalEntries, 1)
	default:
		logger.Warning("Commit log write channel full, dropping delete entry")
	}
}

// writeWorker handles asynchronous batched writes
func (cl *CommitLog) writeWorker() {
	defer cl.wg.Done()

	ticker := time.NewTicker(BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case entry := <-cl.writeChan:
			cl.batchBuffer = append(cl.batchBuffer, entry)

			// Flush if batch is full
			if len(cl.batchBuffer) >= BatchSize {
				cl.flushBatch()
			}

		case <-ticker.C:
			// Flush on timeout if there's data
			if len(cl.batchBuffer) > 0 {
				cl.flushBatch()
			}

		case <-cl.compactionChan:
			// Trigger compaction
			cl.compact()

		case <-cl.shutdownChan:
			// Immediate shutdown requested
			logger.Debug("Commit log writeWorker received shutdown signal")

			// Flush any remaining entries
			if len(cl.batchBuffer) > 0 {
				cl.flushBatch()
			}

			// Drain any remaining items in channel
			for len(cl.writeChan) > 0 {
				entry := <-cl.writeChan
				cl.batchBuffer = append(cl.batchBuffer, entry)
				if len(cl.batchBuffer) >= BatchSize {
					cl.flushBatch()
				}
			}

			// Final flush
			if len(cl.batchBuffer) > 0 {
				cl.flushBatch()
			}

			logger.Debug("Commit log writeWorker shutdown complete")
			return
		}
	}
}

// flushBatch writes the current batch to disk
func (cl *CommitLog) flushBatch() {
	if len(cl.batchBuffer) == 0 {
		return
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	// Check if we need to rotate segment
	if cl.currentSegment.Size >= cl.maxSegmentSize {
		if err := cl.rotateSegment(); err != nil {
			logger.Error("Failed to rotate segment: %v", err)
			return
		}
	}

	// Write all entries in batch
	for _, entry := range cl.batchBuffer {
		if err := cl.writeEntry(entry); err != nil {
			logger.Error("Failed to write entry: %v", err)
			continue
		}
	}

	// Flush to disk
	if err := cl.currentSegment.Writer.Flush(); err != nil {
		logger.Error("Failed to flush segment: %v", err)
	}

	// Sync to ensure durability
	if err := cl.currentSegment.File.Sync(); err != nil {
		logger.Error("Failed to sync segment: %v", err)
	}

	// Clear batch buffer
	cl.batchBuffer = cl.batchBuffer[:0]
}

// writeEntry writes a single entry to the current segment
func (cl *CommitLog) writeEntry(entry *Entry) error {
	// Entry format:
	// [4 bytes: total length][1 byte: type][8 bytes: timestamp][2 bytes: operation length][operation][2 bytes: key length][key][4 bytes: value length][value][4 bytes: CRC]

	operationLen := len(entry.Operation)
	keyLen := len(entry.Key)
	valueLen := len(entry.Value)
	totalLen := 1 + 8 + 2 + operationLen + 2 + keyLen + 4 + valueLen + 4 // type + timestamp + opLen + operation + keyLen + key + valueLen + value + CRC

	// Write total length
	if err := binary.Write(cl.currentSegment.Writer, binary.BigEndian, uint32(totalLen)); err != nil {
		return err
	}

	// Write type
	if err := cl.currentSegment.Writer.WriteByte(entry.Type); err != nil {
		return err
	}

	// Write timestamp
	if err := binary.Write(cl.currentSegment.Writer, binary.BigEndian, entry.Timestamp); err != nil {
		return err
	}

	// Write operation length and operation
	if err := binary.Write(cl.currentSegment.Writer, binary.BigEndian, uint16(operationLen)); err != nil {
		return err
	}
	if operationLen > 0 {
		if _, err := cl.currentSegment.Writer.Write([]byte(entry.Operation)); err != nil {
			return err
		}
	}

	// Write key length and key
	if err := binary.Write(cl.currentSegment.Writer, binary.BigEndian, uint16(keyLen)); err != nil {
		return err
	}
	if _, err := cl.currentSegment.Writer.Write(entry.Key); err != nil {
		return err
	}

	// Write value length and value
	if err := binary.Write(cl.currentSegment.Writer, binary.BigEndian, uint32(valueLen)); err != nil {
		return err
	}
	if valueLen > 0 {
		if _, err := cl.currentSegment.Writer.Write(entry.Value); err != nil {
			return err
		}
	}

	// Write CRC
	if err := binary.Write(cl.currentSegment.Writer, binary.BigEndian, entry.CRC); err != nil {
		return err
	}

	// Update segment size
	cl.currentSegment.Size += int64(4 + totalLen) // Include length prefix
	atomic.AddInt64(&cl.totalBytes, int64(4+totalLen))

	return nil
}

// initializeSegments loads existing segments or creates the first one
func (cl *CommitLog) initializeSegments() error {
	// Find existing segments
	files, err := filepath.Glob(filepath.Join(cl.dataDir, "*"+SegmentFileExtension))
	if err != nil {
		return err
	}

	if len(files) == 0 {
		// No existing segments, create first one
		return cl.createNewSegment()
	}

	// Parse segment IDs and find the highest one
	var maxSegmentID uint64
	var lastSegmentPath string

	for _, file := range files {
		filename := filepath.Base(file)
		// Extract segment ID from filename (format: 0000000000000001.log)
		segmentIDStr := filename[:len(filename)-len(SegmentFileExtension)]

		var segmentID uint64
		if _, err := fmt.Sscanf(segmentIDStr, "%016d", &segmentID); err != nil {
			logger.Warning("Invalid segment filename format: %s", filename)
			continue
		}

		if segmentID > maxSegmentID {
			maxSegmentID = segmentID
			lastSegmentPath = file
		}
	}

	// Set the current segment ID to the highest found
	atomic.StoreUint64(&cl.segmentID, maxSegmentID)

	// Open the last segment for append (to continue writing)
	return cl.openExistingSegment(lastSegmentPath, maxSegmentID)
}

// openExistingSegment opens an existing segment file for append
func (cl *CommitLog) openExistingSegment(path string, segmentID uint64) error {
	// Get current file size
	fileInfo, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("failed to stat existing segment %s: %w", path, err)
	}

	// Open file for append
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open existing segment %s: %w", path, err)
	}

	segment := &Segment{
		ID:        segmentID,
		Path:      path,
		File:      file,
		Writer:    bufio.NewWriterSize(file, WriteBufferSize),
		Size:      fileInfo.Size(),
		StartTime: fileInfo.ModTime(), // Use file modification time as approximation
	}

	cl.currentSegment = segment
	logger.Info("Opened existing segment %s (ID: %d, Size: %d bytes) for append",
		filepath.Base(path), segmentID, fileInfo.Size())

	return nil
}

// createNewSegment creates a new segment file
func (cl *CommitLog) createNewSegment() error {
	segmentID := atomic.AddUint64(&cl.segmentID, 1)
	filename := fmt.Sprintf("%016d%s", segmentID, SegmentFileExtension)
	path := filepath.Join(cl.dataDir, filename)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	segment := &Segment{
		ID:        segmentID,
		Path:      path,
		File:      file,
		Writer:    bufio.NewWriterSize(file, WriteBufferSize),
		StartTime: time.Now(),
	}

	cl.currentSegment = segment
	return nil
}

// rotateSegment closes current segment and creates a new one
func (cl *CommitLog) rotateSegment() error {
	// Close current segment
	if cl.currentSegment != nil {
		cl.currentSegment.EndTime = time.Now()
		cl.currentSegment.Writer.Flush()
		cl.currentSegment.File.Close()
		cl.currentSegment.closed = true
	}

	// Create new segment
	return cl.createNewSegment()
}

// compactionWorker runs periodic compaction
func (cl *CommitLog) compactionWorker() {
	defer cl.wg.Done()

	ticker := time.NewTicker(CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Trigger compaction
			select {
			case cl.compactionChan <- struct{}{}:
			default:
				// Compaction already scheduled
			}

		case <-cl.shutdownChan:
			// Immediate shutdown requested
			logger.Debug("Commit log compactionWorker received shutdown signal")
			return
		}
	}
}

// compact performs log compaction
func (cl *CommitLog) compact() {
	compactor := NewCompactor(cl.dataDir)

	cl.mu.RLock()
	currentSegmentID := cl.segmentID
	cl.mu.RUnlock()

	if err := compactor.Compact(currentSegmentID); err != nil {
		logger.Error("Compaction failed: %v", err)
	} else {
		cl.lastCompaction = time.Now()
		// Update compacted bytes stats
		atomic.AddInt64(&cl.compactedBytes, compactor.lastCompaction.Unix()) // Placeholder
	}
}

// Close closes the commit log
func (cl *CommitLog) Close() error {
	cl.mu.Lock()
	if cl.closed {
		cl.mu.Unlock()
		return nil // Already closed
	}
	cl.closed = true
	cl.mu.Unlock()

	logger.Debug("Commit log shutdown initiated")

	// Signal shutdown to workers
	close(cl.shutdownChan)

	// Wait for workers to finish (with timeout)
	done := make(chan struct{})
	go func() {
		cl.wg.Wait()
		close(done)
	}()

	var closeErr error
	select {
	case <-done:
		logger.Debug("All commit log workers stopped gracefully")
	case <-time.After(5 * time.Second):
		logger.Warning("Timeout waiting for commit log workers to stop - forcing close")
		closeErr = errors.New("timeout waiting for commit log workers")
	}

	// Close current segment only after workers have stopped or timeout
	// This is safe because after timeout we accept potential data loss
	if cl.currentSegment != nil {
		if err := cl.currentSegment.Writer.Flush(); err != nil {
			logger.Error("Failed to flush segment: %v", err)
			if closeErr == nil {
				closeErr = err
			}
		}
		if err := cl.currentSegment.File.Close(); err != nil {
			logger.Error("Failed to close segment file: %v", err)
			if closeErr == nil {
				closeErr = err
			}
		}
	}

	logger.Debug("Commit log shutdown complete")
	return closeErr
}

// Stats returns commit log statistics
func (cl *CommitLog) Stats() map[string]interface{} {
	return map[string]interface{}{
		"total_entries":   atomic.LoadInt64(&cl.totalEntries),
		"total_bytes":     atomic.LoadInt64(&cl.totalBytes),
		"compacted_bytes": atomic.LoadInt64(&cl.compactedBytes),
		"last_compaction": cl.lastCompaction,
		"current_segment": cl.segmentID,
	}
}

// TriggerCompaction triggers manual compaction
func (cl *CommitLog) TriggerCompaction() error {
	cl.mu.RLock()
	if cl.closed {
		cl.mu.RUnlock()
		return fmt.Errorf("commit log is closed")
	}
	cl.mu.RUnlock()

	// Trigger compaction via channel (non-blocking)
	select {
	case cl.compactionChan <- struct{}{}:
		return nil
	default:
		return fmt.Errorf("compaction already in progress")
	}
}
