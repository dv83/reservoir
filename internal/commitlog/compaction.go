package commitlog

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"reservoir/pkg/logger"
)

// Compactor handles commit log compaction
type Compactor struct {
	dataDir        string
	mu             sync.Mutex
	running        bool
	lastCompaction time.Time
}

// NewCompactor creates a new compactor instance
func NewCompactor(dataDir string) *Compactor {
	return &Compactor{
		dataDir: dataDir,
	}
}

// Compact performs log compaction to remove old and duplicate data
func (c *Compactor) Compact(currentSegmentID uint64) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return fmt.Errorf("compaction already running")
	}
	c.running = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.running = false
		c.lastCompaction = time.Now()
		c.mu.Unlock()
	}()

	logger.Info("Starting commit log compaction...")
	startTime := time.Now()

	// Find all segments except the current one
	segments, err := c.findSegmentsToCompact(currentSegmentID)
	if err != nil {
		return fmt.Errorf("failed to find segments: %w", err)
	}

	if len(segments) < 2 {
		logger.Info("Not enough segments to compact (found %d)", len(segments))
		return nil
	}

	// Build the current state from all segments
	state, stats, err := c.buildCurrentState(segments)
	if err != nil {
		return fmt.Errorf("failed to build state: %w", err)
	}

	// Check if compaction is worthwhile
	garbageRatio := float64(stats.totalEntries-stats.liveEntries) / float64(stats.totalEntries)
	if garbageRatio < CompactionThreshold {
		logger.Info("Garbage ratio %.2f%% below threshold %.2f%%, skipping compaction",
			garbageRatio*100, CompactionThreshold*100)
		return nil
	}

	// Write compacted data to new segment
	compactedPath, err := c.writeCompactedSegment(state, segments[0])
	if err != nil {
		return fmt.Errorf("failed to write compacted segment: %w", err)
	}

	// Atomically replace old segments with compacted one
	if err := c.replaceSegments(segments, compactedPath); err != nil {
		// Clean up failed compaction
		os.Remove(compactedPath)
		return fmt.Errorf("failed to replace segments: %w", err)
	}

	duration := time.Since(startTime)
	logger.Info("Compaction completed in %v: %d segments -> 1 segment, %d entries -> %d entries (%.2f%% reduction)",
		duration, len(segments), stats.totalEntries, stats.liveEntries, garbageRatio*100)

	return nil
}

// compactionStats tracks statistics during compaction
type compactionStats struct {
	totalEntries int
	liveEntries  int
	totalBytes   int64
	liveBytes    int64
}

// findSegmentsToCompact finds all segments that can be compacted
func (c *Compactor) findSegmentsToCompact(currentSegmentID uint64) ([]string, error) {
	pattern := filepath.Join(c.dataDir, "*"+SegmentFileExtension)
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	// Filter out current segment and sort by name (which includes timestamp)
	var segments []string
	currentSegmentName := fmt.Sprintf("%016d%s", currentSegmentID, SegmentFileExtension)

	for _, file := range files {
		if !strings.HasSuffix(file, currentSegmentName) {
			segments = append(segments, file)
		}
	}

	sort.Strings(segments)
	return segments, nil
}

// buildCurrentState reads all segments and builds the current state
func (c *Compactor) buildCurrentState(segments []string) (map[string]*Entry, *compactionStats, error) {
	state := make(map[string]*Entry)
	stats := &compactionStats{}

	reader, err := NewReader(c.dataDir)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()

	// Process each segment in order
	for _, segment := range segments {
		err := reader.readSegment(segment, func(entry *Entry) error {
			stats.totalEntries++
			key := string(entry.Key)

			switch entry.Type {
			case EntryTypeWrite:
				// Check if this is a newer version
				if existing, ok := state[key]; !ok || entry.Timestamp > existing.Timestamp {
					state[key] = entry
				}
			case EntryTypeDelete:
				// Remove from state if this delete is newer
				if existing, ok := state[key]; ok && entry.Timestamp > existing.Timestamp {
					delete(state, key)
				}
			}

			return nil
		})

		if err != nil {
			return nil, nil, err
		}
	}

	// Calculate live entries
	stats.liveEntries = len(state)
	for _, entry := range state {
		stats.liveBytes += int64(len(entry.Key) + len(entry.Value))
	}

	return state, stats, nil
}

// writeCompactedSegment writes the compacted state to a new segment
func (c *Compactor) writeCompactedSegment(state map[string]*Entry, oldestSegment string) (string, error) {
	// Create temporary file for compacted segment
	tempFile := filepath.Join(c.dataDir, fmt.Sprintf("compact_%d.tmp", time.Now().UnixNano()))

	file, err := os.Create(tempFile)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, WriteBufferSize)

	// Get sorted keys for deterministic output
	keys := make([]string, 0, len(state))
	for k := range state {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create a minimal commit log with just the writer functionality
	cl := &CommitLog{
		currentSegment: &Segment{
			Writer: writer,
		},
	}

	// Write all entries
	for _, key := range keys {
		entry := state[key]
		// Update timestamp to preserve ordering
		entry.Timestamp = time.Now().UnixNano()
		entry.CRC = entry.CalculateCRC()

		if err := cl.writeEntry(entry); err != nil {
			return "", fmt.Errorf("failed to write entry: %w", err)
		}
	}

	// Flush and sync
	if err := writer.Flush(); err != nil {
		return "", err
	}
	if err := file.Sync(); err != nil {
		return "", err
	}

	// Rename to final name (use timestamp from oldest segment to maintain order)
	baseName := filepath.Base(oldestSegment)
	compactedName := strings.TrimSuffix(baseName, SegmentFileExtension) + ".compacted" + SegmentFileExtension
	finalPath := filepath.Join(c.dataDir, compactedName)

	if err := os.Rename(tempFile, finalPath); err != nil {
		return "", err
	}

	return finalPath, nil
}

// replaceSegments atomically replaces old segments with the compacted one
func (c *Compactor) replaceSegments(oldSegments []string, compactedPath string) error {
	// First, create a marker file to indicate compaction in progress
	markerPath := filepath.Join(c.dataDir, ".compaction_in_progress")
	if err := os.WriteFile(markerPath, []byte(compactedPath), 0644); err != nil {
		return err
	}

	// Delete old segments
	for _, segment := range oldSegments {
		if err := os.Remove(segment); err != nil {
			logger.Warning("Failed to remove old segment %s: %v", segment, err)
			// Continue anyway
		}
	}

	// Rename compacted file to remove .compacted suffix
	finalName := strings.Replace(filepath.Base(compactedPath), ".compacted", "", 1)
	finalPath := filepath.Join(c.dataDir, finalName)

	if err := os.Rename(compactedPath, finalPath); err != nil {
		return err
	}

	// Remove marker file
	os.Remove(markerPath)

	return nil
}

// RecoverFromInterruptedCompaction checks for and recovers from interrupted compaction
func (c *Compactor) RecoverFromInterruptedCompaction() error {
	markerPath := filepath.Join(c.dataDir, ".compaction_in_progress")

	data, err := os.ReadFile(markerPath)
	if os.IsNotExist(err) {
		// No interrupted compaction
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to read compaction marker: %w", err)
	}

	compactedPath := string(data)
	logger.Warning("Found interrupted compaction, recovering from %s", compactedPath)

	// Check if compacted file exists
	if _, err := os.Stat(compactedPath); err == nil {
		// Compacted file exists, finish the compaction
		finalName := strings.Replace(filepath.Base(compactedPath), ".compacted", "", 1)
		finalPath := filepath.Join(c.dataDir, finalName)

		if err := os.Rename(compactedPath, finalPath); err != nil {
			return fmt.Errorf("failed to finish compaction: %w", err)
		}
	}

	// Remove marker
	os.Remove(markerPath)

	return nil
}
