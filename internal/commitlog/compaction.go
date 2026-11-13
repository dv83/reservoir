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

// opKind classifies a logged operation by how it affects a key during replay.
type opKind int

const (
	// opIncremental modifies an existing key without carrying its full state
	// (EXPIRE, PERSIST, SADD, SREM, HSET, HMSET, HDEL). It must be kept.
	opIncremental opKind = iota
	// opReset fully overwrites the key — value, type and TTL — on replay.
	opReset
	// opDelete removes the key (and its TTL).
	opDelete
	// opFlushAll clears every key.
	opFlushAll
)

// classifyEntry returns how an entry affects replay. Operations the compactor
// does not recognize are treated as incremental, so they are kept rather than
// risk collapsing state they carry.
func classifyEntry(e *Entry) opKind {
	if e.Type == EntryTypeDelete {
		return opDelete
	}
	switch e.Operation {
	case "SET", "INCR", "INCRBY", "DECR", "DECRBY":
		// Counters are logged as their absolute result and replayed as a SET.
		return opReset
	case "FLUSHALL", "FLUSHDB":
		return opFlushAll
	}
	return opIncremental
}

// buildCurrentState reads all segments and returns, per key, the entries that
// must survive compaction so that replaying them rebuilds exactly the state the
// original segments do: everything since the key's last reset (SET, absolute
// counter write, DEL or FLUSHALL). Keeping only the last entry per key — as
// before — lost the value of a key followed by EXPIRE/PERSIST and every set
// member or hash field but the last one.
func (c *Compactor) buildCurrentState(segments []string) (map[string][]*Entry, *compactionStats, error) {
	stats := &compactionStats{}

	reader, err := NewReader(c.dataDir)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Close()

	var all []*Entry
	for _, segment := range segments {
		err := reader.readSegment(segment, func(entry *Entry) error {
			stats.totalEntries++
			all = append(all, entry)
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
	}

	// Recovery replays entries in timestamp order, not file order, so decide
	// what survives in that same order. Stable, to keep file order on ties.
	sort.SliceStable(all, func(i, j int) bool { return all[i].Timestamp < all[j].Timestamp })

	state := make(map[string][]*Entry)
	for _, entry := range all {
		key := string(entry.Key)
		switch classifyEntry(entry) {
		case opFlushAll:
			// Everything before it is cleared on replay. The FLUSHALL itself is
			// dropped too: compaction covers the oldest segments contiguously and
			// recovery starts from an empty store, so nothing before it survives.
			state = make(map[string][]*Entry)
		case opDelete:
			delete(state, key)
		case opReset:
			state[key] = []*Entry{entry}
		default:
			state[key] = append(state[key], entry)
		}
	}

	for _, entries := range state {
		stats.liveEntries += len(entries)
		for _, entry := range entries {
			stats.liveBytes += int64(len(entry.Key) + len(entry.Value))
		}
	}

	return state, stats, nil
}

// writeCompactedSegment writes the compacted state to a new segment
func (c *Compactor) writeCompactedSegment(state map[string][]*Entry, oldestSegment string) (string, error) {
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

	// Write every retained entry, PRESERVING its original timestamp. Recovery
	// sorts every entry across all segments by timestamp, so stamping compacted
	// entries with time.Now() would make these (old) values appear newer than
	// writes already recorded in the live current segment and resurrect stale
	// data. Each key's entries are already in timestamp order.
	for _, key := range keys {
		for _, entry := range state[key] {
			entry.CRC = entry.CalculateCRC() // CRC is unchanged, recomputed defensively

			if err := cl.writeEntry(entry); err != nil {
				return "", fmt.Errorf("failed to write entry: %w", err)
			}
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
