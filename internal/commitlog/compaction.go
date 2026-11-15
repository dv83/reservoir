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

	// Pass 1: each key's last reset/delete and the last FLUSHALL. Memory is
	// bounded by the number of keys; no value is held.
	cut, err := c.scanCutoffs(segments)
	if err != nil {
		return fmt.Errorf("failed to scan segments: %w", err)
	}

	// Pass 2: stream the surviving entries into a new segment.
	compactedPath, live, err := c.writeCompactedSegment(segments, cut)
	if err != nil {
		return fmt.Errorf("failed to write compacted segment: %w", err)
	}

	// Keep the originals if compaction would not reclaim enough.
	garbageRatio := float64(cut.total-live) / float64(cut.total)
	if garbageRatio < CompactionThreshold {
		os.Remove(compactedPath)
		logger.Info("Garbage ratio %.2f%% below threshold %.2f%%, skipping compaction",
			garbageRatio*100, CompactionThreshold*100)
		return nil
	}

	// Atomically replace old segments with compacted one
	if err := c.replaceSegments(segments, compactedPath); err != nil {
		// Clean up failed compaction
		os.Remove(compactedPath)
		return fmt.Errorf("failed to replace segments: %w", err)
	}

	duration := time.Since(startTime)
	logger.Info("Compaction completed in %v: %d segments -> 1 segment, %d entries -> %d entries (%.2f%% reduction)",
		duration, len(segments), cut.total, live, garbageRatio*100)

	return nil
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

// entryOrder is an entry's position in replay order: recovery replays entries
// sorted by timestamp, and a stable sort keeps file order (segment, then index
// within it) on ties. It is a total order, so the latest entry of a kind can be
// found by taking a maximum in any read order — no sort, and no entries held.
type entryOrder struct {
	ts  int64
	seg int
	idx int
}

func (a entryOrder) less(b entryOrder) bool {
	if a.ts != b.ts {
		return a.ts < b.ts
	}
	if a.seg != b.seg {
		return a.seg < b.seg
	}
	return a.idx < b.idx
}

// cutoffs is what compaction keeps in memory: for each key the replay position
// of its last reset (SET, absolute counter write) or delete, and the position of
// the last FLUSHALL. Its size depends on the number of keys, never on values or
// on how many entries the log holds.
type cutoffs struct {
	key      map[string]entryOrder
	flush    entryOrder
	hasFlush bool
	total    int
}

// scanCutoffs is compaction's first pass: it streams every segment once and
// records each key's last reset or delete and the last FLUSHALL.
func (c *Compactor) scanCutoffs(segments []string) (*cutoffs, error) {
	reader, err := NewReader(c.dataDir)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	cut := &cutoffs{key: make(map[string]entryOrder)}
	for si, segment := range segments {
		idx := 0
		err := reader.readSegment(segment, func(e *Entry) error {
			o := entryOrder{ts: e.Timestamp, seg: si, idx: idx}
			idx++
			cut.total++
			switch classifyEntry(e) {
			case opFlushAll:
				if !cut.hasFlush || cut.flush.less(o) {
					cut.flush, cut.hasFlush = o, true
				}
			case opReset, opDelete:
				k := string(e.Key)
				if cur, ok := cut.key[k]; !ok || cur.less(o) {
					cut.key[k] = o
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return cut, nil
}

// survives reports whether replaying the original segments in timestamp order
// would still reflect e in the final state — exactly the entries the compacted
// log must keep:
//   - a FLUSHALL or DEL is never needed: compaction covers the oldest segments
//     contiguously and recovery starts from an empty store, so what they clear
//     is simply not written;
//   - nothing replayed before the last FLUSHALL survives it;
//   - for each key, nothing replayed before its last reset or delete survives;
//     the reset itself does.
//
// Keeping only the last entry per key — as compaction once did — lost the
// value behind an EXPIRE/PERSIST and every set member or hash field but the last.
func (cut *cutoffs) survives(e *Entry, o entryOrder) bool {
	switch classifyEntry(e) {
	case opFlushAll, opDelete:
		return false
	}
	if cut.hasFlush && !cut.flush.less(o) {
		return false
	}
	if k, ok := cut.key[string(e.Key)]; ok && o.less(k) {
		return false
	}
	return true
}

// writeCompactedSegment is compaction's second pass: it streams the segments
// again and writes every surviving entry to a new segment, each with its
// original timestamp (recovery sorts by timestamp across all segments, so
// restamping would make old values look newer than the live segment's writes).
// It returns the new segment's path and how many entries it kept.
func (c *Compactor) writeCompactedSegment(segments []string, cut *cutoffs) (string, int, error) {
	tempFile := filepath.Join(c.dataDir, fmt.Sprintf("compact_%d.tmp", time.Now().UnixNano()))
	file, err := os.Create(tempFile)
	if err != nil {
		return "", 0, err
	}
	fail := func(err error) (string, int, error) {
		file.Close()
		os.Remove(tempFile)
		return "", 0, err
	}

	writer := bufio.NewWriterSize(file, WriteBufferSize)
	// A minimal commit log with just the writer functionality.
	cl := &CommitLog{currentSegment: &Segment{Writer: writer}}

	reader, err := NewReader(c.dataDir)
	if err != nil {
		return fail(err)
	}
	defer reader.Close()

	live := 0
	for si, segment := range segments {
		idx := 0
		err := reader.readSegment(segment, func(e *Entry) error {
			o := entryOrder{ts: e.Timestamp, seg: si, idx: idx}
			idx++
			if !cut.survives(e, o) {
				return nil
			}
			live++
			e.CRC = e.CalculateCRC() // unchanged; recomputed defensively
			if err := cl.writeEntry(e); err != nil {
				return fmt.Errorf("failed to write entry: %w", err)
			}
			return nil
		})
		if err != nil {
			return fail(err)
		}
	}

	if err := writer.Flush(); err != nil {
		return fail(err)
	}
	if err := file.Sync(); err != nil {
		return fail(err)
	}
	if err := file.Close(); err != nil {
		os.Remove(tempFile)
		return "", 0, err
	}

	// Name it after the oldest segment, so segment order is kept.
	baseName := filepath.Base(segments[0])
	compactedName := strings.TrimSuffix(baseName, SegmentFileExtension) + ".compacted" + SegmentFileExtension
	finalPath := filepath.Join(c.dataDir, compactedName)
	if err := os.Rename(tempFile, finalPath); err != nil {
		os.Remove(tempFile)
		return "", 0, err
	}
	return finalPath, live, nil
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
