package commands

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"reservoir/internal/protocol"
	"reservoir/internal/store"
)

// HandleZeroCopyCommitLogStats shows commit log statistics
func HandleZeroCopyCommitLogStats(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	commitLogDir := kvStore.GetCommitLogDir()
	if commitLogDir == "" {
		return writeError(writer, errCommitLogDisabled)
	}

	// Get commit log instance for stats
	stats := kvStore.GetCommitLogStats()
	if stats == nil {
		return writeError(writer, "commit log stats unavailable")
	}

	// Get directory stats
	segmentFiles, totalSize, err := getSegmentFileStats(commitLogDir)
	if err != nil {
		return writeError(writer, fmt.Sprintf("failed to get segment stats: %v", err))
	}

	// Build response
	response := []string{
		fmt.Sprintf("commit_log_enabled:1"),
		fmt.Sprintf("commit_log_dir:%s", commitLogDir),
		fmt.Sprintf("total_entries:%v", stats["total_entries"]),
		fmt.Sprintf("total_bytes:%v", stats["total_bytes"]),
		fmt.Sprintf("compacted_bytes:%v", stats["compacted_bytes"]),
		fmt.Sprintf("current_segment:%v", stats["current_segment"]),
		fmt.Sprintf("segment_files:%d", len(segmentFiles)),
		fmt.Sprintf("total_disk_size:%d", totalSize),
		fmt.Sprintf("last_compaction:%v", stats["last_compaction"]),
	}

	// Write array response
	if err := writeArrayStart(writer, len(response)); err != nil {
		return err
	}

	for _, item := range response {
		if err := writeBulkString(writer, item); err != nil {
			return err
		}
	}

	return nil
}

// HandleZeroCopyCommitLogSegments lists all commit log segments with details
func HandleZeroCopyCommitLogSegments(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	commitLogDir := kvStore.GetCommitLogDir()
	if commitLogDir == "" {
		return writeError(writer, errCommitLogDisabled)
	}

	segmentFiles, _, err := getSegmentFileStats(commitLogDir)
	if err != nil {
		return writeError(writer, fmt.Sprintf("failed to get segments: %v", err))
	}

	if len(segmentFiles) == 0 {
		return writeArrayStart(writer, 0)
	}

	// Write array of segment info
	if err := writeArrayStart(writer, len(segmentFiles)); err != nil {
		return err
	}

	for _, seg := range segmentFiles {
		segmentInfo := []string{
			fmt.Sprintf("name:%s", seg.Name),
			fmt.Sprintf("size:%d", seg.Size),
			fmt.Sprintf("modified:%s", seg.ModTime.Format(time.RFC3339)),
			fmt.Sprintf("entries:%d", seg.EntryCount),
		}

		if err := writeArrayStart(writer, len(segmentInfo)); err != nil {
			return err
		}

		for _, info := range segmentInfo {
			if err := writeBulkString(writer, info); err != nil {
				return err
			}
		}
	}

	return nil
}

// HandleZeroCopyCommitLogCompact triggers manual compaction
func HandleZeroCopyCommitLogCompact(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	commitLogDir := kvStore.GetCommitLogDir()
	if commitLogDir == "" {
		return writeError(writer, errCommitLogDisabled)
	}

	// Trigger compaction
	err := kvStore.TriggerCommitLogCompaction()
	if err != nil {
		return writeError(writer, fmt.Sprintf("compaction failed: %v", err))
	}

	_, err = writer.Write(okResponse)
	return err
}

// HandleZeroCopyCommitLogInfo shows detailed commit log information
func HandleZeroCopyCommitLogInfo(kvStore store.KVStore, cmd *protocol.ZeroCopyCommand, writer *bufio.Writer) error {
	commitLogDir := kvStore.GetCommitLogDir()
	if commitLogDir == "" {
		// Commit log disabled
		response := []string{
			"commit_log_enabled:0",
			"reason:commit log not configured",
		}

		if err := writeArrayStart(writer, len(response)); err != nil {
			return err
		}

		for _, item := range response {
			if err := writeBulkString(writer, item); err != nil {
				return err
			}
		}
		return nil
	}

	// Get detailed stats
	stats := kvStore.GetCommitLogStats()
	segmentFiles, totalSize, err := getSegmentFileStats(commitLogDir)
	if err != nil {
		return writeError(writer, fmt.Sprintf("failed to get info: %v", err))
	}

	// Calculate compression ratio
	totalEntries := int64(0)
	if stats != nil {
		if val, ok := stats["total_entries"].(int64); ok {
			totalEntries = val
		}
	}

	avgEntrySize := int64(0)
	if totalEntries > 0 && totalSize > 0 {
		avgEntrySize = totalSize / totalEntries
	}

	// Get current segment info
	currentSegment := uint64(0)
	if stats != nil {
		if val, ok := stats["current_segment"].(uint64); ok {
			currentSegment = val
		}
	}

	// Build detailed response
	response := []string{
		"commit_log_enabled:1",
		fmt.Sprintf("directory:%s", commitLogDir),
		fmt.Sprintf("total_segments:%d", len(segmentFiles)),
		fmt.Sprintf("current_segment_id:%d", currentSegment),
		fmt.Sprintf("total_disk_size:%d", totalSize),
		fmt.Sprintf("total_entries:%d", totalEntries),
		fmt.Sprintf("avg_entry_size:%d", avgEntrySize),
	}

	if stats != nil {
		response = append(response,
			fmt.Sprintf("total_bytes:%v", stats["total_bytes"]),
			fmt.Sprintf("compacted_bytes:%v", stats["compacted_bytes"]),
		)

		if lastCompaction, ok := stats["last_compaction"].(time.Time); ok && !lastCompaction.IsZero() {
			response = append(response,
				fmt.Sprintf("last_compaction:%s", lastCompaction.Format(time.RFC3339)),
				fmt.Sprintf("time_since_compaction:%v", time.Since(lastCompaction)),
			)
		} else {
			response = append(response, "last_compaction:never")
		}
	}

	// Write response
	if err := writeArrayStart(writer, len(response)); err != nil {
		return err
	}

	for _, item := range response {
		if err := writeBulkString(writer, item); err != nil {
			return err
		}
	}

	return nil
}

// SegmentInfo contains information about a commit log segment
type SegmentInfo struct {
	Name       string
	Size       int64
	ModTime    time.Time
	EntryCount int64
}

// getSegmentFileStats gathers statistics about segment files
func getSegmentFileStats(dir string) ([]SegmentInfo, int64, error) {
	pattern := filepath.Join(dir, "*.log")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, 0, err
	}

	var segments []SegmentInfo
	var totalSize int64

	for _, file := range files {
		stat, err := os.Stat(file)
		if err != nil {
			continue
		}

		// Estimate entry count (rough calculation)
		entryCount := estimateEntryCount(stat.Size())

		segments = append(segments, SegmentInfo{
			Name:       filepath.Base(file),
			Size:       stat.Size(),
			ModTime:    stat.ModTime(),
			EntryCount: entryCount,
		})

		totalSize += stat.Size()
	}

	// Sort by modification time (newest first)
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].ModTime.After(segments[j].ModTime)
	})

	return segments, totalSize, nil
}

// estimateEntryCount provides a rough estimate of entries in a segment
func estimateEntryCount(fileSize int64) int64 {
	if fileSize == 0 {
		return 0
	}
	// Rough estimate: average entry size ~100 bytes
	// (4 byte length + 1 type + 8 timestamp + 2+N operation + 2+N key + 4+N value + 4 CRC)
	avgEntrySize := int64(100)
	return fileSize / avgEntrySize
}
