package commitlog

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"reservoir/pkg/logger"
)

// RecoveryStatus represents the status of async recovery
type RecoveryStatus int32

const (
	RecoveryStatusNotStarted RecoveryStatus = 0
	RecoveryStatusRunning    RecoveryStatus = 1
	RecoveryStatusCompleted  RecoveryStatus = 2
	RecoveryStatusFailed     RecoveryStatus = 3
)

// AsyncRecovery manages asynchronous commit log recovery
type AsyncRecovery struct {
	// Status and progress
	status         int32 // atomic RecoveryStatus
	progress       int64 // atomic - entries processed
	totalEntries   int64 // atomic - total entries to process
	startTime      time.Time
	completionTime time.Time
	error          error
	mu             sync.RWMutex

	// Recovery configuration
	dataDir          string
	applyFunc        func(entry *Entry) error
	progressCallback func(processed, total int64)

	// Cancellation
	cancelChan chan struct{}
	done       chan struct{}
}

// NewAsyncRecovery creates a new asynchronous recovery manager
func NewAsyncRecovery(dataDir string, applyFunc func(entry *Entry) error) *AsyncRecovery {
	return &AsyncRecovery{
		dataDir:    dataDir,
		applyFunc:  applyFunc,
		cancelChan: make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// SetProgressCallback sets a callback for progress updates
func (ar *AsyncRecovery) SetProgressCallback(callback func(processed, total int64)) {
	ar.progressCallback = callback
}

// Start begins asynchronous recovery
func (ar *AsyncRecovery) Start() error {
	if !atomic.CompareAndSwapInt32(&ar.status, int32(RecoveryStatusNotStarted), int32(RecoveryStatusRunning)) {
		return fmt.Errorf("recovery already started")
	}

	ar.startTime = time.Now()

	// Start recovery in background
	go ar.runRecovery()

	logger.Info("Asynchronous commit log recovery started in background")
	return nil
}

// runRecovery performs the actual recovery work
func (ar *AsyncRecovery) runRecovery() {
	defer close(ar.done)

	recovery, err := NewRecovery(ar.dataDir)
	if err != nil {
		ar.setError(fmt.Errorf("failed to create recovery: %w", err))
		return
	}
	defer recovery.reader.Close()

	// Single pass: process entries directly without counting first
	// This avoids reading the commit log twice (2x I/O savings)
	processedCount := int64(0)
	atomic.StoreInt64(&ar.totalEntries, -1) // -1 indicates unknown total

	logger.Info("Commit log recovery: starting single-pass recovery")

	err = recovery.Recover(func(entry *Entry) error {
		select {
		case <-ar.cancelChan:
			return fmt.Errorf("recovery cancelled")
		default:
		}

		// Apply entry using provided function
		if ar.applyFunc != nil {
			if err := ar.applyFunc(entry); err != nil {
				logger.Warning("Failed to apply recovery entry: %v", err)
				// Continue recovery despite individual failures
			}
		}

		// Update progress
		processedCount++
		atomic.StoreInt64(&ar.progress, processedCount)

		// Call progress callback every 1000 entries
		// Note: total is -1 (unknown) during single-pass recovery
		if ar.progressCallback != nil && processedCount%1000 == 0 {
			ar.progressCallback(processedCount, -1)
		}

		return nil
	})

	if err != nil {
		ar.setError(fmt.Errorf("recovery failed: %w", err))
		return
	}

	// Update total entries now that we know the actual count
	atomic.StoreInt64(&ar.totalEntries, processedCount)

	// Final progress callback with known total
	if ar.progressCallback != nil {
		ar.progressCallback(processedCount, processedCount)
	}

	// Mark completion
	ar.completionTime = time.Now()
	atomic.StoreInt32(&ar.status, int32(RecoveryStatusCompleted))

	duration := ar.completionTime.Sub(ar.startTime)
	logger.Info("Asynchronous commit log recovery completed: %d entries processed in %v",
		processedCount, duration)
}

// setError sets error status and logs it
func (ar *AsyncRecovery) setError(err error) {
	ar.mu.Lock()
	ar.error = err
	ar.mu.Unlock()

	atomic.StoreInt32(&ar.status, int32(RecoveryStatusFailed))
	logger.Error("Asynchronous recovery failed: %v", err)
}

// Cancel requests recovery cancellation
func (ar *AsyncRecovery) Cancel() {
	select {
	case <-ar.cancelChan:
		// Already cancelled
	default:
		close(ar.cancelChan)
		logger.Info("Asynchronous recovery cancellation requested")
	}
}

// Wait waits for recovery completion or timeout
func (ar *AsyncRecovery) Wait(timeout time.Duration) error {
	select {
	case <-ar.done:
		return ar.GetError()
	case <-time.After(timeout):
		return fmt.Errorf("recovery wait timeout after %v", timeout)
	}
}

// GetStatus returns current recovery status
func (ar *AsyncRecovery) GetStatus() RecoveryStatus {
	return RecoveryStatus(atomic.LoadInt32(&ar.status))
}

// GetProgress returns current progress (processed, total)
func (ar *AsyncRecovery) GetProgress() (int64, int64) {
	return atomic.LoadInt64(&ar.progress), atomic.LoadInt64(&ar.totalEntries)
}

// GetError returns any error that occurred
func (ar *AsyncRecovery) GetError() error {
	ar.mu.RLock()
	defer ar.mu.RUnlock()
	return ar.error
}

// GetDuration returns recovery duration
func (ar *AsyncRecovery) GetDuration() time.Duration {
	if ar.startTime.IsZero() {
		return 0
	}

	if ar.completionTime.IsZero() {
		return time.Since(ar.startTime)
	}

	return ar.completionTime.Sub(ar.startTime)
}

// GetStats returns detailed recovery statistics
func (ar *AsyncRecovery) GetStats() map[string]interface{} {
	processed, total := ar.GetProgress()
	status := ar.GetStatus()
	duration := ar.GetDuration()

	var progressPercent float64
	if total > 0 {
		progressPercent = float64(processed) / float64(total) * 100
	}

	stats := map[string]interface{}{
		"status":           status,
		"status_string":    ar.getStatusString(),
		"progress":         processed,
		"total_entries":    total,
		"progress_percent": progressPercent,
		"duration_seconds": duration.Seconds(),
		"start_time":       ar.startTime,
		"completion_time":  ar.completionTime,
	}

	if err := ar.GetError(); err != nil {
		stats["error"] = err.Error()
	}

	if processed > 0 && duration > 0 {
		stats["entries_per_second"] = float64(processed) / duration.Seconds()
	}

	return stats
}

// getStatusString returns human-readable status
func (ar *AsyncRecovery) getStatusString() string {
	switch ar.GetStatus() {
	case RecoveryStatusNotStarted:
		return "not_started"
	case RecoveryStatusRunning:
		return "running"
	case RecoveryStatusCompleted:
		return "completed"
	case RecoveryStatusFailed:
		return "failed"
	default:
		return "unknown"
	}
}
