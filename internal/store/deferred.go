package store

import (
	"encoding/hex"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DeferredCommand represents a command that will be executed at a future time
type DeferredCommand struct {
	ID          string    // Unique identifier for the deferred command
	ExecuteAt   time.Time // When to execute this command
	CommandType string    // Type of command (SET, DEL, INCR, etc.)
	Key         string    // Target key
	Args        []string  // Command arguments
	CreatedAt   time.Time // When this deferred command was created
}

// DeferredScheduler manages and executes deferred commands
type DeferredScheduler struct {
	commands  map[string]*DeferredCommand  // Map of command ID to command
	timeIndex map[int64][]*DeferredCommand // Time-based index for efficient lookup
	mu        sync.RWMutex                 // Protects the scheduler data structures
	store     *LockFreeStore               // Reference to the store for command execution
	stopCh    chan struct{}                // Channel to stop the scheduler
	running   bool                         // Whether the scheduler is running
}

// NewDeferredScheduler creates a new deferred command scheduler
func NewDeferredScheduler(store *LockFreeStore) *DeferredScheduler {
	return &DeferredScheduler{
		commands:  make(map[string]*DeferredCommand),
		timeIndex: make(map[int64][]*DeferredCommand),
		store:     store,
		stopCh:    make(chan struct{}),
	}
}

// Start begins the background scheduler
func (ds *DeferredScheduler) Start() {
	ds.mu.Lock()
	if ds.running {
		ds.mu.Unlock()
		return
	}
	ds.running = true
	ds.mu.Unlock()

	go ds.schedulerLoop()
}

// Stop stops the background scheduler
func (ds *DeferredScheduler) Stop() {
	ds.mu.Lock()
	if !ds.running {
		ds.mu.Unlock()
		return
	}
	ds.running = false
	ds.mu.Unlock()

	close(ds.stopCh)
}

// AddDeferredCommand schedules a command for future execution
func (ds *DeferredScheduler) AddDeferredCommand(cmd *DeferredCommand) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Store the command
	ds.commands[cmd.ID] = cmd

	// Add to time index (rounded to seconds for efficient grouping)
	timeKey := cmd.ExecuteAt.Unix()
	ds.timeIndex[timeKey] = append(ds.timeIndex[timeKey], cmd)
}

// CancelDeferredCommand cancels a scheduled command
func (ds *DeferredScheduler) CancelDeferredCommand(commandID string) bool {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	cmd, exists := ds.commands[commandID]
	if !exists {
		return false
	}

	// Remove from commands map
	delete(ds.commands, commandID)

	// Remove from time index
	timeKey := cmd.ExecuteAt.Unix()
	if timeSlice, exists := ds.timeIndex[timeKey]; exists {
		// Find and remove the command from the time slice
		for i, c := range timeSlice {
			if c.ID == commandID {
				ds.timeIndex[timeKey] = append(timeSlice[:i], timeSlice[i+1:]...)
				// If time slice is empty, remove it
				if len(ds.timeIndex[timeKey]) == 0 {
					delete(ds.timeIndex, timeKey)
				}
				break
			}
		}
	}

	return true
}

// ListDeferredCommands returns all scheduled commands
func (ds *DeferredScheduler) ListDeferredCommands() []*DeferredCommand {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	commands := make([]*DeferredCommand, 0, len(ds.commands))
	for _, cmd := range ds.commands {
		commands = append(commands, cmd)
	}
	return commands
}

// GetStats returns scheduler statistics
func (ds *DeferredScheduler) GetStats() map[string]interface{} {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	return map[string]interface{}{
		"total_commands":    len(ds.commands),
		"time_buckets":      len(ds.timeIndex),
		"scheduler_running": ds.running,
	}
}

// GetPendingKeys returns a map of keys that have pending deferred commands
func (ds *DeferredScheduler) GetPendingKeys() map[string]bool {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	keys := make(map[string]bool)
	for _, cmd := range ds.commands {
		keys[cmd.Key] = true
	}
	return keys
}

// schedulerLoop is the main loop that executes scheduled commands
func (ds *DeferredScheduler) schedulerLoop() {
	ticker := time.NewTicker(1 * time.Second) // Check every second
	defer ticker.Stop()

	for {
		select {
		case <-ds.stopCh:
			return
		case now := <-ticker.C:
			ds.executeReadyCommands(now)
		}
	}
}

// executeReadyCommands finds and executes all commands that are ready to run
func (ds *DeferredScheduler) executeReadyCommands(now time.Time) {
	currentTime := now.Unix()

	ds.mu.Lock()

	// Find all time keys that are ready to execute
	var readyCommands []*DeferredCommand
	var keysToDelete []int64

	for timeKey, commands := range ds.timeIndex {
		if timeKey <= currentTime {
			readyCommands = append(readyCommands, commands...)
			keysToDelete = append(keysToDelete, timeKey)
		}
	}

	// Remove executed commands from data structures
	for _, timeKey := range keysToDelete {
		delete(ds.timeIndex, timeKey)
	}

	for _, cmd := range readyCommands {
		delete(ds.commands, cmd.ID)
	}

	ds.mu.Unlock()

	// Execute commands outside of lock
	for _, cmd := range readyCommands {
		ds.executeCommand(cmd)
	}
}

// executeCommand executes a single deferred command
func (ds *DeferredScheduler) executeCommand(cmd *DeferredCommand) {
	switch cmd.CommandType {
	case "SET":
		if len(cmd.Args) >= 1 {
			// Process template expansion for deferred commands
			key := expandTemplatesIfNeeded(cmd.Key)
			value := expandTemplatesIfNeeded(cmd.Args[0])
			ds.store.Set(key, value)
		}
	case "DEL":
		key := expandTemplatesIfNeeded(cmd.Key)
		ds.store.Delete(key)
	case "INCR":
		key := expandTemplatesIfNeeded(cmd.Key)
		ds.store.Incr(key)
	case "DECR":
		key := expandTemplatesIfNeeded(cmd.Key)
		ds.store.Decr(key)
	case "INCRBY":
		if len(cmd.Args) >= 1 {
			key := expandTemplatesIfNeeded(cmd.Key)
			value := expandTemplatesIfNeeded(cmd.Args[0])
			if increment := parseInt64Fast(value); increment != 0 {
				ds.store.IncrBy(key, increment)
			}
		}
	case "DECRBY":
		if len(cmd.Args) >= 1 {
			key := expandTemplatesIfNeeded(cmd.Key)
			value := expandTemplatesIfNeeded(cmd.Args[0])
			if decrement := parseInt64Fast(value); decrement != 0 {
				ds.store.DecrBy(key, decrement)
			}
		}
	case "APPEND":
		if len(cmd.Args) >= 1 {
			key := expandTemplatesIfNeeded(cmd.Key)
			value := expandTemplatesIfNeeded(cmd.Args[0])
			ds.store.Append(key, value)
		}
	}
}

// generateDeferredCommandID generates a unique ID for deferred commands
// Uses math/rand for performance (cryptographic security not needed for internal IDs)
func generateDeferredCommandID() string {
	// Use timestamp + random for uniqueness without crypto overhead
	timestamp := time.Now().UnixNano()
	randomPart := rand.Uint64()
	bytes := make([]byte, 16)
	// Encode timestamp in first 8 bytes
	for i := 0; i < 8; i++ {
		bytes[i] = byte(timestamp >> (56 - i*8))
	}
	// Encode random in last 8 bytes
	for i := 0; i < 8; i++ {
		bytes[8+i] = byte(randomPart >> (56 - i*8))
	}
	return hex.EncodeToString(bytes)
}

// expandTemplatesIfNeeded expands template placeholders in strings
func expandTemplatesIfNeeded(s string) string {
	if !strings.Contains(s, "__rand_int__") {
		return s
	}

	// Generate random number for each occurrence
	result := s
	for strings.Contains(result, "__rand_int__") {
		randomNum := rand.Intn(1000000) // Random number 0-999999
		result = strings.Replace(result, "__rand_int__", strconv.Itoa(randomNum), 1)
	}

	return result
}
