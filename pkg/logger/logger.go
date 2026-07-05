package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// LogLevel represents the severity of a log message
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
)

var (
	currentLevel LogLevel = INFO
	mu           sync.RWMutex
	logger       *log.Logger
	nodeName     string
)

func init() {
	// Create a custom logger with microsecond precision
	logger = log.New(os.Stdout, "", 0) // No flags, we'll format manually
}

// SetNodeName sets the node name for logging
func SetNodeName(name string) {
	mu.Lock()
	defer mu.Unlock()
	nodeName = name
}

// SetLevelFromString sets the log level from a string
func SetLevelFromString(level string) error {
	mu.Lock()
	defer mu.Unlock()

	switch strings.ToLower(level) {
	case "debug":
		currentLevel = DEBUG
	case "info":
		currentLevel = INFO
	case "warning", "warn":
		currentLevel = WARNING
	case "error":
		currentLevel = ERROR
	case "quiet":
		currentLevel = WARNING // In quiet mode, show only warnings and errors
	default:
		return fmt.Errorf("invalid log level: %s", level)
	}
	return nil
}

// shouldLog checks if a message with the given level should be logged
func shouldLog(level LogLevel) bool {
	mu.RLock()
	defer mu.RUnlock()
	return level >= currentLevel
}

// formatTimestamp formats the current time with microsecond precision
func formatTimestamp() string {
	return time.Now().Format("2006/01/02 15:04:05.000000")
}

// logWithLevel logs a message with the specified level
func logWithLevel(level string, format string, v ...interface{}) {
	mu.RLock()
	currentNodeName := nodeName
	mu.RUnlock()

	timestamp := formatTimestamp()
	var prefix string
	if currentNodeName != "" {
		prefix = fmt.Sprintf("%s [%s] [%s] ", timestamp, currentNodeName, level)
	} else {
		prefix = fmt.Sprintf("%s [%s] ", timestamp, level)
	}

	message := fmt.Sprintf(format, v...)
	logger.Printf("%s%s", prefix, message)
}

// Debug logs a debug message
func Debug(format string, v ...interface{}) {
	if shouldLog(DEBUG) {
		logWithLevel("DEBUG", format, v...)
	}
}

// Info logs an info message
func Info(format string, v ...interface{}) {
	if shouldLog(INFO) {
		logWithLevel("INFO", format, v...)
	}
}

// Warning logs a warning message
func Warning(format string, v ...interface{}) {
	if shouldLog(WARNING) {
		logWithLevel("WARNING", format, v...)
	}
}

// Error logs an error message
func Error(format string, v ...interface{}) {
	if shouldLog(ERROR) {
		logWithLevel("ERROR", format, v...)
	}
}
