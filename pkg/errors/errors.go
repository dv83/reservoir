package errors

import (
	"errors"
	"fmt"
	"runtime"
	"time"
)

// ErrorType represents the type of error
type ErrorType int

const (
	// Storage errors
	ErrTypeKeyTooLarge ErrorType = iota
	ErrTypeValueTooLarge
	ErrTypeMemoryLimit
	ErrTypeKeyNotFound
	ErrTypeExpired

	// Network errors
	ErrTypeConnection
	ErrTypeTimeout
	ErrTypeInvalidMessage

	// Replication errors
	ErrTypeReplicationFailed
	ErrTypeClusterDown
	ErrTypeSyncFailed

	// Configuration errors
	ErrTypeInvalidConfig
	ErrTypeInitFailed

	// Internal errors
	ErrTypeInternal
	ErrTypeNotImplemented

	// Redis protocol errors
	ErrTypeWrongType
	ErrTypeNotInteger
	ErrTypeNotFloat
	ErrTypeIndexOutOfRange
	ErrTypeSyntaxError
	ErrTypeIncrDecrOverflow
)

// String returns the string representation of the error type
func (et ErrorType) String() string {
	switch et {
	case ErrTypeKeyTooLarge:
		return "KEY_TOO_LARGE"
	case ErrTypeValueTooLarge:
		return "VALUE_TOO_LARGE"
	case ErrTypeMemoryLimit:
		return "MEMORY_LIMIT"
	case ErrTypeKeyNotFound:
		return "KEY_NOT_FOUND"
	case ErrTypeExpired:
		return "EXPIRED"
	case ErrTypeConnection:
		return "CONNECTION"
	case ErrTypeTimeout:
		return "TIMEOUT"
	case ErrTypeInvalidMessage:
		return "INVALID_MESSAGE"
	case ErrTypeReplicationFailed:
		return "REPLICATION_FAILED"
	case ErrTypeClusterDown:
		return "CLUSTER_DOWN"
	case ErrTypeSyncFailed:
		return "SYNC_FAILED"
	case ErrTypeInvalidConfig:
		return "INVALID_CONFIG"
	case ErrTypeInitFailed:
		return "INIT_FAILED"
	case ErrTypeInternal:
		return "INTERNAL"
	case ErrTypeNotImplemented:
		return "NOT_IMPLEMENTED"
	case ErrTypeWrongType:
		return "WRONG_TYPE"
	case ErrTypeNotInteger:
		return "NOT_INTEGER"
	case ErrTypeNotFloat:
		return "NOT_FLOAT"
	case ErrTypeIndexOutOfRange:
		return "INDEX_OUT_OF_RANGE"
	case ErrTypeSyntaxError:
		return "SYNTAX_ERROR"
	case ErrTypeIncrDecrOverflow:
		return "INCR_DECR_OVERFLOW"
	default:
		return "UNKNOWN"
	}
}

// ReservoirError represents a structured error with context
type ReservoirError struct {
	Type      ErrorType
	Message   string
	Cause     error
	Context   map[string]interface{}
	Timestamp time.Time
	Location  string
}

// Error implements the error interface
func (re *ReservoirError) Error() string {
	// Redis protocol errors are surfaced verbatim to clients, so their Error()
	// returns just the bare message (which for WRONGTYPE already embeds its own
	// error code). Internal storage/network errors keep the structured
	// "TYPE: message" form for logs.
	switch re.Type {
	case ErrTypeWrongType, ErrTypeNotInteger, ErrTypeNotFloat,
		ErrTypeIndexOutOfRange, ErrTypeSyntaxError, ErrTypeIncrDecrOverflow:
		return re.Message
	}
	if re.Cause != nil {
		return fmt.Sprintf("%s: %s (caused by: %s)", re.Type, re.Message, re.Cause.Error())
	}
	return fmt.Sprintf("%s: %s", re.Type, re.Message)
}

// Unwrap returns the underlying error
func (re *ReservoirError) Unwrap() error {
	return re.Cause
}

// Is checks if the error is of a specific type
func (re *ReservoirError) Is(target error) bool {
	if t, ok := target.(*ReservoirError); ok {
		return re.Type == t.Type
	}
	return false
}

// WithContext adds context to the error
func (re *ReservoirError) WithContext(key string, value interface{}) *ReservoirError {
	if re.Context == nil {
		re.Context = make(map[string]interface{})
	}
	re.Context[key] = value
	return re
}

// GetContext retrieves context value
func (re *ReservoirError) GetContext(key string) (interface{}, bool) {
	if re.Context == nil {
		return nil, false
	}
	value, exists := re.Context[key]
	return value, exists
}

// New creates a new ReservoirError
func New(errType ErrorType, message string) *ReservoirError {
	_, file, line, _ := runtime.Caller(1)
	return &ReservoirError{
		Type:      errType,
		Message:   message,
		Timestamp: time.Now(),
		Location:  fmt.Sprintf("%s:%d", file, line),
	}
}

// Wrap wraps an existing error with ReservoirError
func Wrap(errType ErrorType, message string, cause error) *ReservoirError {
	_, file, line, _ := runtime.Caller(1)
	return &ReservoirError{
		Type:      errType,
		Message:   message,
		Cause:     cause,
		Timestamp: time.Now(),
		Location:  fmt.Sprintf("%s:%d", file, line),
	}
}

// Predefined error constructors for common cases

// Storage errors
func NewKeyTooLarge(size, maxSize int64) *ReservoirError {
	return New(ErrTypeKeyTooLarge, fmt.Sprintf("key too large: %d bytes (max %d)", size, maxSize))
}

func NewValueTooLarge(size, maxSize int64) *ReservoirError {
	return New(ErrTypeValueTooLarge, fmt.Sprintf("value too large: %d bytes (max %d)", size, maxSize))
}

func NewMemoryLimit(current, max int64) *ReservoirError {
	return New(ErrTypeMemoryLimit, fmt.Sprintf("memory limit exceeded: %d bytes (max %d)", current, max))
}

func NewKeyNotFound(key string) *ReservoirError {
	return New(ErrTypeKeyNotFound, fmt.Sprintf("key not found: %s", key))
}

func NewExpired(key string) *ReservoirError {
	return New(ErrTypeExpired, fmt.Sprintf("key expired: %s", key))
}

// Network errors
func NewConnectionError(addr string, cause error) *ReservoirError {
	return Wrap(ErrTypeConnection, fmt.Sprintf("connection failed to %s", addr), cause)
}

func NewTimeoutError(operation string, timeout time.Duration) *ReservoirError {
	return New(ErrTypeTimeout, fmt.Sprintf("operation %s timed out after %v", operation, timeout))
}

func NewInvalidMessage(reason string) *ReservoirError {
	return New(ErrTypeInvalidMessage, fmt.Sprintf("invalid message: %s", reason))
}

// Replication errors
func NewReplicationFailed(nodeID string, cause error) *ReservoirError {
	return Wrap(ErrTypeReplicationFailed, fmt.Sprintf("replication failed to node %s", nodeID), cause)
}

func NewClusterDown(reason string) *ReservoirError {
	return New(ErrTypeClusterDown, fmt.Sprintf("cluster down: %s", reason))
}

func NewSyncFailed(reason string, cause error) *ReservoirError {
	return Wrap(ErrTypeSyncFailed, fmt.Sprintf("sync failed: %s", reason), cause)
}

// Configuration errors
func NewInvalidConfig(field string, reason string) *ReservoirError {
	return New(ErrTypeInvalidConfig, fmt.Sprintf("invalid config field %s: %s", field, reason))
}

func NewInitFailed(component string, cause error) *ReservoirError {
	return Wrap(ErrTypeInitFailed, fmt.Sprintf("initialization failed for %s", component), cause)
}

// Internal errors
func NewInternal(message string) *ReservoirError {
	return New(ErrTypeInternal, message)
}

func NewNotImplemented(feature string) *ReservoirError {
	return New(ErrTypeNotImplemented, fmt.Sprintf("feature not implemented: %s", feature))
}

// Helper functions for error checking
func IsKeyTooLarge(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeKeyTooLarge
}

func IsValueTooLarge(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeValueTooLarge
}

func IsMemoryLimit(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeMemoryLimit
}

func IsKeyNotFound(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeKeyNotFound
}

func IsExpired(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeExpired
}

func IsConnectionError(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeConnection
}

func IsTimeoutError(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeTimeout
}

func IsReplicationError(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeReplicationFailed
}

func IsClusterDown(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeClusterDown
}

func IsConfigError(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeInvalidConfig
}

// Redis protocol errors - predefined instances for common use
var (
	ErrWrongType        = New(ErrTypeWrongType, "WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNotInteger       = New(ErrTypeNotInteger, "value is not an integer or out of range")
	ErrNotFloat         = New(ErrTypeNotFloat, "value is not a valid float")
	ErrIndexOutOfRange  = New(ErrTypeIndexOutOfRange, "index out of range")
	ErrSyntaxError      = New(ErrTypeSyntaxError, "syntax error")
	ErrIncrDecrOverflow = New(ErrTypeIncrDecrOverflow, "increment or decrement would overflow")
)

// Redis error constructors
func NewWrongType() *ReservoirError {
	return New(ErrTypeWrongType, "WRONGTYPE Operation against a key holding the wrong kind of value")
}

func NewNotInteger() *ReservoirError {
	return New(ErrTypeNotInteger, "value is not an integer or out of range")
}

func NewNotFloat() *ReservoirError {
	return New(ErrTypeNotFloat, "value is not a valid float")
}

func NewIndexOutOfRange() *ReservoirError {
	return New(ErrTypeIndexOutOfRange, "index out of range")
}

// Redis error type checkers
func IsWrongType(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeWrongType
}

func IsNotInteger(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeNotInteger
}

func IsIndexOutOfRange(err error) bool {
	var re *ReservoirError
	return errors.As(err, &re) && re.Type == ErrTypeIndexOutOfRange
}
