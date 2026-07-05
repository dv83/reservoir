package errors

import (
	"errors"
	"testing"
	"time"
)

func TestErrorType_String(t *testing.T) {
	tests := []struct {
		errType  ErrorType
		expected string
	}{
		{ErrTypeKeyTooLarge, "KEY_TOO_LARGE"},
		{ErrTypeValueTooLarge, "VALUE_TOO_LARGE"},
		{ErrTypeMemoryLimit, "MEMORY_LIMIT"},
		{ErrTypeKeyNotFound, "KEY_NOT_FOUND"},
		{ErrTypeExpired, "EXPIRED"},
		{ErrTypeConnection, "CONNECTION"},
		{ErrTypeTimeout, "TIMEOUT"},
		{ErrTypeInvalidMessage, "INVALID_MESSAGE"},
		{ErrTypeReplicationFailed, "REPLICATION_FAILED"},
		{ErrTypeClusterDown, "CLUSTER_DOWN"},
		{ErrTypeSyncFailed, "SYNC_FAILED"},
		{ErrTypeInvalidConfig, "INVALID_CONFIG"},
		{ErrTypeInitFailed, "INIT_FAILED"},
		{ErrTypeInternal, "INTERNAL"},
		{ErrTypeNotImplemented, "NOT_IMPLEMENTED"},
		{ErrorType(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if got := tt.errType.String(); got != tt.expected {
				t.Errorf("ErrorType.String() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestReservoirError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      *ReservoirError
		expected string
	}{
		{
			name: "error without cause",
			err: &ReservoirError{
				Type:    ErrTypeKeyTooLarge,
				Message: "key is too large",
			},
			expected: "KEY_TOO_LARGE: key is too large",
		},
		{
			name: "error with cause",
			err: &ReservoirError{
				Type:    ErrTypeConnection,
				Message: "connection failed",
				Cause:   errors.New("network timeout"),
			},
			expected: "CONNECTION: connection failed (caused by: network timeout)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.expected {
				t.Errorf("ReservoirError.Error() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestReservoirError_Unwrap(t *testing.T) {
	cause := errors.New("underlying error")
	err := &ReservoirError{
		Type:    ErrTypeInternal,
		Message: "internal error",
		Cause:   cause,
	}

	if got := err.Unwrap(); got != cause {
		t.Errorf("ReservoirError.Unwrap() = %v, want %v", got, cause)
	}
}

func TestReservoirError_Is(t *testing.T) {
	err1 := &ReservoirError{Type: ErrTypeKeyTooLarge, Message: "key too large"}
	err2 := &ReservoirError{Type: ErrTypeKeyTooLarge, Message: "different message"}
	err3 := &ReservoirError{Type: ErrTypeValueTooLarge, Message: "value too large"}
	otherErr := errors.New("other error")

	if !err1.Is(err2) {
		t.Error("Expected err1.Is(err2) to be true")
	}

	if err1.Is(err3) {
		t.Error("Expected err1.Is(err3) to be false")
	}

	if err1.Is(otherErr) {
		t.Error("Expected err1.Is(otherErr) to be false")
	}
}

func TestReservoirError_WithContext(t *testing.T) {
	err := New(ErrTypeKeyTooLarge, "key too large")

	err.WithContext("key", "testkey")
	err.WithContext("size", 1024)

	value, exists := err.GetContext("key")
	if !exists || value != "testkey" {
		t.Errorf("Expected context key='testkey', got %v (exists: %v)", value, exists)
	}

	size, exists := err.GetContext("size")
	if !exists || size != 1024 {
		t.Errorf("Expected context size=1024, got %v (exists: %v)", size, exists)
	}

	_, exists = err.GetContext("nonexistent")
	if exists {
		t.Error("Expected nonexistent key to not exist in context")
	}
}

func TestNew(t *testing.T) {
	err := New(ErrTypeKeyTooLarge, "test message")

	if err.Type != ErrTypeKeyTooLarge {
		t.Errorf("Expected type %v, got %v", ErrTypeKeyTooLarge, err.Type)
	}

	if err.Message != "test message" {
		t.Errorf("Expected message 'test message', got '%s'", err.Message)
	}

	if err.Cause != nil {
		t.Errorf("Expected nil cause, got %v", err.Cause)
	}

	if err.Timestamp.IsZero() {
		t.Error("Expected timestamp to be set")
	}

	if err.Location == "" {
		t.Error("Expected location to be set")
	}
}

func TestWrap(t *testing.T) {
	cause := errors.New("underlying error")
	err := Wrap(ErrTypeConnection, "connection failed", cause)

	if err.Type != ErrTypeConnection {
		t.Errorf("Expected type %v, got %v", ErrTypeConnection, err.Type)
	}

	if err.Message != "connection failed" {
		t.Errorf("Expected message 'connection failed', got '%s'", err.Message)
	}

	if err.Cause != cause {
		t.Errorf("Expected cause %v, got %v", cause, err.Cause)
	}

	if err.Timestamp.IsZero() {
		t.Error("Expected timestamp to be set")
	}

	if err.Location == "" {
		t.Error("Expected location to be set")
	}
}

func TestPredefinedErrorConstructors(t *testing.T) {
	tests := []struct {
		name         string
		constructor  func() *ReservoirError
		expectedType ErrorType
	}{
		{
			name:         "NewKeyTooLarge",
			constructor:  func() *ReservoirError { return NewKeyTooLarge(100, 50) },
			expectedType: ErrTypeKeyTooLarge,
		},
		{
			name:         "NewValueTooLarge",
			constructor:  func() *ReservoirError { return NewValueTooLarge(1000, 500) },
			expectedType: ErrTypeValueTooLarge,
		},
		{
			name:         "NewMemoryLimit",
			constructor:  func() *ReservoirError { return NewMemoryLimit(2000, 1000) },
			expectedType: ErrTypeMemoryLimit,
		},
		{
			name:         "NewKeyNotFound",
			constructor:  func() *ReservoirError { return NewKeyNotFound("testkey") },
			expectedType: ErrTypeKeyNotFound,
		},
		{
			name:         "NewExpired",
			constructor:  func() *ReservoirError { return NewExpired("testkey") },
			expectedType: ErrTypeExpired,
		},
		{
			name:         "NewConnectionError",
			constructor:  func() *ReservoirError { return NewConnectionError("localhost:6379", errors.New("timeout")) },
			expectedType: ErrTypeConnection,
		},
		{
			name:         "NewTimeoutError",
			constructor:  func() *ReservoirError { return NewTimeoutError("GET", 5*time.Second) },
			expectedType: ErrTypeTimeout,
		},
		{
			name:         "NewInvalidMessage",
			constructor:  func() *ReservoirError { return NewInvalidMessage("malformed") },
			expectedType: ErrTypeInvalidMessage,
		},
		{
			name:         "NewReplicationFailed",
			constructor:  func() *ReservoirError { return NewReplicationFailed("node1", errors.New("network")) },
			expectedType: ErrTypeReplicationFailed,
		},
		{
			name:         "NewClusterDown",
			constructor:  func() *ReservoirError { return NewClusterDown("no majority") },
			expectedType: ErrTypeClusterDown,
		},
		{
			name:         "NewSyncFailed",
			constructor:  func() *ReservoirError { return NewSyncFailed("merkle mismatch", errors.New("hash error")) },
			expectedType: ErrTypeSyncFailed,
		},
		{
			name:         "NewInvalidConfig",
			constructor:  func() *ReservoirError { return NewInvalidConfig("port", "must be positive") },
			expectedType: ErrTypeInvalidConfig,
		},
		{
			name:         "NewInitFailed",
			constructor:  func() *ReservoirError { return NewInitFailed("store", errors.New("memory allocation")) },
			expectedType: ErrTypeInitFailed,
		},
		{
			name:         "NewInternal",
			constructor:  func() *ReservoirError { return NewInternal("unexpected state") },
			expectedType: ErrTypeInternal,
		},
		{
			name:         "NewNotImplemented",
			constructor:  func() *ReservoirError { return NewNotImplemented("feature X") },
			expectedType: ErrTypeNotImplemented,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.constructor()
			if err.Type != tt.expectedType {
				t.Errorf("Expected type %v, got %v", tt.expectedType, err.Type)
			}
			if err.Message == "" {
				t.Error("Expected non-empty message")
			}
		})
	}
}

func TestHelperFunctions(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		checker  func(error) bool
		expected bool
	}{
		{
			name:     "IsKeyTooLarge with correct error",
			err:      NewKeyTooLarge(100, 50),
			checker:  IsKeyTooLarge,
			expected: true,
		},
		{
			name:     "IsKeyTooLarge with wrong error",
			err:      NewValueTooLarge(100, 50),
			checker:  IsKeyTooLarge,
			expected: false,
		},
		{
			name:     "IsValueTooLarge with correct error",
			err:      NewValueTooLarge(100, 50),
			checker:  IsValueTooLarge,
			expected: true,
		},
		{
			name:     "IsMemoryLimit with correct error",
			err:      NewMemoryLimit(2000, 1000),
			checker:  IsMemoryLimit,
			expected: true,
		},
		{
			name:     "IsKeyNotFound with correct error",
			err:      NewKeyNotFound("test"),
			checker:  IsKeyNotFound,
			expected: true,
		},
		{
			name:     "IsExpired with correct error",
			err:      NewExpired("test"),
			checker:  IsExpired,
			expected: true,
		},
		{
			name:     "IsConnectionError with correct error",
			err:      NewConnectionError("localhost", errors.New("timeout")),
			checker:  IsConnectionError,
			expected: true,
		},
		{
			name:     "IsTimeoutError with correct error",
			err:      NewTimeoutError("GET", 5*time.Second),
			checker:  IsTimeoutError,
			expected: true,
		},
		{
			name:     "IsReplicationError with correct error",
			err:      NewReplicationFailed("node1", errors.New("network")),
			checker:  IsReplicationError,
			expected: true,
		},
		{
			name:     "IsClusterDown with correct error",
			err:      NewClusterDown("no majority"),
			checker:  IsClusterDown,
			expected: true,
		},
		{
			name:     "IsConfigError with correct error",
			err:      NewInvalidConfig("port", "invalid"),
			checker:  IsConfigError,
			expected: true,
		},
		{
			name:     "Helper with standard error",
			err:      errors.New("standard error"),
			checker:  IsKeyTooLarge,
			expected: false,
		},
		{
			name:     "Helper with nil error",
			err:      nil,
			checker:  IsKeyTooLarge,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.checker(tt.err); got != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, got)
			}
		})
	}
}

func TestErrorsAsReservoirError(t *testing.T) {
	// Test that errors.As works correctly
	err := NewKeyTooLarge(100, 50)

	var re *ReservoirError
	if !errors.As(err, &re) {
		t.Error("Expected errors.As to return true")
	}

	if re.Type != ErrTypeKeyTooLarge {
		t.Errorf("Expected type %v, got %v", ErrTypeKeyTooLarge, re.Type)
	}
}

func TestErrorsIsReservoirError(t *testing.T) {
	// Test that errors.Is works correctly
	err1 := NewKeyTooLarge(100, 50)
	err2 := NewKeyTooLarge(200, 50)

	if !errors.Is(err1, err2) {
		t.Error("Expected errors.Is to return true for same error types")
	}

	err3 := NewValueTooLarge(100, 50)
	if errors.Is(err1, err3) {
		t.Error("Expected errors.Is to return false for different error types")
	}
}
