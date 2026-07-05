//go:build !amd64

package store

import (
	"unsafe"
)

// FNV-1a constants
const (
	fnvOffsetBasis32 = 2166136261
	fnvPrime32       = 16777619
)

// FastHash implements inline FNV-1a hash without allocations
func FastHash(key string) uint32 {
	// Convert string to []byte without allocation using unsafe
	keyBytes := unsafe.Slice(unsafe.StringData(key), len(key))

	// Optimized FNV-1a hash implementation
	hash := uint32(fnvOffsetBasis32)
	for _, b := range keyBytes {
		hash ^= uint32(b)
		hash *= fnvPrime32
	}
	return hash
}

// ShardIndex returns shard index (0-255) for a key
func ShardIndex(key string) uint8 {
	return uint8(FastHash(key))
}
