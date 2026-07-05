//go:build amd64

package store

import (
	"golang.org/x/sys/cpu"
	"unsafe"
)

// FastHasher provides SIMD-optimized hashing
type FastHasher struct {
	hasAVX2 bool
	hasSSE2 bool
}

// NewFastHasher creates optimized hasher based on CPU capabilities
func NewFastHasher() *FastHasher {
	return &FastHasher{
		hasAVX2: cpu.X86.HasAVX2,
		hasSSE2: cpu.X86.HasSSE2,
	}
}

// Hash computes hash using fastest available method
func (h *FastHasher) Hash(key string) uint32 {
	data := unsafe.Slice(unsafe.StringData(key), len(key))

	if len(data) >= 32 && h.hasAVX2 {
		return h.hashAVX2(data)
	}
	if len(data) >= 16 && h.hasSSE2 {
		return h.hashSSE2(data)
	}
	return h.hashScalar(data)
}

// hashScalar - fallback implementation for small keys or old CPUs
func (h *FastHasher) hashScalar(data []byte) uint32 {
	const (
		prime32 = uint32(0x9E3779B1)
		seed32  = uint32(0x165667B1)
	)

	hash := seed32

	// Process 4 bytes at a time
	for len(data) >= 4 {
		val := *(*uint32)(unsafe.Pointer(&data[0]))
		hash = ((hash << 13) | (hash >> 19)) + val*prime32
		data = data[4:]
	}

	// Process remaining bytes
	for _, b := range data {
		hash = ((hash << 5) | (hash >> 27)) + uint32(b)*prime32
	}

	// Final avalanche
	hash ^= hash >> 16
	hash *= 0x85EBCA6B
	hash ^= hash >> 13
	hash *= 0xC2B2AE35
	hash ^= hash >> 16

	return hash
}

// hashSSE2 - 128-bit SIMD processing
func (h *FastHasher) hashSSE2(data []byte) uint32 {
	// This would use SSE2 intrinsics for 16-byte processing
	// For now, use scalar with 8-byte chunks
	const (
		prime64 = 0x9E3779B185EBCA87
		seed64  = 0x165667B19E3779F9
	)

	hash := uint64(seed64)

	// Process 8 bytes at a time (simulating SSE2)
	for len(data) >= 8 {
		val := *(*uint64)(unsafe.Pointer(&data[0]))
		hash = ((hash << 31) | (hash >> 33)) + val*prime64
		data = data[8:]
	}

	// Process remaining bytes with scalar method
	for _, b := range data {
		hash = ((hash << 7) | (hash >> 57)) + uint64(b)*prime64
	}

	// Mix down to 32-bit
	return uint32(hash ^ (hash >> 32))
}

// hashAVX2 - 256-bit SIMD processing
func (h *FastHasher) hashAVX2(data []byte) uint32 {
	// This would use AVX2 intrinsics for 32-byte processing
	// For now, use scalar with 16-byte chunks
	const (
		prime1 = 0x9E3779B185EBCA87
		prime2 = 0xC2B2AE3D27D4EB4F
		seed   = 0x165667B19E3779F9
	)

	hash1 := uint64(seed)
	hash2 := uint64(seed + prime1)

	// Process 16 bytes at a time (simulating AVX2)
	for len(data) >= 16 {
		val1 := *(*uint64)(unsafe.Pointer(&data[0]))
		val2 := *(*uint64)(unsafe.Pointer(&data[8]))

		hash1 = ((hash1 << 31) | (hash1 >> 33)) + val1*prime1
		hash2 = ((hash2 << 31) | (hash2 >> 33)) + val2*prime2

		data = data[16:]
	}

	// Combine hashes
	hash := hash1 ^ hash2

	// Process remaining bytes
	for _, b := range data {
		hash = ((hash << 7) | (hash >> 57)) + uint64(b)*prime1
	}

	// Final mix
	return uint32(hash ^ (hash >> 32))
}

// Global hasher instance
var globalHasher = NewFastHasher()

// FastHash is the public API for fast hashing
func FastHash(key string) uint32 {
	return globalHasher.Hash(key)
}

// ShardIndex returns shard index (0-255) for a key
func ShardIndex(key string) uint8 {
	return uint8(FastHash(key))
}
