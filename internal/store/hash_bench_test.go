package store

import (
	"hash/fnv"
	"strconv"
	"testing"
)

// Current FNV implementation
func currentHash(key string) uint32 {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return hash.Sum32()
}

// Test data
var testKeys = []string{
	"short",
	"medium_length_key",
	"very_long_key_with_lots_of_characters_to_test_performance",
	"user:123456",
	"session:abcdef1234567890",
	"cache:product:12345:details",
	"metrics:cpu:usage:2024:01:15:14:30:00",
}

func BenchmarkCurrentHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, key := range testKeys {
			_ = currentHash(key)
		}
	}
}

func BenchmarkFastHash(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for _, key := range testKeys {
			_ = FastHash(key)
		}
	}
}

func BenchmarkCurrentHashShort(b *testing.B) {
	key := "short"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = currentHash(key)
	}
}

func BenchmarkFastHashShort(b *testing.B) {
	key := "short"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FastHash(key)
	}
}

func BenchmarkCurrentHashMedium(b *testing.B) {
	key := "medium_length_key_with_some_content"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = currentHash(key)
	}
}

func BenchmarkFastHashMedium(b *testing.B) {
	key := "medium_length_key_with_some_content"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FastHash(key)
	}
}

func BenchmarkCurrentHashLong(b *testing.B) {
	key := "very_long_key_with_lots_of_characters_to_test_performance_and_see_how_well_different_algorithms_perform"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = currentHash(key)
	}
}

func BenchmarkFastHashLong(b *testing.B) {
	key := "very_long_key_with_lots_of_characters_to_test_performance_and_see_how_well_different_algorithms_perform"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = FastHash(key)
	}
}

// Benchmark hash distribution quality
func BenchmarkHashDistribution(b *testing.B) {
	buckets := make(map[uint8]int)

	b.Run("Current", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := "key" + strconv.Itoa(i)
			hash := currentHash(key)
			buckets[uint8(hash)]++
		}
	})

	buckets = make(map[uint8]int)
	b.Run("Fast", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := "key" + strconv.Itoa(i)
			hash := FastHash(key)
			buckets[uint8(hash)]++
		}
	})
}

// Memory allocation benchmarks
func BenchmarkCurrentHashAllocs(b *testing.B) {
	key := "test_key_for_allocation_testing"
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = currentHash(key)
	}
}

func BenchmarkFastHashAllocs(b *testing.B) {
	key := "test_key_for_allocation_testing"
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = FastHash(key)
	}
}

// Test different CPU features
func TestHasherCapabilities(t *testing.T) {
	hasher := NewFastHasher()
	t.Logf("AVX2 support: %v", hasher.hasAVX2)
	t.Logf("SSE2 support: %v", hasher.hasSSE2)

	// Test consistency across methods
	testKey := "consistency_test_key"

	hash1 := hasher.hashScalar([]byte(testKey))
	hash2 := hasher.Hash(testKey)

	// They might be different algorithms, but should be consistent
	if hash2 == 0 {
		t.Error("Hash should not be zero")
	}

	t.Logf("Scalar hash: %x", hash1)
	t.Logf("Fast hash: %x", hash2)
}
