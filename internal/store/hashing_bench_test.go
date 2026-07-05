package store

import (
	"hash/maphash"
	"testing"
)

// Standard Go hash for comparison
func StandardHash(key string) uint64 {
	var h maphash.Hash
	h.WriteString(key)
	return h.Sum64()
}

func BenchmarkFastHash_Small(b *testing.B) {
	key := "user:1000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FastHash(key)
	}
}

func BenchmarkStandardHash_Small(b *testing.B) {
	key := "user:1000"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		StandardHash(key)
	}
}

func BenchmarkFastHash_Large(b *testing.B) {
	key := make([]byte, 1024)
	for i := range key {
		key[i] = 'a'
	}
	sKey := string(key)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FastHash(sKey)
	}
}

func BenchmarkStandardHash_Large(b *testing.B) {
	key := make([]byte, 1024)
	for i := range key {
		key[i] = 'a'
	}
	sKey := string(key)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		StandardHash(sKey)
	}
}
