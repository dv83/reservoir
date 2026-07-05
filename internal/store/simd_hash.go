package store

import (
	"hash/fnv"
	"unsafe"
)

// SIMD capabilities detection
var (
	hasAVX512 bool = false // Будемо detect runtime
	hasAVX2   bool = false
	hasSSE42  bool = false
)

// init детектує SIMD capabilities
func init() {
	// В production треба використати CPUID для детекції
	// Поки що використовуємо fallback реалізації
	hasSSE42 = true   // Більшість сучасних CPU мають SSE4.2
	hasAVX2 = false   // Консервативний підхід
	hasAVX512 = false // Дуже мало CPU підтримують
}

// SIMDHash обчислює хеш з використанням найкращої доступної SIMD інструкції
func SIMDHash(data []byte) uint64 {
	switch {
	case hasAVX512:
		return avx512Hash(data)
	case hasAVX2:
		return avx2Hash(data)
	case hasSSE42:
		return sse42Hash(data)
	default:
		return fallbackHash(data)
	}
}

// SIMDHashString оптимізована версія для strings
func SIMDHashString(s string) uint64 {
	// Zero-copy перетворення string -> []byte
	return SIMDHash(stringToBytes(s))
}

// stringToBytes перетворює string в []byte без копіювання
func stringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&struct {
		string
		Cap int
	}{s, len(s)}))
}

// bytesToString перетворює []byte в string без копіювання
func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// avx512Hash - реалізація з AVX-512 (симуляція)
func avx512Hash(data []byte) uint64 {
	// В реальній реалізації тут були б AVX-512 інструкції
	// Поки що використовуємо оптимізований fallback

	if len(data) == 0 {
		return 0
	}

	// Симулюємо 64-byte parallel processing
	const chunkSize = 64
	var hash uint64 = 14695981039346656037 // FNV offset basis

	// Обробляємо по 64 байти за раз
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		// Симулюємо векторні операції
		chunk := data[i:end]
		hash = fnvHashChunk(hash, chunk)
	}

	return hash
}

// avx2Hash - реалізація з AVX2 (симуляція)
func avx2Hash(data []byte) uint64 {
	// Симулюємо 32-byte parallel processing
	const chunkSize = 32
	var hash uint64 = 14695981039346656037

	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := data[i:end]
		hash = fnvHashChunk(hash, chunk)
	}

	return hash
}

// sse42Hash - реалізація з SSE4.2 (симуляція)
func sse42Hash(data []byte) uint64 {
	// Симулюємо 16-byte parallel processing
	const chunkSize = 16
	var hash uint64 = 14695981039346656037

	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}

		chunk := data[i:end]
		hash = fnvHashChunk(hash, chunk)
	}

	return hash
}

// fallbackHash - стандартна реалізація без SIMD
func fallbackHash(data []byte) uint64 {
	h := fnv.New64a()
	h.Write(data)
	return h.Sum64()
}

// fnvHashChunk обчислює FNV hash для chunk
func fnvHashChunk(hash uint64, chunk []byte) uint64 {
	const fnvPrime = 1099511628211

	for _, b := range chunk {
		hash ^= uint64(b)
		hash *= fnvPrime
	}

	return hash
}

// FastStringHash - швидкий хеш для коротких рядків (ключі Redis)
func FastStringHash(s string) uint32 {
	if len(s) == 0 {
		return 0
	}

	// Для коротких рядків використовуємо простий хеш
	if len(s) <= 16 {
		return shortStringHash(s)
	}

	// Для довгих рядків використовуємо SIMD
	return uint32(SIMDHashString(s))
}

// shortStringHash оптимізований хеш для коротких рядків
func shortStringHash(s string) uint32 {
	var hash uint32 = 2166136261 // FNV-1a offset basis
	const prime = 16777619       // FNV-1a prime

	// Unroll loop для коротких рядків
	bytes := stringToBytes(s)
	for _, b := range bytes {
		hash ^= uint32(b)
		hash *= prime
	}

	return hash
}

// XXHash3 - симуляція сучасного швидкого хешу
func XXHash3(data []byte) uint64 {
	if len(data) == 0 {
		return 0
	}

	// Константи для xxHash3
	const (
		prime64_1 = 11400714785074694791
		prime64_2 = 14029467366897019727
		prime64_3 = 1609587929392839161
		prime64_4 = 9650029242287828579
		prime64_5 = 2870177450012600261
	)

	var hash uint64 = prime64_5

	// Обробляємо по 8 байт
	for len(data) >= 8 {
		k1 := *(*uint64)(unsafe.Pointer(&data[0]))
		k1 *= prime64_2
		k1 = rotateLeft64(k1, 31)
		k1 *= prime64_1
		hash ^= k1
		hash = rotateLeft64(hash, 27)*prime64_1 + prime64_4
		data = data[8:]
	}

	// Обробляємо залишок
	for _, b := range data {
		hash ^= uint64(b) * prime64_5
		hash = rotateLeft64(hash, 11) * prime64_1
	}

	// Фінальний mix
	hash ^= hash >> 33
	hash *= prime64_2
	hash ^= hash >> 29
	hash *= prime64_3
	hash ^= hash >> 32

	return hash
}

// rotateLeft64 виконує лівий циклічний зсув
func rotateLeft64(x uint64, k uint) uint64 {
	return (x << k) | (x >> (64 - k))
}

// GetHashMethodStats повертає інформацію про використовувані hash методи
func GetHashMethodStats() map[string]bool {
	return map[string]bool{
		"avx512": hasAVX512,
		"avx2":   hasAVX2,
		"sse42":  hasSSE42,
	}
}

// BenchmarkHash для порівняння різних hash функцій
func BenchmarkHash(data []byte) map[string]uint64 {
	return map[string]uint64{
		"simd":     SIMDHash(data),
		"fallback": fallbackHash(data),
		"xxhash3":  XXHash3(data),
	}
}
