package store

import (
	"fmt"
	"strconv"
	"testing"
)

// Benchmark тести для вимірювання продуктивності SET/GET/DEL операцій

func BenchmarkSetFast(b *testing.B) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 1024 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		store.Set(key, value)
	}
}

func BenchmarkGetFast(b *testing.B) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 1024 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	// Заповнюємо store значеннями
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		store.Set(key, value)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i%10000)
		store.Get(key)
	}
}

func BenchmarkDeleteFast(b *testing.B) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 1024 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	// Заповнюємо store значеннями
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		store.Set(key, value)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		store.DeleteFast(key)
	}
}

func BenchmarkSetGetDelete(b *testing.B) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 1024 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)

		// SET
		store.Set(key, value)

		// GET
		store.Get(key)

		// DELETE
		store.DeleteFast(key)
	}
}

func BenchmarkStoredValueCreation(b *testing.B) {
	value := "test_value_for_benchmark"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sv := NewStoredString(value)
		_ = sv.MemoryUsage()
	}
}

func BenchmarkStringTypeCheck(b *testing.B) {
	sv := NewStoredString("test_value")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = sv.IsString()
		str, _ := sv.AsString()
		_ = str
	}
}

// Benchmark порівняння різних способів type checking
func BenchmarkTypeCheckSwitch(b *testing.B) {
	sv := NewStoredString("test_value")
	var value interface{} = sv

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		switch v := value.(type) {
		case *StoredValue:
			if v.Type == StringType {
				_ = v.Data.(string)
			}
		case string:
			_ = v
		}
	}
}

func BenchmarkTypeCheckAssert(b *testing.B) {
	sv := NewStoredString("test_value")
	var value interface{} = sv

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if storedValue, ok := value.(*StoredValue); ok {
			if storedValue.IsString() {
				str, _ := storedValue.AsString()
				_ = str
			}
		}
	}
}

// Benchmark для тестування різних розмірів значень
func BenchmarkSetFastSmallValues(b *testing.B) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 1024 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		value := "small"
		store.Set(key, value)
	}
}

func BenchmarkSetFastMediumValues(b *testing.B) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 1024 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	mediumValue := make([]byte, 1024)
	for i := range mediumValue {
		mediumValue[i] = byte(i % 256)
	}
	value := string(mediumValue)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		store.Set(key, value)
	}
}

func BenchmarkSetFastLargeValues(b *testing.B) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 1024 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	largeValue := make([]byte, 10*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	value := string(largeValue)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := strconv.Itoa(i)
		store.Set(key, value)
	}
}

// Benchmark для паралельних операцій
func BenchmarkSetFastParallel(b *testing.B) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 1024 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i)
			value := fmt.Sprintf("value%d", i)
			store.Set(key, value)
			i++
		}
	})
}

func BenchmarkGetFastParallel(b *testing.B) {
	limits := &Limits{
		MaxKeySize:     1024,
		MaxValueSize:   1024 * 1024,
		MaxMemoryUsage: 1024 * 1024 * 1024,
	}
	store := NewLockFreeStore(limits)

	// Заповнюємо store значеннями
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		store.Set(key, value)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i%10000)
			store.Get(key)
			i++
		}
	})
}
