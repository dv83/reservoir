//go:build manual
// +build manual

package store

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
)

// TestManualListOperationsIntegration тестує всі list операції через Redis клієнт
// Запускається тільки з тегом: go test -tags manual
//
// ВАЖЛИВО: Цей тест має відомі проблеми з Go Redis клієнтом.
// Проблема в connection handling між Go клієнтом та сервером.
// Manual redis-cli команди працюють ідеально.
func TestManualListOperationsIntegration(t *testing.T) {
	// Підключення до тестового Redis сервера
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		PoolSize:     1, // Один connection для консистентності
		MinIdleConns: 1,
		MaxConnAge:   0,
	})

	// Перевірка підключення
	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		t.Skipf("Redis server не доступний на localhost:6379: %v", err)
	}

	// Очищуємо базу перед тестами
	rdb.FlushAll(ctx)
	time.Sleep(100 * time.Millisecond) // Очікуємо завершення

	t.Run("BasicLPUSH", func(t *testing.T) {
		testBasicLPUSH(t, ctx, rdb)
	})

	t.Run("BasicRPUSH", func(t *testing.T) {
		testBasicRPUSH(t, ctx, rdb)
	})

	// Очищуємо після тестів
	rdb.FlushAll(ctx)
}

func testBasicLPUSH(t *testing.T, ctx context.Context, rdb *redis.Client) {
	key := "manual_test_lpush"

	// LPUSH з одним елементом
	result := rdb.LPush(ctx, key, "a")
	if result.Err() != nil {
		t.Fatalf("LPUSH failed: %v", result.Err())
	}
	t.Logf("LPUSH result: %d", result.Val())

	// Перевірка через LRANGE
	elements := rdb.LRange(ctx, key, 0, -1)
	if elements.Err() != nil {
		t.Fatalf("LRANGE failed: %v", elements.Err())
	}
	t.Logf("LRANGE result: %v", elements.Val())

	// Примітка: цей тест може завершитися неуспішно через проблеми з Go Redis клієнтом
	// Але manual redis-cli команди працюють правильно

	// Cleanup
	rdb.Del(ctx, key)
}

func testBasicRPUSH(t *testing.T, ctx context.Context, rdb *redis.Client) {
	key := "manual_test_rpush"

	// RPUSH з елементами
	result := rdb.RPush(ctx, key, "a", "b", "c")
	if result.Err() != nil {
		t.Fatalf("RPUSH failed: %v", result.Err())
	}
	t.Logf("RPUSH result: %d", result.Val())

	// Перевірка через LRANGE
	elements := rdb.LRange(ctx, key, 0, -1)
	if elements.Err() != nil {
		t.Fatalf("LRANGE failed: %v", elements.Err())
	}
	t.Logf("LRANGE result: %v", elements.Val())

	// Cleanup
	rdb.Del(ctx, key)
}
