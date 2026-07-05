package store

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// LatencyHistogram зберігає latency дані для обчислення percentiles
type LatencyHistogram struct {
	mu       sync.RWMutex
	samples  []int64 // latency samples в наносекундах
	capacity int     // максимальна кількість samples
	index    int64   // atomic - поточний індекс для circular buffer
	count    int64   // atomic - загальна кількість зразків

	// Cached percentiles (оновлюються періодично)
	cachedP50  int64 // atomic
	cachedP95  int64 // atomic
	cachedP99  int64 // atomic
	lastUpdate int64 // atomic - timestamp останнього оновлення cache
}

// LatencyMetrics керує всіма latency метриками
type LatencyMetrics struct {
	setLatency    *LatencyHistogram
	getLatency    *LatencyHistogram
	lpushLatency  *LatencyHistogram
	rpushLatency  *LatencyHistogram
	lrangeLatency *LatencyHistogram

	// Control channels
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewLatencyHistogram створює нову histogram з заданою ємністю
func NewLatencyHistogram(capacity int) *LatencyHistogram {
	return &LatencyHistogram{
		samples:  make([]int64, capacity),
		capacity: capacity,
	}
}

// NewLatencyMetrics створює нову систему latency метрик
func NewLatencyMetrics() *LatencyMetrics {
	// Використовуємо circular buffer для 10,000 зразків кожного типу
	lm := &LatencyMetrics{
		setLatency:    NewLatencyHistogram(10000),
		getLatency:    NewLatencyHistogram(10000),
		lpushLatency:  NewLatencyHistogram(10000),
		rpushLatency:  NewLatencyHistogram(10000),
		lrangeLatency: NewLatencyHistogram(10000),
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	// Запускаємо background процес для оновлення percentiles
	go lm.startPercentileUpdater()

	return lm
}

// RecordLatency записує latency для заданої операції
func (lm *LatencyMetrics) RecordLatency(operation string, duration time.Duration) {
	nanos := duration.Nanoseconds()

	switch operation {
	case "SET":
		lm.setLatency.AddSample(nanos)
	case "GET":
		lm.getLatency.AddSample(nanos)
	case "LPUSH":
		lm.lpushLatency.AddSample(nanos)
	case "RPUSH":
		lm.rpushLatency.AddSample(nanos)
	case "LRANGE":
		lm.lrangeLatency.AddSample(nanos)
	}
}

// AddSample додає новий latency зразок в histogram
func (lh *LatencyHistogram) AddSample(latencyNanos int64) {
	// Використовуємо atomic для thread-safe circular buffer
	idx := atomic.AddInt64(&lh.index, 1) % int64(lh.capacity)
	atomic.AddInt64(&lh.count, 1)

	// Записуємо в circular buffer без блокування читання
	lh.mu.Lock()
	lh.samples[idx] = latencyNanos
	lh.mu.Unlock()
}

// GetPercentiles повертає cached percentiles
func (lh *LatencyHistogram) GetPercentiles() (p50, p95, p99 time.Duration) {
	p50 = time.Duration(atomic.LoadInt64(&lh.cachedP50))
	p95 = time.Duration(atomic.LoadInt64(&lh.cachedP95))
	p99 = time.Duration(atomic.LoadInt64(&lh.cachedP99))
	return
}

// updatePercentiles обчислює та кешує percentiles
func (lh *LatencyHistogram) updatePercentiles() {
	lh.mu.RLock()

	// Збираємо всі актуальні зразки
	count := atomic.LoadInt64(&lh.count)
	sampleCount := int(count)
	if sampleCount > lh.capacity {
		sampleCount = lh.capacity
	}

	if sampleCount == 0 {
		lh.mu.RUnlock()
		return
	}

	// Копіюємо зразки для сортування
	samples := make([]int64, sampleCount)
	copy(samples, lh.samples[:sampleCount])
	lh.mu.RUnlock()

	// Сортуємо для обчислення percentiles
	sort.Slice(samples, func(i, j int) bool {
		return samples[i] < samples[j]
	})

	// Обчислюємо percentiles
	p50Index := (sampleCount * 50) / 100
	p95Index := (sampleCount * 95) / 100
	p99Index := (sampleCount * 99) / 100

	if p50Index >= sampleCount {
		p50Index = sampleCount - 1
	}
	if p95Index >= sampleCount {
		p95Index = sampleCount - 1
	}
	if p99Index >= sampleCount {
		p99Index = sampleCount - 1
	}

	// Оновлюємо cached значення атомарно
	atomic.StoreInt64(&lh.cachedP50, samples[p50Index])
	atomic.StoreInt64(&lh.cachedP95, samples[p95Index])
	atomic.StoreInt64(&lh.cachedP99, samples[p99Index])
	atomic.StoreInt64(&lh.lastUpdate, time.Now().UnixNano())
}

// startPercentileUpdater запускає background процес для оновлення percentiles
func (lm *LatencyMetrics) startPercentileUpdater() {
	ticker := time.NewTicker(5 * time.Second) // Оновлюємо кожні 5 секунд
	defer ticker.Stop()
	defer close(lm.doneCh)

	for {
		select {
		case <-ticker.C:
			// Оновлюємо percentiles для всіх histograms
			lm.setLatency.updatePercentiles()
			lm.getLatency.updatePercentiles()
			lm.lpushLatency.updatePercentiles()
			lm.rpushLatency.updatePercentiles()
			lm.lrangeLatency.updatePercentiles()

		case <-lm.stopCh:
			return
		}
	}
}

// GetMetrics повертає всі latency метрики
func (lm *LatencyMetrics) GetMetrics() map[string]map[string]time.Duration {
	metrics := make(map[string]map[string]time.Duration)

	// SET metrics
	p50, p95, p99 := lm.setLatency.GetPercentiles()
	metrics["SET"] = map[string]time.Duration{
		"P50": p50,
		"P95": p95,
		"P99": p99,
	}

	// GET metrics
	p50, p95, p99 = lm.getLatency.GetPercentiles()
	metrics["GET"] = map[string]time.Duration{
		"P50": p50,
		"P95": p95,
		"P99": p99,
	}

	// LPUSH metrics
	p50, p95, p99 = lm.lpushLatency.GetPercentiles()
	metrics["LPUSH"] = map[string]time.Duration{
		"P50": p50,
		"P95": p95,
		"P99": p99,
	}

	// RPUSH metrics
	p50, p95, p99 = lm.rpushLatency.GetPercentiles()
	metrics["RPUSH"] = map[string]time.Duration{
		"P50": p50,
		"P95": p95,
		"P99": p99,
	}

	// LRANGE metrics
	p50, p95, p99 = lm.lrangeLatency.GetPercentiles()
	metrics["LRANGE"] = map[string]time.Duration{
		"P50": p50,
		"P95": p95,
		"P99": p99,
	}

	return metrics
}

// Stop зупиняє latency metrics систему
func (lm *LatencyMetrics) Stop() {
	close(lm.stopCh)
	<-lm.doneCh
}

// Helper function to format duration for display
func FormatLatency(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%.0fns", float64(d.Nanoseconds()))
	} else if d < time.Millisecond {
		return fmt.Sprintf("%.1fμs", float64(d.Nanoseconds())/1000.0)
	} else {
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1000000.0)
	}
}
