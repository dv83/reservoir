package connection

import (
	"sync"
	"time"
)

// Manager tracks and limits client connections
type Manager struct {
	mu               sync.RWMutex
	totalConnections int
	connectionsByIP  map[string]int
	connectionRates  map[string][]time.Time // Track connection attempts per IP

	// Configuration
	maxConnections      int
	maxConnectionsPerIP int
	connectionRateLimit int

	// Control channels for graceful shutdown
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewManager creates a new connection manager
func NewManager(maxConns, maxConnsPerIP, rateLimit int) *Manager {
	cm := &Manager{
		connectionsByIP:     make(map[string]int),
		connectionRates:     make(map[string][]time.Time),
		maxConnections:      maxConns,
		maxConnectionsPerIP: maxConnsPerIP,
		connectionRateLimit: rateLimit,
		stopCh:              make(chan struct{}),
		doneCh:              make(chan struct{}),
	}

	// Start cleanup goroutine for rate limiting
	go cm.cleanupRates()

	return cm
}

func (cm *Manager) cleanupRates() {
	defer close(cm.doneCh)
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.mu.Lock()
			now := time.Now()
			cutoff := now.Add(-1 * time.Minute)

			for ip, times := range cm.connectionRates {
				// Remove entries older than 1 minute
				filtered := times[:0]
				for _, t := range times {
					if t.After(cutoff) {
						filtered = append(filtered, t)
					}
				}

				if len(filtered) == 0 {
					delete(cm.connectionRates, ip)
				} else {
					cm.connectionRates[ip] = filtered
				}
			}
			cm.mu.Unlock()
		}
	}
}

// CanAcceptConnection checks if a new connection from the given IP can be accepted
func (cm *Manager) CanAcceptConnection(clientIP string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Check global connection limit
	if cm.totalConnections >= cm.maxConnections {
		return false
	}

	// Check per-IP connection limit
	if cm.connectionsByIP[clientIP] >= cm.maxConnectionsPerIP {
		return false
	}

	// Check rate limiting (skip if disabled)
	if cm.connectionRateLimit > 0 {
		now := time.Now()
		cutoff := now.Add(-1 * time.Minute)

		// Count recent connections from this IP
		times, exists := cm.connectionRates[clientIP]
		if exists {
			recentConnections := 0
			for _, t := range times {
				if t.After(cutoff) {
					recentConnections++
				}
			}

			if recentConnections >= cm.connectionRateLimit {
				return false
			}
		}
	}

	return true
}

// AddConnection records a new connection from the given IP
func (cm *Manager) AddConnection(clientIP string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.totalConnections++
	cm.connectionsByIP[clientIP]++

	// Record connection attempt time
	now := time.Now()
	cm.connectionRates[clientIP] = append(cm.connectionRates[clientIP], now)
}

// RemoveConnection removes a connection from the given IP
func (cm *Manager) RemoveConnection(clientIP string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.totalConnections--
	if cm.totalConnections < 0 {
		cm.totalConnections = 0
	}

	cm.connectionsByIP[clientIP]--
	if cm.connectionsByIP[clientIP] <= 0 {
		delete(cm.connectionsByIP, clientIP)
	}
}

// GetStats returns current connection statistics
func (cm *Manager) GetStats() (int, int) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.totalConnections, len(cm.connectionsByIP)
}

// Stop gracefully stops the connection manager
func (cm *Manager) Stop() {
	close(cm.stopCh)
	<-cm.doneCh // Wait for cleanup goroutine to finish
}
