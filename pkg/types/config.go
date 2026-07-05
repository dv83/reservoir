package types

import (
	"sync"
	"time"
)

// Config represents the global configuration that needs to be accessible
// by multiple components
type GlobalConfig struct {
	HeartbeatInterval time.Duration
}

var (
	globalConfig     *GlobalConfig
	globalConfigLock sync.RWMutex
)

// SetGlobalConfig sets the global configuration
func SetGlobalConfig(config *GlobalConfig) {
	globalConfigLock.Lock()
	defer globalConfigLock.Unlock()
	globalConfig = config
}

// GetGlobalConfig retrieves the global configuration
func GetGlobalConfig() (*GlobalConfig, bool) {
	globalConfigLock.RLock()
	defer globalConfigLock.RUnlock()
	if globalConfig == nil {
		return nil, false
	}
	return globalConfig, true
}
