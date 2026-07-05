package config

import (
	"flag"
)

// Config holds all configuration for the Reservoir server
type Config struct {
	// Server configuration
	Port      string
	Debug     bool
	ReusePort bool
	LogLevel  string

	// Memory limits
	MaxKeySize     int64
	MaxValueSize   int64
	MaxMemoryUsage int64

	// Connection limits
	MaxConnections      int
	MaxConnectionsPerIP int
	ConnectionRateLimit int

	// UI configuration
	UIPort string

	// Cluster configuration
	ClusterEnabled   bool
	ClusterNodeAddr  string
	ClusterNodePort  int
	ClusterSeedNodes string // comma-separated list of seed nodes
	ClusterID        string // cluster ID (will be auto-generated if empty)

	// Commit Log configuration
	CommitLogEnabled bool
	CommitLogDir     string
}

// DefaultConfig returns a configuration with default values
func DefaultConfig() *Config {
	return &Config{
		Port:                "6379",
		Debug:               false,
		ReusePort:           false,
		LogLevel:            "info",
		MaxKeySize:          512,
		MaxValueSize:        512 * 1024 * 1024,
		MaxMemoryUsage:      1024 * 1024 * 1024,
		MaxConnections:      10000,
		MaxConnectionsPerIP: 10000,
		ConnectionRateLimit: 10000, // Increased for high-load scenarios
		// Bind the admin/visualization UI to loopback by default. It has no
		// authentication and exposes key names, sizes, TTLs and cluster
		// topology, so it must not be reachable from other hosts unless the
		// operator explicitly opts in (e.g. --ui-port 0.0.0.0:8080 behind a
		// trusted network or reverse proxy).
		UIPort: "127.0.0.1:8080",

		ClusterEnabled:   false,
		ClusterNodeAddr:  "127.0.0.1",
		ClusterNodePort:  7379,
		ClusterSeedNodes: "",
		ClusterID:        "",
		CommitLogEnabled: false,
		CommitLogDir:     "./data/commitlog",
	}
}

// ParseFlags parses command line flags and returns a config
func ParseFlags() *Config {
	config := DefaultConfig()

	debug := flag.Bool("debug", config.Debug, "Enable debug mode (deprecated, use -log-level=debug)")
	port := flag.String("port", config.Port, "Port to listen on")
	reusePort := flag.Bool("reuseport", config.ReusePort, "Enable SO_REUSEPORT for better load distribution")
	logLevel := flag.String("log-level", config.LogLevel, "Log level: debug, info, warning, error, quiet")

	// Memory limit flags
	maxKey := flag.Int64("max-key-size", config.MaxKeySize, "Maximum key size in bytes")
	maxValue := flag.Int64("max-value-size", config.MaxValueSize, "Maximum value size in bytes")
	maxMem := flag.Int64("max-memory", config.MaxMemoryUsage, "Maximum total memory usage in bytes")

	// Connection limit flags
	maxConns := flag.Int("max-connections", config.MaxConnections, "Maximum total connections")
	maxConnsPerIP := flag.Int("max-connections-per-ip", config.MaxConnectionsPerIP, "Maximum connections per IP address")
	connRateLimit := flag.Int("connection-rate-limit", config.ConnectionRateLimit, "Connection attempts per minute per IP (0 to disable)")

	// UI flags
	uiPort := flag.String("ui-port", config.UIPort, "Port for the Memory Visualization UI")

	// Cluster flags
	clusterEnabled := flag.Bool("cluster-enabled", config.ClusterEnabled, "Enable cluster mode")
	clusterNodeAddr := flag.String("cluster-node-addr", config.ClusterNodeAddr, "Address for cluster communication")
	clusterNodePort := flag.Int("cluster-node-port", config.ClusterNodePort, "Port for cluster communication")
	clusterSeedNodes := flag.String("cluster-seed-nodes", config.ClusterSeedNodes, "Comma-separated list of seed nodes (host:port)")
	clusterID := flag.String("cluster-id", config.ClusterID, "Cluster ID (auto-generated if empty)")

	// Commit Log flags
	commitLogEnabled := flag.Bool("commit-log-enabled", config.CommitLogEnabled, "Enable persistent commit log")
	commitLogDir := flag.String("commit-log-dir", config.CommitLogDir, "Directory for commit log files")

	flag.Parse()

	// Apply parsed values
	config.Debug = *debug
	config.Port = *port
	config.ReusePort = *reusePort
	config.LogLevel = *logLevel

	// If debug flag is set, override log level to debug for backward compatibility
	if config.Debug {
		config.LogLevel = "debug"
	}
	config.MaxKeySize = *maxKey
	config.MaxValueSize = *maxValue
	config.MaxMemoryUsage = *maxMem
	config.MaxConnections = *maxConns
	config.MaxConnectionsPerIP = *maxConnsPerIP
	config.ConnectionRateLimit = *connRateLimit
	config.UIPort = *uiPort

	config.ClusterEnabled = *clusterEnabled
	config.ClusterNodeAddr = *clusterNodeAddr
	config.ClusterNodePort = *clusterNodePort
	config.ClusterSeedNodes = *clusterSeedNodes
	config.ClusterID = *clusterID
	config.CommitLogEnabled = *commitLogEnabled
	config.CommitLogDir = *commitLogDir

	return config
}
