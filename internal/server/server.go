package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"reservoir/internal/cluster"
	"reservoir/internal/commands"
	"reservoir/internal/commitlog"
	"reservoir/internal/connection"
	"reservoir/internal/store"
	"reservoir/pkg/config"
	"reservoir/pkg/logger"
)

// Compile-time assertion that the concrete commit log satisfies the store's
// CommitLogger interface. If the interface and implementation ever drift again
// (e.g. a changed method signature), this fails to build instead of silently
// disabling persistence at runtime via a failed type assertion.
var _ store.CommitLogger = (*commitlog.CommitLog)(nil)

// Server represents the Reservoir server instance
type Server struct {
	cfg               *config.Config
	kvStore           store.KVStore
	commandRegistry   *commands.ZeroCopyRegistry
	connectionManager *connection.Manager
	clusterManager    *cluster.ClusterManager
	listener          net.Listener
	uiServer          *http.Server
	shutdownChan      chan struct{}
	wg                sync.WaitGroup
	mu                sync.Mutex
	running           bool
}

// New creates a new Server instance with the given configuration
func New(cfg *config.Config) (*Server, error) {
	// Initialize connection manager
	connectionManager := connection.NewManager(
		cfg.MaxConnections,
		cfg.MaxConnectionsPerIP,
		cfg.ConnectionRateLimit,
	)

	// Initialize store (now lock-free by default)
	storeLimits := &store.Limits{
		MaxKeySize:     cfg.MaxKeySize,
		MaxValueSize:   cfg.MaxValueSize,
		MaxMemoryUsage: cfg.MaxMemoryUsage,
	}
	kvStore := store.NewStore(storeLimits)

	// Initialize commit log if enabled
	if cfg.CommitLogEnabled {
		commitLog, err := commitlog.NewCommitLog(cfg.CommitLogDir)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize commit log: %v", err)
		}
		kvStore.SetCommitLog(commitLog)
		kvStore.SetCommitLogDir(cfg.CommitLogDir)

		// Set up asynchronous recovery (don't block server startup)
		asyncRecovery := commitlog.NewAsyncRecovery(cfg.CommitLogDir, func(entry *commitlog.Entry) error {
			// Apply entry to store (with recovery mode enabled to prevent commit log writes)
			switch entry.Operation {
			case "SET":
				return kvStore.Set(string(entry.Key), string(entry.Value))
			case "DEL":
				kvStore.Delete(string(entry.Key))
				return nil
			case "INCR", "INCRBY", "DECR", "DECRBY":
				// Value contains the final result after increment/decrement
				return kvStore.Set(string(entry.Key), string(entry.Value))
			case "EXPIRE":
				// Value is the absolute expiry time (unix nanoseconds). The SET
				// that created the key has an earlier timestamp and is replayed
				// first, so the key exists by the time this is applied.
				nano, err := strconv.ParseInt(string(entry.Value), 10, 64)
				if err != nil {
					return nil // skip a corrupt expiry entry rather than abort recovery
				}
				kvStore.SetExpiry(string(entry.Key), time.Unix(0, nano))
				return nil
			case "PERSIST":
				kvStore.RemoveExpiry(string(entry.Key))
				return nil
			case "FLUSHALL", "FLUSHDB":
				// Clear all data
				kvStore.Clear()
				return nil
			default:
				// Other operations can be added as needed
				return nil
			}
		})

		// Set progress callback for logging
		asyncRecovery.SetProgressCallback(func(processed, total int64) {
			if processed%10000 == 0 || processed == total {
				logger.Info("Commit log recovery progress: %d/%d entries (%.1f%%)",
					processed, total, float64(processed)/float64(total)*100)
			}
		})

		// Enable recovery mode (disables commit log writes during recovery)
		kvStore.SetRecoveryActive(true)

		// Start async recovery
		if err := asyncRecovery.Start(); err != nil {
			logger.Error("Failed to start async recovery: %v", err)
			kvStore.SetRecoveryActive(false)
		} else {
			// Disable recovery mode after completion in background
			go func() {
				// Wait for recovery completion (with 5 minute timeout)
				if err := asyncRecovery.Wait(5 * time.Minute); err != nil {
					logger.Error("Async recovery failed or timed out: %v", err)
				}

				// Always disable recovery mode
				kvStore.SetRecoveryActive(false)
				logger.Info("Recovery mode disabled, commit log writes resumed")
			}()
		}

		logger.Info("Commit log enabled, writing to %s", cfg.CommitLogDir)
	}

	// Initialize cluster if enabled
	var clusterManager *cluster.ClusterManager
	if cfg.ClusterEnabled {
		// Generate cluster ID if not provided
		clusterIDStr := cfg.ClusterID
		if clusterIDStr == "" {
			clusterIDStr = cluster.NewUUIDv7().String()
			logger.Info("Generated new cluster ID: %s", clusterIDStr)
		}

		// Parse cluster ID
		clusterID, err := cluster.ParseUUID(clusterIDStr)
		if err != nil {
			logger.Error("Invalid cluster ID: %v. Generating new one.", err)
			clusterID = cluster.NewUUIDv7()
		}

		clusterManager = cluster.NewClusterManager(
			cfg.ClusterNodeAddr,
			cfg.ClusterNodePort,
			clusterID,
		)

		// Authenticate inter-node traffic if a shared secret was configured.
		if cfg.ClusterSecret != "" {
			clusterManager.SetAuthKey(cfg.ClusterSecret)
			logger.Info("Cluster inter-node authentication enabled")
		} else {
			logger.Warning("Cluster secret not set: inter-node traffic is unauthenticated (set --cluster-secret or RESERVOIR_CLUSTER_SECRET)")
		}

		// Set up replication handler
		clusterManager.SetReplicationHandler(&StoreReplicationHandler{Store: kvStore})

		// Persist Raft voting state so a restarted node cannot vote twice in a
		// term. Keyed by cluster port so co-located nodes don't share a file.
		stateDir := "data/cluster"
		if err := os.MkdirAll(stateDir, 0o755); err != nil {
			logger.Warning("Could not create cluster state dir %s: %v (raft state will not persist)", stateDir, err)
		} else {
			clusterManager.SetStatePath(filepath.Join(stateDir, fmt.Sprintf("raft-state-%d.json", cfg.ClusterNodePort)))
		}

		logger.Info("Cluster mode enabled: NodeID=%s, ClusterID=%s",
			clusterManager.GetLocalNode().NodeID, clusterID)
	}

	// Initialize zero-copy command registry
	commandRegistry := commands.NewZeroCopyRegistry()

	// Enable template expansion
	commands.EnableTemplateExpansion()

	// Initialize UI Server
	var uiServer *http.Server
	if cfg.UIPort != "" {
		uiServer = StartUIServer(cfg.UIPort, kvStore, clusterManager)
	}

	return &Server{
		cfg:               cfg,
		kvStore:           kvStore,
		commandRegistry:   commandRegistry,
		connectionManager: connectionManager,
		clusterManager:    clusterManager,
		shutdownChan:      make(chan struct{}),
		uiServer:          uiServer,
	}, nil
}

// Start starts the server and blocks until shutdown
func (s *Server) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.mu.Unlock()

	// Start cluster manager if enabled
	if s.clusterManager != nil {
		if err := s.clusterManager.Start(); err != nil {
			return fmt.Errorf("failed to start cluster: %v", err)
		}

		// Join cluster if seed nodes are provided
		if s.cfg.ClusterSeedNodes != "" {
			s.joinCluster()
		}
	}

	listener, err := s.createListener("tcp", ":"+s.cfg.Port, s.cfg.ReusePort)
	if err != nil {
		return fmt.Errorf("failed to start listener: %v", err)
	}
	s.listener = listener
	defer func() { _ = listener.Close() }()

	modeStr := " (LOCK-FREE)"
	if s.cfg.ReusePort {
		modeStr += " (SO_REUSEPORT)"
	}
	if s.cfg.Debug {
		modeStr += " (DEBUG MODE)"
	}

	logger.Info("Reservoir server started on :%s%s", s.cfg.Port, modeStr)

	// Setup signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start accept loop in a goroutine
	go s.acceptLoop()

	// Wait for shutdown signal
	<-sigChan
	logger.Info("Received shutdown signal, gracefully stopping server...")

	// Execute graceful shutdown
	s.Stop()

	return nil
}

// Stop executes graceful shutdown
func (s *Server) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	s.mu.Unlock()

	// Close listener to stop accepting new connections
	if s.listener != nil {
		_ = s.listener.Close()
	}

	// Stop UI server
	if s.uiServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.uiServer.Shutdown(ctx); err != nil {
			logger.Error("UI server shutdown error: %v", err)
		}
	}

	// Signal shutdown to all goroutines
	close(s.shutdownChan)

	// Give existing connections time to finish (with timeout)
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All connections closed gracefully")
	case <-time.After(30 * time.Second):
		logger.Warning("Timeout waiting for connections to close")
	}

	// Stop the store (this will stop background goroutines)
	s.kvStore.Stop()

	// Stop the connection manager
	s.connectionManager.Stop()

	logger.Info("Server shutdown complete")
}

func (s *Server) joinCluster() {
	seedNodes := strings.Split(s.cfg.ClusterSeedNodes, ",")
	for _, seedNode := range seedNodes {
		parts := strings.Split(strings.TrimSpace(seedNode), ":")
		if len(parts) == 2 {
			port := s.cfg.ClusterNodePort // Default port
			if parsedPort, err := strconv.Atoi(parts[1]); err == nil {
				port = parsedPort
			}

			if err := s.clusterManager.JoinCluster(parts[0], port); err != nil {
				logger.Warning("Failed to join via seed node %s: %v", seedNode, err)
			} else {
				logger.Info("Attempting to join cluster via %s", seedNode)
				break // Successfully sent join request
			}
		}
	}
}

func (s *Server) createListener(network, address string, _ bool) (net.Listener, error) {
	// For cross-platform compatibility, we'll simply use standard net.Listen
	// SO_REUSEPORT is primarily useful on Linux/Unix systems
	// On Windows (including WSL), we fall back to regular listener
	return net.Listen(network, address)
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.shutdownChan:
				// Expected error during shutdown
				return
			default:
				logger.Error("Failed to accept connection: %v", err)
				continue
			}
		}

		// Extract client IP for connection limiting
		clientAddr := conn.RemoteAddr().String()
		clientIP := clientAddr
		if host, _, err := net.SplitHostPort(clientAddr); err == nil {
			clientIP = host
		}

		// Check connection limits before accepting
		if !s.connectionManager.CanAcceptConnection(clientIP) {
			logger.Warning("Connection rejected due to limits (rate/count): %s", clientAddr)
			_ = conn.Close()
			continue
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(conn)
		}()
	}
}
