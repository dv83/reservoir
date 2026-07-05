package main

import (
	"os"

	"reservoir/internal/server"
	"reservoir/pkg/config"
	"reservoir/pkg/logger"
)

func main() {
	cfg := config.ParseFlags()

	// Set log level
	if err := logger.SetLevelFromString(cfg.LogLevel); err != nil {
		logger.Error("Invalid log level: %v", err)
		os.Exit(1)
	}

	logger.Info("Starting Reservoir server...")

	// Create new server instance
	srv, err := server.New(cfg)
	if err != nil {
		logger.Error("Failed to create server: %v", err)
		os.Exit(1)
	}

	// Start server (blocks until shutdown)
	if err := srv.Start(); err != nil {
		logger.Error("Server error: %v", err)
		os.Exit(1)
	}
}
