// Package main provides the entry point for the cloud-data-sync application
package main

import (
	"context"
	"flag"
	"log/slog" // Import slog
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DjonatanS/cloud-data-sync/internal/config"
	"github.com/DjonatanS/cloud-data-sync/internal/database"
	"github.com/DjonatanS/cloud-data-sync/internal/storage"
	syncPkg "github.com/DjonatanS/cloud-data-sync/internal/sync"
)

func main() {
	configPath := flag.String("config", "config.json", "Path to the configuration file")
	generateConfig := flag.Bool("generate-config", false, "Generate a configuration file with default values")
	runOnce := flag.Bool("once", false, "Run synchronization once and exit")
	interval := flag.Int("interval", 300, "Interval between synchronizations (in seconds)")
	flag.Parse()

	// Setup structured logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger) // Set as default for convenience, though explicit passing is better

	if *generateConfig {
		logger.Info("Generating default configuration file...")
		if err := config.SaveDefaultConfig(*configPath); err != nil {
			logger.Error("Error generating configuration file", "error", err)
			os.Exit(1) // Replace log.Fatalf
		}
		logger.Info("Configuration file generated", "path", *configPath)
		return
	}

	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		logger.Error("Error loading configuration", "path", *configPath, "error", err)
		os.Exit(1) // Replace log.Fatalf
	}
	logger.Info("Configuration loaded successfully", "path", *configPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := database.NewDB(cfg.DatabasePath)
	if err != nil {
		logger.Error("Error initializing database", "path", cfg.DatabasePath, "error", err)
		os.Exit(1) // Replace log.Fatalf
	}
	defer db.Close()
	logger.Info("Database initialized successfully", "path", cfg.DatabasePath)

	// Pass logger to factory (assuming factory might use it later)
	factory, err := storage.NewFactory(ctx, cfg, logger)
	if err != nil {
		logger.Error("Error initializing provider factory", "error", err)
		os.Exit(1) // Replace log.Fatalf
	}
	defer factory.Close()
	logger.Info("Storage providers initialized successfully")

	// Pass logger to synchronizer
	synchronizer := syncPkg.NewSynchronizer(db, cfg, factory, logger.With("component", "synchronizer")) // Add component context
	logger.Info("Synchronizer initialized successfully")

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	if *runOnce {
		logger.Info("Executing single synchronization run...")
		if err := synchronizer.SyncAll(ctx); err != nil {
			// Log the error but don't necessarily exit for a single run error
			logger.Error("Error during single synchronization run", "error", err)
		} else {
			logger.Info("Single synchronization run completed successfully")
		}
		return // Exit after single run
	}

	logger.Info("Starting continuous synchronization service", "interval_seconds", *interval)

	ticker := time.NewTicker(time.Duration(*interval) * time.Second)
	defer ticker.Stop()

	logger.Info("Executing initial synchronization...")
	if err := synchronizer.SyncAll(ctx); err != nil {
		logger.Error("Error during initial synchronization", "error", err)
		// Continue running even if initial sync fails
	} else {
		logger.Info("Initial synchronization completed")
	}

	for {
		select {
		case <-ticker.C:
			logger.Info("Starting synchronization cycle...")
			if err := synchronizer.SyncAll(ctx); err != nil {
				logger.Error("Error during synchronization cycle", "error", err)
			} else {
				logger.Info("Synchronization cycle completed")
			}

		case sig := <-signalCh:
			logger.Info("Signal received, shutting down...", "signal", sig.String())
			cancel() // Trigger context cancellation
			// Allow some time for graceful shutdown if needed, though SyncAll should respect context
			// time.Sleep(1 * time.Second) // Optional grace period
			return // Exit main loop
		}
	}
}
