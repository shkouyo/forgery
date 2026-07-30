package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"git.0x0f.dev/forgery/internal/config"
	"git.0x0f.dev/forgery/internal/dispatch"
	"git.0x0f.dev/forgery/internal/north"
	"git.0x0f.dev/forgery/internal/observability"
	"git.0x0f.dev/forgery/internal/run"
	"git.0x0f.dev/forgery/internal/session"
	"git.0x0f.dev/forgery/internal/south"
	"git.0x0f.dev/forgery/internal/store"
)

func main() {
	// 1. Load configuration
	// 1.1 Default logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 1.2 Config
	cfg := config.MustLoad(logger)

	// 1.3 Structured logger
	logger = observability.NewLogger(cfg.LogLevel, cfg.LogFormat)
	logger.Info("starting forgery", "addr", cfg.ListenAddr, "forgejo_url", cfg.ForgejoURL)

	// 2. Store
	taskStore := store.NewMemStore()

	// 3. North client
	northClient := north.New(cfg, taskStore, cfg.MaxParallelTasks, logger)

	// 4. Dispatcher
	dispatcher := dispatch.NewDispatcher(cfg, logger)

	// 5. Session manager
	sessionMgr := session.NewManager()

	// 6. South handler
	southHandler := south.NewHandler(taskStore, sessionMgr, northClient, cfg, logger)

	// 7. South HTTP server
	srv := south.NewServer(southHandler, cfg.ListenAddr)

	// 8. Runner
	runner := run.New(cfg, northClient, dispatcher, taskStore, logger)

	// 9. Background services
	// 9.1 South HTTP server
	go func() {
		logger.Info("south HTTP server starting", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("south HTTP server error", "err", err)
		}
	}()

	// 9.2 Health checks
	healthChecker := observability.NewHealthChecker()
	observability.StartHealthServer(cfg.HealthAddr, healthChecker)

	// 10. Forgejo registration
	// 10.1 Context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 10.2 Register & declare
	logger.Info("registering runner with Forgejo")
	if err := northClient.Register(ctx); err != nil {
		logger.Error("runner registration failed", "err", err)
		os.Exit(1)
	}
	logger.Info("declaring runner labels and version")
	if err := northClient.Declare(ctx); err != nil {
		logger.Error("runner declaration failed", "err", err)
		os.Exit(1)
	}
	logger.Info("runner registered successfully")
	taskCh := make(chan *store.TaskCtx, cfg.MaxParallelTasks)
	go northClient.PollLoop(ctx, taskCh)

	// 11. Task dispatch
	go func() {
		for taskCtx := range taskCh {
			go runner.HandleTask(ctx, taskCtx)
		}
	}()

	// 12. Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("received signal, starting graceful shutdown", "signal", sig.String())

	// ── Graceful Shutdown Sequence (DETAIL-DESIGN §9.2) ──

	// Step 1: Stop accepting new work.
	cancel()

	// Step 2: Wait for active tasks to drain (with timeout).
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.GAStartupTimeout)
	defer shutdownCancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		active := taskStore.CountActive()
		if active == 0 {
			logger.Info("all tasks drained")
			break
		}
		select {
		case <-ticker.C:
			logger.Info("waiting for tasks to drain", "active", active)
		case <-shutdownCtx.Done():
			logger.Warn("shutdown timeout, forcing exit", "remaining", active)
			goto shutdown
		}
	}

shutdown:
	// Step 3: Shut down south HTTP server gracefully.
	httpShutdownCtx, httpCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer httpCancel()
	if err := srv.Shutdown(httpShutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error", "err", err)
	}

	logger.Info("forgery stopped gracefully")
}
