package main

import (
	"context"
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
	cfg := config.MustLoad()

	// 2. Initialize structured logging
	logger := observability.NewLogger(cfg.LogLevel, cfg.LogFormat)
	logger.Info("starting forgery", "listen_addr", cfg.ListenAddr, "forgejo_url", cfg.ForgejoURL)

	// 3. Initialize store (in-memory task store)
	taskStore := store.NewMemStore()

	// 4. Initialize north client (forgery → Forgejo)
	northClient := north.New(cfg, taskStore, cfg.MaxParallelTasks, logger)

	// 5. Initialize GitHub Actions dispatcher
	dispatcher := dispatch.NewDispatcher(cfg, logger)

	// 6. Initialize session manager
	sessionMgr := session.NewManager()

	// 7. Initialize south handler (serves internal runner)
	southHandler := south.NewHandler(taskStore, sessionMgr, northClient, cfg, logger)

	// 8. Create south HTTP server
	srv := south.NewServer(southHandler, cfg.ListenAddr)

	// 9. Create Runner for task lifecycle management (replaces direct dispatch)
	runner := run.New(cfg, northClient, dispatcher, taskStore, logger)

	// 10. Start south HTTP server in goroutine
	go func() {
		logger.Info("south HTTP server starting", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("south HTTP server error", "err", err)
		}
	}()

	// 10a. Initialize metrics (Prometheus-style JSON at /metrics)
	metrics := observability.NewMetrics()
	observability.StartMetricsServer(cfg.MetricsAddr, metrics)

	// 10b. Initialize health checks (Kubernetes-style /healthz + /readyz)
	healthChecker := observability.NewHealthChecker()
	observability.StartHealthServer(cfg.HealthAddr, healthChecker)


	// 11. Create context for all background work
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 11a. Register and declare with Forgejo before polling
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
	healthChecker.SetReady(true)
	taskCh := make(chan *store.TaskCtx, cfg.MaxParallelTasks)
	go northClient.PollLoop(ctx, taskCh)

	// 13. Task dispatch loop using Runner
	go func() {
		for taskCtx := range taskCh {
			go runner.HandleTask(ctx, taskCtx)
		}
	}()

	// 14. Wait for shutdown signal
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
