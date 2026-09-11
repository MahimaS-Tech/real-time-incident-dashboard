package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"incident-dashboard/pkg/config"
	"incident-dashboard/pkg/dashboard"
	"incident-dashboard/pkg/store"
)

func main() {
	redisStore := store.NewRedisStore(
		config.Env("REDIS_ADDR", "localhost:6379"),
		config.Env("REDIS_PASSWORD", ""),
		config.EnvInt("REDIS_DB", 0),
		config.EnvDuration("DEDUPE_TTL", 7*24*time.Hour),
	)
	defer redisStore.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := redisStore.Ping(ctx); err != nil {
		cancel()
		slog.Error("redis unavailable", "error", err)
		os.Exit(1)
	}
	cancel()

	app := dashboard.NewServer(redisStore, config.Env("STATIC_DIR", "web"))
	server := &http.Server{
		Addr:              ":" + config.Env("PORT", "8081"),
		Handler:           app.Routes(),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		slog.Info("dashboard listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("dashboard failed", "error", err)
			os.Exit(1)
		}
	}()
	shutdown(server)
}

func shutdown(server *http.Server) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}
