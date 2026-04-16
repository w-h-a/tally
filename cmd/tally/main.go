package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	defaultPort := 8080
	if env := os.Getenv("TALLY_HEALTH_PORT"); env != "" {
		if p, err := strconv.Atoi(env); err == nil && p > 0 {
			defaultPort = p
		}
	}

	healthPort := flag.Int("health-port", defaultPort, "HTTP health check port")
	flag.Parse()

	version := "dev"
	if v := os.Getenv("VERSION"); v != "" {
		version = v
	}

	ctx := context.Background()

	res, err := newResource(ctx, version)
	if err != nil {
		slog.Error("otel resource", "error", err)
		os.Exit(1)
	}

	shutdownTracer, err := initTracer(ctx, res)
	if err != nil {
		slog.Error("otel tracer", "error", err)
		os.Exit(1)
	}

	shutdownLogger, err := initLogger(ctx, res)
	if err != nil {
		slog.Error("otel logger", "error", err)
		os.Exit(1)
	}

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", *healthPort),
		Handler: newHealthMux(),
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health server", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	slog.Info("start shutdown")

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown", "error", err)
	}

	if err := shutdownTracer(shutdownCtx); err != nil {
		slog.Error("tracer shutdown", "error", err)
	}

	if err := shutdownLogger(shutdownCtx); err != nil {
		slog.Error("logger shutdown", "error", err)
	}

	slog.Info("shutdown complete")
}
