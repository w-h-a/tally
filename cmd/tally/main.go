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

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", *healthPort),
		Handler: newHealthMux(),
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("health server", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("shutdown", "error", err)
		return
	}

	logger.Info("shutdown complete")
}
