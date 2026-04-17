package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/w-h-a/tally/internal/handler/http/gateway"
	"github.com/w-h-a/tally/internal/handler/http/health"
	tallyotel "github.com/w-h-a/tally/internal/util/otel"
	api "github.com/w-h-a/tally/proto/log/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	defaultPort := 8080
	if env := os.Getenv("TALLY_HTTP_PORT"); env != "" {
		if p, err := strconv.Atoi(env); err == nil && p > 0 {
			defaultPort = p
		}
	}

	defaultAddr := "localhost:9090"
	if env := os.Getenv("TALLY_ADDR"); env != "" {
		defaultAddr = env
	}

	httpPort := flag.Int("http-port", defaultPort, "HTTP listen port")
	tallyAddr := flag.String("tally-addr", defaultAddr, "tally gRPC address")
	flag.Parse()

	version := "dev"
	if v := os.Getenv("VERSION"); v != "" {
		version = v
	}

	ctx := context.Background()

	res, err := tallyotel.NewResource(ctx, "tally-gateway", version)
	if err != nil {
		log.Fatalf("otel resource: %v", err)
	}

	shutdownTracer, err := tallyotel.InitTracer(ctx, res)
	if err != nil {
		log.Fatalf("otel tracer: %v", err)
	}

	shutdownLogger, err := tallyotel.InitLogger(ctx, res, "tally-gateway")
	if err != nil {
		log.Fatalf("otel logger: %v", err)
	}

	conn, err := grpc.NewClient(
		*tallyAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalf("grpc dial: %v", err)
	}

	client := api.NewLogServiceClient(conn)

	gw := gateway.New(client)
	h := health.New()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /produce", gw.Produce)
	mux.HandleFunc("GET /consume", gw.Consume)
	mux.HandleFunc("GET /stream", gw.Stream)
	mux.HandleFunc("GET /healthz", h.Healthz)
	mux.HandleFunc("GET /readyz", h.Readyz)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", *httpPort),
		Handler: otelhttp.NewMiddleware("tally-gateway")(mux),
	}

	go func() {
		slog.Info("gateway listening", "port", *httpPort, "tally", *tallyAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown", "error", err)
	}

	if err := conn.Close(); err != nil {
		slog.Error("grpc conn close", "error", err)
	}

	if err := shutdownTracer(shutdownCtx); err != nil {
		slog.Error("tracer shutdown", "error", err)
	}

	if err := shutdownLogger(shutdownCtx); err != nil {
		slog.Error("logger shutdown", "error", err)
	}

	slog.Info("shutdown complete")
}
