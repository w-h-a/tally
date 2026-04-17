package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	"github.com/w-h-a/tally/internal/client/commit_log/file"
	grpchandler "github.com/w-h-a/tally/internal/handler/grpc"
	"github.com/w-h-a/tally/internal/handler/http/health"
	distributedlog "github.com/w-h-a/tally/internal/service/distributed_log"
	tallyotel "github.com/w-h-a/tally/internal/util/otel"
	api "github.com/w-h-a/tally/proto/log/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

func main() {
	defaultHealthPort := 8080
	if env := os.Getenv("TALLY_HEALTH_PORT"); env != "" {
		if p, err := strconv.Atoi(env); err == nil && p > 0 {
			defaultHealthPort = p
		}
	}

	defaultGRPCPort := 9090
	if env := os.Getenv("TALLY_GRPC_PORT"); env != "" {
		if p, err := strconv.Atoi(env); err == nil && p > 0 {
			defaultGRPCPort = p
		}
	}

	defaultDataDir := "/tmp/tally-data"
	if env := os.Getenv("TALLY_DATA_DIR"); env != "" {
		defaultDataDir = env
	}

	healthPort := flag.Int("health-port", defaultHealthPort, "HTTP health check port")
	grpcPort := flag.Int("grpc-port", defaultGRPCPort, "gRPC listen port")
	dataDir := flag.String("data-dir", defaultDataDir, "commit log data directory")
	flag.Parse()

	version := "dev"
	if v := os.Getenv("VERSION"); v != "" {
		version = v
	}

	ctx := context.Background()

	res, err := tallyotel.NewResource(ctx, "tally", version)
	if err != nil {
		log.Fatalf("otel resource: %v", err)
	}

	shutdownTracer, err := tallyotel.InitTracer(ctx, res)
	if err != nil {
		log.Fatalf("otel tracer: %v", err)
	}

	shutdownLogger, err := tallyotel.InitLogger(ctx, res, "tally")
	if err != nil {
		log.Fatalf("otel logger: %v", err)
	}

	clog, err := file.NewCommitLog(
		commitlog.WithLocation(*dataDir),
		commitlog.WithMaxStoreBytes(1024*1024),
		commitlog.WithMaxIndexBytes(1024*1024),
	)
	if err != nil {
		log.Fatalf("commit log: %v", err)
	}

	service := distributedlog.New(clog)

	grpcSrv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	api.RegisterLogServiceServer(grpcSrv, grpchandler.New(service))

	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		log.Fatalf("grpc listen: %v", err)
	}

	go func() {
		slog.Info("grpc listening", "port", *grpcPort)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			log.Fatalf("grpc server: %v", err)
		}
	}()

	h := health.New()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.Healthz)
	mux.HandleFunc("GET /readyz", h.Readyz)

	httpSrv := &http.Server{
		Addr:    fmt.Sprintf(":%d", *healthPort),
		Handler: mux,
	}

	go func() {
		slog.Info("health listening", "port", *healthPort)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	grpcSrv.GracefulStop()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown", "error", err)
	}

	if err := service.Close(shutdownCtx); err != nil {
		slog.Error("service close", "error", err)
	}

	if err := shutdownTracer(shutdownCtx); err != nil {
		slog.Error("tracer shutdown", "error", err)
	}

	if err := shutdownLogger(shutdownCtx); err != nil {
		slog.Error("logger shutdown", "error", err)
	}

	slog.Info("shutdown complete")
}
