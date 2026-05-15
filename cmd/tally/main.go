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
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	"github.com/w-h-a/tally/internal/client/commit_log/file"
	"github.com/w-h-a/tally/internal/client/consensus"
	"github.com/w-h-a/tally/internal/client/consensus/raft"
	"github.com/w-h-a/tally/internal/client/discovery"
	serfdisc "github.com/w-h-a/tally/internal/client/discovery/serf"
	grpchandler "github.com/w-h-a/tally/internal/handler/grpc"
	"github.com/w-h-a/tally/internal/handler/http/health"
	distributedlog "github.com/w-h-a/tally/internal/service/distributed_log"
	"github.com/w-h-a/tally/internal/service/membership"
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

	defaultNodeID, _ := os.Hostname()
	if env := os.Getenv("TALLY_NODE_ID"); env != "" {
		defaultNodeID = env
	}

	defaultRPCAddr := ""
	if env := os.Getenv("TALLY_RPC_ADDR"); env != "" {
		defaultRPCAddr = env
	}

	defaultRaftAddr := "127.0.0.1:0"
	if env := os.Getenv("TALLY_RAFT_ADDR"); env != "" {
		defaultRaftAddr = env
	}

	defaultBootstrap := true
	if env := os.Getenv("TALLY_BOOTSTRAP"); env != "" {
		if b, err := strconv.ParseBool(env); err == nil {
			defaultBootstrap = b
		}
	}

	defaultSerfAddr := "127.0.0.1:0"
	if env := os.Getenv("TALLY_SERF_ADDR"); env != "" {
		defaultSerfAddr = env
	}

	defaultSerfJoin := ""
	if env := os.Getenv("TALLY_SERF_JOIN"); env != "" {
		defaultSerfJoin = env
	}

	healthPort := flag.Int("health-port", defaultHealthPort, "HTTP health check port")
	grpcPort := flag.Int("grpc-port", defaultGRPCPort, "gRPC listen port")
	dataDir := flag.String("data-dir", defaultDataDir, "commit log data directory")
	nodeID := flag.String("node-id", defaultNodeID, "unique node identifier")
	rpcAddr := flag.String("rpc-addr", defaultRPCAddr, "advertised gRPC address for peer discovery")
	raftAddr := flag.String("raft-addr", defaultRaftAddr, "Raft peer traffic bind address")
	bootstrap := flag.Bool("bootstrap", defaultBootstrap, "bootstrap Raft cluster as single voter")
	serfAddr := flag.String("serf-addr", defaultSerfAddr, "Serf gossip bind address")
	serfJoin := flag.String("serf-join", defaultSerfJoin, "comma-separated Serf seed addresses")
	flag.Parse()

	if *rpcAddr == "" {
		*rpcAddr = fmt.Sprintf("localhost:%d", *grpcPort)
	}

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

	var startJoinAddrs []string
	if *serfJoin != "" {
		startJoinAddrs = strings.Split(*serfJoin, ",")
	}

	disc, err := serfdisc.NewDiscovery(
		discovery.WithNodeName(*nodeID),
		discovery.WithBindAddr(*serfAddr),
		discovery.WithTags(map[string]string{
			"raft_addr": *raftAddr,
			"rpc_addr":  *rpcAddr,
		}),
		discovery.WithStartJoinAddrs(startJoinAddrs),
	)
	if err != nil {
		log.Fatalf("discovery: %v", err)
	}

	logSvc := distributedlog.New(clog, disc, *nodeID, *rpcAddr)

	cons, err := raft.NewConsensus(
		consensus.WithApplyFn(logSvc.ApplyFn()),
		consensus.WithSnapshotFn(logSvc.SnapshotFn()),
		consensus.WithRestoreFn(logSvc.RestoreFn()),
		consensus.WithDataDir(filepath.Join(*dataDir, "raft")),
		consensus.WithBindAddr(*raftAddr),
		consensus.WithLocalID(*nodeID),
		consensus.WithBootstrap(*bootstrap),
	)
	if err != nil {
		log.Fatalf("consensus: %v", err)
	}

	logSvc.SetConsensus(cons)

	membershipSvc := membership.New(disc, cons)
	membershipSvc.Start()

	grpcSrv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	api.RegisterLogServiceServer(grpcSrv, grpchandler.New(logSvc))

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

	h := health.New(version, func() string { return logSvc.Ready(context.Background()) })
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
	sig := <-sigCh

	go func() {
		<-sigCh
		slog.Warn("received second signal, forcing exit")
		os.Exit(1)
	}()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	consensusState := cons.State(shutdownCtx)

	slog.Info("shutdown starting", "signal", sig.String(), "node_id", *nodeID, "consensus_state", consensusState)

	start := time.Now()

	// 1. gRPC graceful shutdown
	grpcDone := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcDone)
	}()

	drainTimer := time.NewTimer(15 * time.Second)
	select {
	case <-grpcDone:
		drainTimer.Stop()
	case <-drainTimer.C:
		slog.Warn("grpc graceful stop timed out, forcing", "node_id", *nodeID)
		grpcSrv.Stop()
		<-grpcDone
	}

	// 2. Stop membership loop
	membershipSvc.Stop(shutdownCtx)

	// 3. Leadership transfer if leader
	leadershipTransferred := false
	if cons.State(shutdownCtx) == "Leader" {
		if err := cons.LeadershipTransfer(shutdownCtx); err != nil {
			slog.Warn("leadership transfer failed, cluster will re-elect", "node_id", *nodeID, "error", err)
		} else {
			leadershipTransferred = true
		}
	}

	// 4. Leave discovery cluster
	discoveryLeft := true
	if err := membershipSvc.Close(shutdownCtx); err != nil {
		slog.Error("membership close", "error", err)
		discoveryLeft = false
	}

	// 5. Close distributed log
	logClosed := true
	if err := logSvc.Close(shutdownCtx); err != nil {
		slog.Error("log close", "error", err)
		logClosed = false
	}

	// 6. HTTP shutdown
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown", "error", err)
	}

	slog.Info("shutdown complete",
		"node_id", *nodeID,
		"drain_duration_ms", time.Since(start).Milliseconds(),
		"leadership_transferred", leadershipTransferred,
		"discovery_left", discoveryLeft,
		"commitlog_closed", logClosed,
	)

	if err := shutdownTracer(shutdownCtx); err != nil {
		slog.Error("tracer shutdown", "error", err)
	}

	if err := shutdownLogger(shutdownCtx); err != nil {
		slog.Error("logger shutdown", "error", err)
	}

	slog.Info("shutdown complete")
}
