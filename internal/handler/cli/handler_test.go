package cli_test

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	"github.com/w-h-a/tally/internal/client/commit_log/file"
	"github.com/w-h-a/tally/internal/client/consensus"
	"github.com/w-h-a/tally/internal/client/consensus/raft"
	"github.com/w-h-a/tally/internal/client/discovery"
	serfdisc "github.com/w-h-a/tally/internal/client/discovery/serf"
	"github.com/w-h-a/tally/internal/handler/cli"
	grpchandler "github.com/w-h-a/tally/internal/handler/grpc"
	"github.com/w-h-a/tally/internal/handler/http/gateway"
	distributedlog "github.com/w-h-a/tally/internal/service/distributed_log"
	api "github.com/w-h-a/tally/proto/log/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestProduce(t *testing.T) {
	// arrange
	handler := setupTest(t)

	// act
	var buf bytes.Buffer
	err := handler.Produce(context.Background(), "hello", &buf)

	// assert
	require.NoError(t, err)
	require.Equal(t, "0\n", buf.String())
}

func TestConsume(t *testing.T) {
	// arrange
	handler := setupTest(t)

	var discard bytes.Buffer
	err := handler.Produce(context.Background(), "hello", &discard)
	require.NoError(t, err)

	// act
	var buf bytes.Buffer
	err = handler.Consume(context.Background(), 0, &buf)

	// assert
	require.NoError(t, err)
	require.Equal(t, "hello\n", buf.String())
}

func TestConsumeNotFound(t *testing.T) {
	// arrange
	handler := setupTest(t)

	// act
	var buf bytes.Buffer
	err := handler.Consume(context.Background(), 999, &buf)

	// assert
	require.Error(t, err)
}

func TestServers(t *testing.T) {
	// arrange
	handler := setupTest(t)

	// act
	var buf bytes.Buffer
	err := handler.Servers(context.Background(), &buf)

	// assert
	require.NoError(t, err)
	require.Contains(t, buf.String(), "test-node")
	require.Contains(t, buf.String(), "leader")
}

func TestStream(t *testing.T) {
	// arrange
	handler := setupTest(t)

	var discard bytes.Buffer
	for _, v := range []string{"first", "second", "third"} {
		err := handler.Produce(context.Background(), v, &discard)
		require.NoError(t, err)
	}

	// act
	var buf bytes.Buffer
	err := handler.Stream(context.Background(), 0, &buf)

	// assert
	require.NoError(t, err)
	require.Equal(t, "first\nsecond\nthird\n", buf.String())
}

func TestStreamFromOffset(t *testing.T) {
	// arrange
	handler := setupTest(t)

	var discard bytes.Buffer
	for _, v := range []string{"first", "second", "third"} {
		err := handler.Produce(context.Background(), v, &discard)
		require.NoError(t, err)
	}

	// act
	var buf bytes.Buffer
	err := handler.Stream(context.Background(), 2, &buf)

	// assert
	require.NoError(t, err)
	require.Equal(t, "third\n", buf.String())
}

func setupTest(t *testing.T) *cli.Handler {
	t.Helper()

	dir := t.TempDir()

	clog, err := file.NewCommitLog(
		commitlog.WithLocation(filepath.Join(dir, "log")),
		commitlog.WithMaxStoreBytes(1024),
		commitlog.WithMaxIndexBytes(1024),
	)
	require.NoError(t, err)

	disc, err := serfdisc.NewDiscovery(
		discovery.WithNodeName("test-node"),
		discovery.WithBindAddr("127.0.0.1:0"),
		discovery.WithTags(map[string]string{
			"raft_addr": "localhost:0",
			"rpc_addr":  "localhost:0",
		}),
	)
	require.NoError(t, err)

	service := distributedlog.New(clog, disc, "test-node", "localhost:0")

	r, err := raft.NewConsensus(
		consensus.WithApplyFn(service.ApplyFn()),
		consensus.WithSnapshotFn(service.SnapshotFn()),
		consensus.WithRestoreFn(service.RestoreFn()),
		consensus.WithDataDir(filepath.Join(dir, "raft")),
		consensus.WithBindAddr("127.0.0.1:0"),
		consensus.WithLocalID("test-node"),
		consensus.WithBootstrap(true),
		raft.WithLogStore(hraft.NewInmemStore()),
		raft.WithStableStore(hraft.NewInmemStore()),
	)
	require.NoError(t, err)

	service.SetConsensus(r)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, service.WaitForLeader(ctx))

	grpcSrv := grpc.NewServer()
	api.RegisterLogServiceServer(grpcSrv, grpchandler.New(service))

	lis := bufconn.Listen(1024 * 1024)

	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			t.Error(err)
		}
	}()

	t.Cleanup(func() {
		grpcSrv.GracefulStop()
		disc.Leave(context.Background())
		service.Close(context.Background())
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close()
	})

	client := api.NewLogServiceClient(conn)

	gw := gateway.New(client)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /produce", gw.Produce)
	mux.HandleFunc("GET /consume", gw.Consume)
	mux.HandleFunc("GET /servers", gw.GetServers)
	mux.HandleFunc("GET /stream", gw.Stream)

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return cli.New(srv.URL, srv.Client())
}
