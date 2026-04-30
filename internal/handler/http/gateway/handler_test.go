package gateway_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	gatewayapi "github.com/w-h-a/tally/api/gateway/v1"
	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	"github.com/w-h-a/tally/internal/client/commit_log/file"
	"github.com/w-h-a/tally/internal/client/consensus"
	"github.com/w-h-a/tally/internal/client/consensus/raft"
	"github.com/w-h-a/tally/internal/client/discovery"
	serfdisc "github.com/w-h-a/tally/internal/client/discovery/serf"
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
	srv := setupTest(t)
	defer srv.Close()

	// act
	resp, err := http.Post(srv.URL+"/produce", "application/json", strings.NewReader(`{"value":"hello"}`))
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got gatewayapi.ProduceResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, uint64(0), got.Offset)
}

func TestConsume(t *testing.T) {
	// arrange
	srv := setupTest(t)
	defer srv.Close()

	_, err := http.Post(srv.URL+"/produce", "application/json", strings.NewReader(`{"value":"hello"}`))
	require.NoError(t, err)

	// act
	resp, err := http.Get(srv.URL + "/consume?offset=0")
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got gatewayapi.ConsumeResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Equal(t, uint64(0), got.Offset)
	require.Equal(t, "hello", got.Value)
}

func TestConsumeNotFound(t *testing.T) {
	// arrange
	srv := setupTest(t)
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL + "/consume?offset=999")
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestConsumeMissingOffset(t *testing.T) {
	// arrange
	srv := setupTest(t)
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL + "/consume")
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestGetServers(t *testing.T) {
	// arrange
	srv := setupTest(t)
	defer srv.Close()

	// act
	resp, err := http.Get(srv.URL + "/servers")
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got gatewayapi.GetServersResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got.Servers, 1)
	require.Equal(t, "test-node", got.Servers[0].ID)
	require.NotEmpty(t, got.Servers[0].RpcAddr)
	require.True(t, got.Servers[0].IsLeader)
}

func TestStream(t *testing.T) {
	// arrange
	srv := setupTest(t)
	defer srv.Close()

	for i := range 3 {
		_, err := http.Post(srv.URL+"/produce", "application/json",
			strings.NewReader(fmt.Sprintf(`{"value":"msg-%d"}`, i)))
		require.NoError(t, err)
	}

	// act
	resp, err := http.Get(srv.URL + "/stream?from=0")
	require.NoError(t, err)
	defer resp.Body.Close()

	// assert
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "text/event-stream", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	events := parseSSE(string(body))
	require.Len(t, events, 3)

	for i, ev := range events {
		require.Equal(t, fmt.Sprintf("%d", i), ev.id)

		var rec gatewayapi.ConsumeResponse
		require.NoError(t, json.Unmarshal([]byte(ev.data), &rec))
		require.Equal(t, uint64(i), rec.Offset)
		require.Equal(t, fmt.Sprintf("msg-%d", i), rec.Value)
	}
}

type sseEvent struct {
	id   string
	data string
}

func parseSSE(raw string) []sseEvent {
	var events []sseEvent
	var current sseEvent

	for _, line := range strings.Split(raw, "\n") {
		switch {
		case strings.HasPrefix(line, "id: "):
			current.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			current.data = strings.TrimPrefix(line, "data: ")
		case line == "":
			if current.id != "" || current.data != "" {
				events = append(events, current)
				current = sseEvent{}
			}
		}
	}

	return events
}

func setupTest(t *testing.T) *httptest.Server {
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

	return httptest.NewServer(mux)
}
