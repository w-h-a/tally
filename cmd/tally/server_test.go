package main

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	"github.com/w-h-a/tally/internal/client/commit_log/file"
	distributedlog "github.com/w-h-a/tally/internal/service/distributed_log"
	api "github.com/w-h-a/tally/proto/log/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestServer(t *testing.T) {
	client := setupTest(t)

	t.Run("produce and consume a record", func(t *testing.T) {
		// arrange
		want := &api.Record{Value: []byte("hello world")}

		// act
		produceResp, err := client.Produce(context.Background(), &api.ProduceRequest{Record: want})

		// assert
		require.NoError(t, err)
		require.Equal(t, uint64(0), produceResp.Offset)

		// act
		consumeResp, err := client.Consume(context.Background(), &api.ConsumeRequest{Offset: produceResp.Offset})

		// assert
		require.NoError(t, err)
		require.Equal(t, want.Value, consumeResp.Record.Value)
	})

	t.Run("produce multiple and consume each", func(t *testing.T) {
		// arrange
		records := []*api.Record{
			{Value: []byte("first")},
			{Value: []byte("second")},
			{Value: []byte("third")},
		}

		// act + assert (produce)
		// The previous subtest already wrote offset 0, so offsets here start at 1.
		for i, rec := range records {
			resp, err := client.Produce(context.Background(), &api.ProduceRequest{Record: rec})
			require.NoError(t, err)
			require.Equal(t, uint64(i+1), resp.Offset)
		}

		// act + assert (consume)
		for i, rec := range records {
			resp, err := client.Consume(context.Background(), &api.ConsumeRequest{Offset: uint64(i + 1)})
			require.NoError(t, err)
			require.Equal(t, rec.Value, resp.Record.Value)
		}
	})

	t.Run("consume out of range returns not found", func(t *testing.T) {
		// act
		_, err := client.Consume(context.Background(), &api.ConsumeRequest{Offset: 999})

		// assert
		require.Error(t, err)
		got := status.Code(err)
		require.Equal(t, codes.NotFound, got)
	})
}

func setupTest(t *testing.T) api.LogServiceClient {
	t.Helper()

	clog, err := file.NewCommitLog(
		commitlog.WithLocation(t.TempDir()),
		commitlog.WithMaxStoreBytes(1024),
		commitlog.WithMaxIndexBytes(1024),
	)
	require.NoError(t, err)

	service := distributedlog.New(clog)

	srv := newGRPCServer(Config{Service: service})

	lis := bufconn.Listen(1024 * 1024)

	go func() {
		if err := srv.Serve(lis); err != nil {
			t.Error(err)
		}
	}()

	t.Cleanup(func() {
		srv.GracefulStop()
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

	return client
}
