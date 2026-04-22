package grpchandler_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	"github.com/w-h-a/tally/internal/client/commit_log/file"
	grpchandler "github.com/w-h-a/tally/internal/handler/grpc"
	distributedlog "github.com/w-h-a/tally/internal/service/distributed_log"
	api "github.com/w-h-a/tally/proto/log/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestHandler(t *testing.T) {
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

func TestHandlerStreaming(t *testing.T) {
	client := setupTest(t)

	// Produce 10 records via stream so all streaming subtests can consume them.
	records := make([]*api.Record, 10)
	for i := range records {
		records[i] = &api.Record{Value: []byte(fmt.Sprintf("record-%d", i))}
	}

	t.Run("produce 10 records via stream and verify all offsets", func(t *testing.T) {
		// arrange
		stream, err := client.ProduceStream(context.Background())
		require.NoError(t, err)

		// act
		for _, rec := range records {
			err := stream.Send(&api.ProduceStreamRequest{Record: rec})
			require.NoError(t, err)
		}

		err = stream.CloseSend()
		require.NoError(t, err)

		// assert
		for i := range records {
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, uint64(i), resp.Offset)
		}

		_, err = stream.Recv()
		require.Equal(t, io.EOF, err)
	})

	t.Run("consume stream from offset 0 receives all 10 records", func(t *testing.T) {
		// act
		stream, err := client.ConsumeStream(context.Background(), &api.ConsumeStreamRequest{Offset: 0})
		require.NoError(t, err)

		// assert
		for i, want := range records {
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, want.Value, resp.Record.Value)
			require.Equal(t, uint64(i), resp.Record.Offset)
		}

		_, err = stream.Recv()
		require.Equal(t, io.EOF, err)
	})

	t.Run("consume stream from mid-offset receives correct subset", func(t *testing.T) {
		// arrange
		midOffset := uint64(5)

		// act
		stream, err := client.ConsumeStream(context.Background(), &api.ConsumeStreamRequest{Offset: midOffset})
		require.NoError(t, err)

		// assert
		for i := midOffset; i < uint64(len(records)); i++ {
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, records[i].Value, resp.Record.Value)
			require.Equal(t, i, resp.Record.Offset)
		}

		_, err = stream.Recv()
		require.Equal(t, io.EOF, err)
	})
}

func TestConsumeEmptyLog(t *testing.T) {
	// arrange
	client := setupTest(t)

	// act
	_, err := client.Consume(context.Background(), &api.ConsumeRequest{Offset: 0})

	// assert
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestGetServers(t *testing.T) {
	// arrange
	client := setupTest(t)

	// act
	resp, err := client.GetServers(context.Background(), &api.GetServersRequest{})

	// assert
	require.NoError(t, err)
	require.Len(t, resp.Servers, 1)
	require.Equal(t, "test-node", resp.Servers[0].Id)
	require.Equal(t, "localhost:0", resp.Servers[0].RpcAddr)
	require.True(t, resp.Servers[0].IsLeader)
}

func setupTest(t *testing.T) api.LogServiceClient {
	t.Helper()

	clog, err := file.NewCommitLog(
		commitlog.WithLocation(t.TempDir()),
		commitlog.WithMaxStoreBytes(1024),
		commitlog.WithMaxIndexBytes(1024),
	)
	require.NoError(t, err)

	service := distributedlog.New(clog, "test-node", "localhost:0")

	srv := grpc.NewServer()
	api.RegisterLogServiceServer(srv, grpchandler.New(service))

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

	return api.NewLogServiceClient(conn)
}
