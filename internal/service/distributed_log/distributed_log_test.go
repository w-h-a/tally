package distributedlog_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	"github.com/w-h-a/tally/internal/client/commit_log/file"
	"github.com/w-h-a/tally/internal/client/consensus"
	"github.com/w-h-a/tally/internal/client/consensus/raft"
	distributedlog "github.com/w-h-a/tally/internal/service/distributed_log"
	api "github.com/w-h-a/tally/proto/log/v1"
)

func TestAppendAndRead(t *testing.T) {
	service := setupTest(t)

	// act
	offset, err := service.Append(context.Background(), &api.Record{Value: []byte("hello")})

	// assert
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	// act
	rec, err := service.Read(context.Background(), 0)

	// assert
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), rec.Value)
}

func TestAppendMultiple(t *testing.T) {
	service := setupTest(t)

	// arrange
	values := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	// act + assert (append)
	for i, val := range values {
		offset, err := service.Append(context.Background(), &api.Record{Value: val})
		require.NoError(t, err)
		require.Equal(t, uint64(i), offset)
	}

	// act + assert (read)
	for i, val := range values {
		rec, err := service.Read(context.Background(), uint64(i))
		require.NoError(t, err)
		require.Equal(t, val, rec.Value)
	}
}

func TestWaitForLeader(t *testing.T) {
	service := setupTest(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := service.WaitForLeader(ctx)
	require.NoError(t, err)
}

func TestGetServers(t *testing.T) {
	service := setupTest(t)

	// act
	servers, err := service.GetServers(context.Background())

	// assert
	require.NoError(t, err)
	require.Len(t, servers, 1)
	require.Equal(t, "test-node-0", servers[0].Id)
	require.NotEmpty(t, servers[0].RpcAddr)
	require.True(t, servers[0].IsLeader)
}

func TestSnapshotAndRestore(t *testing.T) {
	service := setupTest(t)

	// arrange: append records
	values := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}
	for i, val := range values {
		offset, err := service.Append(context.Background(), &api.Record{Value: val})
		require.NoError(t, err)
		require.Equal(t, uint64(i), offset)
	}

	// act: snapshot
	reader, err := service.SnapshotFn()()
	require.NoError(t, err)

	// act: restore (Reset + re-append)
	err = service.RestoreFn()(reader)
	require.NoError(t, err)

	// assert: all records recovered
	for i, val := range values {
		rec, err := service.Read(context.Background(), uint64(i))
		require.NoError(t, err)
		require.Equal(t, val, rec.Value)
	}
}

func setupTest(t *testing.T) *distributedlog.Service {
	t.Helper()

	dir := t.TempDir()

	clog, err := file.NewCommitLog(
		commitlog.WithLocation(filepath.Join(dir, "log")),
		commitlog.WithMaxStoreBytes(1024),
		commitlog.WithMaxIndexBytes(1024),
	)
	require.NoError(t, err)

	service := distributedlog.New(clog, "test-node-0", "localhost:0")

	r, err := raft.NewConsensus(
		consensus.WithApplyFn(service.ApplyFn()),
		consensus.WithSnapshotFn(service.SnapshotFn()),
		consensus.WithRestoreFn(service.RestoreFn()),
		consensus.WithDataDir(filepath.Join(dir, "raft")),
		consensus.WithBindAddr("localhost:0"),
		consensus.WithLocalID("test-node-0"),
		consensus.WithBootstrap(true),
		raft.WithLogStore(hraft.NewInmemStore()),
		raft.WithStableStore(hraft.NewInmemStore()),
	)
	require.NoError(t, err)

	service.SetConsensus(r)

	t.Cleanup(func() {
		service.Close(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, service.WaitForLeader(ctx))

	return service
}
