package distributedlog_test

import (
	"context"
	"fmt"
	"net"
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
	distributedlog "github.com/w-h-a/tally/internal/service/distributed_log"
	"github.com/w-h-a/tally/internal/service/membership"
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

func TestFollowerReads(t *testing.T) {
	// arrange
	nodes := setupTestCluster(t, 2)
	requireServerCount(t, nodes[0].dlog, 2, 10*time.Second)

	values := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	// act (write to leader)
	for i, val := range values {
		offset, err := nodes[0].dlog.Append(context.Background(), &api.Record{Value: val})
		require.NoError(t, err)
		require.Equal(t, uint64(i), offset)
	}

	// Follower reads are eventually consistent: the follower applies
	// committed entries via the FSM after the leader's Apply returns.
	requireReplication(t, nodes[1].dlog, uint64(len(values)-1), 10*time.Second)

	// assert (read from follower)
	for i, val := range values {
		rec, err := nodes[1].dlog.Read(context.Background(), uint64(i))
		require.NoError(t, err)
		require.Equal(t, val, rec.Value)
	}
}

type testNode struct {
	dlog       *distributedlog.Service
	membership *membership.Service
}

func setupTestCluster(t *testing.T, count int) []testNode {
	t.Helper()

	serfPorts := make([]int, count)
	raftPorts := make([]int, count)
	for i := range count {
		serfPorts[i] = getFreePort(t)
		raftPorts[i] = getFreePort(t)
	}

	nodes := make([]testNode, count)

	for i := range count {
		dir := t.TempDir()
		nodeID := fmt.Sprintf("node-%d", i)
		raftAddr := fmt.Sprintf("127.0.0.1:%d", raftPorts[i])
		serfAddr := fmt.Sprintf("127.0.0.1:%d", serfPorts[i])

		clog, err := file.NewCommitLog(
			commitlog.WithLocation(filepath.Join(dir, "log")),
			commitlog.WithMaxStoreBytes(1024),
			commitlog.WithMaxIndexBytes(1024),
		)
		require.NoError(t, err)

		dlog := distributedlog.New(clog, nodeID, raftAddr)

		raftConsensus, err := raft.NewConsensus(
			consensus.WithApplyFn(dlog.ApplyFn()),
			consensus.WithSnapshotFn(dlog.SnapshotFn()),
			consensus.WithRestoreFn(dlog.RestoreFn()),
			consensus.WithDataDir(filepath.Join(dir, "raft")),
			consensus.WithBindAddr(raftAddr),
			consensus.WithLocalID(nodeID),
			consensus.WithBootstrap(i == 0),
			raft.WithLogStore(hraft.NewInmemStore()),
			raft.WithStableStore(hraft.NewInmemStore()),
		)
		require.NoError(t, err)

		dlog.SetConsensus(raftConsensus)

		var startJoinAddrs []string
		if i > 0 {
			startJoinAddrs = []string{fmt.Sprintf("127.0.0.1:%d", serfPorts[0])}
		}

		disc, err := serfdisc.NewDiscovery(
			discovery.WithNodeName(nodeID),
			discovery.WithBindAddr(serfAddr),
			discovery.WithTags(map[string]string{
				"rpc_addr": raftAddr,
			}),
			discovery.WithStartJoinAddrs(startJoinAddrs),
		)
		require.NoError(t, err)

		svc := membership.New(disc, raftConsensus)
		svc.Start()

		nodes[i] = testNode{
			dlog:       dlog,
			membership: svc,
		}

		t.Cleanup(func() {
			svc.Close(context.Background())
			dlog.Close(context.Background())
		})

		if i == 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			require.NoError(t, dlog.WaitForLeader(ctx))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, nodes[0].dlog.WaitForLeader(ctx))

	return nodes
}

func requireServerCount(t *testing.T, dlog *distributedlog.Service, expected int, timeout time.Duration) {
	t.Helper()

	deadline := time.After(timeout)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			servers, _ := dlog.GetServers(context.Background())
			t.Fatalf("timed out waiting for %d servers, got %d", expected, len(servers))
		case <-ticker.C:
			servers, err := dlog.GetServers(context.Background())
			if err != nil {
				continue
			}
			if len(servers) == expected {
				return
			}
		}
	}
}

func requireReplication(t *testing.T, dlog *distributedlog.Service, offset uint64, timeout time.Duration) {
	t.Helper()

	deadline := time.After(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for offset %d to replicate", offset)
		case <-ticker.C:
			_, err := dlog.Read(context.Background(), offset)
			if err == nil {
				return
			}
		}
	}
}

func getFreePort(t *testing.T) int {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	return port
}
