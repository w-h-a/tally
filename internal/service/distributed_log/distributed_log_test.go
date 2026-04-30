package distributedlog_test

import (
	"context"
	"fmt"
	"io"
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
	grpchandler "github.com/w-h-a/tally/internal/handler/grpc"
	distributedlog "github.com/w-h-a/tally/internal/service/distributed_log"
	"github.com/w-h-a/tally/internal/service/membership"
	api "github.com/w-h-a/tally/proto/log/v1"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func TestThreeNodeReplication(t *testing.T) {
	// arrange
	nodes := setupTestCluster(t, 3)
	requireServerCount(t, nodes[0].dlog, 3, 10*time.Second)

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

	// wait for replication to all followers
	for _, follower := range nodes[1:] {
		requireReplication(t, follower.dlog, uint64(len(values)-1), 10*time.Second)
	}

	// assert (all 3 nodes return the same records)
	for _, node := range nodes {
		for i, val := range values {
			rec, err := node.dlog.Read(context.Background(), uint64(i))
			require.NoError(t, err)
			require.Equal(t, val, rec.Value)
		}
	}
}

func TestMultiNodeIntegration(t *testing.T) {
	// Registered first → runs last (LIFO) → checks after all node cleanups.
	t.Cleanup(func() {
		goleak.VerifyNone(t,
			goleak.IgnoreAnyFunction("github.com/hashicorp/raft.(*NetworkTransport).handleCommand"),
		)
	})

	// arrange
	nodes := setupIntegrationCluster(t, 3)
	requireServerCount(t, nodes[0].dlog, 3, 10*time.Second)

	values := [][]byte{
		[]byte("alpha"),
		[]byte("bravo"),
		[]byte("charlie"),
		[]byte("delta"),
		[]byte("echo"),
	}

	// act (write to leader)
	for i, val := range values {
		resp, err := nodes[0].client.Produce(context.Background(), &api.ProduceRequest{
			Record: &api.Record{Value: val},
		})
		require.NoError(t, err)
		require.Equal(t, uint64(i), resp.Offset)
	}

	for _, node := range nodes[1:] {
		requireReplication(t, node.dlog, uint64(len(values)-1), 10*time.Second)
	}

	t.Run("ordering consistent across all nodes", func(t *testing.T) {
		// assert
		for _, node := range nodes {
			for i, val := range values {
				resp, err := node.client.Consume(context.Background(), &api.ConsumeRequest{
					Offset: uint64(i),
				})
				require.NoError(t, err)
				require.Equal(t, val, resp.Record.Value)
			}
		}
	})

	t.Run("GetServers returns 3 servers with correct addresses and leader", func(t *testing.T) {
		// act
		resp, err := nodes[0].client.GetServers(context.Background(), &api.GetServersRequest{})
		require.NoError(t, err)

		// assert
		require.Len(t, resp.Servers, 3)

		leaderCount := 0

		for _, srv := range resp.Servers {
			if srv.IsLeader {
				leaderCount++
			}

			var found bool

			for _, node := range nodes {
				if node.nodeID == srv.Id {
					require.Equal(t, node.grpcAddr, srv.RpcAddr,
						"server %s: RpcAddr should be gRPC address, not Raft address", srv.Id)
					found = true
					break
				}
			}

			require.True(t, found, "server %s not found in test nodes", srv.Id)
		}

		require.Equal(t, 1, leaderCount, "expected exactly 1 leader")
	})

	t.Run("ConsumeStream from follower returns all records in order", func(t *testing.T) {
		// act
		stream, err := nodes[1].client.ConsumeStream(context.Background(), &api.ConsumeStreamRequest{
			Offset: 0,
		})
		require.NoError(t, err)

		// assert
		for i, val := range values {
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, val, resp.Record.Value)
			require.Equal(t, uint64(i), resp.Record.Offset)
		}

		_, err = stream.Recv()
		require.Equal(t, io.EOF, err)
	})
}

// --- helpers ---

func setupTest(t *testing.T) *distributedlog.Service {
	t.Helper()

	dir := t.TempDir()

	clog, err := file.NewCommitLog(
		commitlog.WithLocation(filepath.Join(dir, "log")),
		commitlog.WithMaxStoreBytes(1024),
		commitlog.WithMaxIndexBytes(1024),
	)
	require.NoError(t, err)

	disc, err := serfdisc.NewDiscovery(
		discovery.WithNodeName("test-node-0"),
		discovery.WithBindAddr("127.0.0.1:0"),
		discovery.WithTags(map[string]string{
			"raft_addr": "localhost:0",
			"rpc_addr":  "localhost:0",
		}),
	)
	require.NoError(t, err)

	service := distributedlog.New(clog, disc, "test-node-0", "localhost:0")

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
		disc.Leave(context.Background())
		service.Close(context.Background())
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, service.WaitForLeader(ctx))

	return service
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

		var startJoinAddrs []string
		if i > 0 {
			startJoinAddrs = []string{fmt.Sprintf("127.0.0.1:%d", serfPorts[0])}
		}

		disc, err := serfdisc.NewDiscovery(
			discovery.WithNodeName(nodeID),
			discovery.WithBindAddr(serfAddr),
			discovery.WithTags(map[string]string{
				"raft_addr": raftAddr,
				"rpc_addr":  raftAddr,
			}),
			discovery.WithStartJoinAddrs(startJoinAddrs),
		)
		require.NoError(t, err)

		dlog := distributedlog.New(clog, disc, nodeID, raftAddr)

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

type integrationNode struct {
	dlog       *distributedlog.Service
	membership *membership.Service
	client     api.LogServiceClient
	nodeID     string
	grpcAddr   string
}

func setupIntegrationCluster(t *testing.T, count int) []integrationNode {
	t.Helper()

	serfPorts := make([]int, count)
	raftPorts := make([]int, count)
	grpcPorts := make([]int, count)
	for i := range count {
		serfPorts[i] = getFreePort(t)
		raftPorts[i] = getFreePort(t)
		grpcPorts[i] = getFreePort(t)
	}

	nodes := make([]integrationNode, count)

	for i := range count {
		dir := t.TempDir()
		nodeID := fmt.Sprintf("node-%d", i)
		raftAddr := fmt.Sprintf("127.0.0.1:%d", raftPorts[i])
		serfAddr := fmt.Sprintf("127.0.0.1:%d", serfPorts[i])
		grpcAddr := fmt.Sprintf("127.0.0.1:%d", grpcPorts[i])

		clog, err := file.NewCommitLog(
			commitlog.WithLocation(filepath.Join(dir, "log")),
			commitlog.WithMaxStoreBytes(1024),
			commitlog.WithMaxIndexBytes(1024),
		)
		require.NoError(t, err)

		var startJoinAddrs []string
		if i > 0 {
			startJoinAddrs = []string{fmt.Sprintf("127.0.0.1:%d", serfPorts[0])}
		}

		disc, err := serfdisc.NewDiscovery(
			discovery.WithNodeName(nodeID),
			discovery.WithBindAddr(serfAddr),
			discovery.WithTags(map[string]string{
				"raft_addr": raftAddr,
				"rpc_addr":  grpcAddr,
			}),
			discovery.WithStartJoinAddrs(startJoinAddrs),
		)
		require.NoError(t, err)

		dlog := distributedlog.New(clog, disc, nodeID, grpcAddr)

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

		svc := membership.New(disc, raftConsensus)
		svc.Start()

		lis, err := net.Listen("tcp", grpcAddr)
		require.NoError(t, err)

		grpcSrv := grpc.NewServer()
		api.RegisterLogServiceServer(grpcSrv, grpchandler.New(dlog))

		go grpcSrv.Serve(lis)

		conn, err := grpc.NewClient(
			grpcAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		require.NoError(t, err)

		nodes[i] = integrationNode{
			dlog:       dlog,
			membership: svc,
			client:     api.NewLogServiceClient(conn),
			nodeID:     nodeID,
			grpcAddr:   grpcAddr,
		}

		t.Cleanup(func() {
			conn.Close()
			grpcSrv.GracefulStop()
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
