package membership_test

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
)

func TestJoinAndLeave(t *testing.T) {
	// arrange
	members := setupTestCluster(t, 3)

	// act + assert (join)
	requireServerCount(t, members[0].consensus, 3, 10*time.Second)

	// act (leave)
	err := members[2].service.Close(context.Background())
	require.NoError(t, err)

	// assert (leave)
	requireServerCount(t, members[0].consensus, 2, 10*time.Second)
}

type testMember struct {
	service   *membership.Service
	consensus consensus.Consensus
}

func setupTestCluster(t *testing.T, count int) []testMember {
	t.Helper()

	serfPorts := make([]int, count)
	raftPorts := make([]int, count)
	for i := range count {
		serfPorts[i] = getFreePort(t)
		raftPorts[i] = getFreePort(t)
	}

	members := make([]testMember, count)

	for i := range count {
		dir := t.TempDir()
		nodeID := fmt.Sprintf("node-%d", i)
		raftAddr := fmt.Sprintf("127.0.0.1:%d", raftPorts[i])
		serfAddr := fmt.Sprintf("127.0.0.1:%d", serfPorts[i])

		// CommitLog + DistributedLog provide FSM callbacks for Raft
		clog, err := file.NewCommitLog(
			commitlog.WithLocation(filepath.Join(dir, "log")),
			commitlog.WithMaxStoreBytes(1024),
			commitlog.WithMaxIndexBytes(1024),
		)
		require.NoError(t, err)

		// Serf — nodes 1+ join node 0
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

		// Raft — only node 0 bootstraps
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

		// MembershipService bridges discovery → consensus
		svc := membership.New(disc, raftConsensus)
		svc.Start()

		members[i] = testMember{
			service:   svc,
			consensus: raftConsensus,
		}

		t.Cleanup(func() {
			svc.Close(context.Background())
			raftConsensus.Close(context.Background())
		})

		if i == 0 {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			require.NoError(t, members[0].consensus.WaitForLeader(ctx))
		}
	}

	// Wait for leader election before returning
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, members[0].consensus.WaitForLeader(ctx))

	return members
}

func requireServerCount(t *testing.T, c consensus.Consensus, expected int, timeout time.Duration) {
	t.Helper()

	deadline := time.After(timeout)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			servers, _ := c.GetServers(context.Background())
			t.Fatalf("timed out waiting for %d servers, got %d", expected, len(servers))
		case <-ticker.C:
			servers, err := c.GetServers(context.Background())
			if err != nil {
				continue
			}
			if len(servers) == expected {
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
