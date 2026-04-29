package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	consensus "github.com/w-h-a/tally/internal/client/consensus"
	api "github.com/w-h-a/tally/proto/log/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	retainSnapshotCount = 1
	applyTimeout        = 10 * time.Second
)

// raftConsensus wraps hashicorp/raft behind the consensus.Consensus port
// interface. It owns the Raft node and its backing stores.
// LogStore persists Raft's internal replication log (the sequence of
// proposed/committed commands, which is distinct from Tally's CommitLog).
// StableStore persists Raft metadata: current term and vote record.
type raftConsensus struct {
	raft        *hraft.Raft
	logStore    hraft.LogStore
	stableStore hraft.StableStore
	tracer      trace.Tracer
}

// NewConsensus creates a Raft node and returns it as a consensus.Consensus.
// Setup:
//  1. Validate that we have all 3 FSM callbacks.
//  2. Create Raft config with the node's LocalID.
//  3. Listen on BindAddr for Raft peer traffic.
//  4. Create the network transport over our plain-TCP streamLayer.
//  5. Create or inject LogStore and StableStore.
//  6. If BootStrap is true and no prior state exists, bootstrap this
//     node as the sole voter. This is idempotent: a node that already
//     has state skips bootstrap and recovers from its persisted log.
//  7. Start the Raft node.
func NewConsensus(opts ...consensus.Option) (consensus.Consensus, error) {
	options := consensus.NewOptions(opts...)

	if options.ApplyFn == nil {
		return nil, fmt.Errorf("consensus/raft: ApplyFn is required")
	}

	if options.SnapshotFn == nil {
		return nil, fmt.Errorf("consensus/raft: Snapshot is required")
	}

	if options.RestoreFn == nil {
		return nil, fmt.Errorf("consensus/raft: RestoreFn is required")
	}

	if err := os.MkdirAll(options.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("consensus/raft: create data dir: %w", err)
	}

	config := hraft.DefaultConfig()
	config.LocalID = hraft.ServerID(options.LocalID)

	fsm := newFSM(options.ApplyFn, options.SnapshotFn, options.RestoreFn)

	ln, err := net.Listen("tcp", options.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("consensus/raft: listen: %w", err)
	}

	ok := false
	defer func() {
		if !ok {
			ln.Close()
		}
	}()

	bindAddr := ln.Addr().String()

	stream := newStreamLayer(ln)

	transport := hraft.NewNetworkTransport(stream, 5, 10*time.Second, os.Stderr)

	snapStore, err := hraft.NewFileSnapshotStore(options.DataDir, retainSnapshotCount, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("consensus/raft: snapshot store: %w", err)
	}

	var logStore hraft.LogStore
	var stableStore hraft.StableStore

	if store, ok := LogStoreFrom(options.Context); ok {
		logStore = store
	} else {
		boltDB, err := raftboltdb.NewBoltStore(filepath.Join(options.DataDir, "raft-log.db"))
		if err != nil {
			return nil, fmt.Errorf("consensus/raft: log store: %w", err)
		}

		logStore = boltDB
	}

	if store, ok := StableStoreFrom(options.Context); ok {
		stableStore = store
	} else {
		boltDB, err := raftboltdb.NewBoltStore(filepath.Join(options.DataDir, "raft-stable.db"))
		if err != nil {
			return nil, fmt.Errorf("consensus/raft: stable store: %w", err)
		}

		stableStore = boltDB
	}

	if options.Bootstrap {
		hasState, err := hraft.HasExistingState(logStore, stableStore, snapStore)
		if err != nil {
			return nil, fmt.Errorf("consensus/raft: check existing state: %w", err)
		}

		if !hasState {
			err := hraft.BootstrapCluster(
				config,
				logStore,
				stableStore,
				snapStore,
				transport,
				hraft.Configuration{
					Servers: []hraft.Server{
						{
							ID:      config.LocalID,
							Address: hraft.ServerAddress(bindAddr),
						},
					},
				},
			)
			if err != nil {
				return nil, fmt.Errorf("consensus/raft: bootstrap: %w", err)
			}
		}
	}

	r, err := hraft.NewRaft(config, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		return nil, fmt.Errorf("consensus/raft: new raft: %w", err)
	}

	ok = true

	return &raftConsensus{
		raft:        r,
		logStore:    logStore,
		stableStore: stableStore,
		tracer:      otel.Tracer("tally/internal/client/consensus/raft"),
	}, nil
}

// Apply proposes data to the Raft cluster. Blocks until a quorum commits
// the entry or the timeout expires. Only the leader can apply.
func (c *raftConsensus) Apply(ctx context.Context, data []byte) (uint64, error) {
	_, span := c.tracer.Start(ctx, "consensus.Apply", trace.WithAttributes(
		attribute.Int("consensus.data_len", len(data)),
	))
	defer span.End()

	future := c.raft.Apply(data, applyTimeout)
	if err := future.Error(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if errors.Is(err, hraft.ErrNotLeader) || errors.Is(err, hraft.ErrLeadershipLost) {
			return 0, consensus.ErrNotLeader
		}
		return 0, err
	}

	resp := future.Response().(*fsmResponse)
	if resp.err != nil {
		span.RecordError(resp.err)
		span.SetStatus(codes.Error, resp.err.Error())
		return 0, resp.err
	}

	span.SetAttributes(attribute.Int64("consensus.offset", int64(resp.result)))

	return resp.result, nil
}

// AddVoter adds a server to the Raft cluster as a voting member.
// Only the leader can modify cluster membership.
func (c *raftConsensus) AddVoter(ctx context.Context, id string, addr string) error {
	_, span := c.tracer.Start(ctx, "consensus.AddVoter", trace.WithAttributes(
		attribute.String("consensus.server_id", id),
		attribute.String("consensus.server_addr", addr),
	))
	defer span.End()

	future := c.raft.AddVoter(hraft.ServerID(id), hraft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

// RemoveServer removes a server from the Raft cluster.
// Only the leader can modify cluster membership.
func (c *raftConsensus) RemoveServer(ctx context.Context, id string) error {
	_, span := c.tracer.Start(ctx, "consensus.RemoveServer", trace.WithAttributes(
		attribute.String("consensus.server_id", id),
	))
	defer span.End()

	future := c.raft.RemoveServer(hraft.ServerID(id), 0, 0)
	if err := future.Error(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

// Leader returns the address of the current Raft leader, or empty
// string if no leader is known
func (c *raftConsensus) Leader(ctx context.Context) string {
	_, span := c.tracer.Start(ctx, "consensus.Leader")
	defer span.End()

	addr, _ := c.raft.LeaderWithID()

	return string(addr)
}

// GetServers returns the current Raft cluster membership with
// leader status for each server
func (c *raftConsensus) GetServers(ctx context.Context) ([]*api.Server, error) {
	_, span := c.tracer.Start(ctx, "consensus.GetServers")
	defer span.End()

	future := c.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}

	leaderAddr, _ := c.raft.LeaderWithID()

	var servers []*api.Server

	for _, srv := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(srv.ID),
			RpcAddr:  string(srv.Address),
			IsLeader: srv.Address == leaderAddr,
		})
	}

	return servers, nil
}

// State returns the current Raft state as a string:
// "Leader", "Follower", "Candidate", or "Shutdown".
func (c *raftConsensus) State(ctx context.Context) string {
	_, span := c.tracer.Start(ctx, "consensus.State")
	defer span.End()

	return c.raft.State().String()
}

// WaitForLeader polls until a leader is elected or the context
// deadline expires. Used during startup to block until the cluster
// is ready to accept writes
func (c *raftConsensus) WaitForLeader(ctx context.Context) error {
	_, span := c.tracer.Start(ctx, "consensus.WaitForLeader")
	defer span.End()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			addr, _ := c.raft.LeaderWithID()
			if addr != "" {
				return nil
			}
		}
	}
}

// Close shuts down the Raft node, stopping all background routines
// and releasing resources
func (c *raftConsensus) Close(ctx context.Context) error {
	_, span := c.tracer.Start(ctx, "consensus.Close")
	defer span.End()

	future := c.raft.Shutdown()
	if err := future.Error(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}
