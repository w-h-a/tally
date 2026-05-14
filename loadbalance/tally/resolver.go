package tally

import (
	"context"
	"log/slog"
	"sync"
	"time"

	api "github.com/w-h-a/tally/proto/log/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials/insecure"
	grpcresolver "google.golang.org/grpc/resolver"
)

// isLeaderKey is the attribute key the resolver sets on each address
// and the picker reads to distinguish leader from follower connections.
type isLeaderKey struct{}

// resolver implements resolver.Resolver. It periodically calls GetServers
// to keep leader/follower attributes current. If the bootstrap address is
// unreachable, it falls back to the last known addresses.
type resolver struct {
	mtx        sync.Mutex
	cc         grpcresolver.ClientConn
	client     api.LogServiceClient
	conn       *grpc.ClientConn
	knownAddrs []string
	lastLeader string
	done       chan struct{}
	wg         sync.WaitGroup
}

func (r *resolver) ResolveNow(grpcresolver.ResolveNowOptions) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := r.client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		slog.Warn("resolver: bootstrap unreachable, trying known addresses", "error", err)
		resp = r.tryKnownAddrs()
	}
	if resp == nil {
		slog.Error("resolver: all addresses unreachable")
		r.cc.ReportError(err)
		return
	}

	var addrs []grpcresolver.Address
	var known []string
	var leader string
	for _, srv := range resp.Servers {
		if srv.RpcAddr == "" {
			continue
		}
		addrs = append(addrs, grpcresolver.Address{
			Addr:       srv.RpcAddr,
			Attributes: attributes.New(isLeaderKey{}, srv.IsLeader),
		})
		known = append(known, srv.RpcAddr)
		if srv.IsLeader {
			leader = srv.Id
		}
	}

	if leader != r.lastLeader && r.lastLeader != "" {
		slog.Info("resolver: leadership changed", "old_leader", r.lastLeader, "new_leader", leader)
	}

	r.knownAddrs = known
	r.lastLeader = leader
	r.cc.UpdateState(grpcresolver.State{Addresses: addrs})
}

func (r *resolver) tryKnownAddrs() *api.GetServersResponse {
	for _, addr := range r.knownAddrs {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			cancel()
			continue
		}
		resp, err := api.NewLogServiceClient(conn).GetServers(ctx, &api.GetServersRequest{})
		cancel()
		conn.Close()
		if err == nil {
			slog.Info("resolver: recovered via fallback address", "addr", addr)
			return resp
		}
	}
	return nil
}

func (r *resolver) Close() {
	close(r.done)
	r.wg.Wait()
	r.conn.Close()
}

// resolverBuilder implements resolver.Builder. gRPC calls Build when a client
// dials a target with the tally scheme.
type resolverBuilder struct{}

// Build creates a Resolver that discovers cluster members by calling
// GetServers on the bootstrap address. A background goroutine re-resolves
// every 3 seconds to pick up leadership changes.
func (b *resolverBuilder) Build(target grpcresolver.Target, cc grpcresolver.ClientConn, opts grpcresolver.BuildOptions) (grpcresolver.Resolver, error) {
	conn, err := grpc.NewClient(
		target.Endpoint(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	r := &resolver{
		cc:     cc,
		client: api.NewLogServiceClient(conn),
		conn:   conn,
		done:   make(chan struct{}),
	}

	r.ResolveNow(grpcresolver.ResolveNowOptions{})

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-r.done:
				return
			case <-ticker.C:
				r.ResolveNow(grpcresolver.ResolveNowOptions{})
			}
		}
	}()

	return r, nil
}

func (b *resolverBuilder) Scheme() string {
	return Scheme
}
