package tally

import (
	"context"
	"sync"

	api "github.com/w-h-a/tally/proto/log/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials/insecure"
	grpcresolver "google.golang.org/grpc/resolver"
)

// isLeaderKey is the attribute key the resolver sets on each address
// and the picker reads to distinguish leader from follower connections.
type isLeaderKey struct{}

// resolver implements resolver.Resolver. gRPC calls ResolveNow when it
// wants a fresh server list. Each address is annotated with an is_leader
// attribute so the picker can make routing decisions.
type resolver struct {
	mtx    sync.Mutex
	cc     grpcresolver.ClientConn
	client api.LogServiceClient
	conn   *grpc.ClientConn
}

func (r *resolver) ResolveNow(grpcresolver.ResolveNowOptions) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	resp, err := r.client.GetServers(context.Background(), &api.GetServersRequest{})
	if err != nil {
		r.cc.ReportError(err)
		return
	}

	addrs := []grpcresolver.Address{}
	for _, srv := range resp.Servers {
		if srv.RpcAddr == "" {
			continue
		}
		addrs = append(addrs, grpcresolver.Address{
			Addr:       srv.RpcAddr,
			Attributes: attributes.New(isLeaderKey{}, srv.IsLeader),
		})
	}

	r.cc.UpdateState(grpcresolver.State{Addresses: addrs})
}

func (r *resolver) Close() {
	r.conn.Close()
}

// resolverBuilder implements resolver.Builder. gRPC calls Build when a client
// dials a target with the tally scheme.
type resolverBuilder struct{}

// Build creates a Resolver that discovers cluster members by calling
// GetServers on the bootstrap address.
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
	}

	r.ResolveNow(grpcresolver.ResolveNowOptions{})

	return r, nil
}

func (b *resolverBuilder) Scheme() string {
	return Scheme
}
