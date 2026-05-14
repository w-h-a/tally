package tally

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

// Scheme is the URI scheme for tally's custom resolver.
const Scheme = "tally"

// Name is the balancer name used in gRPC service config:
//
//	{"loadBalancingConfig": [{"tally": {}}]}
const Name = "tally"

// lbConfig holds the gRPC dial options for tally's custom
// resolver and load balancer.
type lbConfig struct {
	resolver *resolverBuilder
}

// DialOptions returns the grpc.DialOptions that wire the tally
// resolver and picker into a gRPC client.
func (c *lbConfig) DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithResolvers(c.resolver),
		grpc.WithDefaultServiceConfig(
			fmt.Sprintf(`{"loadBalancingConfig": [{"%s": {}}]}`, Name),
		),
	}
}

func NewLBConfig() *lbConfig {
	balancer.Register(base.NewBalancerBuilder(Name, &pickerBuilder{}, base.Config{}))
	return &lbConfig{resolver: &resolverBuilder{}}
}
