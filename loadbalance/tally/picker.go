package tally

import (
	"strings"
	"sync/atomic"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

// picker implements balancer.Picker. gRPC calls Pick on every RPC.
// Writes (Produce, ProduceStream) go to the leader. Reads (Consume,
// ConsumeStream, GetServers) round-robin across followers, falling
// back to the leader when no followers are available.
type picker struct {
	leader    balancer.SubConn
	followers []balancer.SubConn
	idx       atomic.Uint64
}

func (p *picker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if strings.Contains(info.FullMethodName, "Produce") {
		if p.leader != nil {
			return balancer.PickResult{SubConn: p.leader}, nil
		}
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	if len(p.followers) > 0 {
		idx := p.idx.Add(1)
		sc := p.followers[idx%uint64(len(p.followers))]
		return balancer.PickResult{SubConn: sc}, nil
	}

	if p.leader != nil {
		return balancer.PickResult{SubConn: p.leader}, nil
	}

	return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
}

// pickerBuilder implements base.PickerBuilder. gRPC calls Build whenever
// SubConn readiness changes, passing the current set of ready connections
// with their address metadata. We partition them into leader and followers.
type pickerBuilder struct{}

func (b *pickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	var leader balancer.SubConn
	var followers []balancer.SubConn

	for sc, scInfo := range info.ReadySCs {
		isLeader, _ := scInfo.Address.Attributes.Value(isLeaderKey{}).(bool)
		if isLeader {
			leader = sc
		} else {
			followers = append(followers, sc)
		}
	}

	return &picker{
		leader:    leader,
		followers: followers,
	}
}
