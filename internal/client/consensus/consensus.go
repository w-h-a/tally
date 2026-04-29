package consensus

import (
	"context"
	"errors"

	api "github.com/w-h-a/tally/proto/log/v1"
)

var (
	ErrNotLeader = errors.New("not leader")
)

type Consensus interface {
	Apply(ctx context.Context, data []byte) (uint64, error)
	AddVoter(ctx context.Context, id string, addr string) error
	RemoveServer(ctx context.Context, id string) error
	Leader(ctx context.Context) string
	GetServers(ctx context.Context) ([]*api.Server, error)
	State(ctx context.Context) string
	WaitForLeader(ctx context.Context) error
	Close(ctx context.Context) error
}
