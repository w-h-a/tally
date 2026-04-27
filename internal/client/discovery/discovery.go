package discovery

import "context"

type Discovery interface {
	Join(ctx context.Context, addrs []string) error
	Leave(ctx context.Context) error
	Members(ctx context.Context) []Member
	Events() <-chan MemberEvent
}
