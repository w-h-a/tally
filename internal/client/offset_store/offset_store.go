package offsetstore

import "context"

type OffsetStore interface {
	CommitOffset(ctx context.Context, consumerID string, offset uint64) error
	GetOffset(ctx context.Context, consumerID string) (uint64, error)
	Close(ctx context.Context) error
}
