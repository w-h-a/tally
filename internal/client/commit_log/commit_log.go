package commitlog

import (
	"context"
	"errors"

	api "github.com/w-h-a/tally/proto/log/v1"
)

var (
	ErrOffsetOutOfRange = errors.New("offset out of range")
)

type CommitLog interface {
	Append(ctx context.Context, rec *api.Record) (uint64, error)
	Read(ctx context.Context, offset uint64) (*api.Record, error)
	LowestOffset(ctx context.Context) (uint64, error)
	HighestOffset(ctx context.Context) (uint64, error)
	Truncate(ctx context.Context, lowest uint64) error
	Reset(ctx context.Context) error
	Close(ctx context.Context) error
}
