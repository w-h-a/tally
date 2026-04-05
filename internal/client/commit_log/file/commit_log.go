package file

import (
	"context"
	"encoding/binary"

	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	api "github.com/w-h-a/tally/proto/log/v1"
)

var (
	encoding = binary.BigEndian
)

type fileCommitLog struct {
	options commitlog.Options
}

func NewCommitLog(opts ...commitlog.Option) (commitlog.CommitLog, error) {
	options := commitlog.NewOptions(opts...)

	l := &fileCommitLog{
		options: options,
	}

	return l, nil
}

func (l *fileCommitLog) Append(ctx context.Context, rec *api.Record) (uint64, error) {
	return 0, nil
}

func (l *fileCommitLog) Read(ctx context.Context, offset uint64) (*api.Record, error) {
	return nil, nil
}

func (l *fileCommitLog) LowestOffset(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (l *fileCommitLog) HighestOffset(ctx context.Context) (uint64, error) {
	return 0, nil
}

func (l *fileCommitLog) Truncate(ctx context.Context, lowest uint64) error {
	return nil
}

func (l *fileCommitLog) Reset(ctx context.Context) error {
	return nil
}

func (l *fileCommitLog) Close(ctx context.Context) error {
	return nil
}
