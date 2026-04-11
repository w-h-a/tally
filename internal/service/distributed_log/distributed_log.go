package distributedlog

import (
	"context"

	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	api "github.com/w-h-a/tally/proto/log/v1"
)

type Service struct {
	commitlog commitlog.CommitLog
}

func New(commitLog commitlog.CommitLog) *Service {
	return &Service{
		commitlog: commitLog,
	}
}

func (s *Service) Append(ctx context.Context, rec *api.Record) (uint64, error) {
	return s.commitlog.Append(ctx, rec)
}

func (s *Service) Read(ctx context.Context, offset uint64) (*api.Record, error) {
	return s.commitlog.Read(ctx, offset)
}

func (s *Service) Close(ctx context.Context) error {
	return s.commitlog.Close(ctx)
}
