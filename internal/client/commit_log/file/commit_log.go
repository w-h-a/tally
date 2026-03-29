package file

import (
	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	api "github.com/w-h-a/tally/proto/log/v1"
)

type fileCommitLog struct {
	options commitlog.Options
}

func (cl *fileCommitLog) Append(rec *api.Record) (uint64, error) {
	return 0, nil
}

func (cl *fileCommitLog) Read(uint64) (*api.Record, error) {
	return nil, nil
}

func (cl *fileCommitLog) LowestOffset() (uint64, error) {
	return 0, nil
}

func (cl *fileCommitLog) HighestOffset() (uint64, error) {
	return 0, nil
}

func (cl *fileCommitLog) Truncate(lowest uint64) error {
	return nil
}

func (cl *fileCommitLog) Reset() error {
	return nil
}

func (cl *fileCommitLog) Close() error {
	return nil
}

func NewCommitLog(opts ...commitlog.Option) (commitlog.CommitLog, error) {
	options := commitlog.NewOptions(opts...)

	cl := &fileCommitLog{
		options: options,
	}

	return cl, nil
}
