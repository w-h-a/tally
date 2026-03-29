package commitlog

import (
	"errors"

	api "github.com/w-h-a/tally/proto/log/v1"
)

var (
	ErrOffsetOutOfRange = errors.New("offset out of range")
)

type CommitLog interface {
	Append(rec *api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
	LowestOffset() (uint64, error)
	HighestOffset() (uint64, error)
	Truncate(lowest uint64) error
	Reset() error
	Close() error
}
