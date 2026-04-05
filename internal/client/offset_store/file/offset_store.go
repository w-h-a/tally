package file

import (
	"context"
	"encoding/binary"
	"errors"
	"io/fs"
	"os"
	"path/filepath"

	offsetstore "github.com/w-h-a/tally/internal/client/offset_store"
)

var (
	errOffsetCorrupt = errors.New("offset file is not 8 bytes")
)

// fileOffsetStore persists one file per consumer in a configured directory.
// Each file contains a single uint64 offset encoded as 8 big-endian bytes [8 bytes encoding <offset>].
// CommitOffset uses a write-to-temp-then-rename strategy for crash safety.
// Writing directly to the consumer's file risks a crash mid-write, which
// would leave a truncated or partially-written file; i.e., a corrupted offset.
// Instead, we write the new offset to a temporary file in the same directory,
// close it, then call os.Rename to atomically replace the consumer's file.
// Rename is atomic: the destination either points to the old file or the new file,
// never to a half-written one.
// GetOffset returns 0 if no file exists.
// fileOffsetStore is NOT concurrent-safe for the same consumerID; the caller
// must synchronize if needed.
type fileOffsetStore struct {
	options offsetstore.Options
}

func NewOffsetStore(opts ...offsetstore.Option) (offsetstore.OffsetStore, error) {
	options := offsetstore.NewOptions(opts...)

	if err := os.MkdirAll(options.Location, 0755); err != nil {
		return nil, err
	}

	s := &fileOffsetStore{
		options: options,
	}

	return s, nil
}

func (s *fileOffsetStore) CommitOffset(ctx context.Context, consumerID string, offset uint64) error {
	tmp, err := os.CreateTemp(s.options.Location, consumerID+".tmp.*")
	if err != nil {
		return err
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], offset)

	if _, err := tmp.Write(buf[:]); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}

	if err := os.Rename(tmp.Name(), filepath.Join(s.options.Location, consumerID)); err != nil {
		os.Remove(tmp.Name())
		return err
	}

	return nil
}

func (s *fileOffsetStore) GetOffset(ctx context.Context, consumerID string) (uint64, error) {
	bs, err := os.ReadFile(filepath.Join(s.options.Location, consumerID))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	if len(bs) != 8 {
		return 0, errOffsetCorrupt
	}

	return binary.BigEndian.Uint64(bs), nil
}

func (s *fileOffsetStore) Close(ctx context.Context) error {
	return nil
}
