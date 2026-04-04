package file

import (
	"fmt"
	"os"
	"path/filepath"

	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	api "github.com/w-h-a/tally/proto/log/v1"
	"google.golang.org/protobuf/proto"
)

// fileSegment pairs one fileStore and one fileIndex for a contiguous range of
// offsets starting at baseOffset. The fileCommitLog manages a list of segments. When the
// active segment fills up (store or index hits its configured max), the log
// creates a new segment. To truncate old data, the log drops entire segments.
// Without segments, truncation would mean rewriting store and index.
// fileSegment is NOT concurrent-safe; the caller (fileCommitLog) must
// synchronize access.
// If store.append succeeds but index.write fails, the record exists
// in the store but is unreachable via the index. The error is returned to the
// caller.
type fileSegment struct {
	store      *fileStore
	index      *fileIndex
	baseOffset uint64
	nextOffset uint64
	options    commitlog.Options
}

func newSegment(dir string, baseOffset uint64, options commitlog.Options) (*fileSegment, error) {
	storeFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d.store", baseOffset)),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	store, err := newStore(storeFile, options.MaxStoreBytes)
	if err != nil {
		storeFile.Close()
		return nil, err
	}

	indexFile, err := os.OpenFile(
		filepath.Join(dir, fmt.Sprintf("%d.index", baseOffset)),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		store.close()
		return nil, err
	}

	index, err := newIndex(indexFile, options.MaxIndexBytes)
	if err != nil {
		store.close()
		indexFile.Close()
		return nil, err
	}

	// Recover nextOffset from the last index entry.
	// New segment: index is empty, nextOffset = baseOffset.
	// Reopened segment: nextOffset = baseOffset + lastRelOffset + 1.
	nextOffset := baseOffset

	off, _, err := index.read(-1)
	if err == nil {
		nextOffset = baseOffset + uint64(off) + 1
	} else if err != errIndexEmpty {
		store.close()
		index.close()
		return nil, err
	}

	return &fileSegment{
		store:      store,
		index:      index,
		baseOffset: baseOffset,
		nextOffset: nextOffset,
		options:    options,
	}, nil
}

// append serializes rec, writes it to the store, indexes the offset-to-position
// mapping, sets rec.Offset to the assigned absolute offset, and advances nextOffset.
func (s *fileSegment) append(rec *api.Record) (uint64, error) {
	absOffset := s.nextOffset
	rec.Offset = absOffset

	bs, err := proto.Marshal(rec)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.append(bs)
	if err != nil {
		return 0, err
	}

	if err := s.index.write(uint32(absOffset-s.baseOffset), pos); err != nil {
		return 0, err
	}

	s.nextOffset++

	return absOffset, nil
}

// read returns the record at the given absolute offset.
func (s *fileSegment) read(absOffset uint64) (*api.Record, error) {
	if absOffset < s.baseOffset {
		return nil, commitlog.ErrOffsetOutOfRange
	}

	_, pos, err := s.index.read(int64(absOffset - s.baseOffset))
	if err != nil {
		return nil, err
	}

	bs, err := s.store.read(pos)
	if err != nil {
		return nil, err
	}

	rec := &api.Record{}
	if err := proto.Unmarshal(bs, rec); err != nil {
		return nil, err
	}

	return rec, nil
}

// isMaxed returns true when either the store or the index has reached its
// configured max.
func (s *fileSegment) isMaxed() bool {
	return s.store.size >= s.options.MaxStoreBytes || s.index.size >= s.options.MaxIndexBytes
}

// remove closes the segment and deletes the underlying store and index files.
func (s *fileSegment) remove() error {
	if err := s.close(); err != nil {
		return err
	}

	if err := os.Remove(s.store.file.Name()); err != nil {
		return err
	}

	if err := os.Remove(s.index.file.Name()); err != nil {
		return err
	}

	return nil
}

// close closes the index and the store.
func (s *fileSegment) close() error {
	indexErr := s.index.close()
	storeErr := s.store.close()

	if indexErr != nil {
		return indexErr
	}

	return storeErr
}
