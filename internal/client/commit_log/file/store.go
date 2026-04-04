package file

import (
	"bufio"
	"encoding/binary"
	"errors"
	"os"
)

const (
	// lenWidth is the number of bytes used to encode the record's byte count.
	// For example, a 25-byte record is stored as: [8 bytes encoding "25"][25 bytes of data].
	lenWidth = 8
)

var (
	errStoreFull = errors.New("store is full")
)

// fileStore is an append-only file that persists records as length-prefixed byte slices.
// Access is sequential: appends go to the end, reads jump to a known byte position
// (provided by the index). Buffered file I/O suits this pattern; so, it needs bufio.Writer.
// fileStore is NOT concurrent-safe; the caller (fileCommitLog) must synchronize access with
// a mutex or similar mechanism.
// Writes are buffered via bufio.Writer and are not durable until flush.
type fileStore struct {
	file     *os.File
	buf      *bufio.Writer
	size     uint64
	maxBytes uint64
}

func newStore(f *os.File, maxStoreBytes uint64) (*fileStore, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	buf := bufio.NewWriter(f)

	return &fileStore{
		file:     f,
		buf:      buf,
		size:     size,
		maxBytes: maxStoreBytes,
	}, nil
}

// append writes a uint64 big-endian length prefix followed by bs to the store.
// It returns the number of bytes written (including the prefix) and the
// position at which the record starts.
func (s *fileStore) append(bs []byte) (uint64, uint64, error) {
	if s.size+lenWidth+uint64(len(bs)) > s.maxBytes {
		return 0, 0, errStoreFull
	}

	pos := s.size

	// binary.Write serializes uint64(len(bs)) as exactly 8 bytes (BigEndian).
	// This is the length prefix that read uses to know how many data bytes follow.
	if err := binary.Write(s.buf, encoding, uint64(len(bs))); err != nil {
		return 0, 0, err
	}

	n, err := s.buf.Write(bs)
	if err != nil {
		return 0, 0, err
	}

	written := uint64(n) + lenWidth
	s.size += written

	return written, pos, nil
}

// read reads the record at the given position by first reading the length
// prefix, then reading that many bytes of data.
func (s *fileStore) read(pos uint64) ([]byte, error) {
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	if _, err := s.file.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, encoding.Uint64(size))
	if _, err := s.file.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// close flushes the buffer and closes the underlying file.
func (s *fileStore) close() error {
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.file.Close()
}
