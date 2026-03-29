package file

import (
	"bufio"
	"encoding/binary"
	"os"
)

const (
	lenWidth = 8
)

var (
	encoding = binary.BigEndian
)

// fileStore is an append-only file that persists records as length-prefixed byte slices.
// fileStore is NOT concurrent-safe; the caller (fileCommitLog) must synchronize access with
// a mutex or similar mechanism.
// Writes are buffered via bufio.Writer and are not durable until flush.
type fileStore struct {
	file *os.File
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*fileStore, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &fileStore{
		file: f,
		buf:  bufio.NewWriter(f),
		size: uint64(fi.Size()),
	}, nil
}

// append writes a uint64 big-endian length prefix followed by bs to the store.
// It returns the number of bytes written (including the prefix) and the
// position at which the record starts.
func (s *fileStore) append(bs []byte) (uint64, uint64, error) {
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
