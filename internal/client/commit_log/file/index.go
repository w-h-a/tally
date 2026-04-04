package file

import (
	"errors"
	"os"

	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	"golang.org/x/sys/unix"
)

const (
	// offWidth is the number of bytes used to encode the relative offset.
	// A relative offset is the record's sequence number within a segment.
	// For example, the 3rd record has relative offset 3, stored as: [4 bytes encoding "3"].
	offWidth = 4

	// posWidth is the number of bytes used to encode the store position.
	// A store position is the byte offset where a record starts in the store file.
	// For example, a record at byte 4096 is stored as: [8 bytes encoding "4096"].
	posWidth = 8

	entryWidth = offWidth + posWidth
)

var (
	errIndexFull    = errors.New("index is full")
	errIndexEmpty   = errors.New("index is empty")
	errIndexCorrupt = errors.New("index file size it not a multiple of entry width")
)

// fileIndex is a memory-mapped file that maps relative offsets to store positions.
// Each entry is a fixed-width 12-byte pair.
// Access is random: any entry can be looked up by offset number at any time.
// Mmap maps the on-disk file into the process's memory space so lookups are
// array indexing rather than a syscall per access. The data still lives on disk.
// The OS loads it into RAM and flushes writes back as needed.
// fileIndex is NOT concurrent-safe; the caller (fileCommitLog) must synchronize access.
// On crash, close never runs, so the file stays at maxIndexBytes instead of
// being trimmed to the actual data size. On restart, newIndex will overestimate
// size. The fileCommitLog must detect and correct this, as fileIndex does not.
type fileIndex struct {
	file *os.File
	mmap []byte
	size uint64 // bytes written = entryWidth * entry count
}

func newIndex(f *os.File, maxIndexBytes uint64) (*fileIndex, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := uint64(fi.Size())

	if size > 0 && size%entryWidth != 0 {
		return nil, errIndexCorrupt
	}

	// Grow the file to maxIndexBytes.
	if err := f.Truncate(int64(maxIndexBytes)); err != nil {
		return nil, err
	}

	// Map the entire file into the process's address space.
	// MAP_SHARED means writes to the byte slice go back to the file on disk.
	mmap, err := unix.Mmap(
		int(f.Fd()),
		0,
		int(maxIndexBytes),
		unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_SHARED,
	)
	if err != nil {
		return nil, err
	}

	return &fileIndex{
		file: f,
		mmap: mmap,
		size: size,
	}, nil
}

// write appends a fixed-width entry mapping relOffset to pos at the next slot.
func (i *fileIndex) write(relOffset uint32, pos uint64) error {
	if i.size+entryWidth > uint64(len(i.mmap)) {
		return errIndexFull
	}

	encoding.PutUint32(i.mmap[i.size:i.size+offWidth], relOffset)
	encoding.PutUint64(i.mmap[i.size+offWidth:i.size+entryWidth], pos)

	i.size += entryWidth

	return nil
}

// read returns the offset and store position for the given entry index.
// Pass -1 to read the last entry.
func (i *fileIndex) read(relOffset int64) (uint32, uint64, error) {
	if i.size == 0 {
		return 0, 0, errIndexEmpty
	}

	var off uint64

	if relOffset == -1 {
		off = i.size - entryWidth
	} else {
		off = uint64(relOffset) * entryWidth
		if off+entryWidth > i.size {
			return 0, 0, commitlog.ErrOffsetOutOfRange
		}
	}

	offsetVal := encoding.Uint32(i.mmap[off : off+offWidth])
	posVal := encoding.Uint64(i.mmap[off+offWidth : off+entryWidth])

	return offsetVal, posVal, nil
}

// close syncs the mmap to disk, unmaps it, truncates the file to the actual
// data size, and closes the underlying file.
func (i *fileIndex) close() error {
	var firstErr error

	// Force mmap writes to disk synchronously.
	if err := unix.Msync(i.mmap, unix.MS_SYNC); err != nil && firstErr == nil {
		firstErr = err
	}

	// Release the mapping.
	if err := unix.Munmap(i.mmap); err != nil && firstErr == nil {
		firstErr = err
	}

	// Shrink the file to actual data size.
	if err := i.file.Truncate(int64(i.size)); err != nil && firstErr == nil {
		firstErr = err
	}

	// Close the file.
	if err := i.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	return firstErr
}
