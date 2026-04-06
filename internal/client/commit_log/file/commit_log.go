package file

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	api "github.com/w-h-a/tally/proto/log/v1"
)

var (
	encoding = binary.BigEndian
)

// fileCommitLog manages an ordered list of segments with rotation.
// Append writes to the active segment; when it fills up, a new segment
// is created. Read uses binary search by baseOffset to find the right
// segment. Protected by sync.RWMutex for concurrent access.
// Segment rotation holds the write lock, so rotation latency is visible
// to concurrent readers. They block until the new segment is ready.
type fileCommitLog struct {
	options       commitlog.Options
	segments      []*fileSegment
	activeSegment *fileSegment
	mtx           sync.RWMutex
}

func NewCommitLog(opts ...commitlog.Option) (commitlog.CommitLog, error) {
	options := commitlog.NewOptions(opts...)

	if err := os.MkdirAll(options.Location, 0755); err != nil {
		return nil, err
	}

	l := &fileCommitLog{
		options:       options,
		segments:      nil,
		activeSegment: nil,
		mtx:           sync.RWMutex{},
	}

	if err := l.setup(); err != nil {
		return nil, err
	}

	return l, nil
}

// setup scans the directory for existing segment files, recovers them in
// baseOffset order, and sets the active segment. If no segment exists,
// it creates the initial segment at baseOffset 0.
func (l *fileCommitLog) setup() error {
	entries, err := os.ReadDir(l.options.Location)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	seen := map[uint64]bool{}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		ext := filepath.Ext(entry.Name())
		if ext != ".store" && ext != ".index" {
			continue
		}

		base := strings.TrimSuffix(entry.Name(), ext)

		off, err := strconv.ParseUint(base, 10, 64)
		if err != nil {
			continue
		}

		if !seen[off] {
			seen[off] = true
			baseOffsets = append(baseOffsets, off)
		}
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for _, off := range baseOffsets {
		seg, err := newSegment(l.options.Location, off, l.options)
		if err != nil {
			for _, s := range l.segments {
				s.close()
			}
			l.segments = nil
			return err
		}

		l.segments = append(l.segments, seg)
	}

	if len(l.segments) == 0 {
		seg, err := newSegment(l.options.Location, 0, l.options)
		if err != nil {
			return err
		}

		l.segments = append(l.segments, seg)
	}

	l.activeSegment = l.segments[len(l.segments)-1]

	return nil
}

func (l *fileCommitLog) Append(ctx context.Context, rec *api.Record) (uint64, error) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if l.activeSegment.isMaxed() {
		if err := l.newSegmentLocked(l.activeSegment.nextOffset); err != nil {
			return 0, err
		}
	}

	return l.activeSegment.append(rec)
}

func (l *fileCommitLog) Read(ctx context.Context, offset uint64) (*api.Record, error) {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	// Binary search: find the last segment whose baseOffset <= offset.
	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > offset
	}) - 1

	if i < 0 {
		return nil, commitlog.ErrOffsetOutOfRange
	}

	return l.segments[i].read(offset)
}

func (l *fileCommitLog) LowestOffset(ctx context.Context) (uint64, error) {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	return l.segments[0].baseOffset, nil
}

func (l *fileCommitLog) HighestOffset(ctx context.Context) (uint64, error) {
	l.mtx.RLock()
	defer l.mtx.RUnlock()

	// if no record exists, nextOffset equals the
	// first segment's baseOffset and nextOffset - 1 would underflow.
	if l.activeSegment.nextOffset == l.segments[0].baseOffset {
		return 0, commitlog.ErrOffsetOutOfRange
	}

	return l.activeSegment.nextOffset - 1, nil
}

func (l *fileCommitLog) Truncate(ctx context.Context, lowest uint64) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	var remaining []*fileSegment
	var firstErr error

	for _, seg := range l.segments {
		if seg.nextOffset > seg.baseOffset && seg.nextOffset <= lowest {
			if err := seg.remove(); err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}

		remaining = append(remaining, seg)
	}

	if len(remaining) == 0 {
		seg, err := newSegment(l.options.Location, lowest, l.options)
		if err != nil {
			return err
		}

		remaining = append(remaining, seg)
		l.activeSegment = seg
	}

	l.segments = remaining

	return firstErr
}

func (l *fileCommitLog) Reset(ctx context.Context) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	var firstErr error

	for _, seg := range l.segments {
		if err := seg.remove(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	l.segments = nil
	l.activeSegment = nil

	if err := l.setup(); err != nil {
		return err
	}

	return firstErr
}

func (l *fileCommitLog) Close(ctx context.Context) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	var firstErr error

	for _, seg := range l.segments {
		if err := seg.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func (l *fileCommitLog) newSegmentLocked(baseOffset uint64) error {
	seg, err := newSegment(l.options.Location, baseOffset, l.options)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, seg)
	l.activeSegment = seg

	return nil
}
