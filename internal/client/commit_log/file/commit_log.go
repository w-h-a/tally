package file

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	commitlog "github.com/w-h-a/tally/internal/client/commit_log"
	api "github.com/w-h-a/tally/proto/log/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	cancel        context.CancelFunc
	cleanerDone   chan struct{}
	tracer        trace.Tracer
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
		tracer:        otel.Tracer("tally/internal/client/commit_log/file"),
	}

	if err := l.setup(); err != nil {
		return nil, err
	}

	if options.Retention.MaxAge > 0 || options.Retention.MaxBytes > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		l.cancel = cancel
		l.cleanerDone = make(chan struct{})
		go l.cleaner(ctx)
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

func (l *fileCommitLog) cleaner(ctx context.Context) {
	defer close(l.cleanerDone)

	interval := l.options.Retention.Interval
	if interval == 0 {
		interval = time.Hour
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.cleanOnce(time.Now())
		}
	}
}

func (l *fileCommitLog) cleanOnce(now time.Time) {
	toRemove := l.selectExpiredSegments(now)

	for _, seg := range toRemove {
		if err := seg.remove(); err != nil {
			l.reinsertSegment(seg)
		}
	}
}

func (l *fileCommitLog) selectExpiredSegments(now time.Time) []*fileSegment {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	if len(l.segments) <= 1 {
		return nil
	}

	// Work on non-active segments only. Active segment is always kept.
	inactive := make([]*fileSegment, len(l.segments)-1)
	copy(inactive, l.segments[:len(l.segments)-1])

	expired := []*fileSegment{}

	// Age-based: remove segments whose newest record is older than MaxAge.
	if l.options.Retention.MaxAge > 0 {
		cutoff := now.Add(-l.options.Retention.MaxAge)
		kept := inactive[:0]

		for _, seg := range inactive {
			if seg.lastWriteAt.Before(cutoff) {
				expired = append(expired, seg)
				continue
			}

			kept = append(kept, seg)
		}

		// reset inactive
		inactive = kept
	}

	// Size-based: remove oldest segments until total store size <= MaxBytes
	if l.options.Retention.MaxBytes > 0 {
		total := l.activeSegment.store.size
		for _, seg := range inactive {
			total += seg.store.size
		}

		kept := inactive[:0]

		for _, seg := range inactive {
			if total > l.options.Retention.MaxBytes {
				total -= seg.store.size
				expired = append(expired, seg)
				continue
			}

			kept = append(kept, seg)
		}

		// reset inactive
		inactive = kept
	}

	l.segments = append(inactive, l.activeSegment)

	return expired
}

func (l *fileCommitLog) reinsertSegment(seg *fileSegment) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	pos := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > seg.baseOffset
	})

	l.segments = append(l.segments, nil)
	copy(l.segments[pos+1:], l.segments[pos:])
	l.segments[pos] = seg
}

func (l *fileCommitLog) Append(ctx context.Context, rec *api.Record) (uint64, error) {
	ctx, span := l.tracer.Start(ctx, "commitlog.Append")
	defer span.End()

	l.mtx.Lock()
	defer l.mtx.Unlock()

	sizeBefore := l.activeSegment.store.size

	if l.activeSegment.isMaxed() {
		if err := l.newSegmentLocked(l.activeSegment.nextOffset); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return 0, err
		}

		sizeBefore = l.activeSegment.store.size
	}

	offset, err := l.activeSegment.append(rec)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return 0, err
	}

	span.SetAttributes(
		attribute.Int64("commitlog.offset", int64(offset)),
		attribute.Int64("commitlog.bytes_written", int64(l.activeSegment.store.size-sizeBefore)),
		attribute.Int("commitlog.segment_count", len(l.segments)),
	)

	return offset, nil
}

func (l *fileCommitLog) Read(ctx context.Context, offset uint64) (*api.Record, error) {
	ctx, span := l.tracer.Start(ctx, "commitlog.Read", trace.WithAttributes(attribute.Int64("commitlog.offset", int64(offset))))
	defer span.End()

	l.mtx.RLock()
	defer l.mtx.RUnlock()

	// Binary search: find the last segment whose baseOffset <= offset.
	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > offset
	}) - 1

	if i < 0 {
		span.SetAttributes(attribute.Bool("commitlog.offset_out_of_range", true))
		return nil, commitlog.ErrOffsetOutOfRange
	}

	rec, err := l.segments[i].read(offset)
	if err != nil {
		if !errors.Is(err, commitlog.ErrOffsetOutOfRange) {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetAttributes(attribute.Bool("commitlog.offset_out_of_range", true))
		}
		return nil, err
	}

	return rec, nil
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
			if err := seg.remove(); err != nil {
				seg.close()
				if firstErr == nil {
					firstErr = err
				}
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
		if err := seg.remove(); err != nil {
			seg.close()
			if firstErr == nil {
				firstErr = err
			}
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
	if l.cancel != nil {
		l.cancel()
		<-l.cleanerDone
	}

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
