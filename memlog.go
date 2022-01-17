package memlog

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

var (
	// ErrRecordTooLarge is returned when the record data is larger than the
	// configured maximum record size
	ErrRecordTooLarge = errors.New("record data too large")
	// ErrFutureOffset is returned on reads when the specified offset is in the
	// future and not written yet
	ErrFutureOffset = errors.New("future offset")
	// ErrOutOfRange is returned when the specified offset is invalid for the log
	// configuration or already purged from history
	ErrOutOfRange = errors.New("offset out of range")
)

// Offset is a monotonically increasing position of a record in the log
type Offset int

// Header is metadata associated with a record
type Header struct {
	// Offset is the record offset relative to the log start
	Offset Offset `json:"offset,omitempty"`
	// Created is the UTC timestamp when a record was successfully written to the
	// log
	Created time.Time `json:"created"` // UTC
}

// Record is an immutable entry in the log
type Record struct {
	Metadata Header `json:"metadata"`
	Data     []byte `json:"data,omitempty"`
}

func (r Record) deepCopy() Record {
	if r.Metadata.Offset == 0 && r.Metadata.Created.IsZero() {
		return Record{}
	}

	dCopy := append([]byte(nil), r.Data...)
	return Record{
		Metadata: Header{
			Offset:  r.Metadata.Offset,
			Created: r.Metadata.Created,
		},
		Data: dCopy,
	}
}

type config struct {
	startOffset   Offset // logical start offset
	segmentSize   int    // offsets per segment
	maxRecordSize int    // bytes
}

// Log is an append-only in-memory data structure storing records. Records are
// stored and retrieved using unique offsets. The log can be customized during
// initialization with New() to define a custom start offset, and size limits
// for the log and individual records.
//
// The log is divided into an active and history segment. When the active
// segment is full (MaxSegmentSize), it becomes the read-only history segment
// and a new empty active segment with the same size is created.
//
// The maximum number of records in a log is twice the configured segment size
// (active + history). When this limit is reached, the history segment is
// purged, replaced with the current active segment and a new empty active
// segment is created.
//
// Safe for concurrent use.
type Log struct {
	conf config

	mu      sync.RWMutex
	history *segment // read-only
	active  *segment // read-write
	offset  Offset   // monotonic offset counter tracking next write
	clock   clock.Clock
}

// New creates an empty log with default options applied, unless specified
// otherwise.
func New(_ context.Context, options ...Option) (*Log, error) {
	var l Log

	// apply defaults
	for _, opt := range defaultOptions {
		if err := opt(&l); err != nil {
			return nil, fmt.Errorf("configure log default option: %v", err)
		}
	}

	// apply custom settings
	for _, opt := range options {
		if err := opt(&l); err != nil {
			return nil, fmt.Errorf("configure log custom option: %v", err)
		}
	}

	s, err := newSegment(l.conf.startOffset, l.conf.segmentSize)
	if err != nil {
		return nil, fmt.Errorf("create active segment: %v", err)
	}
	l.active = s
	l.offset = l.conf.startOffset

	return &l, nil
}

// Write creates a new record in the log with the provided data. The write offset
// of the new record is returned. If an error occurs, an invalid offset (-1) and
// the error is returned.
//
// Safe for concurrent use.
func (l *Log) Write(ctx context.Context, data []byte) (Offset, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.write(ctx, data)
}

func (l *Log) write(ctx context.Context, data []byte) (Offset, error) {
	if ctx.Err() != nil {
		return -1, ctx.Err()
	}

	if len(data) > l.conf.maxRecordSize {
		return -1, ErrRecordTooLarge
	}

	if len(data) == 0 {
		return -1, errors.New("no data provided")
	}

	dcopy := append([]byte(nil), data...)
	r := Record{
		Metadata: Header{
			Offset:  l.offset,
			Created: l.clock.Now().UTC(),
		},
		Data: dcopy,
	}

	err := l.active.write(ctx, r)
	for err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return -1, err
		}

		if errors.Is(err, errFull) {
			err = l.extend()
			if err != nil {
				panic(err.Error()) // abnormal program state
			}

			err = l.active.write(ctx, r)
			continue
		}

		if errors.Is(err, errSealed) {
			panic(err.Error()) // abnormal program state
		}

		// logger.Error(err, "segment write failed")
		panic("write error: " + err.Error())
	}

	l.offset++
	return r.Metadata.Offset, nil
}

// Read reads a record from the log at the specified offset. If an error occurs, an
// invalid (empty) record and the error is returned.
//
// Safe for concurrent use.
func (l *Log) Read(ctx context.Context, offset Offset) (Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.read(ctx, offset)
}

// ReadBatch reads multiple records into batch starting at the specified offset.
// The number of records read into batch and the error, if any, is returned.
//
// ReadBatch will read at most len(batch) records, always starting at batch
// index 0. ReadBatch stops reading at the end of the log, indicated by
// ErrFutureOffset.
//
// The caller must expect partial batch results and must not read more records
// from batch than indicated by the returned number of records. See the example
// for how to use this API.
//
// Safe for concurrent use.
func (l *Log) ReadBatch(ctx context.Context, offset Offset, batch []Record) (int, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	for i := 0; i < len(batch); i++ {
		r, err := l.read(ctx, offset)
		if err != nil {
			// invalid start offset or empty log
			if errors.Is(err, ErrOutOfRange) {
				return 0, err
			}

			// return what we have
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return i, err
			}

			// end of log
			if errors.Is(err, ErrFutureOffset) {
				return i, ErrFutureOffset
			}

		}
		batch[i] = r
		offset++
	}

	return len(batch), nil
}

func (l *Log) read(ctx context.Context, offset Offset) (Record, error) {
	if ctx.Err() != nil {
		return Record{}, ctx.Err()
	}

	if offset >= l.offset {
		return Record{}, ErrFutureOffset
	}

	if offset < l.conf.startOffset {
		return Record{}, ErrOutOfRange
	}

	s, err := l.getSegment(offset)
	if err != nil {
		return Record{}, err
	}

	r, err := s.read(ctx, offset)
	if err != nil {
		return Record{}, err
	}

	return r.deepCopy(), nil
}

// Range returns the earliest and latest available record offset in the log. If
// the log is empty, an invalid offset (-1) for both return values is returned.
// If the log has been purged one or more times, earliest points to the oldest
// available record offset in the log, i.e. not the configured start offset.
//
// Note that these values might have changed after retrieval, e.g. due to
// concurrent writes.
//
// Safe for concurrent use.
func (l *Log) Range(_ context.Context) (earliest, latest Offset) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	earliest, latest = l.offsetRange()
	return
}

// offsetRange returns the earliest and latest available record offset in the
// log. If the log is empty, -1 for both return values is returned. If the log
// has been purged one or more times, earliest points to the oldest available
// record offset in the log, i.e. not the configured start offset. Must be
// protected with a lock by the caller.
func (l *Log) offsetRange() (Offset, Offset) {
	if l.history == nil {
		// empty log
		if l.active.currentOffset() == -1 {
			return -1, -1
		}

		// no purge since start
		return l.conf.startOffset, l.active.currentOffset()
	}

	return l.history.start, l.active.currentOffset()
}

// getSegment retrieves the segment for the specified offset. If the offset is
// in the future, ErrFutureOffset will be returned. If the offset is invalid or
// has been purged ErrOutOfRange is returned. Must be protected with a lock by
// the caller.
func (l *Log) getSegment(offset Offset) (*segment, error) {
	// check if offset is within active segment
	if offset >= l.active.start {
		if offset <= l.active.currentOffset() {
			return l.active, nil
		}
		return nil, ErrFutureOffset
	}

	// search history
	history := l.history
	if history != nil {
		min := history.start
		max := history.start + Offset(l.conf.segmentSize) - 1

		if min <= offset && offset <= max {
			return history, nil
		}
	}
	return nil, ErrOutOfRange
}

// extend creates a new active and history segment by replacing it with the
// current active segment. The old segment is sealed. If history is not empty,
// history will be purged before replacing it. Must be protected with a lock by
// the caller.
func (l *Log) extend() error {
	l.active.seal()

	l.history = l.active
	seg, err := newSegment(l.offset, l.conf.segmentSize)
	if err != nil {
		return err
	}

	l.active = seg
	return nil
}
