package memlog

import (
	"context"
	"errors"
	"fmt"
)

var (
	errSealed = errors.New("segment sealed")
	errFull   = errors.New("segment full")
)

// segment is an append-only data structure for records. Not safe for concurrent
// use.
type segment struct {
	start  Offset // logical start offset
	sealed bool   // false set segment to read-only
	data   []Record
}

func newSegment(startOffset Offset, size int) (*segment, error) {
	if startOffset < 0 {
		return nil, fmt.Errorf("start offset must not be negative")
	}

	if size <= 0 {
		return nil, fmt.Errorf("size must be greater than 0")
	}

	s := segment{
		start: startOffset,
		data:  make([]Record, 0, size),
	}

	return &s, nil
}

func (s *segment) write(ctx context.Context, r Record) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if s.sealed {
		return errSealed
	}

	if len(s.data) == cap(s.data) {
		return errFull
	}

	s.data = append(s.data, r)
	return nil
}

func (s *segment) read(ctx context.Context, offset Offset) (Record, error) {
	if ctx.Err() != nil {
		return Record{}, ctx.Err()
	}

	records := len(s.data)
	index := offset - s.start
	if index > Offset(records)-1 || index < 0 {
		return Record{}, ErrOutOfRange
	}

	return s.data[index], nil
}

// seal closes a segment and sets it to read-only
func (s *segment) seal() {
	s.sealed = true
}

// currentOffset returns the last write offset starting at segment startOffset.
// If no write has been performed against the segment before, -1 is returned to
// denote an empty segment
func (s *segment) currentOffset() Offset {
	if len(s.data) == 0 {
		return -1
	}

	offset := s.start + Offset(len(s.data)) - 1
	return offset
}
