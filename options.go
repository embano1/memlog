package memlog

import (
	"errors"

	"github.com/benbjohnson/clock"
)

const (
	// DefaultStartOffset is the start offset of the log
	DefaultStartOffset = Offset(0)
	// DefaultSegmentSize is the segment size of the log
	DefaultSegmentSize = 1024
	// DefaultMaxRecordDataBytes is the maximum data (payload) size of a record
	DefaultMaxRecordDataBytes = 1024 << 10 // 1MiB
)

// Option customizes a log
type Option func(*Log) error

var defaultOptions = []Option{
	WithClock(clock.New()),
	WithStartOffset(DefaultStartOffset),
	WithMaxSegmentSize(DefaultSegmentSize),
	WithMaxRecordDataSize(DefaultMaxRecordDataBytes),
}

// WithClock uses the specified clock for setting record timestamps
func WithClock(c clock.Clock) Option {
	return func(log *Log) error {
		if c == nil {
			return errors.New("clock must not be nil")
		}

		log.clock = c
		return nil
	}
}

// WithMaxSegmentSize sets the maximum size, i.e. number of offsets, in a log
// segment. Must be greater than 0.
func WithMaxSegmentSize(size int) Option {
	return func(log *Log) error {
		if size <= 0 {
			return errors.New("size must be greater than 0")
		}
		log.conf.segmentSize = size
		return nil
	}
}

// WithMaxRecordDataSize sets the maximum record data (payload) size in bytes
func WithMaxRecordDataSize(size int) Option {
	return func(log *Log) error {
		if size <= 0 {
			return errors.New("size must be greater than 0")
		}
		log.conf.maxRecordSize = size
		return nil
	}
}

// WithStartOffset sets the start offset of the log. Must be equal or greater
// than 0.
func WithStartOffset(offset Offset) Option {
	return func(log *Log) error {
		if offset < 0 {
			return errors.New("start offset must not be negative")
		}
		log.conf.startOffset = offset
		return nil
	}
}
