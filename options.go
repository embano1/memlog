package memlog

import (
	"errors"

	"github.com/benbjohnson/clock"
)

const (
	// DefaultStartOffset is the start offset of the log unless explicitly specified
	DefaultStartOffset = Offset(0)
	// DefaultSegmentSize is the segment size of the log unless explicitly specified
	DefaultSegmentSize = 1024
	// DefaultMaxRecordSize is the maximum record size of the log unless explicitly
	// specified
	DefaultMaxRecordSize = 1024 << 10 // 1MiB
)

type Option func(*Log) error

var defaultOptions = []Option{
	WithClock(clock.New()),
	WithStartOffset(DefaultStartOffset),
	WithMaxSegmentSize(DefaultSegmentSize),
	WithMaxRecordSizeBytes(DefaultMaxRecordSize),
}

func WithClock(c clock.Clock) Option {
	return func(log *Log) error {
		if c == nil {
			return errors.New("clock must not be nil")
		}

		log.clock = c
		return nil
	}
}

func WithMaxSegmentSize(size int) Option {
	return func(log *Log) error {
		if size <= 0 {
			return errors.New("size must be greater than 0")
		}
		log.conf.segmentSize = size
		return nil
	}
}

func WithMaxRecordSizeBytes(size int) Option {
	return func(log *Log) error {
		if size <= 0 {
			return errors.New("size must be greater than 0")
		}
		log.conf.maxRecordSize = size
		return nil
	}
}

func WithStartOffset(offset Offset) Option {
	return func(log *Log) error {
		if offset < 0 {
			return errors.New("start offset must not be negative")
		}
		log.conf.startOffset = offset
		return nil
	}
}
