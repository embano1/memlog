package sharded

import (
	"errors"

	"github.com/benbjohnson/clock"

	"github.com/embano1/memlog"
)

const (
	// DefaultShards is the number of shards unless specified otherwise
	DefaultShards = 1000
	// DefaultStartOffset is the start offset in a shard
	DefaultStartOffset = memlog.DefaultStartOffset
	// DefaultSegmentSize is the segment size, i.e. number of offsets, in a shard
	DefaultSegmentSize = memlog.DefaultSegmentSize
	// DefaultMaxRecordDataBytes is the maximum data (payload) size of a record in a shard
	DefaultMaxRecordDataBytes = memlog.DefaultMaxRecordDataBytes
)

// Option customizes a log
type Option func(*Log) error

var defaultOptions = []Option{
	WithClock(clock.New()),
	WithMaxRecordDataSize(DefaultMaxRecordDataBytes),
	WithMaxSegmentSize(DefaultSegmentSize),
	WithNumShards(DefaultShards),
	WithSharder(newDefaultSharder()),
	WithStartOffset(DefaultStartOffset),
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

// WithMaxRecordDataSize sets the maximum record data (payload) size in bytes in
// each shard
func WithMaxRecordDataSize(size int) Option {
	return func(log *Log) error {
		if size <= 0 {
			return errors.New("size must be greater than 0")
		}
		log.conf.maxRecordSize = size
		return nil
	}
}

// WithMaxSegmentSize sets the maximum size, i.e. number of offsets, in each shard.
// Must be greater than 0.
func WithMaxSegmentSize(size int) Option {
	return func(log *Log) error {
		if size <= 0 {
			return errors.New("size must be greater than 0")
		}
		log.conf.segmentSize = size
		return nil
	}
}

// WithNumShards sets the number of shards in a log
func WithNumShards(n uint) Option {
	return func(log *Log) error {
		// sharded log with < 2 shards is a standard log
		if n < 2 {
			return errors.New("number of shards must be greater than 1")
		}
		log.conf.shards = n
		return nil
	}
}

// WithSharder uses the specified sharder for key sharding
func WithSharder(s Sharder) Option {
	return func(log *Log) error {
		if s == nil {
			return errors.New("sharder must not be nil")
		}
		log.sharder = s
		return nil
	}
}

// WithStartOffset sets the start offset of each shard. Must be equal or greater
// than 0.
func WithStartOffset(offset memlog.Offset) Option {
	return func(log *Log) error {
		if offset < 0 {
			return errors.New("start offset must not be negative")
		}
		log.conf.startOffset = offset
		return nil
	}
}
