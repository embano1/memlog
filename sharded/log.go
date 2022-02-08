package sharded

import (
	"context"
	"errors"
	"fmt"

	"github.com/benbjohnson/clock"

	"github.com/embano1/memlog"
)

type config struct {
	shards uint

	// memlog.Log settings
	startOffset   memlog.Offset
	segmentSize   int // offsets per segment
	maxRecordSize int // bytes
}

// Log is a sharded log implementation on top of memlog.Log. It uses a
// configurable sharding strategy (see Sharder interface) during reads and
// writes.
type Log struct {
	sharder Sharder
	clock   clock.Clock
	conf    config
	shards  []*memlog.Log
}

// New creates a new sharded log which can be customized with options. If not
// specified, the default sharding strategy uses fnv.New32a for key hashing.
func New(ctx context.Context, options ...Option) (*Log, error) {
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

	shards := l.conf.shards
	l.shards = make([]*memlog.Log, shards)
	opts := []memlog.Option{
		memlog.WithClock(l.clock),
		memlog.WithMaxRecordDataSize(l.conf.maxRecordSize),
		memlog.WithStartOffset(l.conf.startOffset),
		memlog.WithMaxSegmentSize(l.conf.segmentSize),
	}

	for i := 0; i < int(shards); i++ {
		ml, err := memlog.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("create shard: %w", err)
		}
		l.shards[i] = ml
	}

	return &l, nil
}

// Write writes data to the log using the specified key for sharding
func (l *Log) Write(ctx context.Context, key []byte, data []byte) (memlog.Offset, error) {
	if key == nil {
		return -1, errors.New("invalid key")
	}

	shard, err := l.sharder.Shard(key, l.conf.shards)
	if err != nil {
		return -1, fmt.Errorf("get shard: %w", err)
	}

	offset, err := l.shards[shard].Write(ctx, data)
	if err != nil {
		return -1, fmt.Errorf("write to shard: %w", err)
	}

	return offset, nil
}

// Read reads a record from the log at offset using the specified key for shard
// lookup
func (l *Log) Read(ctx context.Context, key []byte, offset memlog.Offset) (memlog.Record, error) {
	if key == nil {
		return memlog.Record{}, errors.New("invalid key")
	}

	shard, err := l.sharder.Shard(key, l.conf.shards)
	if err != nil {
		return memlog.Record{}, fmt.Errorf("get shard: %w", err)
	}

	r, err := l.shards[shard].Read(ctx, offset)
	if err != nil {
		return memlog.Record{}, fmt.Errorf("read from shard: %w", err)
	}

	return r, nil
}
