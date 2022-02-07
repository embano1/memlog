package sharded

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"gotest.tools/v3/assert"

	"github.com/embano1/memlog"
)

func TestNew(t *testing.T) {
	t.Run("fails to create new log", func(t *testing.T) {
		const (
			defaultShards  = 10
			defaultStart   = 0
			defaultSegSize = 10
		)

		testCases := []struct {
			name    string
			shards  int
			sharder Sharder
			clock   clock.Clock
			start   memlog.Offset // log start
			segSize int
			wantErr string
		}{
			{
				name:    "fails with invalid shard count",
				shards:  0,
				sharder: &defaultSharder{},
				clock:   clock.NewMock(),
				start:   defaultStart,
				segSize: defaultSegSize,
				wantErr: "must be greater than 1",
			},
			{
				name:    "fails with invalid segment size",
				shards:  defaultShards,
				sharder: &defaultSharder{},
				clock:   clock.NewMock(),
				start:   defaultStart,
				segSize: 0,
				wantErr: "must be greater than 0",
			},
			{
				name:    "fails with invalid start offset",
				shards:  defaultShards,
				sharder: &defaultSharder{},
				clock:   clock.NewMock(),
				start:   -10,
				segSize: defaultSegSize,
				wantErr: "must not be negative",
			},
			{
				name:    "fails with invalid clock",
				shards:  defaultShards,
				sharder: &defaultSharder{},
				clock:   nil,
				start:   defaultStart,
				segSize: defaultSegSize,
				wantErr: "must not be nil",
			},
			{
				name:    "fails with invalid sharder",
				shards:  defaultShards,
				sharder: nil,
				clock:   clock.NewMock(),
				start:   defaultStart,
				segSize: defaultSegSize,
				wantErr: "must not be nil",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				opts := []Option{
					WithNumShards(uint(tc.shards)),
					WithClock(tc.clock),
					WithStartOffset(tc.start),
					WithMaxSegmentSize(tc.segSize),
					WithSharder(tc.sharder),
				}
				l, err := New(ctx, opts...)
				assert.ErrorContains(t, err, tc.wantErr)
				assert.DeepEqual(t, l, (*Log)(nil))
			})
		}
	})

	t.Run("successfully creates new log with defaults", func(t *testing.T) {
		l, err := New(context.Background())
		assert.NilError(t, err)
		assert.Assert(t, l.conf.startOffset == DefaultStartOffset)
		assert.Assert(t, l.conf.segmentSize == DefaultSegmentSize)
		assert.Assert(t, l.conf.shards == DefaultShards)
		assert.Assert(t, l.conf.maxRecordSize == DefaultMaxRecordDataBytes)
		assert.Assert(t, len(l.shards) == DefaultShards)
	})
}
