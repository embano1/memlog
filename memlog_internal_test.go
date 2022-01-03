package memlog

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"gotest.tools/v3/assert"
)

func TestRecord_deepCopy(t *testing.T) {
	now := time.Now().UTC()
	data := newTestData(t, "1")

	testCases := []struct {
		name   string
		record Record
	}{
		{name: "nil Record", record: Record{}},
		{name: "valid Record", record: Record{Metadata: Header{Offset: 1, Created: now}, Data: data}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.record.deepCopy()
			assert.DeepEqual(t, tc.record, got)
		})
	}
}

func Test_New(t *testing.T) {
	t.Run("fails when invalid option is specified", func(t *testing.T) {
		testCases := []struct {
			name  string
			opt   Option
			error string
		}{
			{"clock is nil", WithClock(nil), "must not be nil"},
			{"invalid start offset", WithStartOffset(-1), "must not be negative"},
			{"invalid segment size", WithMaxSegmentSize(-4), "must be greater than 0"},
			{"invalid record size", WithMaxRecordSizeBytes(0), "must be greater than 0"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				l, err := New(ctx, tc.opt)
				assert.ErrorContains(t, err, tc.error)
				assert.Assert(t, l == nil)
			})
		}
	})

	t.Run("creates log with defaults", func(t *testing.T) {
		ctx := context.Background()
		l, err := New(ctx)
		assert.NilError(t, err)
		assert.Assert(t, l != nil)

		// config
		assert.Equal(t, l.conf.startOffset, DefaultStartOffset)
		assert.Equal(t, l.conf.segmentSize, DefaultSegmentSize)
		assert.Equal(t, l.conf.maxRecordSize, DefaultMaxRecordSize)

		// 	fields
		assert.Assert(t, l.clock != nil)
		assert.Assert(t, l.active != nil)
		assert.Equal(t, l.active.start, DefaultStartOffset)
		assert.Equal(t, l.active.currentOffset(), Offset(-1))
		assert.DeepEqual(t, l.history, (*segment)(nil))
	})
}

func TestLog_write(t *testing.T) {
	t.Run("fails when record too large", func(t *testing.T) {
		ctx := context.Background()
		l, err := New(ctx, WithMaxRecordSizeBytes(10))
		assert.NilError(t, err)

		d := newTestData(t, "1")
		offset, err := l.write(ctx, d)
		assert.ErrorContains(t, err, "too large")
		assert.Equal(t, offset, Offset(-1))
	})

	t.Run("fails when record has no data", func(t *testing.T) {
		ctx := context.Background()
		l, err := New(ctx, WithMaxRecordSizeBytes(10))
		assert.NilError(t, err)

		offset, err := l.write(ctx, []byte{})
		assert.ErrorContains(t, err, "no data")
		assert.Equal(t, offset, Offset(-1))
	})

	t.Run("fails when ctx is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		l, err := New(ctx, WithMaxRecordSizeBytes(10))
		assert.NilError(t, err)

		cancel()
		offset, err := l.write(ctx, []byte{})
		assert.Assert(t, errors.Is(err, context.Canceled))
		assert.Equal(t, offset, Offset(-1))
	})

	t.Run("writes to log succeed", func(t *testing.T) {
		testCases := []struct {
			name      string
			start     Offset
			segSize   int
			records   [][]byte
			expOffset Offset
		}{
			{
				name:      "write 5, start at 0, segment size 10, no purge",
				start:     0,
				segSize:   10,
				records:   NewTestDataSlice(t, 5),
				expOffset: 5,
			},
			{
				name:      "write 5, start at 10, segment size 10, no purge",
				start:     10,
				segSize:   10,
				records:   NewTestDataSlice(t, 5),
				expOffset: 15,
			},
			{
				name:      "write 20, start at 0, segment size 10, with purge",
				start:     0,
				segSize:   10,
				records:   NewTestDataSlice(t, 20),
				expOffset: 20,
			},
			{
				name:      "write 20, start at 10, segment size 10, with purge",
				start:     10,
				segSize:   10,
				records:   NewTestDataSlice(t, 20),
				expOffset: 30,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				opts := []Option{
					WithStartOffset(tc.start),
					WithMaxSegmentSize(tc.segSize),
				}

				l, err := New(ctx, opts...)
				assert.NilError(t, err)

				for i, d := range tc.records {
					offset, writeErr := l.write(ctx, d)
					assert.NilError(t, writeErr)
					assert.Equal(t, offset, Offset(i)+tc.start)
				}

				assert.Equal(t, l.offset, tc.expOffset)

				// assert no history/purge
				if len(tc.records) < tc.segSize {
					assert.DeepEqual(t, l.history, (*segment)(nil))
				}

				if len(tc.records) > tc.segSize {
					assert.Equal(t, len(l.active.data), len(tc.records)-tc.segSize)
					assert.Equal(t, len(l.history.data), tc.segSize)
				}
			})
		}
	})
}

func TestLog_read(t *testing.T) {
	t.Run("read fails when context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		l, err := New(ctx)
		assert.NilError(t, err)

		cancel()
		r, err := l.read(ctx, 0)
		assert.Assert(t, errors.Is(err, context.Canceled))
		assert.Assert(t, r.Metadata.Created.IsZero())
	})

	t.Run("read fails with invalid offset", func(t *testing.T) {
		testCases := []struct {
			name    string
			start   Offset
			record  Offset
			wantErr error
		}{
			{name: "start offset 0, read -5", start: 0, record: -5, wantErr: ErrOutOfRange},
			{name: "start offset 10, read 0", start: 10, record: 0, wantErr: ErrOutOfRange},
			{name: "start offset 10, read 9", start: 10, record: 9, wantErr: ErrOutOfRange},
			{name: "start offset 0, read 0", start: 0, record: 0, wantErr: ErrFutureOffset},
			{name: "start offset 10, read 100", start: 10, record: 100, wantErr: ErrFutureOffset},
			{name: "start offset 100, read 100", start: 100, record: 100, wantErr: ErrFutureOffset},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				l, err := New(ctx, WithStartOffset(tc.start))
				assert.NilError(t, err)

				r, err := l.read(ctx, tc.record)
				assert.Assert(t, errors.Is(err, tc.wantErr))
				assert.Assert(t, r.Metadata.Created.IsZero())
			})
		}
	})

	t.Run("read fails when record is purged", func(t *testing.T) {
		testCases := []struct {
			name         string
			start        Offset
			segSize      int
			writeRecords [][]byte
			record       Offset
			wantErr      error
		}{
			{
				name:         "start offset 0, segment size 5, write 20, read offset 0",
				start:        0,
				segSize:      5,
				writeRecords: NewTestDataSlice(t, 20),
				record:       0,
				wantErr:      ErrOutOfRange,
			},
			{
				name:         "start offset 10, segment size 2, write 5, read offset 10",
				start:        10,
				segSize:      2,
				writeRecords: NewTestDataSlice(t, 5),
				record:       10,
				wantErr:      ErrOutOfRange,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				opts := []Option{
					WithStartOffset(tc.start),
					WithMaxSegmentSize(tc.segSize),
				}

				l, err := New(ctx, opts...)
				assert.NilError(t, err)

				for i, d := range tc.writeRecords {
					offset, writeErr := l.write(ctx, d)
					assert.NilError(t, writeErr)
					assert.Equal(t, offset, tc.start+Offset(i))
				}

				r, err := l.read(ctx, tc.record)
				assert.Assert(t, errors.Is(err, tc.wantErr))
				assert.Assert(t, r.Metadata.Created.IsZero())
			})
		}
	})

	t.Run("read from log succeeds", func(t *testing.T) {
		testCases := []struct {
			name         string
			start        Offset
			segSize      int
			writeRecords [][]byte
		}{
			{
				name:         "start offset 0, segment size 5, write and read 3",
				start:        0,
				segSize:      5,
				writeRecords: NewTestDataSlice(t, 3),
			},
			{
				name:         "start offset 10, segment size 10, write and read 10",
				start:        10,
				segSize:      10,
				writeRecords: NewTestDataSlice(t, 10),
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				mockClock := clock.NewMock()

				opts := []Option{
					WithStartOffset(tc.start),
					WithMaxSegmentSize(tc.segSize),
					WithClock(mockClock),
				}

				now := time.Now().UTC()
				mockClock.Set(now)

				l, err := New(ctx, opts...)
				assert.NilError(t, err)

				for i, d := range tc.writeRecords {
					offset, writeErr := l.write(ctx, d)
					assert.NilError(t, writeErr)
					assert.Equal(t, offset, tc.start+Offset(i))

					got, writeErr := l.read(ctx, offset)
					expected := Record{
						Metadata: Header{
							Offset:  Offset(i) + tc.start,
							Created: now,
						},
						Data: tc.writeRecords[i],
					}

					assert.NilError(t, writeErr)
					assert.DeepEqual(t, got, expected)
				}
			})
		}
	})
}

func Test_offsetRange(t *testing.T) {
	type wantOffsets struct {
		earliest Offset
		latest   Offset
	}

	testCases := []struct {
		name         string
		start        Offset
		segSize      int
		writeRecords [][]byte
		want         wantOffsets
	}{
		{
			name:         "empty log, starts at 0",
			start:        0,
			segSize:      10,
			writeRecords: nil,
			want: wantOffsets{
				earliest: -1,
				latest:   -1,
			},
		},
		{
			name:         "empty log, starts at 100",
			start:        100,
			segSize:      10,
			writeRecords: nil,
			want: wantOffsets{
				earliest: -1,
				latest:   -1,
			},
		},
		{
			name:         "log with 10 records, starts at 0, no purge",
			start:        0,
			segSize:      20,
			writeRecords: NewTestDataSlice(t, 10),
			want: wantOffsets{
				earliest: 0,
				latest:   9,
			},
		},
		{
			name:         "log with 10 records, starts at 60, no purge",
			start:        60,
			segSize:      20,
			writeRecords: NewTestDataSlice(t, 10),
			want: wantOffsets{
				earliest: 60,
				latest:   69,
			},
		},
		{
			name:         "log with 30 records, starts at 10, segment size 10, purged history",
			start:        10,
			segSize:      10,
			writeRecords: NewTestDataSlice(t, 30),
			want: wantOffsets{
				earliest: 20,
				latest:   39,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			opts := []Option{
				WithStartOffset(tc.start),
				WithMaxSegmentSize(tc.segSize),
			}

			l, err := New(ctx, opts...)
			assert.NilError(t, err)

			for i, r := range tc.writeRecords {
				offset, writeErr := l.write(ctx, r)
				assert.NilError(t, writeErr)
				assert.Equal(t, offset, tc.start+Offset(i))
			}

			earliest, latest := l.offsetRange()
			assert.Equal(t, earliest, tc.want.earliest)
			assert.Equal(t, latest, tc.want.latest)
		})
	}
}

func newTestData(t *testing.T, id string) []byte {
	r := map[string]string{
		"id":     id,
		"type":   "record.created.event.v0",
		"source": "/api/v1/memlog_test",
	}

	b, err := json.Marshal(r)
	assert.NilError(t, err)

	return b
}

func NewTestDataSlice(t *testing.T, count int) [][]byte {
	t.Helper()

	records := make([][]byte, count)
	for i := 0; i < count; i++ {
		records[i] = newTestData(t, strconv.Itoa(i+1))
	}

	return records
}
