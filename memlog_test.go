package memlog_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/benbjohnson/clock"
	"golang.org/x/sync/errgroup"
	"gotest.tools/v3/assert"

	"github.com/embano1/memlog"
)

func TestLog_ReadBatch(t *testing.T) {
	t.Run("fails to read batch", func(t *testing.T) {
		testCases := []struct {
			name        string
			start       memlog.Offset // log start
			segSize     int
			records     [][]byte
			offset      memlog.Offset // read offset
			batchSize   int
			wantRecords int // total number records read
			wantErr     error
		}{
			{
				name:        "fails on empty log",
				start:       0,
				segSize:     10,
				records:     nil,
				offset:      0,
				batchSize:   10,
				wantRecords: 0,
				wantErr:     memlog.ErrFutureOffset,
			},
			{
				name:        "fails on invalid start offset",
				start:       10,
				segSize:     10,
				records:     memlog.NewTestDataSlice(t, 10),
				offset:      0,
				batchSize:   10,
				wantRecords: 0,
				wantErr:     memlog.ErrOutOfRange,
			},
			{
				name:        "fails on invalid read offset",
				start:       10,
				segSize:     10,
				records:     memlog.NewTestDataSlice(t, 10),
				offset:      20,
				batchSize:   10,
				wantRecords: 0,
				wantErr:     memlog.ErrFutureOffset,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()

				opts := []memlog.Option{
					memlog.WithClock(clock.NewMock()),
					memlog.WithStartOffset(tc.start),
					memlog.WithMaxSegmentSize(tc.segSize),
				}

				l, err := memlog.New(ctx, opts...)
				assert.NilError(t, err)

				records := make([]memlog.Record, tc.batchSize)
				count, err := l.ReadBatch(ctx, tc.offset, records)
				assert.Assert(t, errors.Is(err, tc.wantErr))
				assert.Equal(t, count, tc.wantRecords)
			})
		}
	})

	t.Run("fails on cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		l, err := memlog.New(ctx)
		assert.NilError(t, err)

		records := make([]memlog.Record, 10)
		cancel()
		count, err := l.ReadBatch(ctx, 0, records)
		assert.Assert(t, errors.Is(err, context.Canceled))
		assert.Equal(t, count, 0)
	})

	t.Run("fails when deadline exceeded", func(t *testing.T) {
		ctx := context.Background()
		l, err := memlog.New(ctx)
		assert.NilError(t, err)

		ctx, cancel := context.WithTimeout(ctx, 0)
		defer cancel()

		records := make([]memlog.Record, 10)
		count, err := l.ReadBatch(ctx, 0, records)
		assert.Assert(t, errors.Is(err, context.DeadlineExceeded))
		assert.Equal(t, count, 0)
	})

	t.Run("reads one batch", func(t *testing.T) {
		testCases := []struct {
			name      string
			start     memlog.Offset // log start
			segSize   int
			records   [][]byte
			offset    memlog.Offset // read offset
			batchSize int
		}{
			{
				name:      "log starts at 0, write 10 records, no purge, batch size 10",
				start:     0,
				segSize:   10,
				records:   memlog.NewTestDataSlice(t, 10),
				offset:    0,
				batchSize: 10,
			},
			{
				name:      "log starts at 0, write 10 records, no purge, batch size 5",
				start:     0,
				segSize:   10,
				records:   memlog.NewTestDataSlice(t, 10),
				offset:    0,
				batchSize: 5,
			},
			{
				name:      "log starts at 10, write 10 records, no purge, batch size 0",
				start:     10,
				segSize:   10,
				records:   memlog.NewTestDataSlice(t, 10),
				offset:    0,
				batchSize: 0,
			},
			{
				name:      "log starts at 10, write 5 records, no purge, batch size 5",
				start:     10,
				segSize:   10,
				records:   memlog.NewTestDataSlice(t, 5),
				offset:    10,
				batchSize: 5,
			},
			{
				name:      "log starts at 10, write 30 records, with purge, batch size 10",
				start:     10,
				segSize:   10,
				records:   memlog.NewTestDataSlice(t, 30),
				offset:    30,
				batchSize: 10,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()

				opts := []memlog.Option{
					memlog.WithClock(clock.NewMock()),
					memlog.WithStartOffset(tc.start),
					memlog.WithMaxSegmentSize(tc.segSize),
				}

				l, err := memlog.New(ctx, opts...)
				assert.NilError(t, err)

				for _, d := range tc.records {
					_, err = l.Write(ctx, d)
					assert.NilError(t, err)
				}

				records := make([]memlog.Record, tc.batchSize)
				count, err := l.ReadBatch(ctx, tc.offset, records)
				assert.NilError(t, err)
				assert.Equal(t, count, tc.batchSize)
			})
		}
	})

	t.Run("reads multiple batches until end of log", func(t *testing.T) {
		testCases := []struct {
			name      string
			start     memlog.Offset // log start
			segSize   int
			records   [][]byte
			offset    memlog.Offset // read offset
			batchSize int
		}{
			{
				name:      "log starts at 0, write 30 records, no purge, batch size 10",
				start:     0,
				segSize:   30,
				records:   memlog.NewTestDataSlice(t, 30),
				offset:    0,
				batchSize: 10,
			},
			{
				name:      "log starts at 0, write 10 records, no purge, read from 9, batch size 5",
				start:     0,
				segSize:   30,
				records:   memlog.NewTestDataSlice(t, 10),
				offset:    9,
				batchSize: 5,
			},
			{
				name:      "log starts at 0, write 30 records, no purge, read from 10, batch size 5",
				start:     0,
				segSize:   30,
				records:   memlog.NewTestDataSlice(t, 30),
				offset:    10,
				batchSize: 5,
			},
			{
				name:      "log starts at 10, write 40 records, no purge, read from 20, batch size 1",
				start:     10,
				segSize:   40,
				records:   memlog.NewTestDataSlice(t, 40),
				offset:    20,
				batchSize: 1,
			},
			{
				name:      "log starts at 0, write 30 records, with purge, read from 10, batch size 5",
				start:     0,
				segSize:   10,
				records:   memlog.NewTestDataSlice(t, 30),
				offset:    10,
				batchSize: 5,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()

				opts := []memlog.Option{
					memlog.WithClock(clock.NewMock()),
					memlog.WithStartOffset(tc.start),
					memlog.WithMaxSegmentSize(tc.segSize),
				}

				l, err := memlog.New(ctx, opts...)
				assert.NilError(t, err)

				for _, d := range tc.records {
					_, err = l.Write(ctx, d)
					assert.NilError(t, err)
				}

				records := make([]memlog.Record, tc.batchSize)

				var (
					count  int
					total  int
					offset = tc.offset
				)
				for err == nil {
					count, err = l.ReadBatch(ctx, offset, records)
					total += count
					offset += memlog.Offset(count)
				}

				assert.Assert(t, errors.Is(err, memlog.ErrFutureOffset))
				switch {
				// with purge
				case len(tc.records) > tc.segSize:
					assert.Equal(t, total, len(tc.records)-(2*tc.segSize)+int(tc.offset))
				// 	no purge
				default:
					assert.Equal(t, total, len(tc.records)-int(tc.offset-tc.start))
				}
			})
		}
	})
}

func TestLog_Checkpoint_Resume(t *testing.T) {
	const (
		sourceDataCount = 50
		start           = memlog.Offset(0)
		segSize         = 20
	)

	var (
		log *memlog.Log

		ctx        = context.Background()
		sourceData = memlog.NewTestDataSlice(t, sourceDataCount)
		checkpoint memlog.Offset
		records    []memlog.Record
	)

	t.Run("create log", func(t *testing.T) {
		opts := []memlog.Option{
			memlog.WithClock(clock.NewMock()),
			memlog.WithStartOffset(start),
			memlog.WithMaxSegmentSize(segSize),
		}

		l, err := memlog.New(ctx, opts...)
		assert.NilError(t, err)
		log = l
	})

	t.Run("writes 20 records", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			offset, err := log.Write(ctx, sourceData[i])
			assert.NilError(t, err)
			assert.Equal(t, offset, memlog.Offset(i))
		}
	})

	t.Run("reads 20 records, creates checkpoint at 10", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			r, err := log.Read(ctx, memlog.Offset(i))
			assert.NilError(t, err)
			assert.Equal(t, r.Metadata.Offset, memlog.Offset(i))
			records = append(records, r)

			if r.Metadata.Offset == 10 {
				checkpoint = r.Metadata.Offset
				t.Logf("checkpoint created at offset %d", checkpoint)
			}
		}
	})

	t.Run("log crashes, writes 20 records starting from last checkpoint", func(t *testing.T) {
		log = nil

		opts := []memlog.Option{
			memlog.WithClock(clock.NewMock()),
			memlog.WithStartOffset(checkpoint),
			memlog.WithMaxSegmentSize(segSize),
		}

		l, err := memlog.New(ctx, opts...)
		assert.NilError(t, err)
		log = l

		for i := checkpoint; i < checkpoint+20; i++ {
			offset, writeErr := log.Write(ctx, sourceData[i])
			assert.NilError(t, writeErr)
			assert.Equal(t, offset, i)
		}
	})

	t.Run("reader resumes, catches up until no new data, creates checkpoint at last successful record", func(t *testing.T) {
		for i := checkpoint; ; i++ {
			r, err := log.Read(ctx, i)
			if err != nil {
				assert.Assert(t, errors.Is(err, memlog.ErrFutureOffset))
				checkpoint = memlog.Offset(i) - 1 // last successful read
				t.Logf("checkpoint created at offset %d", checkpoint)
				break
			}

			assert.Equal(t, r.Metadata.Offset, i)
			records = append(records, r)
		}
	})

	t.Run("continue writes, records purged", func(t *testing.T) {
		for i := int(checkpoint); i < len(sourceData); i++ {
			_, err := log.Write(ctx, sourceData[i])
			assert.NilError(t, err)
		}
	})

	t.Run("slow reader attempts to read purged record", func(t *testing.T) {
		_, err := log.Read(ctx, checkpoint)
		assert.Assert(t, errors.Is(err, memlog.ErrOutOfRange))
	})

	t.Run("retrieve earliest and latest, read until end", func(t *testing.T) {
		earliest, latest := log.Range(ctx)

		for i := earliest; i <= latest; i++ {
			r, err := log.Read(ctx, i)
			assert.NilError(t, err)
			records = append(records, r)
		}
	})

	t.Run("all records received", func(t *testing.T) {
		d := dedupe(t, records)
		assert.Equal(t, len(d), len(sourceData))

		for i, r := range d {
			assert.DeepEqual(t, r.Data, sourceData[i])
		}
	})
}

func TestLog_Concurrent(t *testing.T) {
	type wantOffsets struct {
		earliest memlog.Offset
		latest   memlog.Offset
	}
	testCases := []struct {
		name    string
		start   memlog.Offset
		segSize int
		worker  int
		want    wantOffsets
	}{
		{
			name:    "100 workers, starts at 0, no purge",
			start:   0,
			segSize: 100,
			worker:  100,
			want: wantOffsets{
				earliest: 0,
				latest:   99,
			},
		},
		{
			name:    "100 workers, starts at 100, with purge",
			start:   100,
			segSize: 10,
			worker:  50,
			want: wantOffsets{
				earliest: 130,
				latest:   149,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			opts := []memlog.Option{
				memlog.WithStartOffset(tc.start),
				memlog.WithMaxSegmentSize(tc.segSize),
			}

			l, err := memlog.New(ctx, opts...)
			assert.NilError(t, err)

			eg, egCtx := errgroup.WithContext(ctx)
			testData := memlog.NewTestDataSlice(t, tc.worker)

			for i := 0; i < tc.worker; i++ {
				data := testData[i]
				eg.Go(func() error {
					offset, writeErr := l.Write(egCtx, data)
					assert.Assert(t, offset != -1)

					// assert earliest/latest never return invalid offsets
					earliest, latest := l.Range(ctx)
					assert.Assert(t, earliest != memlog.Offset(-1))
					assert.Assert(t, latest != memlog.Offset(-1))
					return writeErr
				})
			}

			err = eg.Wait()
			assert.NilError(t, err)

			earliest, latest := l.Range(ctx)
			assert.Equal(t, earliest, tc.want.earliest)
			assert.Equal(t, latest, tc.want.latest)
		})
	}
}

func dedupe(t *testing.T, records []memlog.Record) []memlog.Record {
	t.Helper()

	type dataSchema struct {
		ID string `json:"id"`
	}

	var (
		deduped []memlog.Record
		d       dataSchema
	)

	seen := make(map[string]struct{})
	for _, r := range records {
		err := json.Unmarshal(r.Data, &d)
		assert.NilError(t, err)

		if _, ok := seen[d.ID]; !ok {
			seen[d.ID] = struct{}{}
			deduped = append(deduped, r)
		}
	}

	return deduped
}
