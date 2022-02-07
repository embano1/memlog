package sharded_test

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/benbjohnson/clock"
	"gotest.tools/v3/assert"

	"github.com/embano1/memlog"
	"github.com/embano1/memlog/sharded"
)

const (
	defaultShards  = 10
	defaultStart   = 0
	defaultSegSize = 10
)

func TestLog_Read_SingleRecord(t *testing.T) {
	testCases := []struct {
		name string

		clock   clock.Clock
		shards  int
		sharder sharded.Sharder
		start   memlog.Offset // log start
		segSize int
		records map[string][][]byte

		offset       memlog.Offset // read start offset
		keys         []string      // read keys
		wantRecords  int           // total number records read
		wantReadErr  string
		wantWriteErr string
	}{
		{
			name:         "read fails with out of range error",
			clock:        clock.NewMock(),
			shards:       defaultShards,
			sharder:      sharded.NewKeySharder([]string{"users"}),
			start:        defaultStart,
			segSize:      defaultSegSize,
			records:      newTestDataMap(t, 100, "users"),
			offset:       0, // purged
			keys:         []string{"users"},
			wantRecords:  0,
			wantReadErr:  "out of range",
			wantWriteErr: "",
		},
		{
			name:         "read fails with future offset error",
			clock:        clock.NewMock(),
			shards:       defaultShards,
			sharder:      sharded.NewKeySharder([]string{"users"}),
			start:        defaultStart,
			segSize:      defaultSegSize,
			records:      newTestDataMap(t, 100, "users"),
			offset:       101, // future
			keys:         []string{"users"},
			wantRecords:  0,
			wantReadErr:  "future offset",
			wantWriteErr: "",
		},
		{
			name:         "read fails due to invalid offset",
			clock:        clock.NewMock(),
			shards:       defaultShards,
			sharder:      sharded.NewKeySharder([]string{"users"}),
			start:        defaultStart,
			segSize:      defaultSegSize,
			records:      newTestDataMap(t, 100, "users"),
			offset:       -10,
			keys:         []string{"users"},
			wantRecords:  0,
			wantReadErr:  "out of range",
			wantWriteErr: "",
		},
		{
			name:         "read fails due to non-existing shard key",
			clock:        clock.NewMock(),
			shards:       defaultShards,
			sharder:      sharded.NewKeySharder([]string{"users"}),
			start:        defaultStart,
			segSize:      defaultSegSize,
			records:      newTestDataMap(t, 100, "users"),
			offset:       0,
			keys:         []string{"groups"},
			wantRecords:  0,
			wantReadErr:  "shard not found",
			wantWriteErr: "",
		},
		{
			name:         "read fails due to key shard count mismatch",
			clock:        clock.NewMock(),
			shards:       2, // must be smaller than key count
			sharder:      sharded.NewKeySharder([]string{"users", "groups", "machines"}),
			start:        defaultStart,
			segSize:      defaultSegSize,
			records:      newTestDataMap(t, 100, "users", "groups", "machines"),
			offset:       0,
			keys:         []string{"users", "groups", "machines"},
			wantRecords:  0,
			wantReadErr:  "greater than available shards",
			wantWriteErr: "greater than available shards",
		},
		{
			name:         "read succeeds, one key, start offset 0, read offset 0",
			clock:        clock.NewMock(),
			shards:       defaultShards,
			sharder:      sharded.NewKeySharder([]string{"users"}),
			start:        defaultStart,
			segSize:      defaultSegSize,
			records:      newTestDataMap(t, 10, "users"),
			offset:       0,
			keys:         []string{"users"},
			wantRecords:  1,
			wantReadErr:  "",
			wantWriteErr: "",
		},
		{
			name:         "read succeeds, one key, start offset 100, read offset 100",
			clock:        clock.NewMock(),
			shards:       defaultShards,
			sharder:      sharded.NewKeySharder([]string{"users"}),
			start:        100,
			segSize:      defaultSegSize,
			records:      newTestDataMap(t, 10, "users"),
			offset:       100,
			keys:         []string{"users"},
			wantRecords:  1,
			wantReadErr:  "",
			wantWriteErr: "",
		},
		{
			name:         "read succeeds, two keys, read offset 5",
			clock:        clock.NewMock(),
			shards:       defaultShards,
			sharder:      sharded.NewKeySharder([]string{"users", "groups"}),
			start:        defaultStart,
			segSize:      defaultSegSize,
			records:      newTestDataMap(t, 10, "users", "groups"),
			offset:       5,
			keys:         []string{"users", "groups"},
			wantRecords:  2,
			wantReadErr:  "",
			wantWriteErr: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			opts := []sharded.Option{
				sharded.WithNumShards(uint(tc.shards)),
				sharded.WithClock(tc.clock),
				sharded.WithStartOffset(tc.start),
				sharded.WithMaxSegmentSize(tc.segSize),
				sharded.WithSharder(tc.sharder),
			}
			l, err := sharded.New(ctx, opts...)
			assert.NilError(t, err)

			// seed log
			for k, records := range tc.records {
				for _, r := range records {
					_, err := l.Write(ctx, []byte(k), r)
					if tc.wantWriteErr != "" {
						assert.ErrorContains(t, err, tc.wantWriteErr)
					} else {
						assert.NilError(t, err)
					}
				}
			}

			records := 0
			for _, k := range tc.keys {
				r, err := l.Read(ctx, []byte(k), tc.offset)
				if tc.wantReadErr != "" {
					assert.ErrorContains(t, err, tc.wantReadErr)
				} else {
					assert.NilError(t, err)
				}

				if !r.Metadata.Created.IsZero() {
					// valid record
					records++
				}
			}

			assert.Equal(t, records, tc.wantRecords)
		})
	}
}

func TestLog_Read_AllRecords_Concurrent(t *testing.T) {
	keys := []string{"users", "groups", "machines", "clusters", "datacenters"}

	ctx := context.Background()
	opts := []sharded.Option{
		sharded.WithNumShards(uint(defaultShards)),
		sharded.WithStartOffset(defaultStart),
		sharded.WithMaxSegmentSize(defaultSegSize),
		sharded.WithSharder(sharded.NewKeySharder(keys)),
	}
	l, err := sharded.New(ctx, opts...)
	assert.NilError(t, err)

	// seed log
	data := newTestDataMap(t, defaultSegSize, keys...)
	for k, records := range data {
		for _, r := range records {
			_, err := l.Write(ctx, []byte(k), r)
			assert.NilError(t, err)
		}
	}

	var (
		got int32
		wg  sync.WaitGroup
	)

	for _, k := range keys {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()

			offset := memlog.Offset(defaultStart)
			for {
				r, err := l.Read(ctx, []byte(key), offset)
				if err != nil {
					if errors.Is(err, memlog.ErrFutureOffset) {
						break
					}
				}
				assert.NilError(t, err) // catches all err cases
				assert.Equal(t, r.Metadata.Offset, offset)

				atomic.AddInt32(&got, 1)
				offset++
			}
		}(k)
	}

	wg.Wait()

	want := len(keys) * defaultSegSize
	assert.Equal(t, got, int32(want))
}

func TestLog_Read_AllRecords(t *testing.T) {
	keys := []string{"friends", "family", "colleagues"}

	ctx := context.Background()
	opts := []sharded.Option{
		sharded.WithNumShards(uint(defaultShards)),
		sharded.WithStartOffset(defaultStart),
		sharded.WithMaxSegmentSize(defaultSegSize),
	}

	// uses default sharder
	l, err := sharded.New(ctx, opts...)
	assert.NilError(t, err)

	// seed log
	data := newTestDataMap(t, defaultSegSize, keys...)
	for k, records := range data {
		for _, r := range records {
			_, err := l.Write(ctx, []byte(k), r)
			assert.NilError(t, err)
		}
	}

	var got int
	for _, k := range keys {
		offset := memlog.Offset(defaultStart)
		for {
			r, err := l.Read(ctx, []byte(k), offset)
			if err != nil {
				if errors.Is(err, memlog.ErrFutureOffset) {
					break
				}
			}
			assert.NilError(t, err)                                // catches all err cases
			assert.Assert(t, r.Metadata.Created.IsZero() == false) // must be valid record
			assert.Equal(t, r.Metadata.Offset, offset)

			hasKey := func(key string) bool {
				// matches testdata
				d := struct {
					Key string `json:"key,omitempty"`
				}{}

				err := json.Unmarshal(r.Data, &d)
				assert.NilError(t, err)
				return d.Key == key
			}

			// default sharder is hash-based so a shard (memlog.Log) could contain multiple
			// keys, only count unique items
			if hasKey(k) {
				got++
			}
			offset++
		}
	}

	want := len(keys) * defaultSegSize
	assert.Equal(t, got, want)
}

func newTestData(t *testing.T, id, key string) []byte {
	r := map[string]string{
		"id":     id,
		"key":    key,
		"type":   "record.created.event.v0",
		"source": "/api/v1/memlog_test",
	}

	b, err := json.Marshal(r)
	assert.NilError(t, err)

	return b
}

// map of key/records, creates "count" records per key
func newTestDataMap(t *testing.T, count int, keys ...string) map[string][][]byte {
	t.Helper()

	records := make(map[string][][]byte)
	for i := 0; i < count; i++ {
		for _, k := range keys {
			records[k] = append(records[k], newTestData(t, strconv.Itoa(i+1), k))
		}
	}

	return records
}
