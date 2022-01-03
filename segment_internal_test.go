package memlog

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func Test_newSegment(t *testing.T) {
	t.Run("new fails with size 0", func(t *testing.T) {
		const (
			start Offset = 0
			size         = 0
		)
		_, err := newSegment(start, size)
		assert.ErrorContains(t, err, "size must be")
	})

	t.Run("new fails with negative size", func(t *testing.T) {
		const (
			start Offset = 0
			size         = -10
		)
		_, err := newSegment(start, size)
		assert.ErrorContains(t, err, "size must be")
	})

	t.Run("new fails with invalid start offset", func(t *testing.T) {
		const (
			start = -10
			size  = 10
		)
		_, err := newSegment(start, size)
		assert.ErrorContains(t, err, "start offset must not be")
	})

	t.Run("verify defaults on new segment", func(t *testing.T) {
		const (
			start Offset = 0
			size         = 10
		)

		s, err := newSegment(start, size)
		assert.NilError(t, err)
		assert.Equal(t, s.start, start)
		assert.Equal(t, s.currentOffset(), Offset(-1))
		assert.Equal(t, s.sealed, false)
	})
}

func TestSegment_ReadWrite(t *testing.T) {
	t.Run("read fails on cancelled context", func(t *testing.T) {
		const (
			start Offset = 0
			size         = 10
		)

		s, err := newSegment(start, size)
		assert.NilError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		r, err := s.read(ctx, start)
		assert.Assert(t, errors.Is(err, context.Canceled))
		assert.Assert(t, r.Metadata.Created.IsZero())
	})

	t.Run("read fails on out of range offset", func(t *testing.T) {
		type testCase struct {
			name     string
			segSize  int
			segStart Offset
			invalid  Offset
		}
		testMatrix := []testCase{
			{
				name:     "empty segment",
				segSize:  10,
				segStart: 0,
				invalid:  0,
			},
			{
				name:     "offset smaller than start",
				segSize:  10,
				segStart: 10,
				invalid:  0,
			},
			{
				name:     "offset greater than segment range",
				segSize:  10,
				segStart: 10,
				invalid:  20,
			},
		}

		for _, tc := range testMatrix {
			t.Run(tc.name, func(t *testing.T) {
				ctx := context.Background()
				s, err := newSegment(tc.segStart, tc.segSize)
				assert.NilError(t, err)

				r, err := s.read(ctx, tc.invalid)
				assert.Assert(t, errors.Is(err, ErrOutOfRange))
				assert.Equal(t, len(r.Data), 0)
				assert.Assert(t, r.Metadata.Created.IsZero())
				assert.Equal(t, s.currentOffset(), Offset(-1))
			})
		}
	})
}

func TestSegment_Write(t *testing.T) {
	t.Run("write fails on cancelled context", func(t *testing.T) {
		const (
			start Offset = 0
			size         = 10
		)

		s, err := newSegment(start, size)
		assert.NilError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = s.write(ctx, Record{})
		assert.Assert(t, errors.Is(err, context.Canceled))
		assert.Equal(t, s.currentOffset(), Offset(-1))
	})

	t.Run("write fails on sealed segment", func(t *testing.T) {
		const (
			start Offset = 0
			size         = 10
		)

		ctx := context.Background()

		s, err := newSegment(start, size)
		assert.NilError(t, err)

		s.seal()
		assert.Equal(t, s.sealed, true)

		err = s.write(ctx, Record{})
		assert.Assert(t, errors.Is(err, errSealed))
		assert.Equal(t, s.currentOffset(), Offset(-1))
	})

	t.Run("write fails on full segment", func(t *testing.T) {
		const (
			start Offset = 0
			size         = 5
		)

		ctx := context.Background()

		s, err := newSegment(start, size)
		assert.NilError(t, err)

		for i := 0; i < 5; i++ {
			err = s.write(ctx, Record{})
			assert.NilError(t, err)
			assert.Equal(t, s.currentOffset(), Offset(i))
		}

		err = s.write(ctx, Record{})
		assert.Assert(t, errors.Is(err, errFull))
		assert.Equal(t, s.currentOffset(), Offset(size-1))
	})

	t.Run("write and read one record, starts at virtual offset 0", func(t *testing.T) {
		const (
			start Offset = 0
			size         = 10
		)

		ctx := context.Background()

		s, err := newSegment(start, size)
		assert.NilError(t, err)

		now := time.Now().UTC()
		r := Record{
			Metadata: Header{
				Offset:  0,
				Created: now,
			},
			Data: newTestData(t, "1"),
		}

		err = s.write(ctx, r)
		assert.NilError(t, err)
		assert.Equal(t, s.currentOffset(), start)
		assert.Equal(t, len(s.data), 1)

		res, err := s.read(ctx, start)
		assert.NilError(t, err)
		assert.DeepEqual(t, r, res)
	})

	t.Run("write and read five records, starts virtual offset 10", func(t *testing.T) {
		const (
			start        Offset = 10
			size                = 10
			eventIDRange        = 10
			numRecords          = 5
		)

		var (
			testRecords []Record
			resRecords  []Record
		)

		ctx := context.Background()

		s, err := newSegment(start, size)
		assert.NilError(t, err)

		now := time.Now().UTC()
		// write
		for i := 0; i < numRecords; i++ {
			r := Record{
				Metadata: Header{
					Offset:  start + Offset(i),
					Created: now,
				},
				Data: newTestData(t, strconv.Itoa(i+eventIDRange)),
			}
			testRecords = append(testRecords, r)

			err = s.write(ctx, r)
			assert.NilError(t, err)
			assert.Equal(t, s.currentOffset(), Offset(i)+start)
		}

		// read
		for i := 0; i < numRecords; i++ {
			res, readErr := s.read(ctx, Offset(i)+start)
			assert.NilError(t, readErr)

			resRecords = append(resRecords, res)
		}

		assert.Equal(t, s.currentOffset(), start+Offset(numRecords)-1)
		assert.DeepEqual(t, testRecords, resRecords)
	})
}
