package memlog

import (
	"context"
	"errors"
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestLog_Stream(t *testing.T) {
	t.Run("streams records from log start then cancels stream", func(t *testing.T) {
		testCases := []struct {
			name        string
			logStart    Offset
			streamStart Offset
			segSize     int
			stopAfter   int
		}{
			{
				name:        "log starts at 0, stream starts at 0, receives 5, cancels stream",
				logStart:    0,
				streamStart: 0,
				segSize:     10,
				stopAfter:   5,
			},
			{
				name:        "log starts at 10, stream starts at 10, receives 5, cancels stream",
				logStart:    10,
				streamStart: 10,
				segSize:     10,
				stopAfter:   5,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				testData := NewTestDataSlice(t, tc.segSize)
				opts := []Option{
					WithStartOffset(tc.logStart),
					WithMaxSegmentSize(tc.segSize),
				}

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				l, err := New(ctx, opts...)
				assert.NilError(t, err)

				// seed log
				for _, d := range testData {
					_, err = l.Write(ctx, d)
					assert.NilError(t, err)
				}

				streamCh, errCh := l.Stream(ctx, tc.streamStart)

				counter := 0
			LOOP:
				for {
					select {
					case r := <-streamCh:
						assert.Equal(t, r.Metadata.Earliest, tc.logStart)
						assert.Equal(t, r.Metadata.Latest, tc.logStart+Offset(tc.segSize-1))
						assert.Equal(t, r.Record.Metadata.Offset, Offset(counter)+tc.streamStart)

						counter++
						if counter == tc.stopAfter {
							cancel()
						}
					case streamErr := <-errCh:
						assert.Assert(t, errors.Is(streamErr, context.Canceled))
						break LOOP
					}
				}

				assert.Equal(t, counter, tc.stopAfter)
			})
		}
	})

	t.Run("stream returns out of range error", func(t *testing.T) {
		testCases := []struct {
			name         string
			logStart     Offset
			writeRecords int
			streamStart  Offset
			segSize      int
		}{
			{
				name:         "log starts at 0, stream starts at -10",
				logStart:     0,
				writeRecords: 10,
				streamStart:  -10,
				segSize:      10,
			},
			{
				name:         "log starts at 10, stream starts at 0",
				logStart:     10,
				writeRecords: 10,
				streamStart:  0,
				segSize:      10,
			},
			{
				name:         "log starts at 0, first records purged, stream starts at 60",
				logStart:     0,
				writeRecords: 100,
				streamStart:  60,
				segSize:      10,
			},
			{
				name:         "log starts at 100, first records purged, stream starts at 150",
				logStart:     100,
				writeRecords: 100,
				streamStart:  150,
				segSize:      10,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				testData := NewTestDataSlice(t, tc.writeRecords)
				opts := []Option{
					WithStartOffset(tc.logStart),
					WithMaxSegmentSize(tc.segSize),
				}

				ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
				defer cancel()

				l, err := New(ctx, opts...)
				assert.NilError(t, err)

				// seed log
				for _, d := range testData {
					_, err = l.Write(ctx, d)
					assert.NilError(t, err)
				}

				streamCh, errCh := l.Stream(ctx, tc.streamStart)

				select {
				case <-ctx.Done():
					t.Fatalf("should not fail with %v", ctx.Err())
				case <-streamCh:
					t.Fatalf("should not receive from stream")
				case streamErr := <-errCh:
					assert.Assert(t, errors.Is(streamErr, ErrOutOfRange))

				}
			})
		}
	})

	t.Run("stream starts at future offset, streams records then cancels stream", func(t *testing.T) {
		testCases := []struct {
			name         string
			logStart     Offset
			streamStart  Offset
			segSize      int
			writeRecords int
			stopAfter    int
		}{
			{
				name:         "log starts at 0, stream starts at 5, receives 5, cancels stream",
				logStart:     0,
				streamStart:  5,
				segSize:      10,
				writeRecords: 10,
				stopAfter:    5,
			},
			{
				name:         "log starts at 10, stream starts at 18, receives 2, cancels stream",
				logStart:     10,
				streamStart:  18,
				segSize:      10,
				writeRecords: 10,
				stopAfter:    2,
			},
			{
				name:         "log starts at 0, stream starts at 10, receives 30 with purge, cancels stream",
				logStart:     0,
				streamStart:  10,
				segSize:      10,
				writeRecords: 50,
				stopAfter:    30,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				testData := NewTestDataSlice(t, tc.writeRecords)
				opts := []Option{
					WithStartOffset(tc.logStart),
					WithMaxSegmentSize(tc.segSize),
				}

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				l, err := New(ctx, opts...)
				assert.NilError(t, err)

				// seed log with initial data
				for _, d := range testData[:tc.writeRecords/2] {
					_, writeErr := l.Write(ctx, d)
					assert.NilError(t, writeErr)
				}

				go func() {
					// write more
					for _, d := range testData[tc.writeRecords/2:] {
						_, writeErr := l.Write(ctx, d)

						validErr := func(err error) bool {
							return err == nil || errors.Is(err, context.Canceled)
						}
						assert.Assert(t, validErr(writeErr))

						time.Sleep(time.Millisecond * 50)
					}
				}()

				streamCh, errCh := l.Stream(ctx, tc.streamStart)

				counter := 0
			LOOP:
				for {
					select {
					case r := <-streamCh:
						assert.Equal(t, r.Record.Metadata.Offset, Offset(counter)+tc.streamStart)

						counter++
						if counter == tc.stopAfter {
							cancel()
						}
					case streamErr := <-errCh:
						assert.Assert(t, errors.Is(streamErr, context.Canceled))
						break LOOP
					}
				}

				assert.Equal(t, counter, tc.stopAfter)
			})
		}
	})

	t.Run("returns error when stream reader is too slow", func(t *testing.T) {
		t.Parallel()

		const (
			logStart    = Offset(0)
			segSize     = 1000
			streamStart = Offset(0)
		)

		ctx := context.Background()
		opts := []Option{
			WithStartOffset(logStart),
			WithMaxSegmentSize(segSize),
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()

		l, err := New(ctx, opts...)
		assert.NilError(t, err)

		_, errCh := l.Stream(ctx, streamStart)

		go func() {
			for {
				_, writeErr := l.Write(ctx, []byte(`{"id":"someID","message":"write data"}`))
				validErr := func(err error) bool {
					return err == nil || errors.Is(err, context.Canceled)
				}
				assert.Assert(t, validErr(writeErr))

				time.Sleep(time.Millisecond * 10) // 100 writes/s
			}
		}()

		streamErr := <-errCh
		assert.Assert(t, errors.Is(streamErr, ErrSlowReader))
	})
}
