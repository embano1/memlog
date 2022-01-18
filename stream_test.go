package memlog

import (
	"context"
	"errors"
	"sync"
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

				stream := l.Stream(ctx, tc.streamStart)
				counter := 0

				for {
					if r, ok := stream.Next(); ok {
						assert.Equal(t, r.Metadata.Offset, Offset(counter)+tc.streamStart)

						counter++
						if counter == tc.stopAfter {
							cancel()
						}
						continue
					}
					break
				}

				assert.Assert(t, errors.Is(stream.Err(), context.Canceled))
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

				stream := l.Stream(ctx, tc.streamStart)

				for {
					if _, ok := stream.Next(); ok {
						t.Fatalf("should not receive from stream")
					} else {
						break
					}
				}
				assert.Assert(t, errors.Is(stream.Err(), ErrOutOfRange))
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

				stream := l.Stream(ctx, tc.streamStart)
				counter := 0

				for {
					if r, ok := stream.Next(); ok {
						assert.Equal(t, r.Metadata.Offset, Offset(counter)+tc.streamStart)

						counter++
						if counter == tc.stopAfter {
							cancel()
						}
						continue
					}
					break
				}

				assert.Assert(t, errors.Is(stream.Err(), context.Canceled))
				assert.Equal(t, counter, tc.stopAfter)
			})
		}
	})

	t.Run("two stream receivers, starting at different offsets until stream cancelled", func(t *testing.T) {
		t.Parallel()

		const (
			logStart       = Offset(0)
			segSize        = 1000
			streamOneStart = Offset(0)
			streamTwoStart = Offset(5)
			writeRecords   = 10
		)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		opts := []Option{
			WithStartOffset(logStart),
			WithMaxSegmentSize(segSize),
		}

		l, err := New(ctx, opts...)
		assert.NilError(t, err)

		var wg sync.WaitGroup

		// writer
		wg.Add(1)
		go func() {
			ticker := time.NewTicker(time.Millisecond * 10) // 100 wps

			defer func() {
				ticker.Stop()
				wg.Done()
			}()

			counter := 0
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if counter == writeRecords {
						return
					}
					_, writeErr := l.Write(ctx, []byte(`{"id":"someID","message":"write data"}`))
					assert.NilError(t, writeErr)
					counter++
				}
			}
		}()

		// stream reader one
		wg.Add(1)
		s1Counter := 0
		go func() {
			defer wg.Done()

			stream := l.Stream(ctx, streamOneStart)
			for {
				r, ok := stream.Next()
				if ok {
					assert.Equal(t, r.Metadata.Offset, logStart+Offset(s1Counter)+streamOneStart)
					s1Counter++
					continue
				}
				break
			}
			assert.Assert(t, errors.Is(stream.Err(), context.DeadlineExceeded))
		}()

		// stream reader two
		wg.Add(1)
		s2Counter := 0
		go func() {
			defer wg.Done()

			stream := l.Stream(ctx, streamTwoStart)
			for {
				r, ok := stream.Next()
				if ok {
					assert.Equal(t, r.Metadata.Offset, logStart+Offset(s2Counter)+streamTwoStart)
					s2Counter++
					continue
				}
				break
			}
			assert.Assert(t, errors.Is(stream.Err(), context.DeadlineExceeded))
		}()

		wg.Wait()
		assert.Equal(t, s1Counter, 10)
		assert.Equal(t, s2Counter, 5)
	})
}
