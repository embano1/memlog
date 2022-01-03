package memlog

import (
	"context"
	"errors"
	"time"
)

const (
	streamBuffer          = 100 // limit before ErrSlowReader
	streamBackoffInterval = time.Millisecond * 10
)

// ErrSlowReader is returned by a stream when the stream buffer is full
var ErrSlowReader = errors.New("slow reader blocking stream channel send")

// StreamHeader is metadata associated with a stream record
type StreamHeader struct {
	Earliest Offset
	Latest   Offset
}

// StreamRecord is a record received from a stream
type StreamRecord struct {
	Metadata StreamHeader
	Record   Record
}

// Stream streams records over the returned channel, starting at the given start
// offset. If the start offset is in the future, stream will wait until this
// offset is written.
//
// Both channels are closed when the provided context is cancelled or an
// unrecoverable error, e.g. ErrOutOfRange, occurs.
//
// If the caller is not keeping up with the record stream (buffered channel),
// ErrSlowReader will be returned and the stream terminates.
//
// The caller must drain both (closed) channels to release all associated
// resources.
//
// Safe for concurrent use.
func (l *Log) Stream(ctx context.Context, start Offset) (<-chan StreamRecord, <-chan error) {
	var (
		// when buffer is full, returns with ErrSlowReader
		streamCh = make(chan StreamRecord, streamBuffer)

		// unbuffered to guarantee delivery before returning. avoids coding
		// complexity/bugs on caller side when streamCh is closed (returning invalid
		// empty Records to receiver)
		errCh = make(chan error)
	)

	go func() {
		defer func() {
			close(streamCh)
			close(errCh)
		}()

		offset := start
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return

			default:
				sendOne := func() error {
					if len(streamCh) == streamBuffer {
						return ErrSlowReader
					}

					l.mu.RLock()
					defer l.mu.RUnlock()

					earliest, latest := l.offsetRange()
					r, err := l.read(ctx, offset)
					if err != nil {
						if errors.Is(err, ErrFutureOffset) {
							// back off and continue polling
							time.Sleep(streamBackoffInterval)
							return nil
						}

						return err
					}

					rec := StreamRecord{
						Metadata: StreamHeader{
							Earliest: earliest,
							Latest:   latest,
						},
						Record: r,
					}

					streamCh <- rec
					offset = r.Metadata.Offset + 1

					return nil
				}

				if err := sendOne(); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	return streamCh, errCh
}
