package memlog

import (
	"context"
	"errors"
	"time"
)

const (
	streamBuffer       = 100 // limit before panic
	streamPollInterval = time.Millisecond * 10
)

// ErrSlowReader is returned by a stream when the stream buffer is full
var ErrSlowReader = errors.New("slow reader blocking stream channel send")

type StreamHeader struct {
	Earliest Offset
	Latest   Offset
}

type StreamRecord struct {
	Metadata StreamHeader
	Record   Record
}

func (l *Log) Stream(ctx context.Context, start Offset) (<-chan StreamRecord, <-chan error) {
	var (
		streamCh = make(chan StreamRecord, streamBuffer)

		// unbuffered to guarantee delivery before returning. avoids issue that streamCh
		// is closed on return and returning (invalid) default values to receiver
		errCh = make(chan error)
	)

	go func() {
		ticker := time.NewTicker(streamPollInterval)
		defer func() {
			close(streamCh)
			close(errCh)
			ticker.Stop()
		}()

		offset := start
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return

			case <-ticker.C:
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
							// continue polling
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
