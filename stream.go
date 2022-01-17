package memlog

import (
	"context"
	"errors"
	"time"
)

const (
	streamBackoffInterval = time.Millisecond * 10
)

// Stream is an iterator to stream records in order from a log. It must only be
// used within the same goroutine.
type Stream struct {
	ctx      context.Context
	log      *Log
	position Offset
	done     bool
	err      error
}

// Next blocks until the next Record is available. ok is true if the iterator
// has not stopped, otherwise ok is false and any subsequent calls return an
// invalid record and false.
//
// The caller must consult Err() which error caused stopping the error.
func (s *Stream) Next() (r Record, ok bool) {
	for {
		if s.done {
			return Record{}, false
		}

		if s.ctx.Err() != nil {
			s.err = s.ctx.Err()
			s.done = true
			return Record{}, false
		}

		r, err := s.log.Read(s.ctx, s.position)
		if err != nil {
			if errors.Is(err, ErrFutureOffset) {
				// back off and continue polling
				time.Sleep(streamBackoffInterval)
				continue
			}

			s.err = err
			s.done = true
			return Record{}, false
		}

		s.position = r.Metadata.Offset + 1
		return r, true
	}
}

// Err returns the first error that has ocurred during streaming. This method
// should be called to inspect the error that caused stopping the iterator.
func (s *Stream) Err() error {
	return s.err
}

// Stream returns a stream iterator to stream records, starting at the given
// start offset. If the start offset is in the future, stream will continuously
// poll until this offset is written.
//
// Use Stream.Next() to read from the stream. See the example for how to use
// this API.
//
// The returned stream iterator must only be used within the same goroutine.
func (l *Log) Stream(ctx context.Context, start Offset) Stream {
	return Stream{
		ctx:      ctx,
		log:      l,
		position: start,
	}
}
