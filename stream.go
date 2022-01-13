package memlog

import (
	"context"
	"errors"
	"time"
)

const (
	streamBackoffInterval = time.Millisecond * 10
)

// Stream is an iterator to stream records in order from a log
type Stream struct {
	ctx      context.Context
	log      *Log
	position Offset
	done     bool
	err      error
}

// Next returns the next Record, but only if ok is true. If ok is false, the
// iterator was stopped and any subsequent calls will return an invalid record
// and false. 
//
// The caller must consult Err() which error, if any, caused stopping the error.
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

// Err returns the first error, if any, that has ocurred during streaming. When
// the stream iterator is stopped this method should be called to inspect
// whether the iterator was stopped due to an error.
func (s *Stream) Err() error {
	return s.err
}

// Stream returns a stream iterator to stream records, starting at the given
// start offset. If the start offset is in the future, stream will continuously
// poll until this offset is written.
//
// The returned stream iterator must only be used within the same goroutine.
func (l *Log) Stream(ctx context.Context, start Offset) Stream {
	return Stream{
		ctx:      ctx,
		log:      l,
		position: start,
	}
}
