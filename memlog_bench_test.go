package memlog

import (
	"context"
	"testing"
)

func BenchmarkLog_write(b *testing.B) {
	const (
		start   = Offset(0)
		segSize = 1000
	)

	var (
		offset Offset
		result Offset
		err    error
	)

	ctx := context.Background()
	opts := []Option{
		WithStartOffset(start),
		WithMaxSegmentSize(segSize),
	}

	l, err := New(ctx, opts...)
	if err != nil {
		b.Fatalf("create log: %v", err)
	}

	d := []byte(`{"id":"1","message":"benchmark"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset, err = l.write(ctx, d)
		if err != nil {
			b.Fatalf("write data: %v", err)
		}

		result = offset
	}

	_ = result
}

func BenchmarkLog_read(b *testing.B) {
	const (
		start   = Offset(0)
		segSize = 1000
	)

	var (
		record Record
		result Record
		err    error
	)

	ctx := context.Background()
	opts := []Option{
		WithStartOffset(start),
		WithMaxSegmentSize(segSize),
	}

	l, err := New(ctx, opts...)
	if err != nil {
		b.Fatalf("create log: %v", err)
	}

	d := []byte(`{"id":"1","message":"benchmark"}`)
	offset, err := l.write(ctx, d)
	if err != nil {
		b.Fatalf("write data: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record, err = l.read(ctx, offset)
		if err != nil {
			b.Fatalf("read data: %v", err)
		}
		result = record
	}

	_ = result
}
