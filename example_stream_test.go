package memlog_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/embano1/memlog"
)

func Example_stream() {
	// showing some custom options in action
	const (
		logStart     = 10
		logSize      = 100
		writeRecords = 10
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opts := []memlog.Option{
		memlog.WithStartOffset(logStart),
		memlog.WithMaxSegmentSize(logSize),
	}
	l, err := memlog.New(ctx, opts...)
	if err != nil {
		fmt.Printf("create log: %v", err)
		os.Exit(1)
	}

	// write some records (offsets 10-14)
	for i := 0; i < writeRecords/2; i++ {
		d := fmt.Sprintf(`{"id":%d,"message","hello world"}`, i+logStart)
		_, err = l.Write(ctx, []byte(d))
		if err != nil {
			fmt.Printf("write: %v", err)
			os.Exit(1)
		}
	}

	eg, egCtx := errgroup.WithContext(ctx)

	_, latest := l.Range(egCtx)
	// stream records
	eg.Go(func() error {
		// start stream from latest (offset 14)
		stream := l.Stream(egCtx, latest)

		for {
			if r, ok := stream.Next(); ok {
				fmt.Printf("Record at offset %d says %q\n", r.Metadata.Offset, r.Data)
				continue
			}
			break
		}
		return stream.Err()
	})

	// continue writing while streaming
	eg.Go(func() error {
		for i := writeRecords / 2; i < writeRecords; i++ {
			d := fmt.Sprintf(`{"id":%d,"message","hello world"}`, i+logStart)
			_, err := l.Write(ctx, []byte(d))
			if err != nil && !errors.Is(err, context.Canceled) {
				return err
			}
		}
		return nil
	})

	// simulate SIGTERM after 2s
	eg.Go(func() error {
		time.Sleep(time.Second * 2)
		cancel()
		return nil
	})

	if err = eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Printf("run example: %v", err)
		os.Exit(1)
	}

	// Output: Record at offset 14 says "{\"id\":14,\"message\",\"hello world\"}"
	// Record at offset 15 says "{\"id\":15,\"message\",\"hello world\"}"
	// Record at offset 16 says "{\"id\":16,\"message\",\"hello world\"}"
	// Record at offset 17 says "{\"id\":17,\"message\",\"hello world\"}"
	// Record at offset 18 says "{\"id\":18,\"message\",\"hello world\"}"
	// Record at offset 19 says "{\"id\":19,\"message\",\"hello world\"}"
}
