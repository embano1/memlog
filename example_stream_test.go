package memlog_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/embano1/memlog"
)

func Example_stream() {
	// showing some custom options in action
	const (
		logStart = 10
		logSize  = 100
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

	const writeRecords = 10
	// write some records (offsets 10-14)
	for i := 0; i < writeRecords/2; i++ {
		d := fmt.Sprintf(`{"id":%d,"message","hello world"}`, i+logStart)
		_, err := l.Write(ctx, []byte(d))
		if err != nil {
			fmt.Printf("write: %v", err)
			os.Exit(1)
		}
	}

	// start stream from latest (offset 14)
	_, latest := l.Range(ctx)
	recChan, errChan := l.Stream(ctx, latest)

	var wg sync.WaitGroup

	// stream records
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case r := <-recChan:
				fmt.Printf("Record at offset %d says %q\n", r.Record.Metadata.Offset, r.Record.Data)
			case streamErr := <-errChan:
				if errors.Is(streamErr, context.Canceled) {
					return
				}
				fmt.Printf("stream: %v", streamErr)
				os.Exit(1)
			}
		}
	}()

	// continue writing while streaming
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
		}()
		for i := writeRecords / 2; i < writeRecords; i++ {
			d := fmt.Sprintf(`{"id":%d,"message","hello world"}`, i+logStart)
			_, err := l.Write(ctx, []byte(d))
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Printf("write: %v", err)
				os.Exit(1)
			}
		}
	}()

	// simulate SIGTERM after 2s
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(time.Second * 2)
		cancel()
	}()

	wg.Wait()

	// drain any remaining records to release the closed (buffered) channel
	for r := range recChan {
		fmt.Printf("Record at offset %d says %q\n", r.Record.Metadata.Offset, r.Record.Data)
	}

	// Output: Record at offset 14 says "{\"id\":14,\"message\",\"hello world\"}"
	// Record at offset 15 says "{\"id\":15,\"message\",\"hello world\"}"
	// Record at offset 16 says "{\"id\":16,\"message\",\"hello world\"}"
	// Record at offset 17 says "{\"id\":17,\"message\",\"hello world\"}"
	// Record at offset 18 says "{\"id\":18,\"message\",\"hello world\"}"
	// Record at offset 19 says "{\"id\":19,\"message\",\"hello world\"}"
}
