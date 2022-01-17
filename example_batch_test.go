package memlog_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/embano1/memlog"
)

func Example_batch() {
	const batchSize = 10

	ctx := context.Background()
	l, err := memlog.New(ctx)
	if err != nil {
		fmt.Printf("create log: %v", err)
		os.Exit(1)
	}

	// seed log with data
	for i := 0; i < 15; i++ {
		d := fmt.Sprintf(`{"id":%d,"message","hello world"}`, i)
		_, err = l.Write(ctx, []byte(d))
		if err != nil {
			fmt.Printf("write: %v", err)
			os.Exit(1)
		}
	}

	startOffset := memlog.Offset(0)
	batch := make([]memlog.Record, batchSize)

	fmt.Printf("reading batch starting at offset %d\n", startOffset)
	count, err := l.ReadBatch(ctx, startOffset, batch)
	if err != nil {
		fmt.Printf("read batch: %v", err)
		os.Exit(1)
	}
	fmt.Printf("records received in batch: %d\n", count)

	// print valid batch entries up to "count"
	for i := 0; i < count; i++ {
		r := batch[i]
		fmt.Printf("batch item: %d\toffset:%d\tdata: %s\n", i, r.Metadata.Offset, r.Data)
	}

	// read next batch and check if end of log reached
	startOffset += memlog.Offset(count)
	fmt.Printf("reading batch starting at offset %d\n", startOffset)
	count, err = l.ReadBatch(ctx, startOffset, batch)
	if err != nil {
		if errors.Is(err, memlog.ErrFutureOffset) {
			fmt.Println("reached end of log")
		} else {
			fmt.Printf("read batch: %v", err)
			os.Exit(1)
		}
	}
	fmt.Printf("records received in batch: %d\n", count)

	// print valid batch entries up to "count"
	for i := 0; i < count; i++ {
		r := batch[i]
		fmt.Printf("batch item: %d\toffset:%d\tdata: %s\n", i, r.Metadata.Offset, r.Data)
	}

	// Output: reading batch starting at offset 0
	// records received in batch: 10
	// batch item: 0	offset:0	data: {"id":0,"message","hello world"}
	// batch item: 1	offset:1	data: {"id":1,"message","hello world"}
	// batch item: 2	offset:2	data: {"id":2,"message","hello world"}
	// batch item: 3	offset:3	data: {"id":3,"message","hello world"}
	// batch item: 4	offset:4	data: {"id":4,"message","hello world"}
	// batch item: 5	offset:5	data: {"id":5,"message","hello world"}
	// batch item: 6	offset:6	data: {"id":6,"message","hello world"}
	// batch item: 7	offset:7	data: {"id":7,"message","hello world"}
	// batch item: 8	offset:8	data: {"id":8,"message","hello world"}
	// batch item: 9	offset:9	data: {"id":9,"message","hello world"}
	// reading batch starting at offset 10
	// reached end of log
	// records received in batch: 5
	// batch item: 0	offset:10	data: {"id":10,"message","hello world"}
	// batch item: 1	offset:11	data: {"id":11,"message","hello world"}
	// batch item: 2	offset:12	data: {"id":12,"message","hello world"}
	// batch item: 3	offset:13	data: {"id":13,"message","hello world"}
	// batch item: 4	offset:14	data: {"id":14,"message","hello world"}
}
