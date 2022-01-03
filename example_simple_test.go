package memlog_test

import (
	"context"
	"fmt"
	"os"

	"github.com/embano1/memlog"
)

func Example() {
	ctx := context.Background()
	l, err := memlog.New(ctx)
	if err != nil {
		fmt.Printf("create log: %v", err)
		os.Exit(1)
	}

	offset, err := l.Write(ctx, []byte("Hello World"))
	if err != nil {
		fmt.Printf("write: %v", err)
		os.Exit(1)
	}

	fmt.Printf("reading record at offset %d\n", offset)
	record, err := l.Read(ctx, offset)
	if err != nil {
		fmt.Printf("read: %v", err)
		os.Exit(1)
	}

	fmt.Printf("Data: %s", record.Data)

	// Output: reading record at offset 0
	// Data: Hello World
}
