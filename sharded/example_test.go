package sharded_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/embano1/memlog"
	"github.com/embano1/memlog/sharded"
)

func Example() {
	ctx := context.Background()

	// the default number of shards (1000) is sufficient to assign a shard per key
	// for this example (i.e. no key overlap within a shard)
	l, err := sharded.New(ctx)
	if err != nil {
		fmt.Printf("create log: %v", err)
		os.Exit(1)
	}

	data := map[string][]string{
		"users":  {"tom", "sarah", "ajit"},
		"groups": {"friends", "family", "colleagues"},
	}

	for key, vals := range data {
		for _, val := range vals {
			_, err := l.Write(ctx, []byte(key), []byte(val))
			if err != nil {
				fmt.Printf("write: %v", err)
				os.Exit(1)
			}
		}
	}

	fmt.Println("reading all users...")
	offset := memlog.Offset(0)
	for {
		record, err := l.Read(ctx, []byte("users"), offset)
		if err != nil {
			if errors.Is(err, memlog.ErrFutureOffset) {
				break // end of log
			}
			fmt.Printf("read: %v", err)
			os.Exit(1)
		}

		fmt.Printf("- %s\n", string(record.Data))
		offset++
	}

	// Output: reading all users...
	// - tom
	// - sarah
	// - ajit
}
