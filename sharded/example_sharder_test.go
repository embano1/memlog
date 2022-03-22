package sharded_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/embano1/memlog"
	"github.com/embano1/memlog/sharded"
)

func Example_sharder() {
	ctx := context.Background()

	keys := []string{"galaxies", "planets"}
	ks := sharded.NewKeySharder(keys)

	opts := []sharded.Option{
		sharded.WithNumShards(uint(len(keys))), // must be >=len(keys)
		sharded.WithSharder(ks),
	}
	l, err := sharded.New(ctx, opts...)
	if err != nil {
		fmt.Printf("create log: %v", err)
		os.Exit(1)
	}

	data := map[string][]string{
		keys[0]: {"Centaurus A", "Andromeda", "Eye of Sauron"},
		keys[1]: {"Mercury", "Venus", "Earth", "Mars", "Jupiter", "Saturn", "Uranus", "Neptune"},
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

KEYS:
	for _, key := range keys {
		fmt.Printf("reading all %s...\n", key)

		offset := memlog.Offset(0)
		for {
			read, err := l.Read(ctx, []byte(key), offset)
			if err != nil {
				if errors.Is(err, memlog.ErrFutureOffset) {
					fmt.Println()
					continue KEYS
				}
				fmt.Printf("read: %v", err)
				os.Exit(1)
			}

			fmt.Printf("- %s\n", string(read.Data))
			offset++
		}

	}

	// Output: reading all galaxies...
	// - Centaurus A
	// - Andromeda
	// - Eye of Sauron
	//
	// reading all planets...
	// - Mercury
	// - Venus
	// - Earth
	// - Mars
	// - Jupiter
	// - Saturn
	// - Uranus
	// - Neptune
}
