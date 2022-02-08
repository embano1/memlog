# About

## tl;dr

A purpose-built implementation of `memlog.Log` with support for sharding
(partitioning) `Records` by `key`.

# Usage

The `sharded.Log` is built on `memlog.Log` and provides a similar API for
reading and writing data (`Records`), `Offset` handling, etc.

The `Read()` and `Write()` methods accept a sharding `key` to distribute the
`Records` based on a (configurable) sharding strategy. 

Unless specified otherwise, the default sharding strategy uses Golang's
[`fnv.New32a`](https://pkg.go.dev/hash/fnv#New32a) to retrieve a *hash* and find
the corresponding `Shard` using a *modulo* operation based on the number of
(configurable) `Shards` in the `Log`. 

💡 Depending on the number of `Shards`, number of distinct `keys` and their
*hashes*, multiple `keys` might be stored in the same `Shard`. If strict `key`
separation is required, a custom `Sharder` can be implemented. For convenience,
a `KeySharder` is provided.

See [pkg.go.dev](https://pkg.go.dev/github.com/embano1/memlog/sharded) for the
API reference and examples.

## Example

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/embano1/memlog"
	"github.com/embano1/memlog/sharded"
)

func main() {
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

```