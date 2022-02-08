[![Go
Reference](https://pkg.go.dev/badge/github.com/embano1/memlog.svg)](https://pkg.go.dev/github.com/embano1/memlog)
[![Tests](https://github.com/embano1/memlog/actions/workflows/tests.yaml/badge.svg)](https://github.com/embano1/memlog/actions/workflows/tests.yaml)
[![Latest
Release](https://img.shields.io/github/release/embano1/memlog.svg?logo=github&style=flat-square)](https://github.com/embano1/memlog/releases/latest)
[![Go Report
Card](https://goreportcard.com/badge/github.com/embano1/memlog)](https://goreportcard.com/report/github.com/embano1/memlog)
[![codecov](https://codecov.io/gh/embano1/memlog/branch/main/graph/badge.svg?token=TC7MW723JO)](https://codecov.io/gh/embano1/memlog)
[![go.mod Go
version](https://img.shields.io/github/go-mod/go-version/embano1/memlog)](https://github.com/embano1/memlog)
[![Mentioned in Awesome Go](https://awesome.re/mentioned-badge.svg)](https://github.com/avelino/awesome-go)  

# About

## tl;dr

An easy to use, lightweight, thread-safe and append-only in-memory data
structure modeled as a *Log*.

The `Log` also serves as an abstraction and building block. See
[`sharded.Log`](./sharded/README.md) for an implementation of a *sharded*
variant of `memlog.Log`.

❌ Note: this package is not about providing an in-memory `logging` library. To
read more about the ideas behind `memlog` please see ["The Log: What every
software engineer should know about real-time data's unifying
abstraction"](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying).

## Motivation

I keep hitting the same user story (use case) over and over again: one or more
clients connected to my application wanting to read an **immutable** stream of
data, e.g. events or sensor data, **in-order**, **concurrently** (thread-safe)
and **asynchronously** (at their own pace) and in a resource (memory)
**efficient** way.

There's many solutions to this problem, e.g. exposing some sort of streaming API
(*gRPC*, HTTP/REST long-polling) based on custom logic using Go channels or an
internal [ring buffer](https://pkg.go.dev/container/ring), or putting data into
an external platform like [Kafka](https://kafka.apache.org/), [Redis
Streams](https://redis.io/topics/streams-intro) or [RabbitMQ
Streams](https://blog.rabbitmq.com/posts/2021/07/rabbitmq-streams-overview).

The challenges I faced with these solutions were that either they were too
**complex** (or simply **overkill**) for my problem. Or, the system I had to
integrate with and read data from did not have a nice streaming API or Go SDK,
thus repeating myself writing complex internal caching, buffering and
concurrency handling logic for the client APIs.

I looked around and could not find a simple and easy to use Go library for this
problem, so I created `memlog`: an **easy to use, lightweight (in-memory),
thread-safe, append-only log** inspired by popular streaming systems with a
**minimal API** using Go's **standard library** primitives 🤩

💡 For an end-to-end API modernization example using `memlog` see the
`vsphere-event-streaming`
[project](https://github.com/embano1/vsphere-event-streaming), which transforms
a SOAP-based events API into an HTTP/REST streaming API.

## A stateless Log? You gotta be kidding!

True, it sounds like an oxymoron. Why would someone use (build) an *in-memory*
append-only log that is not durable?

I'm glad you asked 😀

This library certainly is not intended to replace messaging, queuing or
streaming systems. It was built for use cases where there exists a *durable
data/event source*, e.g. a legacy system, REST API, database, etc. that can't
(or should not) be changed. But the requirement being that the (source) data
should be made available over a streaming-like API, e.g. *gRPC* or processed by
a Go application which requires the properties of a `Log`.

`memlog` helps as it allows to bridge between these different APIs and use cases
as a *building block* to extract and store data `Records` from an external
system into an *in-memory* `Log` (think ordered cache).

These `Records` can then be internally processed (lightweight ETL) or served
asynchronously, in-order (`Offset`-based) and concurrently over a *modern
streaming API*, e.g. *gRPC* or HTTP/REST (chunked encoding via long polling), to
remote clients.

### Checkpointing

Given the data source needs to be durable in this design, one can optionally
build periodic checkpointing logic using the `Record` `Offset` as the checkpoint
value. 

💡 When running in Kubernetes,
[`kvstore`](https://github.com/knative/pkg/tree/main/kvstore) provides a nice
abstraction on top of a `ConfigMap` for such requirements. 

If the `memlog` process crashes, it can then resume from the last checkpointed
`Offset`, load the changes since then from the source and resume streaming. 

💡 This approach is quiet similar to the Kubernetes `ListerWatcher()`
[pattern](https://youtu.be/YIBQrP1grPE?t=1132). See
[`memlog_test.go`](./memlog_test.go) for some inspiration.

# Usage

The API is intentionally kept minimal. A new `Log` is constructed with
`memlog.New(ctx, options...)`. Data as `[]byte` is written to the log with
`Log.Write(ctx, data)`.

The first write to the `Log` using *default* `Options` starts at position
(`Offset`) `0`. Every write creates an immutable `Record` in the `Log`.
`Records` are purged from the `Log` when the *history* `segment` is replaced
(see notes below).

The *earliest* and *latest* `Offset` available in a `Log` can be retrieved with
`Log.Range(ctx)`.

A specified `Record` can be read with `Log.Read(ctx, offset)`.

💡 Instead of manually polling the `Log` for new `Records`, the *streaming* API
`Log.Stream(ctx, startOffset)` should be used.

## (Not) one `Log` to rule them all

One is not constrained by just creating **one** `Log`. For certain use cases,
creating multiple `Logs` might be useful. For example:

- Manage completely different data sets/sizes in the same process
- Setting different `Log` sizes (i.e. retention times), e.g. premium users will
  have access to a larger *history* of `Records`
- Partitioning input data by type or *key*

💡 For use cases where you want to order the log by `key(s)`, consider using the
specialised [`sharded.Log`](sharded/README.md).

## Example

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/embano1/memlog"
)

func main() {
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

	fmt.Printf("data says: %s", record.Data)

	// reading record at offset 0
	// data says: Hello World
}
```

## Purging the `Log`

The `Log` is divided into an *active* and *history* `segment`. When the *active*
`segment` is full (configurable via `WithMaxSegmentSize()`), it is *sealed*
(i.e. read-only) and becomes the *history* `segment`. A new empty *active*
`segment` is created for writes. If there is an existing *history*, it is
replaced, i.e. all `Records` are purged from the *history*.

See [pkg.go.dev](https://pkg.go.dev/github.com/embano1/memlog) for the API
reference and examples.

# Benchmark

I haven't done any extensive benchmarking or code optimization. Feel free to
chime in and provide meaningful feedback/optimizations. 

One could argue, whether using two *slices* (*active* and *history* `data
[]Record` as part of the individual `segments`) is a good engineering choice,
e.g. over using a growable slice as an alternative. 

The reason I went for two `segments` was that for me dividing the `Log` into
multiple `segments` with fixed *size* (and *capacity*) was easier to reason
about in the code (and I followed my intuition from how log-structured data
platforms do it). I did not inspect the Go compiler optimizations, e.g. it might
actually be smart and create one growable slice under the hood. 🤓

These are some results on my MacBook  using a log size of `1,000` (records),
i.e. where the `Log` history is constantly purged and new `segments` (*slices*)
are created.

```console
go test -v -run=none -bench=. -cpu 1,2,4,8,16 -benchmem
goos: darwin
goarch: amd64
pkg: github.com/embano1/memlog
cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
BenchmarkLog_write               9973622                116.7 ns/op           89 B/op          1 allocs/op
BenchmarkLog_write-2            10612510                111.4 ns/op           89 B/op          1 allocs/op
BenchmarkLog_write-4            10465269                112.2 ns/op           89 B/op          1 allocs/op
BenchmarkLog_write-8            10472682                112.7 ns/op           89 B/op          1 allocs/op
BenchmarkLog_write-16           10525519                113.6 ns/op           89 B/op          1 allocs/op
BenchmarkLog_read               19875546                59.97 ns/op           32 B/op          1 allocs/op
BenchmarkLog_read-2             22287092                55.22 ns/op           32 B/op          1 allocs/op
BenchmarkLog_read-4             21024020                54.66 ns/op           32 B/op          1 allocs/op
BenchmarkLog_read-8             20789745                55.03 ns/op           32 B/op          1 allocs/op
BenchmarkLog_read-16            22367100                55.74 ns/op           32 B/op          1 allocs/op
PASS
ok      github.com/embano1/memlog       13.125s
```
