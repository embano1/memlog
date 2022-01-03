
Benchmark with log size `1000`

```console
go test -bench=. -cpu 1,2,4,8,16 -benchmem
goos: darwin
goarch: amd64
pkg: github.com/embano1/memlog
cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
BenchmarkLog_write               9973622               116.7 ns/op            89 B/op          1 allocs/op
BenchmarkLog_write-2            10612510               111.4 ns/op            89 B/op          1 allocs/op
BenchmarkLog_write-4            10465269               112.2 ns/op            89 B/op          1 allocs/op
BenchmarkLog_write-8            10472682               112.7 ns/op            89 B/op          1 allocs/op
BenchmarkLog_write-16           10525519               113.6 ns/op            89 B/op          1 allocs/op
BenchmarkLog_read               19875546                59.97 ns/op           32 B/op          1 allocs/op
BenchmarkLog_read-2             22287092                55.22 ns/op           32 B/op          1 allocs/op
BenchmarkLog_read-4             21024020                54.66 ns/op           32 B/op          1 allocs/op
BenchmarkLog_read-8             20789745                55.03 ns/op           32 B/op          1 allocs/op
BenchmarkLog_read-16            22367100                55.74 ns/op           32 B/op          1 allocs/op
PASS
ok      github.com/embano1/memlog       13.125s
```