# Benchmark Baselines

Machine: AMD Ryzen 9 7950X3D 16-Core Processor, 32 logical cores
OS: Linux 5.15.167.4-microsoft-standard-WSL2
Go: go1.24.3 linux/amd64
Date: 2026-03-19

## Proxy Benchmarks (`internal/lb`)

```
goos: linux
goarch: amd64
pkg: github.com/Weilei424/load-balancer/internal/lb
cpu: AMD Ryzen 9 7950X3D 16-Core Processor

BenchmarkProxyRoundRobin-32        	   10792	    101956 ns/op	   52791 B/op	     109 allocs/op
BenchmarkProxyRoundRobin-32        	   20283	     57150 ns/op	   50471 B/op	     106 allocs/op
BenchmarkProxyRoundRobin-32        	   22376	     62525 ns/op	   50350 B/op	     105 allocs/op
BenchmarkProxyLeastConn-32         	   21429	     55840 ns/op	   50105 B/op	     104 allocs/op
BenchmarkProxyLeastConn-32         	   21454	     56638 ns/op	   50024 B/op	     104 allocs/op
BenchmarkProxyLeastConn-32         	   21039	     56701 ns/op	   50107 B/op	     104 allocs/op
BenchmarkProxyConsistentHash-32    	   18840	     62444 ns/op	   52339 B/op	     120 allocs/op
BenchmarkProxyConsistentHash-32    	   18604	     63347 ns/op	   52560 B/op	     122 allocs/op
BenchmarkProxyConsistentHash-32    	   18552	     59720 ns/op	   52349 B/op	     121 allocs/op
```

## Raft Benchmarks (`internal/raftlite`)

```
goos: linux
goarch: amd64
pkg: github.com/Weilei424/load-balancer/internal/raftlite
cpu: AMD Ryzen 9 7950X3D 16-Core Processor

BenchmarkRaftPropose-32             	    7767	    133452 ns/op	    1545 B/op	      19 allocs/op
BenchmarkRaftPropose-32             	    9846	    129777 ns/op	    1558 B/op	      19 allocs/op
BenchmarkRaftPropose-32             	    9998	    123840 ns/op	    1554 B/op	      19 allocs/op
BenchmarkRaftReplication3Node-32    	    2128	    552805 ns/op	   31493 B/op	     304 allocs/op
BenchmarkRaftReplication3Node-32    	    2202	    544719 ns/op	   31697 B/op	     303 allocs/op
BenchmarkRaftReplication3Node-32    	    2210	    545652 ns/op	   31209 B/op	     303 allocs/op
```

## Notes

- Proxy benchmarks use 3 in-process `httptest` backends returning 200 immediately; allocations include `httptest.NewRequest` + `httptest.NewRecorder` per iteration.
- `BenchmarkRaftPropose` measures single-node commit latency (no network replication; leader commits on local append alone).
- `BenchmarkRaftReplication3Node` measures end-to-end majority-commit latency through a 3-node in-process cluster over loopback TCP.
- Run with `make bench` (3 rounds each) to regenerate; use `benchstat` to compare across runs.
