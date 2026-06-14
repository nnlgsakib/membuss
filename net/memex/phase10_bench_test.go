package memex

import (
	"bytes"
	"testing"
	"time"

	"github.com/nnlgsakib/membuss/core/chunk"
	"github.com/nnlgsakib/membuss/core/dag"
	"github.com/nnlgsakib/membuss/core/store"
)

// BenchmarkAdd1GB measures the end-to-end time to ingest a
// 1 GB file: chunking + DAG build + store. The benchmark
// reports a custom "MB/s" metric so operators can see
// throughput at a glance.
//
// Run with: go test -bench BenchmarkAdd1GB -benchmem ./net/memex
func BenchmarkAdd1GB(b *testing.B) {
	const size = 1 << 30 // 1 GiB
	content := makeContentBench(b, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bs := store.NewMemstore()
		factory := chunk.NewFixed(256 * 1024)
		ch, err := factory(bytes.NewReader(content))
		if err != nil {
			b.Fatalf("chunker: %v", err)
		}
		root, err := dag.NewBuilder(bs).Build(ch)
		if err != nil {
			b.Fatalf("dag build: %v", err)
		}
		if i == 0 {
			b.Logf("add 1GB: root=%s blocks=%d", root.String(), bs.Len())
		}
	}
}

// BenchmarkRetrieve100MB measures the time to resolve a
// 100 MB file from a local blockstore (no network). This
// is the floor for retrieval throughput; the integration
// tests cover the network path.
//
// Run with: go test -bench BenchmarkRetrieve100MB -benchmem ./net/memex
func BenchmarkRetrieve100MB(b *testing.B) {
	const size = 100 * 1024 * 1024 // 100 MiB
	content := makeContentBench(b, size)

	bs := store.NewMemstore()
	factory := chunk.NewFixed(256 * 1024)
	ch, err := factory(bytes.NewReader(content))
	if err != nil {
		b.Fatalf("chunker: %v", err)
	}
	root, err := dag.NewBuilder(bs).Build(ch)
	if err != nil {
		b.Fatalf("dag build: %v", err)
	}
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resolver := dag.NewResolver(bs)
		rc, err := resolver.Resolve(root, nil)
		if err != nil {
			b.Fatalf("resolve: %v", err)
		}
		// Drain the reader so the benchmark measures the
		// full I/O path.
		var sink [4096]byte
		for {
			_, err := rc.Read(sink[:])
			if err != nil {
				break
			}
		}
	}
}

// makeContentBench is a fast deterministic byte source for
// benchmarks. It is not cryptographic and exists purely to
// avoid the cost of crypto/rand in tight loops.
func makeContentBench(b *testing.B, n int) []byte {
	b.Helper()
	buf := make([]byte, n)
	step := uint64(0x9E3779B97F4A7C15)
	for i := 0; i < n; i++ {
		buf[i] = byte(i>>16) ^ byte(step>>(i&63))
	}
	return buf
}

// avoid unused import when this file is compiled standalone.
var _ = time.Second
