package dag

import (
	"bytes"
	"crypto/rand"
	"io"
	"strings"
	"testing"

	"github.com/nnlgsakib/membuss/core/chunk"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
)

func buildResolve(t *testing.T, payload []byte, factory chunk.ChunkerFactory) (mid.MID, []byte) {
	t.Helper()
	bs := store.NewMemstore()
	b := NewBuilder(bs)
	c, err := factory(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("chunker: %v", err)
	}
	root, err := b.Build(c)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if root.IsZero() {
		t.Fatal("zero root MID")
	}
	r := NewResolver(bs)
	rd, err := r.Resolve(root, nil)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	got, err := io.ReadAll(rd)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	return root, got
}

func TestBuildResolveSingleChunk(t *testing.T) {
	payload := []byte("only chunk")
	root, got := buildResolve(t, payload, chunk.NewFixed(chunk.DefaultBlockSize))
	if got == nil || !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch: %q vs %q", got, payload)
	}
	// For a single chunk the root MID is the chunk's MID.
	want := mid.FromBytes(payload)
	if !root.Equal(want) {
		t.Fatalf("root = %s, want %s", root, want)
	}
}

func TestBuildResolveEmptyFails(t *testing.T) {
	bs := store.NewMemstore()
	b := NewBuilder(bs)
	c, err := chunk.NewFixed(chunk.DefaultBlockSize)(bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("chunker: %v", err)
	}
	if _, err := b.Build(c); err == nil {
		t.Fatal("Build on empty input must fail")
	}
}

func TestBuildResolveFixedManyChunks(t *testing.T) {
	const total = 5 * 1024 * 1024 // 5 MiB
	payload := make([]byte, total)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	root, got := buildResolve(t, payload, chunk.NewFixed(64*1024))
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch (len %d vs %d)", len(got), len(payload))
	}
	if root.IsZero() {
		t.Fatal("zero root")
	}
}

func TestBuildResolveRabinManyChunks(t *testing.T) {
	const total = 2 * 1024 * 1024 // 2 MiB
	payload := make([]byte, total)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	root, got := buildResolve(t, payload, chunk.NewRabin())
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip mismatch (len %d vs %d)", len(got), len(payload))
	}
	if root.IsZero() {
		t.Fatal("zero root")
	}
}

func TestBuildResolveDeterministic(t *testing.T) {
	payload := []byte(strings.Repeat("membuss!", 1024))
	r1, _ := buildResolve(t, payload, chunk.NewFixed(1024))
	r2, _ := buildResolve(t, payload, chunk.NewFixed(1024))
	if !r1.Equal(r2) {
		t.Fatalf("root MIDs differ for same input: %s vs %s", r1, r2)
	}
}

func TestBuildResolveDifferentContentDifferentRoot(t *testing.T) {
	a := bytes.Repeat([]byte{0xAA}, 256*1024)
	b := bytes.Repeat([]byte{0xBB}, 256*1024)
	r1, _ := buildResolve(t, a, chunk.NewFixed(64*1024))
	r2, _ := buildResolve(t, b, chunk.NewFixed(64*1024))
	if r1.Equal(r2) {
		t.Fatal("distinct inputs must yield distinct root MIDs")
	}
}

func TestBuildResolveVisitCounts(t *testing.T) {
	payload := bytes.Repeat([]byte{0xCC}, 256*1024) // 4 fixed-64KiB blocks -> 1 internal node
	bs := store.NewMemstore()
	b := NewBuilder(bs)
	c, err := chunk.NewFixed(64*1024)(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("chunker: %v", err)
	}
	root, err := b.Build(c)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	visits := 0
	r := NewResolver(bs)
	rd, err := r.Resolve(root, func(m mid.MID) error {
		visits++
		return nil
	})
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if _, err := io.ReadAll(rd); err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if visits != 1 {
		t.Fatalf("visit count = %d, want 1", visits)
	}
}

func TestBuildResolveMissingBlock(t *testing.T) {
	// Build a DAG, then delete one of the internal nodes and
	// expect the resolver to surface the error.
	payload := bytes.Repeat([]byte{0xDD}, 128*1024)
	bs := store.NewMemstore()
	b := NewBuilder(bs)
	c, err := chunk.NewFixed(64*1024)(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("chunker: %v", err)
	}
	root, err := b.Build(c)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	// Delete the root itself; resolver should fail.
	if err := bs.Delete(root); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	r := NewResolver(bs)
	rd, err := r.Resolve(root, nil)
	if err != nil {
		t.Fatalf("Resolve (constructor): %v", err)
	}
	if _, err := io.ReadAll(rd); err == nil {
		t.Fatal("expected error reading from a DAG with a missing block")
	}
}

func TestBuildRejectsNilDeps(t *testing.T) {
	if _, err := NewBuilder(nil).Build(mustChunk(t, []byte("x"))); err == nil {
		t.Error("Build with nil blockstore must fail")
	}
	bs := store.NewMemstore()
	if _, err := NewBuilder(bs).Build(nil); err == nil {
		t.Error("Build with nil chunker must fail")
	}
}

func mustChunk(t *testing.T, payload []byte) chunk.Chunker {
	t.Helper()
	c, err := chunk.NewFixed(chunk.DefaultBlockSize)(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("chunker: %v", err)
	}
	return c
}
