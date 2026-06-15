// Phase 13 smoke test: drives a MemStore through its
// bloom lifecycle (open with snapshot path -> put ->
// close -> reopen -> assert snapshot is loaded).
//
// Usage:
//   go run ./scripts/phase13-smoke
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("phase13 smoke: %v", err)
	}
	fmt.Println("phase13 smoke: OK")
}

func run() error {
	dir, err := os.MkdirTemp("", "phase13-smoke-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	storePath := filepath.Join(dir, "store")
	snapPath := filepath.Join(dir, "bloom.bin")

	// 1) Open a fresh store with a snapshot path.
	s1, err := store.NewMemStore(store.Options{
		Path: storePath,
		Bloom: store.BloomConfig{
			Capacity:     10_000,
			FPRate:       0.001,
			SnapshotPath: snapPath,
		},
	})
	if err != nil {
		return fmt.Errorf("NewMemStore: %w", err)
	}

	// 2) Insert a few MIDs and Close (triggers snapshot).
	const n = 50
	present := make([]mid.MID, 0, n)
	for i := 0; i < n; i++ {
		data := []byte{byte(i), byte(i >> 8)}
		m := mid.FromBytes(data)
		if err := s1.Put(m, data); err != nil {
			return fmt.Errorf("Put %d: %w", i, err)
		}
		present = append(present, m)
	}
	if err := s1.Close(); err != nil {
		return fmt.Errorf("s1.Close: %w", err)
	}
	st, err := os.Stat(snapPath)
	if err != nil {
		return fmt.Errorf("snapshot missing after Close: %w", err)
	}
	if st.Size() == 0 {
		return fmt.Errorf("snapshot is empty")
	}
	fmt.Printf("phase13: wrote %d-byte snapshot to %s\n", st.Size(), snapPath)

	// 3) Reopen. The snapshot must be loaded; every
	// present MID must be Has()==true.
	s2, err := store.NewMemStore(store.Options{
		Path: storePath,
		Bloom: store.BloomConfig{
			Capacity:     10_000,
			FPRate:       0.001,
			SnapshotPath: snapPath,
		},
	})
	if err != nil {
		return fmt.Errorf("reopen: %w", err)
	}
	t0 := time.Now()
	for _, m := range present {
		ok, err := s2.Has(m)
		if err != nil {
			return fmt.Errorf("Has reopen: %w", err)
		}
		if !ok {
			return fmt.Errorf("Has after reopen returned false")
		}
	}
	fmt.Printf("phase13: reopen confirmed %d MIDs via bloom in %s\n", n, time.Since(t0))
	return s2.Close()
}
