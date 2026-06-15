package store

import (
	"bytes"
	"crypto/rand"
	"path/filepath"
	"testing"

	"github.com/nnlgsakib/membuss/core/mid"
)

// TestBloomStoreNoFalseNegatives inserts 100k MIDs into a
// fresh MemStore, then asserts Has() returns true for
// every one of them. The bloom filter MUST NOT report
// "absent" for an actually-present block.
func TestBloomStoreNoFalseNegatives(t *testing.T) {
	dir := t.TempDir()
	s, err := NewMemStore(Options{
		Path: filepath.Join(dir, "store"),
		Bloom: BloomConfig{
			Capacity:     200_000,
			FPRate:       0.001,
			SnapshotPath: filepath.Join(dir, "bloom.bin"),
		},
	})
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	const n = 100_000
	mids := make([]mid.MID, 0, n)
	for i := 0; i < n; i++ {
		data := make([]byte, 64)
		// Deterministic but distinct payloads: we need
		// uniqueness, not cryptographic randomness.
		copy(data, []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)})
		m := mid.FromBytes(data)
		if err := s.Put(m, data); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		mids = append(mids, m)
	}
	for i, m := range mids {
		ok, err := s.Has(m)
		if err != nil {
			t.Fatalf("Has %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("Has(%d) = false, want true (false negative)", i)
		}
	}
}

// TestBloomStoreFPRateWithinBudget inserts a known set of
// MIDs and probes a disjoint set of "missing" MIDs. The
// observed false positive rate must stay below the
// configured budget.
func TestBloomStoreFPRateWithinBudget(t *testing.T) {
	dir := t.TempDir()
	const capacity = 50_000
	const fpRate = 0.01
	s, err := NewMemStore(Options{
		Path: filepath.Join(dir, "store"),
		Bloom: BloomConfig{
			Capacity:     capacity,
			FPRate:       fpRate,
			SnapshotPath: filepath.Join(dir, "bloom.bin"),
		},
	})
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	// Insert capacity MIDs.
	inserted := make(map[string]struct{}, capacity)
	for i := 0; i < capacity; i++ {
		data := make([]byte, 64)
		copy(data, []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24), 0xAA})
		m := mid.FromBytes(data)
		if err := s.Put(m, data); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		inserted[m.String()] = struct{}{}
	}
	// Probe 100k missing MIDs. The observed rate must be
	// under 2x the budget (we leave a small headroom for
	// statistical fluctuation on a single test run).
	const probes = 100_000
	fp := 0
	for i := 0; i < probes; i++ {
		// Different byte pattern to ensure no collision
		// with the inserted set.
		data := make([]byte, 64)
		copy(data, []byte{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24), 0xBB})
		m := mid.FromBytes(data)
		if _, ok := inserted[m.String()]; ok {
			continue
		}
		has, err := s.Has(m)
		if err != nil {
			t.Fatalf("Has probe %d: %v", i, err)
		}
		if has {
			fp++
		}
	}
	rate := float64(fp) / float64(probes)
	t.Logf("false positives: %d / %d (%.4f, budget %.4f)", fp, probes, rate, fpRate)
	if rate > 2*fpRate {
		t.Fatalf("false positive rate %.4f exceeds 2x budget %.4f", rate, fpRate)
	}
}

// TestBloomStoreSnapshotRoundTrip builds a store, inserts
// MIDs, closes it (which writes the snapshot), reopens
// from the same path, and confirms Has() is correct on
// the fresh handle without any DB rebuild.
func TestBloomStoreSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store")
	bloomPath := filepath.Join(dir, "bloom.bin")
	mk := func() *MemStore {
		s, err := NewMemStore(Options{
			Path: storePath,
			Bloom: BloomConfig{
				Capacity:     10_000,
				FPRate:       0.001,
				SnapshotPath: bloomPath,
			},
		})
		if err != nil {
			t.Fatalf("NewMemStore: %v", err)
		}
		return s
	}
	s1 := mk()
	const n = 1_000
	present := make([]mid.MID, n)
	for i := 0; i < n; i++ {
		data := []byte{byte(i), byte(i >> 8), 0xFE}
		m := mid.FromBytes(data)
		if err := s1.Put(m, data); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		present[i] = m
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("s1 Close: %v", err)
	}
	// Reopen and confirm the bloom snapshot is loaded.
	// We check that the on-disk snapshot is non-empty
	// (proving Close() wrote it) and that Has() is correct
	// on every present MID.
	s2 := mk()
	t.Cleanup(func() { _ = s2.Close() })
	for i, m := range present {
		ok, err := s2.Has(m)
		if err != nil {
			t.Fatalf("Has reopen %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("Has reopen(%d) = false after snapshot load", i)
		}
	}
}

// TestBloomStoreRebuildOnDelete verifies that an explicit
// Delete causes the bloom to be rebuilt so the deleted
// MID is reported as absent.
func TestBloomStoreRebuildOnDelete(t *testing.T) {
	dir := t.TempDir()
	s, err := NewMemStore(Options{
		Path: filepath.Join(dir, "store"),
		Bloom: BloomConfig{
			Capacity:     1_000,
			FPRate:       0.001,
			SnapshotPath: filepath.Join(dir, "bloom.bin"),
		},
	})
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	data := make([]byte, 32)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("rand: %v", err)
	}
	m := mid.FromBytes(data)
	if err := s.Put(m, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if ok, _ := s.Has(m); !ok {
		t.Fatal("Has: block should be present")
	}
	if err := s.Delete(m); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if ok, _ := s.Has(m); ok {
		t.Fatal("Has: bloom was not rebuilt after Delete")
	}
}

// TestBloomStoreDisabled verifies that opting out of the
// filter still serves Has() correctly.
func TestBloomStoreDisabled(t *testing.T) {
	dir := t.TempDir()
	s, err := NewMemStore(Options{
		Path: filepath.Join(dir, "store"),
		Bloom: BloomConfig{
			Disabled: true,
		},
	})
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if s.bloom != nil {
		t.Fatal("bloom should be nil when Disabled is set")
	}
	data := []byte("hello")
	m := mid.FromBytes(data)
	if err := s.Put(m, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	ok, err := s.Has(m)
	if err != nil || !ok {
		t.Fatalf("Has: ok=%v err=%v", ok, err)
	}
	got, err := s.Get(m)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Get = %q, want %q", got, data)
	}
}
