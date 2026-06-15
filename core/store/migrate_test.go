package store

import (
	"path/filepath"
	"testing"

	"github.com/nnlgsakib/membuss/core/mid"
)

// TestMigrateToV1MIDsNoOp confirms that a freshly built
// store passes the migration check unchanged: every key is
// already in the v1-compatible multihash layout.
func TestMigrateToV1MIDsNoOp(t *testing.T) {
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

	// Insert 25 raw blocks and 5 DAG nodes.
	for i := 0; i < 25; i++ {
		data := []byte{byte(i), byte(i >> 8)}
		m := mid.FromBytes(data)
		if err := s.Put(m, data); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		data := []byte{0xDA, 0x67, byte(i)}
		m := mid.FromBytes(data)
		if err := s.PutDAG(m, data); err != nil {
			t.Fatalf("PutDAG %d: %v", i, err)
		}
	}

	res, err := MigrateToV1MIDs(s)
	if err != nil {
		t.Fatalf("MigrateToV1MIDs: %v", err)
	}
	if res.Inspected != 30 {
		t.Fatalf("Inspected = %d, want 30", res.Inspected)
	}
	if res.Rewritten != 0 {
		t.Fatalf("Rewritten = %d, want 0 (on-disk keys are already v1)", res.Rewritten)
	}
	if len(res.Legacy) != 0 {
		t.Fatalf("Legacy = %v, want none", res.Legacy)
	}

	// And every inserted MID must round-trip through the
	// new parser.
	for i := 0; i < 25; i++ {
		data := []byte{byte(i), byte(i >> 8)}
		m := mid.FromBytes(data)
		if _, err := mid.Parse(m.String()); err != nil {
			t.Fatalf("Parse(%q) failed: %v", m.String(), err)
		}
	}
}

// TestMigrateToV1MIDsEmptyStore confirms the migration is
// a no-op on an empty store.
func TestMigrateToV1MIDsEmptyStore(t *testing.T) {
	dir := t.TempDir()
	s, err := NewMemStore(Options{
		Path: filepath.Join(dir, "store"),
		Bloom: BloomConfig{Disabled: true},
	})
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	res, err := MigrateToV1MIDs(s)
	if err != nil {
		t.Fatalf("MigrateToV1MIDs: %v", err)
	}
	if res.Inspected != 0 {
		t.Fatalf("Inspected = %d, want 0", res.Inspected)
	}
	if len(res.Legacy) != 0 {
		t.Fatalf("Legacy = %v, want empty", res.Legacy)
	}
}

// TestDetectLegacyMIDs confirms the convenience wrapper
// returns the legacy count.
func TestDetectLegacyMIDs(t *testing.T) {
	dir := t.TempDir()
	s, err := NewMemStore(Options{
		Path: filepath.Join(dir, "store"),
		Bloom: BloomConfig{Disabled: true},
	})
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Put(mid.FromBytes([]byte("x")), []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	n, err := DetectLegacyMIDs(s)
	if err != nil {
		t.Fatalf("DetectLegacyMIDs: %v", err)
	}
	if n != 0 {
		t.Fatalf("DetectLegacyMIDs = %d, want 0", n)
	}
}
