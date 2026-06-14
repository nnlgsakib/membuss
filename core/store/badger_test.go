package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/mid"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// newTestStore returns a fresh in-memory MemStore.
func newTestStore(t *testing.T) *MemStore {
	t.Helper()
	s, err := NewMemStore(Options{InMemory: true})
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestMemStorePutGetRoundTrip(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 50; i++ {
		data := []byte(fmt.Sprintf("payload-%d", i))
		m := mid.FromBytes(data)
		if err := s.Put(m, data); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		got, err := s.Get(m)
		if err != nil {
			t.Fatalf("Get %d: %v", i, err)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("Get %d = %q, want %q", i, got, data)
		}
		ok, err := s.Has(m)
		if err != nil || !ok {
			t.Fatalf("Has %d: ok=%v err=%v", i, ok, err)
		}
	}
}

func TestMemStorePutDAGGetDAG(t *testing.T) {
	s := newTestStore(t)
	data := []byte("dag-node-payload")
	m := mid.FromBytes(data)
	if err := s.PutDAG(m, data); err != nil {
		t.Fatalf("PutDAG: %v", err)
	}
	got, err := s.GetDAG(m)
	if err != nil {
		t.Fatalf("GetDAG: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("GetDAG = %q, want %q", got, data)
	}
	// Get is cross-namespace: it should find the data in /d/.
	if got2, err := s.Get(m); err != nil || !bytes.Equal(got2, data) {
		t.Fatalf("Get cross-namespace: got=%q err=%v", got2, err)
	}
	ok, err := s.HasDAG(m)
	if err != nil || !ok {
		t.Fatalf("HasDAG: ok=%v err=%v", ok, err)
	}
}

func TestMemStoreMetaRoundTrip(t *testing.T) {
	s := newTestStore(t)
	if err := s.PutMeta("last-gc", []byte("2026-06-14T12:00:00Z")); err != nil {
		t.Fatalf("PutMeta: %v", err)
	}
	got, err := s.GetMeta("last-gc")
	if err != nil {
		t.Fatalf("GetMeta: %v", err)
	}
	if string(got) != "2026-06-14T12:00:00Z" {
		t.Fatalf("GetMeta = %q", got)
	}
	if _, err := s.GetMeta("missing"); err == nil {
		t.Fatal("GetMeta missing must return ErrNotFound")
	}
}

func TestMemStoreRejectsBadMID(t *testing.T) {
	s := newTestStore(t)
	if err := s.Put(mid.FromBytes([]byte("a")), []byte("b")); err == nil {
		t.Fatal("Put with mismatched MID must fail")
	}
	if err := s.PutDAG(mid.FromBytes([]byte("a")), []byte("b")); err == nil {
		t.Fatal("PutDAG with mismatched MID must fail")
	}
}

func TestMemStoreGetMissing(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Get(mid.FromBytes([]byte("nope")))
	if err == nil {
		t.Fatal("Get missing must fail")
	}
}

func TestMemStoreSealUnseal(t *testing.T) {
	s := newTestStore(t)
	root := mid.FromBytes([]byte("root"))
	if err := s.Seal(root, false); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	ok, err := s.IsSealed(root)
	if err != nil || !ok {
		t.Fatalf("IsSealed: ok=%v err=%v", ok, err)
	}
	all, err := s.AllSealed()
	if err != nil {
		t.Fatalf("AllSealed: %v", err)
	}
	if len(all) != 1 || !all[0].Equal(root) {
		t.Fatalf("AllSealed = %v, want just %s", all, root)
	}
	if err := s.Unseal(root); err != nil {
		t.Fatalf("Unseal: %v", err)
	}
	ok, _ = s.IsSealed(root)
	if ok {
		t.Fatal("IsSealed after Unseal must be false")
	}
}

// TestMemStoreRecursiveSealWalksDAG hand-builds a small DAG
// using the same canonical protobuf form the core/dag builder
// produces, then recursive-seals the root and confirms the
// leaves are reachable but have no direct seal record.
func TestMemStoreRecursiveSealWalksDAG(t *testing.T) {
	s := newTestStore(t)

	leaves := make([]mid.MID, 4)
	for i := range leaves {
		data := []byte(fmt.Sprintf("leaf-%d", i))
		leaves[i] = mid.FromBytes(data)
		if err := s.Put(leaves[i], data); err != nil {
			t.Fatalf("Put leaf %d: %v", i, err)
		}
	}
	links := make([]string, len(leaves))
	for i, c := range leaves {
		links[i] = c.String()
	}
	raw, err := proto.Marshal(&membusspb.DAGNode{Links: links})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	root := mid.FromBytes(raw)
	if err := s.PutDAG(root, raw); err != nil {
		t.Fatalf("PutDAG root: %v", err)
	}

	if err := s.Seal(root, true); err != nil {
		t.Fatalf("Seal recursive: %v", err)
	}

	all, err := s.AllSealed()
	if err != nil {
		t.Fatalf("AllSealed: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("AllSealed len = %d, want 1 (only the root has a direct seal)", len(all))
	}
	if !all[0].Equal(root) {
		t.Fatalf("AllSealed[0] = %s, want root %s", all[0], root)
	}
	for _, leaf := range leaves {
		ok, err := s.IsSealed(leaf)
		if err != nil {
			t.Fatalf("IsSealed leaf: %v", err)
		}
		if ok {
			t.Fatalf("leaf %s has a direct seal record; recursive seal should not write one", leaf)
		}
	}
}

// TestMemStoreGC1000Blocks is the headline integration test:
// store 1000 blocks, seal 100 of them, run GC, and assert that
// only the sealed ones remain.
func TestMemStoreGC1000Blocks(t *testing.T) {
	s := newTestStore(t)

	const total = 1000
	const sealed = 100

	allMIDs := make([]mid.MID, total)
	for i := 0; i < total; i++ {
		data := make([]byte, 256) // 256 bytes per block
		if _, err := rand.Read(data); err != nil {
			t.Fatalf("rand: %v", err)
		}
		m := mid.FromBytes(data)
		if err := s.Put(m, data); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		allMIDs[i] = m
	}

	sealedSet := make(map[string]struct{})
	for i := 0; i < total; i += total / sealed {
		if err := s.Seal(allMIDs[i], false); err != nil {
			t.Fatalf("Seal %d: %v", i, err)
		}
		sealedSet[allMIDs[i].String()] = struct{}{}
	}
	if len(sealedSet) != sealed {
		t.Fatalf("sealed set size = %d, want %d", len(sealedSet), sealed)
	}

	all, err := s.AllSealed()
	if err != nil {
		t.Fatalf("AllSealed: %v", err)
	}
	if len(all) != sealed {
		t.Fatalf("AllSealed = %d, want %d", len(all), sealed)
	}

	freed, err := s.GC(context.Background())
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if freed == 0 {
		t.Fatal("GC freed 0 bytes; expected to free the unsealed blocks")
	}
	t.Logf("GC freed %d bytes", freed)

	for i, m := range allMIDs {
		ok, err := s.Has(m)
		if err != nil {
			t.Fatalf("Has %d: %v", i, err)
		}
		_, isSealed := sealedSet[m.String()]
		if isSealed && !ok {
			t.Fatalf("sealed block %d (%s) was deleted by GC", i, m)
		}
		if !isSealed && ok {
			t.Fatalf("unsealed block %d (%s) survived GC", i, m)
		}
	}
}

// TestMemStoreGCDAGKeepsSealedSubtree confirms GC keeps only
// the sealed DAG and removes unsealed siblings.
func TestMemStoreGCDAGKeepsSealedSubtree(t *testing.T) {
	s := newTestStore(t)

	buildDAG := func(seed byte) (root mid.MID, leaves []mid.MID) {
		for i := 0; i < 4; i++ {
			data := []byte{seed, byte(i)}
			m := mid.FromBytes(data)
			if err := s.Put(m, data); err != nil {
				t.Fatalf("Put leaf: %v", err)
			}
			leaves = append(leaves, m)
		}
		links := make([]string, len(leaves))
		for i, c := range leaves {
			links[i] = c.String()
		}
		raw, err := proto.Marshal(&membusspb.DAGNode{Links: links})
		if err != nil {
			t.Fatalf("proto.Marshal: %v", err)
		}
		root = mid.FromBytes(raw)
		if err := s.PutDAG(root, raw); err != nil {
			t.Fatalf("PutDAG: %v", err)
		}
		return root, leaves
	}
	rootA, leavesA := buildDAG(0xAA)
	rootB, leavesB := buildDAG(0xBB)

	if err := s.Seal(rootA, false); err != nil {
		t.Fatalf("Seal A: %v", err)
	}

	if _, err := s.GC(context.Background()); err != nil {
		t.Fatalf("GC: %v", err)
	}

	for _, leaf := range leavesA {
		ok, err := s.Has(leaf)
		if err != nil {
			t.Fatalf("Has A leaf: %v", err)
		}
		if !ok {
			t.Fatalf("A leaf %s vanished after GC", leaf)
		}
	}
	for _, leaf := range leavesB {
		ok, err := s.Has(leaf)
		if err != nil {
			t.Fatalf("Has B leaf: %v", err)
		}
		if ok {
			t.Fatalf("B leaf %s survived GC", leaf)
		}
	}
	ok, _ := s.Has(rootA)
	if !ok {
		t.Fatal("A root vanished after GC")
	}
	ok, _ = s.Has(rootB)
	if ok {
		t.Fatal("B root survived GC")
	}
}

func TestMemStoreClose(t *testing.T) {
	s, err := NewMemStore(Options{InMemory: true})
	if err != nil {
		t.Fatalf("NewMemStore: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Put(mid.FromBytes([]byte("x")), []byte("x")); err == nil {
		t.Fatal("Put after Close must fail")
	}
	if _, err := s.Get(mid.FromBytes([]byte("x"))); err == nil {
		t.Fatal("Get after Close must fail")
	}
}

func TestMemStoreSize(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.Size(); err != nil {
		t.Fatalf("Size: %v", err)
	}
}
