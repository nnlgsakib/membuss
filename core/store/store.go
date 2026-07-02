// Package store defines the Blockstore interface that core/dag
// reads from and writes to, plus an in-memory implementation used
// for tests and for nodes that want ephemeral storage.
//
// Phase 1 ships the interface and the in-memory memstore.
// Phase 2 adds the BadgerDB-backed MemStore behind the same
// interface.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/mid"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

// ErrNotFound is returned by Get and Delete when the requested
// block is not present in the store.
var ErrNotFound = errors.New("store: block not found")

// Blockstore is the interface a DAG builder / resolver reads and
// writes blocks through. Implementations MUST be safe for
// concurrent use.
type Blockstore interface {
	Put(m mid.MID, data []byte) error
	Get(m mid.MID) ([]byte, error)
	Has(m mid.MID) (bool, error)
	Delete(m mid.MID) error
	// PutMeta stores an arbitrary key/value pair under
	// the /m/ namespace. Used by per-MID descriptors
	// (see ObjectInfo), GC timestamps, etc.
	PutMeta(key string, value []byte) error
	// GetMeta returns the value previously stored
	// under key, or ErrNotFound when absent.
	GetMeta(key string) ([]byte, error)
}

// Memstore is an in-memory Blockstore. It is safe for concurrent
// use. Phase 2 introduces a BadgerDB-backed implementation
// (MemStore) behind the same interface.
type Memstore struct {
	mu     sync.RWMutex
	blocks map[string][]byte

	metaMu sync.RWMutex
	meta   map[string][]byte

	sealsMu sync.RWMutex
	seals   map[string]bool
}

// NewMemstore returns an empty in-memory Blockstore.
func NewMemstore() *Memstore {
	return &Memstore{
		blocks: make(map[string][]byte),
		seals:  make(map[string]bool),
	}
}

func (m *Memstore) Put(mid mid.MID, data []byte) error {
	if err := verifyContent(mid, data); err != nil {
		return err
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	m.mu.Lock()
	m.blocks[mid.String()] = cp
	m.mu.Unlock()
	return nil
}

func (m *Memstore) Get(mid mid.MID) ([]byte, error) {
	m.mu.RLock()
	b, ok := m.blocks[mid.String()]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

func (m *Memstore) Has(mid mid.MID) (bool, error) {
	m.mu.RLock()
	_, ok := m.blocks[mid.String()]
	m.mu.RUnlock()
	return ok, nil
}

func (m *Memstore) Delete(mid mid.MID) error {
	m.mu.Lock()
	delete(m.blocks, mid.String())
	m.mu.Unlock()
	return nil
}

// AllSealed returns every sealed MID from the in-memory seals
// map. It is part of the herald SealedLister interface.
func (m *Memstore) AllSealed() ([]mid.MID, error) {
	m.sealsMu.RLock()
	defer m.sealsMu.RUnlock()
	out := make([]mid.MID, 0, len(m.seals))
	for k, isChild := range m.seals {
		if isChild {
			continue
		}
		midID, err := mid.Parse(k)
		if err != nil {
			continue
		}
		out = append(out, midID)
	}
	return out, nil
}

// AllBlocks returns every MID currently held in the in-memory
// store. The order is unspecified.
func (m *Memstore) AllBlocks() ([]mid.MID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]mid.MID, 0, len(m.blocks))
	for k := range m.blocks {
		midID, err := mid.Parse(k)
		if err != nil {
			continue
		}
		out = append(out, midID)
	}
	return out, nil
}

// PutMeta stores a metadata key/value pair. The in-memory
// store keeps meta in a side map so that PutMeta/GetMeta do
// not interfere with content-addressed blocks.
func (m *Memstore) PutMeta(key string, value []byte) error {
	m.metaMu.Lock()
	if m.meta == nil {
		m.meta = make(map[string][]byte)
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	m.meta[key] = cp
	m.metaMu.Unlock()
	return nil
}

// GetMeta returns a metadata value or ErrNotFound.
func (m *Memstore) GetMeta(key string) ([]byte, error) {
	m.metaMu.RLock()
	v, ok := m.meta[key]
	m.metaMu.RUnlock()
	if !ok {
		return nil, ErrNotFound
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

// Size returns the approximate number of bytes held by
// the store. For the in-memory store we approximate by
// the sum of block sizes.
func (m *Memstore) Size() (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var n uint64
	for _, b := range m.blocks {
	n += uint64(len(b))
	}
	return n, nil
}

// Close releases any resources held by the store. The
// in-memory store has nothing to release, so it is a
// no-op. It is part of the store.Store interface so the
// in-memory and BadgerDB implementations are drop-in.
func (m *Memstore) Close() error { return nil }

// Len returns the number of blocks currently stored. It is
// intended for tests and metrics.
func (m *Memstore) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.blocks)
}
// Seal records a seal for the given MID. The in-memory
// store does not enforce GC, but a seal record is needed
// for tests and for the herald roots strategy.
func (m *Memstore) Seal(root mid.MID, recursive bool) error {
	m.sealsMu.Lock()
	if m.seals == nil {
		m.seals = make(map[string]bool)
	}
	m.seals[root.String()] = false
	m.sealsMu.Unlock()

	if !recursive {
		return nil
	}
	_ = Walk(m, root, func(id mid.MID, _ bool) error {
		if id.Equal(root) {
			return nil
		}
		if id.Codec() == mid.CodecMemFS {
			m.sealsMu.Lock()
			m.seals[id.String()] = true
			m.sealsMu.Unlock()
		}
		return nil
	})
	return nil
}

// Unseal removes a seal record and recursively unseals child MemFS nodes.
func (m *Memstore) Unseal(root mid.MID) error {
	m.sealsMu.Lock()
	delete(m.seals, root.String())
	m.sealsMu.Unlock()

	_ = Walk(m, root, func(id mid.MID, _ bool) error {
		if id.Codec() == mid.CodecMemFS {
			m.sealsMu.Lock()
			delete(m.seals, id.String())
			m.sealsMu.Unlock()
		}
		return nil
	})
	return nil
}

// IsSealed reports whether a direct seal record exists.
func (m *Memstore) IsSealed(root mid.MID) (bool, error) {
	m.sealsMu.RLock()
	_, ok := m.seals[root.String()]
	m.sealsMu.RUnlock()
	return ok, nil
}

// GC is a no-op on the in-memory store.
func (m *Memstore) GC(ctx context.Context) (uint64, error) {
	return 0, nil
}

// GCWithMinAge is a no-op on the in-memory store.
func (m *Memstore) GCWithMinAge(ctx context.Context, minAge time.Duration) (uint64, error) {
	return 0, nil
}

// DeleteRecursive removes the given root MID and all its reachable children from the store.
// It also unseals them. It returns the number of blocks deleted and the number of bytes freed.
func (m *Memstore) DeleteRecursive(root mid.MID) (uint64, uint64, error) {
	if root.IsZero() {
		return 0, 0, errors.New("store: zero MID")
	}

	_ = m.Unseal(root)

	reachable := make(map[string]mid.MID)
	var collect func(id mid.MID)
	collect = func(id mid.MID) {
		if id.IsZero() {
			return
		}
		ids := id.String()
		if _, seen := reachable[ids]; seen {
			return
		}

		m.mu.RLock()
		data, exists := m.blocks[ids]
		m.mu.RUnlock()
		if !exists {
			return
		}

		reachable[ids] = id

		var childMIDs []mid.MID
		if id.Codec() == mid.CodecMemFS {
			var node membusspb.MemFSNode
			if uerr := proto.Unmarshal(data, &node); uerr == nil {
				switch node.Type {
				case membusspb.MemFSType_FILE:
					for _, b := range node.Blocks {
						if b == nil || len(b.Mid) == 0 {
							continue
						}
						var codec uint64 = mid.CodecMemFS
						if b.Size > 0 {
							codec = mid.CodecRaw
						}
						child, err := mid.FromMultihash(codec, b.Mid)
						if err == nil {
							childMIDs = append(childMIDs, child)
						}
					}
				case membusspb.MemFSType_DIR:
					for _, e := range node.Entries {
						if e == nil || len(e.Mid) == 0 {
							continue
						}
						var codec uint64 = mid.CodecMemFS
						if e.Type == membusspb.MemFSType_RAW {
							codec = mid.CodecRaw
						}
						child, err := mid.FromMultihash(codec, e.Mid)
						if err == nil {
							childMIDs = append(childMIDs, child)
						}
					}
				}
			}
		} else {
			var node membusspb.DAGNode
			if uerr := proto.Unmarshal(data, &node); uerr == nil && len(node.Links) > 0 {
				for _, s := range node.Links {
					child, err := mid.Parse(s)
					if err == nil {
						childMIDs = append(childMIDs, child)
					}
				}
			}
		}

		for _, child := range childMIDs {
			collect(child)
		}
	}

	collect(root)

	var blocksDeleted uint64
	var bytesFreed uint64

	m.mu.Lock()
	m.metaMu.Lock()
	for ids := range reachable {
		if data, ok := m.blocks[ids]; ok {
			bytesFreed += uint64(len(data))
			delete(m.blocks, ids)
			blocksDeleted++
		}
		delete(m.meta, "obj/"+ids)
		delete(m.meta, "ts/"+ids)
	}
	m.metaMu.Unlock()
	m.mu.Unlock()

	return blocksDeleted, bytesFreed, nil
}

// AllObjectMIDs returns every MID that has an ObjectInfo metadata record
// where IsRoot is true.
func (m *Memstore) AllObjectMIDs() ([]mid.MID, error) {
	m.metaMu.RLock()
	defer m.metaMu.RUnlock()

	var out []mid.MID
	for k, data := range m.meta {
		if !strings.HasPrefix(k, "obj/") {
			continue
		}
		var info ObjectInfo
		if err := json.Unmarshal(data, &info); err != nil {
			continue
		}
		if !info.IsRoot {
			continue
		}
		midStr := k[len("obj/"):]
		midID, err := mid.Parse(midStr)
		if err != nil {
			continue
		}
		out = append(out, midID)
	}
	return out, nil
}

