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
	"errors"
	"sync"
	"time"

	"github.com/nnlgsakib/membuss/core/mid"
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
	seals   map[string]struct{}
}

// NewMemstore returns an empty in-memory Blockstore.
func NewMemstore() *Memstore {
	return &Memstore{blocks: make(map[string][]byte)}
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
	for k := range m.seals {
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
		m.seals = make(map[string]struct{})
	}
	m.seals[root.String()] = struct{}{}
	m.sealsMu.Unlock()

	if !recursive {
		return nil
	}
	_ = Walk(m, root, func(id mid.MID, _ bool) error {
		if id.Codec() == mid.CodecMemFS {
			m.sealsMu.Lock()
			m.seals[id.String()] = struct{}{}
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
