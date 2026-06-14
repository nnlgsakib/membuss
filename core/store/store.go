// Package store defines the Blockstore interface that core/dag
// reads from and writes to, plus an in-memory implementation used
// for tests and for nodes that want ephemeral storage.
//
// Phase 1 ships the interface and the in-memory memstore.
// Phase 2 adds the BadgerDB-backed MemStore behind the same
// interface.
package store

import (
	"errors"
	"sync"

	"github.com/nnlgsakib/membuss/core/mid"
)

// ErrNotFound is returned by Get and Delete when the requested
// block is not present in the store.
var ErrNotFound = errors.New("store: block not found")

// Blockstore is the interface a DAG builder / resolver reads and
// writes blocks through. Implementations MUST be safe for
// concurrent use.
//
// Put and Get take and return raw bytes; the MID is the
// integrity-checked address.
type Blockstore interface {
	// Put stores data under the given MID. The data is
	// integrity-checked: the SHA-256 digest of data MUST match
	// the MID's digest, otherwise Put returns an error and no
	// data is stored.
	Put(m mid.MID, data []byte) error

	// Get returns the bytes stored under m. It returns
	// ErrNotFound if the block is not present.
	Get(m mid.MID) ([]byte, error)

	// Has reports whether a block is present.
	Has(m mid.MID) (bool, error)

	// Delete removes the block. It is not an error to delete a
	// missing block.
	Delete(m mid.MID) error
}

// Memstore is an in-memory Blockstore. It is safe for concurrent
// use. Phase 2 introduces a BadgerDB-backed implementation
// (MemStore) behind the same interface.
type Memstore struct {
	mu     sync.RWMutex
	blocks map[string][]byte
}

// NewMemstore returns an empty in-memory Blockstore.
func NewMemstore() *Memstore {
	return &Memstore{blocks: make(map[string][]byte)}
}

// Put stores a copy of data under the MID after verifying the
// integrity check (data hashes to MID).
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

// Get returns a defensive copy of the bytes stored under mid.
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

// Has reports whether a block is present.
func (m *Memstore) Has(mid mid.MID) (bool, error) {
	m.mu.RLock()
	_, ok := m.blocks[mid.String()]
	m.mu.RUnlock()
	return ok, nil
}

// Delete removes the block. Missing blocks are not an error.
func (m *Memstore) Delete(mid mid.MID) error {
	m.mu.Lock()
	delete(m.blocks, mid.String())
	m.mu.Unlock()
	return nil
}

// Len returns the number of blocks currently stored. It is intended
// for tests and metrics; production code MUST NOT rely on it for
// correctness.
func (m *Memstore) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.blocks)
}
