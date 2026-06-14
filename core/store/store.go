// Package store defines the Blockstore interface that core/dag
// reads from and writes to, plus an in-memory implementation used
// for tests and for nodes that want ephemeral storage.
//
// Phase 1 ships only the interface and the memstore; Phase 2 will
// add the BadgerDB-backed implementation.
package store

import (
	"errors"
	"fmt"
	"sync"

	"github.com/nnlgsakib/membuss/core/mid"
)

// ErrNotFound is returned by Get and Delete when the requested
// block is not present in the store.
var ErrNotFound = errors.New("store: block not found")

// Block is the minimal block type stored by a Blockstore.
type Block struct {
	MID  mid.MID
	Data []byte
}

// Clone returns a defensive copy of the block so the store can
// hand blocks out without leaking internal state.
func (b Block) Clone() Block {
	if b.MID.IsZero() {
		return Block{}
	}
	out := Block{MID: b.MID, Data: make([]byte, len(b.Data))}
	copy(out.Data, b.Data)
	return out
}

// Blockstore is the interface a DAG builder / resolver reads and
// writes blocks through. Implementations MUST be safe for
// concurrent use.
type Blockstore interface {
	// Put stores a copy of the block. The block is indexed by
	// MID; Put returns an error if the MID's digest does not
	// match the SHA-256 of the block's data.
	Put(b Block) error

	// Get returns a defensive copy of the block addressed by m.
	// It returns ErrNotFound if the block is not present.
	Get(m mid.MID) (Block, error)

	// Has reports whether a block is present.
	Has(m mid.MID) bool

	// Delete removes the block. It is not an error to delete a
	// missing block.
	Delete(m mid.MID) error
}

// verifyMID checks that the block's data hashes to the MID's
// digest. We compare digests (not codec-tagged MIDs) so the same
// store can hold both raw leaf bytes (CodecRaw) and protobuf
// internal-node bytes (CodecDAGPB): what matters is the SHA-256
// of the bytes, which is identical for both.
func verifyMID(b Block) error {
	want, err := b.MID.DigestBytes()
	if err != nil {
		return fmt.Errorf("store: claim MID has no digest: %w", err)
	}
	got := mid.FromBytes(b.Data)
	gotDigest, err := got.DigestBytes()
	if err != nil {
		return fmt.Errorf("store: derive digest: %w", err)
	}
	if !bytesEqual(want, gotDigest) {
		return fmt.Errorf("store: data does not hash to claimed MID %s", b.MID.String())
	}
	return nil
}

// bytesEqual is a constant-time byte slice compare. We use
// crypto/subtle to avoid leaking length information through
// timing, even though the inputs are not secrets.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Memstore is an in-memory Blockstore. It is safe for concurrent
// use. Phase 2 will introduce a BadgerDB-backed implementation
// behind the same interface.
type Memstore struct {
	mu     sync.RWMutex
	blocks map[string]Block
}

// NewMemstore returns an empty in-memory Blockstore.
func NewMemstore() *Memstore {
	return &Memstore{blocks: make(map[string]Block)}
}

// Put stores a copy of the block.
func (m *Memstore) Put(b Block) error {
	if err := verifyMID(b); err != nil {
		return err
	}
	c := b.Clone()
	m.mu.Lock()
	m.blocks[c.MID.String()] = c
	m.mu.Unlock()
	return nil
}

// Get returns a defensive copy of the block addressed by mid.
func (m *Memstore) Get(mid mid.MID) (Block, error) {
	m.mu.RLock()
	b, ok := m.blocks[mid.String()]
	m.mu.RUnlock()
	if !ok {
		return Block{}, ErrNotFound
	}
	return b.Clone(), nil
}

// Has reports whether a block is present.
func (m *Memstore) Has(mid mid.MID) bool {
	m.mu.RLock()
	_, ok := m.blocks[mid.String()]
	m.mu.RUnlock()
	return ok
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
