// Package store: BadgerDB-backed implementation of the Store and
// Blockstore interfaces.
//
// Key layout (all keys are byte strings, sortable, no length prefix):
//
//	'/b/' + <raw multihash>    -> raw block bytes
//	'/d/' + <raw multihash>    -> DAGNode protobuf bytes
//	'/s/' + <raw multihash>    -> seal record (empty value)
//	'/m/' + utf8 key           -> metadata (free-form)
//
// The leading byte of the prefix makes iteration cheap and keeps
// the four namespaces disjoint in the key space. All multihash
// keys are the *raw* multihash envelope (see core/mid.Bytes),
// not the public mem-prefixed string form, so the keys stay
// fixed-width and human-friendly in BadgerDB tooling.
//
// Blockstore semantics: Put/PutDAG are explicit about the
// namespace, but Get/Has look in BOTH /b/ and /d/ and return
// whichever hits first. This matches the Blockstore contract
// used by core/dag, which only knows the MID of a node and
// never needs to know whether the node is a leaf or internal.
package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/dgraph-io/badger/v4"

	"github.com/nnlgsakib/membuss/core/mid"
)

// Store is the full Phase 2 surface: it embeds Blockstore and adds
// seal records and GC. Implementations MUST be safe for
// concurrent use.
type Store interface {
	Blockstore

	// Seal marks the given MID (and, if recursive is true, every
	// MID reachable from it via the DAG) as protected from GC.
	// Sealing a MID that is not yet stored is allowed and acts
	// as a forward-looking reservation.
	Seal(root mid.MID, recursive bool) error

	// Unseal removes the seal record for the given MID. It does
	// NOT recursively unseal descendants; that is the caller's
	// responsibility (or simply run GC and let the unreachable
	// descendants be collected).
	Unseal(root mid.MID) error

	// IsSealed reports whether a direct seal record exists for m.
	// Descendant reachability is not considered.
	IsSealed(m mid.MID) (bool, error)

	// AllSealed returns every MID that has a direct seal record.
	// Order is implementation-defined.
	AllSealed() ([]mid.MID, error)

	// AllBlocks returns every MID that has a block or DAG
	// record in the store. Order is implementation-defined.
	// This powers the herald's "all" strategy and the anchor
	// engine's status reporter.
	AllBlocks() ([]mid.MID, error)

	// PutMeta stores an arbitrary key/value pair under the /m/
	// namespace. Meta records are independent of blocks and
	// DAG nodes; they are not part of the content-addressed
	// data set and are never deleted by GC.
	PutMeta(key string, value []byte) error

	// GetMeta returns the value previously stored under key, or
	// ErrNotFound if absent.
	GetMeta(key string) ([]byte, error)

	// GC walks every sealed DAG root, collects the reachable
	// MID set, and deletes every key in the store that is NOT
	// in that set. It returns the number of bytes freed (sum of
	// the deleted key+value lengths).
	//
	// BadgerDB's value-log GC is invoked at most once per call.
	GC(ctx context.Context) (uint64, error)

	// Size returns the approximate on-disk size of the store in
	// bytes. It is the sum of the LSM tree size and the value
	// log size, as reported by BadgerDB.
	Size() (uint64, error)

	// Close releases all resources held by the store. After
	// Close, every other method returns an error.
	Close() error
}

// MemStore is the BadgerDB-backed Store implementation. A zero
// value is invalid; use NewMemStore.
type MemStore struct {
	db *badger.DB
}

// Options configures a MemStore at construction time.
type Options struct {
	// Path is the on-disk directory BadgerDB will use. Required
	// unless InMemory is true.
	Path string

	// InMemory, if true, makes BadgerDB use an in-memory backend
	// and ignores Path. Used for tests.
	InMemory bool

	// ReadOnly opens the store in read-only mode. Writes return
	// an error.
	ReadOnly bool

	// Logger, if non-nil, is passed to BadgerDB. nil = silent.
	Logger badger.Logger
}

// NewMemStore opens (or creates) a MemStore at opts.Path.
//
// The caller MUST call Close when done.
func NewMemStore(opts Options) (*MemStore, error) {
	if !opts.InMemory && filepath.Clean(opts.Path) == "" {
		return nil, errors.New("store: empty path")
	}

	badgerOpts := badger.DefaultOptions(opts.Path).
		WithInMemory(opts.InMemory).
		WithReadOnly(opts.ReadOnly).
		WithLogger(opts.Logger)

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, fmt.Errorf("store: open badger at %q: %w", opts.Path, err)
	}
	return &MemStore{db: db}, nil
}

// Put stores data under the given MID. The data is recorded as a
// raw block (prefix "/b/"). The MID is integrity-checked: the
// SHA-256 digest of data must match the MID's digest.
func (s *MemStore) Put(m mid.MID, data []byte) error {
	if s.db == nil {
		return errors.New("store: closed")
	}
	if m.IsZero() {
		return errors.New("store: zero MID")
	}
	if err := verifyContent(m, data); err != nil {
		return err
	}
	key := blockKey(m)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, append([]byte(nil), data...))
	})
}

// PutDAG stores data under the given MID as a DAG node
// (prefix "/d/"). It is the counterpart of Put for internal
// nodes emitted by core/dag.
func (s *MemStore) PutDAG(m mid.MID, data []byte) error {
	if s.db == nil {
		return errors.New("store: closed")
	}
	if m.IsZero() {
		return errors.New("store: zero MID")
	}
	if err := verifyContent(m, data); err != nil {
		return err
	}
	key := dagKey(m)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, append([]byte(nil), data...))
	})
}

// Get returns the bytes stored under m, looking in BOTH the
// block and DAG namespaces. The first namespace to return a
// match wins; the lookup order is /b/ then /d/. This matches the
// Blockstore contract used by core/dag, where a caller only
// knows the MID and does not want to know whether the node is a
// leaf or an internal node.
func (s *MemStore) Get(m mid.MID) ([]byte, error) {
	if s.db == nil {
		return nil, errors.New("store: closed")
	}
	if m.IsZero() {
		return nil, errors.New("store: zero MID")
	}
	var out []byte
	err := s.db.View(func(txn *badger.Txn) error {
		// Try /b/ first.
		if item, gerr := txn.Get(blockKey(m)); gerr == nil {
			out = make([]byte, item.ValueSize())
			return item.Value(func(v []byte) error {
				copy(out, v)
				return nil
			})
		} else if !errors.Is(gerr, badger.ErrKeyNotFound) {
			return gerr
		}
		// Fall back to /d/.
		item, gerr := txn.Get(dagKey(m))
		if gerr != nil {
			return gerr
		}
		out = make([]byte, item.ValueSize())
		return item.Value(func(v []byte) error {
			copy(out, v)
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get %s: %w", m.String(), err)
	}
	return out, nil
}

// GetDAG returns the DAG-node bytes for m, looking ONLY in the
// DAG namespace. Use Get for the more common cross-namespace
// lookup.
func (s *MemStore) GetDAG(m mid.MID) ([]byte, error) {
	return s.getFrom(dagKey(m))
}

func (s *MemStore) getFrom(key []byte) ([]byte, error) {
	if s.db == nil {
		return nil, errors.New("store: closed")
	}
	var out []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		out = make([]byte, item.ValueSize())
		return item.Value(func(v []byte) error {
			copy(out, v)
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get: %w", err)
	}
	return out, nil
}

// Has reports whether a block OR a DAG node exists for m. It
// returns true if the MID is present in either namespace.
func (s *MemStore) Has(m mid.MID) (bool, error) {
	if s.db == nil {
		return false, errors.New("store: closed")
	}
	if m.IsZero() {
		return false, errors.New("store: zero MID")
	}
	var found bool
	err := s.db.View(func(txn *badger.Txn) error {
		if _, gerr := txn.Get(blockKey(m)); gerr == nil {
			found = true
			return nil
		} else if !errors.Is(gerr, badger.ErrKeyNotFound) {
			return gerr
		}
		if _, gerr := txn.Get(dagKey(m)); gerr == nil {
			found = true
			return nil
		} else if !errors.Is(gerr, badger.ErrKeyNotFound) {
			return gerr
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// HasDAG reports whether a DAG node exists for m. It looks ONLY
// in the DAG namespace.
func (s *MemStore) HasDAG(m mid.MID) (bool, error) {
	return s.hasKey(dagKey(m))
}

func (s *MemStore) hasKey(key []byte) (bool, error) {
	if s.db == nil {
		return false, errors.New("store: closed")
	}
	var found bool
	err := s.db.View(func(txn *badger.Txn) error {
		_, gerr := txn.Get(key)
		if gerr == nil {
			found = true
			return nil
		}
		if errors.Is(gerr, badger.ErrKeyNotFound) {
			return nil
		}
		return gerr
	})
	if err != nil {
		return false, err
	}
	return found, nil
}

// Delete removes the block for m from BOTH namespaces. Missing
// keys are not an error. Use DeleteDAG to remove only from /d/.
func (s *MemStore) Delete(m mid.MID) error {
	if s.db == nil {
		return errors.New("store: closed")
	}
	return s.db.Update(func(txn *badger.Txn) error {
		for _, key := range [][]byte{blockKey(m), dagKey(m)} {
			err := txn.Delete(key)
			if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
		}
		return nil
	})
}

// DeleteDAG removes the DAG node for m.
func (s *MemStore) DeleteDAG(m mid.MID) error {
	return s.deleteKey(dagKey(m))
}

func (s *MemStore) deleteKey(key []byte) error {
	if s.db == nil {
		return errors.New("store: closed")
	}
	return s.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(key)
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	})
}

// Close releases the underlying BadgerDB handle. After Close all
// other methods return errors.
func (s *MemStore) Close() error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

// Size returns the approximate on-disk size in bytes. The value
// is a sum of the LSM and value-log sizes, as reported by
// BadgerDB.
func (s *MemStore) Size() (uint64, error) {
	if s.db == nil {
		return 0, errors.New("store: closed")
	}
	lsm, vlog := s.db.Size()
	if lsm < 0 {
		lsm = 0
	}
	if vlog < 0 {
		vlog = 0
	}
	return uint64(lsm) + uint64(vlog), nil
}

// verifyContent checks that the data's SHA-256 digest matches the
// MID's digest.
func verifyContent(m mid.MID, data []byte) error {
	want, err := m.DigestBytes()
	if err != nil {
		return fmt.Errorf("store: claim MID has no digest: %w", err)
	}
	got := mid.FromBytes(data)
	gotDigest, err := got.DigestBytes()
	if err != nil {
		return fmt.Errorf("store: derive digest: %w", err)
	}
	if !bytesEqual(want, gotDigest) {
		return fmt.Errorf("store: data does not hash to claimed MID %s", m.String())
	}
	return nil
}

// bytesEqual is a defensive byte-slice compare.
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


// AllBlocks returns every MID that has a block or DAG record
// in the store. The result is the union of the /b/ and /d/
// namespaces, deduplicated. Order is implementation-defined.
func (s *MemStore) AllBlocks() ([]mid.MID, error) {
	if s.db == nil {
		return nil, errors.New("store: closed")
	}
	seen := make(map[string]struct{})
	var out []mid.MID
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for _, prefix := range [][]byte{[]byte(prefixBlock), []byte(prefixDAG)} {
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				raw := append([]byte(nil), it.Item().Key()...)
				raw = raw[len(prefix):]
				m, err := mid.FromMultihash(mid.CodecRaw, raw)
				if err != nil {
					continue
				}
				key := m.String()
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				out = append(out, m)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store: all blocks: %w", err)
	}
	return out, nil
}

// PutMeta stores an arbitrary key/value pair under the "/m/"
// namespace.
func (s *MemStore) PutMeta(key string, value []byte) error {
	if s.db == nil {
		return errors.New("store: closed")
	}
	if key == "" {
		return errors.New("store: empty meta key")
	}
	k := metaKey(key)
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(k, append([]byte(nil), value...))
	})
}

// GetMeta returns the value previously stored under key, or
// ErrNotFound if absent.
func (s *MemStore) GetMeta(key string) ([]byte, error) {
	if s.db == nil {
		return nil, errors.New("store: closed")
	}
	k := metaKey(key)
	var out []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		out = make([]byte, item.ValueSize())
		return item.Value(func(v []byte) error {
			copy(out, v)
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("store: get meta %q: %w", key, err)
	}
	return out, nil
}

var _ = binary.BigEndian
