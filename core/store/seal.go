package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/nnlgsakib/membuss/core/mid"
)

// Seal marks root as a pinned root. If recursive is true, Seal
// also walks the DAG rooted at root to confirm the DAG is
// internally consistent.
//
// A Seal is a *forward-looking pin*: the seal record is written
// even if root (or some of its descendants) is not yet in the
// local store. This matches the "Sealing a MID that is not yet
// stored is allowed" contract on the Store interface. The walk
// is a best-effort validation that surfaces obvious errors
// (corrupt DAG, malformed link) but does NOT block the seal on
// missing blocks. Use Stat to confirm a sealed root is fully
// local.
//
// Walk errors are wrapped in ErrSealWalkIncomplete so callers
// that DO want to surface the warning (e.g. for logging or
// metrics) can detect them; a plain errors.Is check will see
// them and a simple `return err` will still return the warning
// to RPC callers. Most callers should treat the warning as
// informational.
func (s *MemStore) Seal(root mid.MID, recursive bool) error {
	if s.db == nil {
		return errors.New("store: closed")
	}
	if root.IsZero() {
		return errors.New("store: zero MID")
	}

	if err := s.writeSeal(root, false); err != nil {
		return err
	}

	if !recursive {
		return nil
	}
	werr := Walk(s, root, func(m mid.MID, _ bool) error {
		if m.Equal(root) {
			return nil
		}
		if m.Codec() == mid.CodecMemFS {
			_ = s.writeSeal(m, true)
		}
		return nil
	})
	if werr != nil {
		// A missing block is a soft warning (forward-looking
		// pin). Other walk errors (parse failures, truncated
		// links) are still surfaced so the operator can spot
		// a corrupt DAG.
		if errors.Is(werr, ErrNotFound) {
			return fmt.Errorf("%w: %v", ErrSealWalkIncomplete, werr)
		}
		return fmt.Errorf("store: seal walk: %w", werr)
	}
	return nil
}

// ErrSealWalkIncomplete signals that a Seal succeeded (the
// pin record is on disk) but the recursive DAG walk did not
// reach every reachable block. This is expected when the
// operator pins a MID they have not fetched yet; the missing
// blocks will be filled in by a later memex fetch or add.
var ErrSealWalkIncomplete = errors.New("store: seal walk incomplete")

// Unseal removes the seal record for the given MID and recursively
// unseals any child MemFS nodes.
func (s *MemStore) Unseal(root mid.MID) error {
	if s.db == nil {
		return errors.New("store: closed")
	}
	if root.IsZero() {
		return errors.New("store: zero MID")
	}
	err := s.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(sealKey(root))
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	_ = Walk(s, root, func(m mid.MID, _ bool) error {
		if m.Codec() == mid.CodecMemFS {
			_ = s.db.Update(func(txn *badger.Txn) error {
				_ = txn.Delete(sealKey(m))
				return nil
			})
		}
		return nil
	})
	return nil
}

// IsSealed reports whether a direct seal record exists for m.
func (s *MemStore) IsSealed(m mid.MID) (bool, error) {
	if s.db == nil {
		return false, errors.New("store: closed")
	}
	if m.IsZero() {
		return false, errors.New("store: zero MID")
	}
	var found bool
	err := s.db.View(func(txn *badger.Txn) error {
		_, gerr := txn.Get(sealKey(m))
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

// AllSealed returns every MID with a direct seal record. Order
// is unspecified.
func (s *MemStore) AllSealed() ([]mid.MID, error) {
	if s.db == nil {
		return nil, errors.New("store: closed")
	}
	var out []mid.MID
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(prefixSeal)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			raw := append([]byte(nil), it.Item().Key()...)
			raw = raw[len(prefix):]
			
			codec := mid.CodecRaw
			isChild := false
			item := it.Item()
			if item.ValueSize() == 8 {
				var val [8]byte
				err := item.Value(func(v []byte) error {
					copy(val[:], v)
					return nil
				})
				if err == nil {
					v := binary.BigEndian.Uint64(val[:])
					isChild = (v & (1 << 63)) != 0
					codec = v &^ (1 << 63)
				}
			}
			
			if isChild {
				continue
			}
			
			m, err := mid.FromMultihash(codec, raw)
			if err != nil {
				continue
			}
			out = append(out, m)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store: all sealed: %w", err)
	}
	return out, nil
}

// GC walks every sealed root, collects the reachable MID set,
// and deletes every key in the store that is NOT in that set
// AND is older than the minimum age. It returns the number of
// bytes freed.
//
// Implementation note: a single BadgerDB Update transaction is
// NOT used to iterate-and-delete, because BadgerDB iterators
// hold a read snapshot at creation time and concurrent deletes
// inside the same transaction are not reflected in the
// iteration. Instead we do:
//
//  1. A View pass to enumerate every key + its size and decide
//     which to delete.
//  2. An Update pass to delete the collected keys.
//
// BadgerDB's value-log GC is invoked at most once per call.
func (s *MemStore) GC(ctx context.Context) (uint64, error) {
	return s.GCWithMinAge(ctx, 0)
}

// GCWithMinAge is like GC but only deletes blocks whose
// BadgerDB commit timestamp is older than minAge. This
// prevents recently-fetched content from being immediately
// garbage-collected. Pass 0 for minAge to disable the age
// check (original GC behavior).
func (s *MemStore) GCWithMinAge(ctx context.Context, minAge time.Duration) (uint64, error) {
	if s.db == nil {
		return 0, errors.New("store: closed")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Phase 1: build the reachable set (indexed by raw multihash bytes for codec-agnostic checks).
	reachable := make(map[string]struct{})
	roots, err := s.AllSealed()
	if err != nil {
		return 0, err
	}
	for _, root := range roots {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if root.IsZero() {
			continue
		}
		reachable[string(root.Bytes())] = struct{}{}
		werr := Walk(s, root, func(m mid.MID, _ bool) error {
			reachable[string(m.Bytes())] = struct{}{}
			return nil
		})
		_ = werr
	}

	// Phase 2: enumerate every key in the store and collect the
	// ones that are NOT reachable and older than minAge.
	type pendingDelete struct {
		key   []byte
		bytes uint64
	}
	var toDelete []pendingDelete
	var minAgeTs uint64
	if minAge > 0 {
		minAgeTs = uint64(time.Now().Add(-minAge).Unix())
	}
	err = s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			item := it.Item()
			k := item.Key()
			ks := string(k)
			// Skip meta records (/m/ prefix) - they are
			// not part of the content-addressed data
			// set and must survive GC.
			if len(ks) > len(prefixMeta) && ks[:len(prefixMeta)] == prefixMeta {
				continue
			}
			// Skip seal records (/s/ prefix) - the
			// operator-visible pin state must survive
			// GC even when the underlying blocks are
			// gone (think: a forward-looking seal for
			// content that has not been fetched yet).
			if len(ks) > len(prefixSeal) && ks[:len(prefixSeal)] == prefixSeal {
				continue
			}
			// Only consider /b/ and /d/ keys; those
			// are the actual content-addressed data.
			isBlock := len(ks) > len(prefixBlock) && ks[:len(prefixBlock)] == prefixBlock
			isDAG := len(ks) > len(prefixDAG) && ks[:len(prefixDAG)] == prefixDAG
			if !isBlock && !isDAG {
				continue
			}
			raw := k
			if isBlock {
				raw = k[len(prefixBlock):]
			} else {
				raw = k[len(prefixDAG):]
			}
			if _, ok := reachable[string(raw)]; ok {
				continue
			}
			if minAgeTs > 0 {
				if uint64(item.Version()) >= minAgeTs {
					continue
				}
			}
			toDelete = append(toDelete, pendingDelete{
				key:   append([]byte(nil), k...),
				bytes: uint64(item.KeySize()+item.ValueSize()),
			})
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("store: gc enumerate: %w", err)
	}

	// Phase 3: delete the collected keys. Each key gets its
	// own short transaction so a single corrupt entry does
	// not abort the whole sweep.
	var freed uint64
	for _, pd := range toDelete {
		if err := s.deleteKey(pd.key); err != nil {
			return freed, fmt.Errorf("store: gc delete: %w", err)
		}
		freed += pd.bytes
	}

	// Phase 4: trigger BadgerDB's value-log GC once.
	if freed > 0 {
		go func() { _ = s.db.RunValueLogGC(0.5) }()
	}

	return freed, nil
}

// writeSeal writes a single seal record.
func (s *MemStore) writeSeal(m mid.MID, child bool) error {
	return s.db.Update(func(txn *badger.Txn) error {
		val := make([]byte, 8)
		codecVal := m.Codec()
		if child {
			codecVal |= 1 << 63
		}
		binary.BigEndian.PutUint64(val, codecVal)
		return txn.Set(sealKey(m), val)
	})
}

// ksToMID converts a BadgerDB key string back to the public MID
// form for lookup in the reachable set.
func ksToMID(ks string) string {
	switch {
	case len(ks) > len(prefixBlock) && ks[:len(prefixBlock)] == prefixBlock:
		raw := ks[len(prefixBlock):]
		m, err := mid.FromMultihash(mid.CodecRaw, []byte(raw))
		if err != nil {
			return ks
		}
		return m.String()
	case len(ks) > len(prefixDAG) && ks[:len(prefixDAG)] == prefixDAG:
		raw := ks[len(prefixDAG):]
		m, err := mid.FromMultihash(mid.CodecRaw, []byte(raw))
		if err != nil {
			return ks
		}
		return m.String()
	}
	return ks
}
