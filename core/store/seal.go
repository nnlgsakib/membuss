package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"

	"github.com/nnlgsakib/membuss/core/mid"
)

// Seal marks root as a pinned root. If recursive is true, Seal
// also walks the DAG rooted at root to confirm the DAG is
// internally consistent.
func (s *MemStore) Seal(root mid.MID, recursive bool) error {
	if s.db == nil {
		return errors.New("store: closed")
	}
	if root.IsZero() {
		return errors.New("store: zero MID")
	}

	if err := s.writeSeal(root); err != nil {
		return err
	}

	if !recursive {
		return nil
	}
	werr := Walk(s, root, func(_ mid.MID, _ bool) error { return nil })
	if werr != nil {
		return fmt.Errorf("store: seal walk: %w", werr)
	}
	return nil
}

// Unseal removes the seal record for the given MID.
func (s *MemStore) Unseal(root mid.MID) error {
	if s.db == nil {
		return errors.New("store: closed")
	}
	if root.IsZero() {
		return errors.New("store: zero MID")
	}
	return s.db.Update(func(txn *badger.Txn) error {
		err := txn.Delete(sealKey(root))
		if err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		return nil
	})
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
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte(prefixSeal)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			raw := append([]byte(nil), it.Item().Key()...)
			raw = raw[len(prefix):]
			m, err := mid.FromMultihash(mid.CodecRaw, raw)
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
// and deletes every key in the store that is NOT in that set.
// It returns the number of bytes freed.
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
	if s.db == nil {
		return 0, errors.New("store: closed")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Phase 1: build the reachable set.
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
		reachable[root.String()] = struct{}{}
		werr := Walk(s, root, func(m mid.MID, _ bool) error {
			reachable[m.String()] = struct{}{}
			return nil
		})
		_ = werr
	}

	// Phase 2: enumerate every key in the store and collect the
	// ones that are NOT reachable.
	type pendingDelete struct {
		key   []byte
		bytes uint64
	}
	var toDelete []pendingDelete
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
			if _, ok := reachable[ksToMID(ks)]; ok {
				continue
			}
			toDelete = append(toDelete, pendingDelete{
				key:   append([]byte(nil), k...),
				bytes: uint64(len(k)) + uint64(item.ValueSize()),
			})
		}
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("store: gc enumerate: %w", err)
	}

	// Phase 3: delete the collected keys in a single Update.
	if len(toDelete) == 0 {
		return 0, nil
	}
	var freed uint64
	err = s.db.Update(func(txn *badger.Txn) error {
		for _, d := range toDelete {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := txn.Delete(d.key); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			freed += d.bytes
		}
		return nil
	})
	if err != nil {
		return freed, fmt.Errorf("store: gc delete: %w", err)
	}

	// Phase 4: trigger BadgerDB's value-log GC once.
	go func() { _ = s.db.RunValueLogGC(0.5) }()

	return freed, nil
}

// writeSeal writes a single seal record.
func (s *MemStore) writeSeal(m mid.MID) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(sealKey(m), nil)
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
	case len(ks) > len(prefixSeal) && ks[:len(prefixSeal)] == prefixSeal:
		raw := ks[len(prefixSeal):]
		m, err := mid.FromMultihash(mid.CodecRaw, []byte(raw))
		if err != nil {
			return ks
		}
		return m.String()
	case len(ks) > len(prefixMeta) && ks[:len(prefixMeta)] == prefixMeta:
		return ks
	default:
		return ks
	}
}
