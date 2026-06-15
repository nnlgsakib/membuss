// Phase 14: one-time MID migration helper.
//
// The Phase 14 redesign switched the public MID string
// from "mem" + base58(multihash) to "mem" + base32lower(CIDv1).
// The on-disk BadgerDB keys, however, are derived from
// mid.MID.Bytes() - the raw multihash envelope - which is
// identical in both formats. So in this codebase there is
// nothing to rewrite at the byte level: every key already
// refers to the same multihash regardless of which public
// string we use to address it.
//
// MigrateToV1MIDs is therefore a *consistency check*: it
// walks every block and DAG key, parses the multihash, and
// reports the result. It returns the number of keys it
// inspected. On a healthy store the count is just the
// number of blocks; on a corrupted store the function
// surfaces the offending MID so the operator can decide
// what to do.
//
// The function is exposed so a daemon startup hook can
// call it once and log a single "all MIDs are v1" line
// (or a "found N legacy MIDs" warning, if a future key
// layout ever diverges).
package store

import (
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"

	"github.com/nnlgsakib/membuss/core/mid"
)

// MigrationResult summarizes a single MigrateToV1MIDs run.
type MigrationResult struct {
	// Inspected is the total number of block and DAG keys
	// visited during the scan.
	Inspected int
	// Rewritten is the number of keys that had to be
	// rewritten. In the current codebase this is always
	// 0 because the on-disk key layout is already
	// multihash-based and is format-agnostic.
	Rewritten int
	// Legacy is the number of keys whose underlying
	// multihash did not parse as a sha2-256 envelope.
	// Such keys are reported but not deleted.
	Legacy []string
}

// MigrateToV1MIDs walks every /b/ and /d/ key in db and
// verifies the on-disk multihash is a v1-compatible sha2-256
// envelope. It is a one-shot helper intended to be called
// on daemon startup when the operator is migrating from a
// pre-Phase-14 deployment.
//
// The function is safe to call repeatedly: it is idempotent
// because the on-disk format is already multihash-agnostic.
func MigrateToV1MIDs(s *MemStore) (*MigrationResult, error) {
	if s == nil || s.db == nil {
		return nil, errors.New("store: nil MemStore")
	}
	res := &MigrationResult{}
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for _, prefix := range []string{prefixBlock, prefixDAG} {
			p := []byte(prefix)
			for it.Seek(p); it.ValidForPrefix(p); it.Next() {
				raw := append([]byte(nil), it.Item().Key()...)
				if len(raw) <= len(p) {
					continue
				}
				rawMh := raw[len(p):]
				m, err := mid.FromMultihash(mid.CodecRaw, rawMh)
				if err != nil {
					res.Legacy = append(res.Legacy, fmt.Sprintf("%x", rawMh))
					continue
				}
				_ = m
				res.Inspected++
			}
		}
		return nil
	})
	if err != nil {
		return res, fmt.Errorf("store: migrate scan: %w", err)
	}
	return res, nil
}

// DetectLegacyMIDs is a convenience wrapper that returns
// the count of legacy keys. Callers that just want a
// boolean answer can compare the result to zero.
func DetectLegacyMIDs(s *MemStore) (int, error) {
	res, err := MigrateToV1MIDs(s)
	if err != nil {
		return 0, err
	}
	return len(res.Legacy), nil
}
