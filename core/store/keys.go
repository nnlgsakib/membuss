package store

import (
	"encoding/binary"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/nnlgsakib/membuss/core/mid"
)

// Key-prefix bytes. The leading '/' mirrors FUSE-style namespace
// separators and keeps the four categories visually distinct in
// BadgerDB tooling output.
const (
	prefixBlock = "/b/"
	prefixDAG   = "/d/"
	prefixSeal  = "/s/"
	prefixMeta  = "/m/"
)

// timestampKey returns the BadgerDB meta key for a creation
// timestamp. Stored in the /m/ namespace so GC already skips it.
func timestampKey(m mid.MID) []byte {
	return metaKey("ts/" + m.String())
}

// putTimestamp writes a wall-clock creation timestamp atomically.
func putTimestamp(txn *badger.Txn, m mid.MID) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(time.Now().Unix()))
	return txn.Set(timestampKey(m), buf[:])
}

// readTimestamp returns the stored creation timestamp for a key.
// Returns 0, nil if no timestamp is recorded (pre-existing block).
func readTimestamp(txn *badger.Txn, m mid.MID) (uint64, error) {
	item, err := txn.Get(timestampKey(m))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return 0, nil
		}
		return 0, err
	}
	var buf [8]byte
	err = item.Value(func(v []byte) error {
		copy(buf[:], v)
		return nil
	})
	return binary.BigEndian.Uint64(buf[:]), err
}

// blockKey returns the BadgerDB key for a raw block.
func blockKey(m mid.MID) []byte {
	return append([]byte(prefixBlock), m.Bytes()...)
}

// dagKey returns the BadgerDB key for a DAG node.
func dagKey(m mid.MID) []byte {
	return append([]byte(prefixDAG), m.Bytes()...)
}

// sealKey returns the BadgerDB key for a seal record.
func sealKey(m mid.MID) []byte {
	return append([]byte(prefixSeal), m.Bytes()...)
}

// metaKey returns the BadgerDB key for a metadata entry.
func metaKey(k string) []byte {
	return append([]byte(prefixMeta), []byte(k)...)
}

// rawKey returns the raw multihash envelope of m, used for
// callers that need to construct a key by hand.
func rawKey(m mid.MID) []byte { return m.Bytes() }
