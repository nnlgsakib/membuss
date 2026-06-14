package store

import "github.com/nnlgsakib/membuss/core/mid"

// Key-prefix bytes. The leading '/' mirrors FUSE-style namespace
// separators and keeps the four categories visually distinct in
// BadgerDB tooling output.
const (
	prefixBlock = "/b/"
	prefixDAG   = "/d/"
	prefixSeal  = "/s/"
	prefixMeta  = "/m/"
)

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
