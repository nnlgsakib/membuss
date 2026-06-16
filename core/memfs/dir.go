package memfs

import "github.com/nnlgsakib/membuss/core/mid"

// NewDirEntry constructs a DirEntry. The Mid argument is the
// child MID (not the public mem-prefixed string). The Builder
// is responsible for filling it in; callers that build a DIR
// node by hand can use this constructor for clarity.
func NewDirEntry(name string, child mid.MID, typ MemFSType, size uint64) DirEntry {
	return DirEntry{
		Name: name,
		Mid:  child,
		Type: typ,
		Size: size,
	}
}

// EntriesFromPairs is a small convenience used by the
// Builder when callers know the (name, mid, type, size) of
// every child up front. Entries are copied so the caller can
// reuse the underlying slice.
func EntriesFromPairs(pairs []DirEntryPair) []DirEntry {
	out := make([]DirEntry, len(pairs))
	for i, p := range pairs {
		out[i] = NewDirEntry(p.Name, p.MID, p.Type, p.Size)
	}
	return out
}

// DirEntryPair is the (name, mid, type, size) tuple accepted
// by EntriesFromPairs. It exists purely so callers can avoid
// importing the generated proto package.
type DirEntryPair struct {
	Name string
	MID  mid.MID
	Type MemFSType
	Size uint64
}
