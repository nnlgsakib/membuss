package memfs

import (
	"context"
	"fmt"
	"io/fs"
	"time"

	"github.com/nnlgsakib/membuss/core/mid"
)

// Stat is the user-facing metadata for a MemFS node, used
// by the API and the gateway when rendering /api/v1/stat and
// the explorer's per-MID view. It is intentionally a value
// type so it can be JSON-serialized without leaking proto
// types into the API.
type Stat struct {
	MID        mid.MID
	Type       MemFSType
	Size       uint64
	BlockCount int
	Mode       fs.FileMode
	MTime      time.Time
	MimeType   string
	Entries    []DirEntry
	Target     string // symlink only
}

// Stat returns the metadata for the node at m. It is a thin
// wrapper around Resolver.Resolve plus the value-type
// extractors on Node.
func (r *Resolver) Stat(ctx context.Context, m mid.MID) (Stat, error) {
	node, err := r.Resolve(ctx, m)
	if err != nil {
		return Stat{}, err
	}
	st := Stat{
		MID:        m,
		Type:       node.GetType(),
		Size:       node.TotalSize(),
		BlockCount: node.BlockCount(),
		Mode:       node.Mode(),
		MTime:      node.MTime(),
		MimeType:   node.MimeType(),
		Target:     node.SymlinkTarget(),
	}
	if node.IsDir() {
		st.Entries = node.EntriesSorted()
	}
	return st, nil
}

// MustStat is the panicking form of Stat.
func (r *Resolver) MustStat(ctx context.Context, m mid.MID) Stat {
	st, err := r.Stat(ctx, m)
	if err != nil {
		panic(fmt.Errorf("memfs: stat %s: %w", m.String(), err))
	}
	return st
}
