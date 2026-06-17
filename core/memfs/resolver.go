package memfs

import (
	"context"
	"errors"
	"fmt"

	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
)

// Resolver fetches MemFS nodes from the local Blockstore. It
// does NOT do cross-node resolution on its own — callers that
// need to fetch from peers (e.g. the daemon's API / memgate
// adapter) layer DHT + Memex on top by pre-populating the
// store. The Resolver is intentionally a thin wrapper around
// the Blockstore so it stays unit-testable without a live
// network.
type Resolver struct {
	bs store.Blockstore
}

// NewResolver returns a Resolver that reads from bs.
func NewResolver(bs store.Blockstore) *Resolver {
	return &Resolver{bs: bs}
}

// Resolve returns the MemFSNode for m. It returns an error
// when the MID is not present in the blockstore; callers that
// need cross-node fetch should look it up via DHT and pull
// the bytes into the store first, then re-call Resolve.
func (r *Resolver) Resolve(ctx context.Context, m mid.MID) (*Node, error) {
	if r.bs == nil {
		return nil, errors.New("memfs: nil blockstore")
	}
	if m.IsZero() {
		return nil, errors.New("memfs: zero MID")
	}
	raw, err := r.bs.Get(m)
	if err != nil {
		return nil, fmt.Errorf("memfs: get %s: %w", m.String(), err)
	}
	return ParseNode(raw)
}

// ResolvePath walks root followed by a slash-separated path
// and returns the final node. Each segment looks up a
// DirEntry in the parent DIR node. If any segment is missing
// the error wraps store.ErrNotFound (when exposed by the
// store) so callers can distinguish "MID not found" from
// "path not found".
//
// An empty path returns the root node.
func (r *Resolver) ResolvePath(ctx context.Context, root mid.MID, path string) (*Node, error) {
	if path == "" {
		return r.Resolve(ctx, root)
	}
	current, err := r.Resolve(ctx, root)
	if err != nil {
		return nil, err
	}
	// Split on "/" and filter empty segments so "/a//b" is
	// equivalent to "a/b".
	segs := splitPath(path)
	for _, seg := range segs {
		if !current.IsDir() {
			return nil, fmt.Errorf("memfs: %q is not a directory", seg)
		}
		var found *DirEntry
		for _, e := range current.EntriesValue() {
			if e.Name == seg {
				ee := e
				found = &ee
				break
			}
		}
		if found == nil {
			return nil, fmt.Errorf("memfs: no such entry %q", seg)
		}
		current, err = r.Resolve(ctx, found.Mid)
		if err != nil {
			return nil, fmt.Errorf("memfs: resolve %q: %w", seg, err)
		}
	}
	return current, nil
}

// Open returns a ReadSeekCloser streaming the file content
// rooted at m. The node must be a FILE; any other type
// returns an error.
func (r *Resolver) Open(ctx context.Context, m mid.MID) (interface {
	Read(p []byte) (int, error)
	Seek(offset int64, whence int) (int64, error)
	Close() error
}, error) {
	node, err := r.Resolve(ctx, m)
	if err != nil {
		return nil, err
	}
	if !node.IsFile() {
		return nil, fmt.Errorf("memfs: %s is not a file", m.String())
	}
	return newReader(r.bs, node)
}

// splitPath is a small helper that splits a slash-separated
// path and drops empty segments. It is local so the package
// has no path-separator assumptions.
func splitPath(p string) []string {
	var out []string
	start := 0
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			if i > start {
				out = append(out, p[start:i])
			}
			start = i + 1
		}
	}
	if start < len(p) {
		out = append(out, p[start:])
	}
	return out
}
