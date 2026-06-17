package memfs

import (
	"context"
	"fmt"

	"github.com/nnlgsakib/membuss/core/mid"
)

// WalkFunc is the callback invoked by Walk for every node
// visited. The path is the slash-separated path from the
// walk root (e.g. "src/main.go"). An error returned from
// the callback aborts the walk and is propagated back to
// the caller.
type WalkFunc func(path string, node *Node, err error) error

// Walk visits every node in the subtree rooted at root, in
// depth-first order. The root node itself is visited with
// path = ""; each child is visited with its basename; etc.
//
// If a node cannot be resolved (e.g. a child MID is missing
// from the local store), the callback is invoked with a
// non-nil err and Walk continues with the next sibling. The
// caller can return an error from the callback to abort the
// walk.
func (r *Resolver) Walk(ctx context.Context, root mid.MID, fn WalkFunc) error {
	if fn == nil {
		return fmt.Errorf("memfs: nil walk func")
	}
	return r.walk(ctx, root, "", fn)
}

func (r *Resolver) walk(ctx context.Context, m mid.MID, path string, fn WalkFunc) error {
	node, err := r.Resolve(ctx, m)
	if err != nil {
		return fn(path, nil, err)
	}
	if err := fn(path, node, nil); err != nil {
		return err
	}
	if !node.IsDir() {
		return nil
	}
	for _, e := range node.EntriesValue() {
		child := e.Name
		if path == "" {
			if err := r.walk(ctx, e.Mid, child, fn); err != nil {
				return err
			}
		} else {
			if err := r.walk(ctx, e.Mid, path+"/"+child, fn); err != nil {
				return err
			}
		}
	}
	return nil
}
