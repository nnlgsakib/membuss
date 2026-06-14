package store

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/mid"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// Walk visits every MID reachable from root in depth-first order
// (root first, then children in link order). For each visited
// node the visit callback is invoked with the MID and a flag
// indicating whether the node is a leaf (true) or an internal
// node (false). If visit returns an error, Walk stops and returns
// the same error.
//
// Walk is the building block for Seal/GC: callers accumulate the
// visited MIDs into a set and then operate on the set.
//
// Walk lives in the store package (not core/dag) to avoid an
// import cycle, since the store depends on a DAG-walking helper
// for GC. The walker itself only depends on Blockstore, which is
// the minimum interface any DAG-aware code needs.
func Walk(bs Blockstore, root mid.MID, visit func(m mid.MID, leaf bool) error) error {
	if bs == nil {
		return errors.New("store: nil blockstore")
	}
	if root.IsZero() {
		return errors.New("store: zero root MID")
	}

	var walk func(m mid.MID) error
	walk = func(m mid.MID) error {
		data, err := bs.Get(m)
		if err != nil {
			return fmt.Errorf("store: walk get %s: %w", m.String(), err)
		}

		var node membusspb.DAGNode
		if uerr := proto.Unmarshal(data, &node); uerr == nil && len(node.Links) > 0 {
			if err := visit(m, false); err != nil {
				return err
			}
			for _, s := range node.Links {
				child, err := mid.Parse(s)
				if err != nil {
					return fmt.Errorf("store: walk parse link %q: %w", s, err)
				}
				if err := walk(child); err != nil {
					return err
				}
			}
			return nil
		}
		return visit(m, true)
	}

	return walk(root)
}
