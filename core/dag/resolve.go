package dag

import (
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// Resolver walks a Merkle DAG stored in a Blockstore and reassembles
// the original content into a sequential io.Reader.
type Resolver struct {
	bs store.Blockstore
}

// NewResolver returns a Resolver that reads from bs.
func NewResolver(bs store.Blockstore) *Resolver {
	return &Resolver{bs: bs}
}

// Resolve returns an io.Reader that yields the bytes of the DAG
// rooted at root, in the order the chunker originally produced
// them.
//
// If root is a leaf, the returned reader yields the leaf's raw
// bytes directly. Otherwise the resolver walks the DAG depth-first
// and concatenates leaves in the order they appear in each
// internal node's Links.
//
// The optional visit hook, if non-nil, is called once for every
// internal node visited. It is provided for instrumentation and
// MUST NOT mutate the Blockstore.
func (r *Resolver) Resolve(root mid.MID, visit func(mid.MID) error) (io.Reader, error) {
	if r.bs == nil {
		return nil, errors.New("dag: nil blockstore")
	}
	if root.IsZero() {
		return nil, errors.New("dag: zero root MID")
	}

	pipeReader, pipeWriter := io.Pipe()
	go r.stream(root, visit, pipeWriter)
	return pipeReader, nil
}

// stream walks the DAG and pushes reassembled bytes into w. Any
// error is delivered to the reader via w.CloseWithError.
//
// Cycle detection is intentionally NOT performed. DAGs are
// immutable and content-addressed, so a cycle can only be
// produced by a malicious or buggy Blockstore, and a defensive
// check would also raise false positives for legitimate content
// that references the same byte sequence at multiple points.
func (r *Resolver) stream(root mid.MID, visit func(mid.MID) error, w *io.PipeWriter) {
	defer w.Close()

	var walk func(m mid.MID) error
	walk = func(m mid.MID) error {
		data, err := r.bs.Get(m)
		if err != nil {
			// Block may not be available yet (streaming
			// assembly). Retry with backoff up to 5s.
			for attempt := 0; attempt < 50; attempt++ {
				time.Sleep(100 * time.Millisecond)
				data, err = r.bs.Get(m)
				if err == nil {
					break
				}
			}
			if err != nil {
				return fmt.Errorf("dag: get %s: %w", m.String(), err)
			}
		}

		// A block is an internal node iff its bytes unmarshal as
		// a DAGNode AND the decoded form has at least one link.
		var node membusspb.DAGNode
		if uerr := proto.Unmarshal(data, &node); uerr == nil && len(node.Links) > 0 {
			if visit != nil {
				if err := visit(m); err != nil {
					return err
				}
			}
			for _, s := range node.Links {
				child, err := mid.Parse(s)
				if err != nil {
					return fmt.Errorf("dag: parse link %q: %w", s, err)
				}
				if err := walk(child); err != nil {
					return err
				}
			}
			return nil
		}

		// Leaf: emit its raw bytes.
		if _, err := w.Write(data); err != nil {
			return err
		}
		return nil
	}

	if err := walk(root); err != nil {
		_ = w.CloseWithError(err)
	}
}
