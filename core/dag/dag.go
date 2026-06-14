// Package dag builds and resolves Merkle DAGs over content
// produced by core/chunk.
//
// A DAG has two kinds of nodes:
//
//   - Leaf nodes carry the raw bytes of a single chunk and have
//     no children.
//   - Internal nodes carry only MIDs of their children, serialized
//     as a DAGNode protobuf message.
//
// The MID of a leaf is the multihash of the raw chunk bytes (see
// core/mid). The MID of an internal node is the multihash of the
// canonical DAGNode form (protobuf-marshaled with the Mid field
// unset). The on-disk block is that same canonical form, so the
// Blockstore integrity check (which re-hashes the stored bytes)
// passes by construction.
//
// Build is bottom-up. A small input that produces N chunks is
// collapsed into ceil(N / fanout) internal nodes, then those
// internal nodes are themselves collapsed until a single root
// remains. A single-chunk input collapses to a single leaf whose
// MID is the root.
package dag

import (
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/chunk"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// Fanout is the maximum number of children an internal node may
// hold.
const Fanout = 174

// Node is the runtime representation of a DAG node. The Mid
// field is set to the node's MID after construction.
type Node struct {
	membusspb.DAGNode
}

// Links returns the MIDs of the children of this node.
func (n *Node) Links() []mid.MID {
	out := make([]mid.MID, 0, len(n.DAGNode.Links))
	for _, s := range n.DAGNode.Links {
		out = append(out, mid.MustParse(s))
	}
	return out
}

// IsLeaf reports whether this node holds inline data and no links.
func (n *Node) IsLeaf() bool {
	return len(n.DAGNode.Links) == 0
}

// MID returns the MID of this node.
func (n *Node) MID() mid.MID {
	if n == nil || n.DAGNode.Mid == "" {
		return mid.MID{}
	}
	return mid.MustParse(n.DAGNode.Mid)
}

// Builder constructs a Merkle DAG over a sequence of chunks and
// writes the resulting nodes into the supplied Blockstore.
type Builder struct {
	bs store.Blockstore
}

// NewBuilder returns a Builder that writes into bs.
func NewBuilder(bs store.Blockstore) *Builder {
	return &Builder{bs: bs}
}

// Build consumes all blocks from c, writes every block (leaf and
// internal) into the Blockstore, and returns the MID of the root.
//
// Build is streaming at the chunk level: it reads one chunk at a
// time, accumulates them, and emits internal nodes in fixed-size
// batches. Memory usage is O(fanout) per layer.
func (b *Builder) Build(c chunk.Chunker) (mid.MID, error) {
	if b.bs == nil {
		return mid.MID{}, errors.New("dag: nil blockstore")
	}
	if c == nil {
		return mid.MID{}, errors.New("dag: nil chunker")
	}

	leaves, err := b.collectLeaves(c)
	if err != nil {
		return mid.MID{}, err
	}
	if len(leaves) == 0 {
		return mid.MID{}, errors.New("dag: empty input")
	}
	if len(leaves) == 1 {
		return leaves[0], nil
	}

	current := leaves
	for len(current) > 1 {
		next, err := b.reduceLevel(current)
		if err != nil {
			return mid.MID{}, err
		}
		current = next
	}
	return current[0], nil
}

// collectLeaves drains c, writes each leaf to the blockstore, and
// returns the ordered list of leaf MIDs.
func (b *Builder) collectLeaves(c chunk.Chunker) ([]mid.MID, error) {
	var leaves []mid.MID
	for {
		blk, err := c.Next()
		if errors.Is(err, io.EOF) {
			return leaves, nil
		}
		if err != nil {
			return nil, fmt.Errorf("dag: read chunk: %w", err)
		}
		if blk.Size() == 0 {
			return nil, errors.New("dag: empty chunk")
		}
		leafMID := blk.MID()
		if leafMID.IsZero() {
			return nil, errors.New("dag: chunk has zero MID")
		}
		if err := b.bs.Put(store.Block{MID: leafMID, Data: blk.Data()}); err != nil {
			return nil, fmt.Errorf("dag: store leaf: %w", err)
		}
		leaves = append(leaves, leafMID)
	}
}

// reduceLevel turns a slice of MIDs into the MIDs of the parent
// layer by grouping children in batches of Fanout. The on-disk
// representation of each parent is the canonical DAGNode
// protobuf form (with the Mid field unset); the resolver
// recognises this form and re-attaches the MID at decode time.
func (b *Builder) reduceLevel(level []mid.MID) ([]mid.MID, error) {
	if len(level) == 0 {
		return nil, errors.New("dag: reduceLevel called on empty level")
	}
	parents := make([]mid.MID, 0, (len(level)+Fanout-1)/Fanout)
	for start := 0; start < len(level); start += Fanout {
		end := start + Fanout
		if end > len(level) {
			end = len(level)
		}
		childMIDs := level[start:end]
		links := make([]string, len(childMIDs))
		for i, c := range childMIDs {
			links[i] = c.String()
		}
		canonical := &membusspb.DAGNode{Links: links}
		raw, err := proto.Marshal(canonical)
		if err != nil {
			return nil, fmt.Errorf("dag: marshal node: %w", err)
		}
		nodeMID := mid.FromBytes(raw)
		if err := b.bs.Put(store.Block{MID: nodeMID, Data: raw}); err != nil {
			return nil, fmt.Errorf("dag: store internal: %w", err)
		}
		parents = append(parents, nodeMID)
	}
	return parents, nil
}
