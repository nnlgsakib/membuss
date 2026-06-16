package memfs

import (
	"errors"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// reader implements io.ReadSeekCloser over a MemFS FILE node.
// It supports nested intermediate FILE nodes by flattening
// them on first use: when the top-level FILE has fewer
// blocks than the file size suggests, each direct block is
// resolved; if it is itself a FILE node, its blocks are
// walked recursively until the raw leaves are reached.
type reader struct {
	bs      store.Blockstore
	file    *Node
	blocks  []mid.MID
	pos     int64
	cur     int
	buf     []byte
	bufUsed int
}

// newReader constructs a streaming reader for a FILE node.
// The caller is responsible for ensuring file is a FILE.
func newReader(bs store.Blockstore, file *Node) (*reader, error) {
	if file == nil {
		return nil, errors.New("memfs: nil file node")
	}
	flat, err := flattenFileBlocks(bs, file)
	if err != nil {
		return nil, err
	}
	return &reader{bs: bs, file: file, blocks: flat}, nil
}

// flattenFileBlocks walks a FILE node tree, returning the
// ordered list of raw-block MIDs. It is recursive but
// shallow in practice: a > 174*256 KiB = ~43 MiB file uses
// a two-level tree, so the recursion depth is at most 2.
func flattenFileBlocks(bs store.Blockstore, n *Node) ([]mid.MID, error) {
	if !n.IsFile() {
		return nil, fmt.Errorf("memfs: not a file (type=%v)", n.GetType())
	}
	var out []mid.MID
	for _, b := range n.BlocksValue() {
		// If the child is a raw block, the size is set;
		// its content lives under /b/ and we record its MID
		// as a leaf.
		if b.Size > 0 {
			out = append(out, b.Mid)
			continue
		}
		// Size==0 means we did not pre-compute it; we have
		// to resolve the child and check whether it is
		// itself a FILE node (intermediate) or a raw block.
		raw, err := bs.Get(b.Mid)
		if err != nil {
			return nil, fmt.Errorf("memfs: get child %s: %w", b.Mid.String(), err)
		}
		var child membusspb.MemFSNode
		if uerr := proto.Unmarshal(raw, &child); uerr == nil && child.GetType() == membusspb.MemFSType_FILE && len(child.GetBlocks()) > 0 {
			cn := NewNode(&child)
			inner, err := flattenFileBlocks(bs, cn)
			if err != nil {
				return nil, err
			}
			out = append(out, inner...)
			continue
		}
		// Otherwise it's a raw block.
		out = append(out, b.Mid)
	}
	return out, nil
}

// Read implements io.Reader. It fills p from the current
// buffered block, then advances and fetches the next block
// on demand.
func (r *reader) Read(p []byte) (int, error) {
	if r.pos >= int64(r.file.GetFileSize()) {
		return 0, io.EOF
	}
	if r.cur >= len(r.blocks) {
		return 0, io.EOF
	}
	if r.buf == nil {
		raw, err := r.bs.Get(r.blocks[r.cur])
		if err != nil {
			return 0, fmt.Errorf("memfs: read block %d: %w", r.cur, err)
		}
		r.buf = raw
		r.bufUsed = 0
	}
	n := copy(p, r.buf[r.bufUsed:])
	r.bufUsed += n
	r.pos += int64(n)
	if r.bufUsed >= len(r.buf) {
		// Exhausted this block; advance.
		r.cur++
		r.buf = nil
		r.bufUsed = 0
	}
	return n, nil
}

// Seek implements io.Seeker. whence=0 sets absolute, 1 is
// relative, 2 is from end.
func (r *reader) Seek(offset int64, whence int) (int64, error) {
	var target int64
	switch whence {
	case io.SeekStart:
		target = offset
	case io.SeekCurrent:
		target = r.pos + offset
	case io.SeekEnd:
		target = int64(r.file.GetFileSize()) + offset
	default:
		return 0, fmt.Errorf("memfs: invalid whence %d", whence)
	}
	if target < 0 {
		return 0, fmt.Errorf("memfs: negative seek %d", target)
	}
	if target == r.pos {
		return r.pos, nil
	}
	// Compute the block that contains `target`. Each block
	// is sized dynamically; we walk the chain, summing
	// block sizes, until we land in a block.
	var sum int64
	newCur := -1
	for i, bm := range r.blocks {
		// Resolve the size by reading the block; for
		// efficiency we could cache sizes, but the most
		// common case is a small file and Seek is rare.
		raw, err := r.bs.Get(bm)
		if err != nil {
			return 0, fmt.Errorf("memfs: get size for block %d: %w", i, err)
		}
		bs := int64(len(raw))
		if target < sum+bs {
			newCur = i
			break
		}
		sum += bs
	}
	if newCur < 0 {
		// Past the end — clamp to EOF.
		r.cur = len(r.blocks)
		r.buf = nil
		r.bufUsed = 0
		r.pos = int64(r.file.GetFileSize())
		return r.pos, nil
	}
	r.cur = newCur
	// Fetch and position within the new block.
	raw, err := r.bs.Get(r.blocks[r.cur])
	if err != nil {
		return 0, fmt.Errorf("memfs: refetch block %d: %w", r.cur, err)
	}
	r.buf = raw
	r.bufUsed = int(target - sum)
	r.pos = target
	return r.pos, nil
}

// Close implements io.Closer. The reader holds no resources
// beyond the in-memory block; Close is a no-op.
func (r *reader) Close() error {
	r.buf = nil
	r.bufUsed = 0
	return nil
}
