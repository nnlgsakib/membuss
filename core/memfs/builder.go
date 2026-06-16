package memfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/chunk"
	"github.com/nnlgsakib/membuss/core/dag"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// DefaultBlockSize is the default chunk size for new Builders.
// It matches core/chunk.DefaultBlockSize (256 KiB).
const DefaultBlockSize = chunk.DefaultBlockSize

// Builder constructs MemFS trees and writes them into a
// Blockstore. It reuses the existing chunker for raw blocks
// and the existing Blockstore Put path for everything else,
// so the dedup, walk, seal and GC machinery all just work.
type Builder struct {
	bs  store.Blockstore
	blk int
}

// NewBuilder returns a Builder that writes into bs. The
// default block size is DefaultBlockSize.
func NewBuilder(bs store.Blockstore) *Builder {
	return &Builder{bs: bs, blk: DefaultBlockSize}
}

// WithBlockSize returns a copy of b with a different chunk
// size. Values outside [chunk.MinBlockSize, chunk.MaxBlockSize]
// are clamped to the nearest bound.
func (b *Builder) WithBlockSize(n int) *Builder {
	if n < chunk.MinBlockSize {
		n = chunk.MinBlockSize
	}
	if n > chunk.MaxBlockSize {
		n = chunk.MaxBlockSize
	}
	cp := *b
	cp.blk = n
	return &cp
}

// AddResult is what AddFile / AddDir return on success.
type AddResult struct {
	MID   mid.MID
	Size  uint64
	Block uint64
}

// blockRef is the internal (MID, size) pair produced by the
// chunker pass before it gets folded into a MemFS FILE node.
type blockRef struct {
	mid  mid.MID
	size uint64
}

// AddFile ingests a file from r, chunks it, stores every raw
// block in the Blockstore, and assembles a MemFS FILE node
// that references those blocks in order. The result is the
// root MID of the file.
//
//   - 1 block (the entire file fits in one chunk): the
//     MemFS FILE node carries the bytes inline in its data
//     field. The raw block is still stored under /b/ for
//     network-level fetch.
//
//   - ≤ fanout blocks (≤ 174 with the default 256 KiB
//     chunker, i.e. ≤ ~43 MiB): one MemFS FILE node with
//     a list of raw-block references.
//
//   - > fanout blocks: a balanced two-level tree of MemFS
//     FILE nodes, exactly like the dag.Builder's reduceLevel.
//
// AddFile also writes the FILE node itself to the Blockstore
// before returning, so callers can Seal the result immediately
// and peers can fetch it.
func (b *Builder) AddFile(name string, r io.Reader, mode fs.FileMode, mtime time.Time, mime string) (AddResult, error) {
	if b.bs == nil {
		return AddResult{}, errors.New("memfs: nil blockstore")
	}
	if r == nil {
		return AddResult{}, errors.New("memfs: nil reader")
	}

	chunker, err := chunk.NewFixed(b.blk)(r)
	if err != nil {
		return AddResult{}, fmt.Errorf("memfs: chunker: %w", err)
	}

	var leaves []blockRef
	var totalSize uint64
	for {
		blk, err := chunker.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return AddResult{}, fmt.Errorf("memfs: read chunk: %w", err)
		}
		lm := blk.MID()
		if lm.IsZero() {
			return AddResult{}, errors.New("memfs: chunk has zero MID")
		}
		if err := b.bs.Put(lm, blk.Data()); err != nil {
			return AddResult{}, fmt.Errorf("memfs: store raw block: %w", err)
		}
		leaves = append(leaves, blockRef{mid: lm, size: uint64(blk.Size())})
		totalSize += uint64(blk.Size())
	}

	if len(leaves) == 0 {
		return AddResult{}, errors.New("memfs: empty input")
	}

	// Build the FILE node envelope.
	pb := &membusspb.MemFSNode{
		Type:     membusspb.MemFSType_FILE,
		FileSize: totalSize,
		Mode:     uint32(mode),
	}
	if mtime.UnixNano() > 0 {
		pb.Mtime = mtime.UnixNano()
	}
	if mime != "" {
		pb.Meta = &membusspb.MemFSMeta{MimeType: mime}
	}

	if len(leaves) == 1 && int(leaves[0].size) <= b.blk {
		// Single block — inline the data in the FILE node.
		raw, err := b.bs.Get(leaves[0].mid)
		if err != nil {
			return AddResult{}, fmt.Errorf("memfs: read single chunk: %w", err)
		}
		pb.Data = raw
		pb.Blocks = []*membusspb.MemFSBlock{
			{Mid: leaves[0].mid.Bytes(), Size: leaves[0].size},
		}
	} else if len(leaves) <= dag.Fanout {
		pb.Blocks = make([]*membusspb.MemFSBlock, len(leaves))
		for i, l := range leaves {
			pb.Blocks[i] = &membusspb.MemFSBlock{Mid: l.mid.Bytes(), Size: l.size}
		}
	} else {
		// Build a balanced two-level tree of MemFS FILE
		// nodes. Each intermediate groups up to dag.Fanout
		// raw blocks.
		intermediates := make([]mid.MID, 0, (len(leaves)+dag.Fanout-1)/dag.Fanout)
		for start := 0; start < len(leaves); start += dag.Fanout {
			end := start + dag.Fanout
			if end > len(leaves) {
				end = len(leaves)
			}
			group := leaves[start:end]
			child := &membusspb.MemFSNode{
				Type:     membusspb.MemFSType_FILE,
				FileSize: sumBlockSizes(group),
			}
			child.Blocks = make([]*membusspb.MemFSBlock, len(group))
			for i, l := range group {
				child.Blocks[i] = &membusspb.MemFSBlock{Mid: l.mid.Bytes(), Size: l.size}
			}
			raw, err := proto.Marshal(child)
			if err != nil {
				return AddResult{}, fmt.Errorf("memfs: marshal intermediate: %w", err)
			}
			im := mid.FromBytesWithCodec(raw, mid.CodecMemFS)
			if err := b.bs.Put(im, raw); err != nil {
				return AddResult{}, fmt.Errorf("memfs: store intermediate: %w", err)
			}
			intermediates = append(intermediates, im)
		}
		rootBlocks := make([]*membusspb.MemFSBlock, len(intermediates))
		for i, im := range intermediates {
			rootBlocks[i] = &membusspb.MemFSBlock{Mid: im.Bytes()}
		}
		pb.Blocks = rootBlocks
	}

	raw, err := proto.Marshal(pb)
	if err != nil {
		return AddResult{}, fmt.Errorf("memfs: marshal file node: %w", err)
	}
	rootMID := mid.FromBytesWithCodec(raw, mid.CodecMemFS)
	if err := b.bs.Put(rootMID, raw); err != nil {
		return AddResult{}, fmt.Errorf("memfs: store file node: %w", err)
	}
	return AddResult{
		MID:   rootMID,
		Size:  totalSize,
		Block: uint64(len(leaves)),
	}, nil
}

// AddDir assembles a DIR node from a pre-built list of
// entries, stores it in the Blockstore, and returns its MID
// plus the cumulative size (sum of entry sizes). The entries
// slice is sorted lexicographically by name before being
// serialized, so callers can pass them in any order.
func (b *Builder) AddDir(name string, entries []DirEntry, mode fs.FileMode, mtime time.Time) (AddResult, error) {
	if b.bs == nil {
		return AddResult{}, errors.New("memfs: nil blockstore")
	}
	if entries == nil {
		entries = []DirEntry{}
	}
	// Defensive copy + sort so two callers producing the
	// same logical directory always get the same MID.
	sorted := make([]DirEntry, len(entries))
	copy(sorted, entries)
	sortDirEntries(sorted)

	pb := &membusspb.MemFSNode{
		Type: membusspb.MemFSType_DIR,
		Mode: uint32(mode),
	}
	if mtime.UnixNano() > 0 {
		pb.Mtime = mtime.UnixNano()
	}
	pb.Entries = make([]*membusspb.DirEntry, len(sorted))
	for i, e := range sorted {
		pb.Entries[i] = &membusspb.DirEntry{
			Name: e.Name,
			Mid:  e.Mid.Bytes(),
			Type: e.Type,
			Size: e.Size,
		}
	}

	var total uint64
	for _, e := range sorted {
		total += e.Size
	}

	raw, err := proto.Marshal(pb)
	if err != nil {
		return AddResult{}, fmt.Errorf("memfs: marshal dir: %w", err)
	}
	rootMID := mid.FromBytesWithCodec(raw, mid.CodecMemFS)
	if err := b.bs.Put(rootMID, raw); err != nil {
		return AddResult{}, fmt.Errorf("memfs: store dir: %w", err)
	}
	return AddResult{
		MID:   rootMID,
		Size:  total,
		Block: 1,
	}, nil
}

// AddSymlink stores a SYMLINK node pointing at target and
// returns its MID.
func (b *Builder) AddSymlink(name, target string, mode fs.FileMode, mtime time.Time) (AddResult, error) {
	if b.bs == nil {
		return AddResult{}, errors.New("memfs: nil blockstore")
	}
	pb := &membusspb.MemFSNode{
		Type:          membusspb.MemFSType_SYMLINK,
		SymlinkTarget: target,
		Mode:          uint32(mode),
	}
	if mtime.UnixNano() > 0 {
		pb.Mtime = mtime.UnixNano()
	}
	raw, err := proto.Marshal(pb)
	if err != nil {
		return AddResult{}, fmt.Errorf("memfs: marshal symlink: %w", err)
	}
	rootMID := mid.FromBytesWithCodec(raw, mid.CodecMemFS)
	if err := b.bs.Put(rootMID, raw); err != nil {
		return AddResult{}, fmt.Errorf("memfs: store symlink: %w", err)
	}
	return AddResult{MID: rootMID, Size: uint64(len(target)), Block: 1}, nil
}

// AddDirectoryFromFS walks fsys starting at root and stores
// the entire subtree. The returned MID is the root DIR.
//
// The walk is a single bottom-up pass: leaves are added
// first, then each directory is added with references to
// its already-stored children. This keeps memory bounded to
// the size of one directory's entries, not the whole tree.
func (b *Builder) AddDirectoryFromFS(fsys fs.FS, root string) (AddResult, error) {
	if b.bs == nil {
		return AddResult{}, errors.New("memfs: nil blockstore")
	}
	if fsys == nil {
		return AddResult{}, errors.New("memfs: nil fs")
	}
	if root == "" {
		root = "."
	}

	// Collect entries as we walk. The post-order walk
	// visits children before their parent, so we can build
	// directories by appending to a parent bucket as soon
	// as each child finishes.
	type pending struct {
		relPath string
		isDir   bool
		entry   AddResult
	}
	var stack []pending

	err := fs.WalkDir(fsys, root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepathRel(root, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		mt := info.ModTime()
		switch {
		case d.Type()&fs.ModeSymlink != 0:
			tgt, err := fs.ReadLink(fsys, p)
			if err != nil {
				return err
			}
			r, err := b.AddSymlink(path.Base(rel), tgt, info.Mode().Perm(), mt)
			if err != nil {
				return err
			}
			stack = append(stack, pending{relPath: rel, entry: r})
		case d.IsDir():
			stack = append(stack, pending{relPath: rel, isDir: true})
		default:
			f, err := fsys.Open(p)
			if err != nil {
				return err
			}
			r, err := b.AddFile(path.Base(rel), f, info.Mode().Perm(), mt, "")
			_ = f.Close()
			if err != nil {
				return err
			}
			stack = append(stack, pending{relPath: rel, entry: r})
		}
		return nil
	})
	if err != nil {
		return AddResult{}, err
	}

	// Bucket children by parent. Then walk stack in reverse
	// so the deepest directories are built first; their
	// children's MIDs are already in the bucket.
	byParent := make(map[string][]DirEntry)
	for _, p := range stack {
		if p.isDir {
			continue
		}
		parent := path.Dir(p.relPath)
		if parent == "." {
			parent = ""
		}
		byParent[parent] = append(byParent[parent], DirEntry{
			Name: path.Base(p.relPath),
			Mid:  p.entry.MID,
			Type: TypeFile,
			Size: p.entry.Size,
		})
	}

	// Sort stack so directories at the deepest paths are
	// processed first. The fs.WalkDir callback gave us
	// entries in path-sorted order, but reversing the slice
	// is not enough on its own because the depth is what
	// matters. Use a stable sort by depth (number of
	// slashes) so deeper paths come first.
	sort.SliceStable(stack, func(i, j int) bool {
		di := strings.Count(stack[i].relPath, "/")
		dj := strings.Count(stack[j].relPath, "/")
		if di != dj {
			return di > dj // deeper first
		}
		return stack[i].relPath > stack[j].relPath
	})

	for _, p := range stack {
		if !p.isDir {
			continue
		}
		entries := byParent[p.relPath]
		r, err := b.AddDir(path.Base(p.relPath), entries, 0o755, time.Time{})
		if err != nil {
			return AddResult{}, err
		}
		parent := path.Dir(p.relPath)
		if parent == "." {
			parent = ""
		}
		byParent[parent] = append(byParent[parent], DirEntry{
			Name: path.Base(p.relPath),
			Mid:  r.MID,
			Type: TypeDir,
			Size: r.Size,
		})
	}

	// The root directory's children are the top-level
	// byParent[""] entries.
	topEntries := byParent[""]
	r, err := b.AddDir(".", topEntries, 0o755, time.Time{})
	if err != nil {
		return AddResult{}, err
	}
	return r, nil
}

// sumBlockSizes returns the sum of a slice of blockRef sizes.
func sumBlockSizes(bs []blockRef) uint64 {
	var s uint64
	for _, b := range bs {
		s += b.size
	}
	return s
}

// filepathRel returns the slash-separated path of p relative
// to base, both interpreted as fs.FS-style paths. If p ==
// base it returns ".".
func filepathRel(base, p string) (string, error) {
	if base == "" {
		base = "."
	}
	if p == base {
		return ".", nil
	}
	// When base is "." the path is already relative — return
	// it verbatim, stripping only a leading "./" if any.
	if base == "." {
		rel := p
		if len(rel) > 2 && rel[:2] == "./" {
			rel = rel[2:]
		}
		if rel == "" {
			return ".", nil
		}
		return rel, nil
	}
	if !hasPathPrefix(p, base) {
		return "", fmt.Errorf("memfs: %q is not under %q", p, base)
	}
	rel := p[len(base):]
	if len(rel) > 0 && rel[0] == '/' {
		rel = rel[1:]
	}
	if rel == "" {
		return ".", nil
	}
	return rel, nil
}

func hasPathPrefix(s, prefix string) bool {
	if prefix == "." {
		return true
	}
	if len(s) < len(prefix) {
		return false
	}
	if s[:len(prefix)] != prefix {
		return false
	}
	if len(s) == len(prefix) {
		return true
	}
	return s[len(prefix)] == '/'
}
