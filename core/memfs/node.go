// Package memfs implements a UnixFS-equivalent filesystem
// layer on top of Membuss's content-addressed Merkle DAG.
//
// Every file, directory, symlink and metadata envelope is
// represented by a MemFSNode protobuf message. The MID of a
// MemFSNode is the SHA-256 of the serialized protobuf bytes
// (using the 0x72 codec), so identical content produces
// identical MIDs across the entire network and the existing
// dedup, walk, seal and GC machinery applies without
// modification.
//
// Node layout (mirrors IPFS UnixFS):
//
//	FILE     — leaf or chained-into-blocks wrapper over raw data
//	DIR      — ordered list of named children (files or sub-dirs)
//	SYMLINK  — stores the target path as a string
//	METADATA — wraps another node with extra MIME/attr data
//	RAW      — raw leaf block (no MemFS wrapper; uses CodecRaw 0x55)
//
// Files are chunked by core/chunk (fixed 256 KiB by default)
// and each raw block is stored under the existing /b/
// namespace. MemFSNode envelopes (FILE / DIR / SYMLINK /
// METADATA) are stored under the /d/ namespace, the same
// way DAGNode intermediates already are.
package memfs

import (
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/mid"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// MemFSType re-exports the protobuf enum for convenience so
// callers do not have to import the generated package directly.
type MemFSType = membusspb.MemFSType

const (
	TypeRaw      = membusspb.MemFSType_RAW
	TypeFile     = membusspb.MemFSType_FILE
	TypeDir      = membusspb.MemFSType_DIR
	TypeSymlink  = membusspb.MemFSType_SYMLINK
	TypeMetadata = membusspb.MemFSType_METADATA
)

// MemFSBlock is one reference from a FILE node to a child.
// The child is either a raw block (type=RAW, codec 0x55) or
// an intermediate FILE node that itself groups more blocks.
//
// MemFSBlock is a value-type wrapper (not a proto alias) so
// that callers can construct and sort slices without dealing
// with the proto mutex. The Builder converts to the proto
// pointer form on serialization.
type MemFSBlock struct {
	Mid  mid.MID
	Size uint64
}

// DirEntry is one named entry inside a DIR node.
type DirEntry struct {
	Name string
	Mid  mid.MID
	Type MemFSType
	Size uint64
}

// MemFSMeta is the optional metadata envelope.
type MemFSMeta struct {
	MimeType string
	Attrs    map[string]string
}

// Node is the runtime view of a MemFSNode. It wraps the
// protobuf message so we can add typed methods. The proto
// message contains a mutex and must be embedded by pointer
// (the standard protobuf pattern).
type Node struct {
	*membusspb.MemFSNode
}

// NewNode returns a Node from a fresh protobuf value.
func NewNode(pb *membusspb.MemFSNode) *Node {
	if pb == nil {
		return &Node{MemFSNode: &membusspb.MemFSNode{}}
	}
	return &Node{MemFSNode: pb}
}

// IsFile reports whether this node is a file.
func (n *Node) IsFile() bool { return n.GetType() == TypeFile }

// IsDir reports whether this node is a directory.
func (n *Node) IsDir() bool { return n.GetType() == TypeDir }

// IsSymlink reports whether this node is a symbolic link.
func (n *Node) IsSymlink() bool { return n.GetType() == TypeSymlink }

// IsRaw reports whether this node is a raw leaf block.
func (n *Node) IsRaw() bool { return n.GetType() == TypeRaw }

// IsMetadata reports whether this node is a metadata wrapper.
func (n *Node) IsMetadata() bool { return n.GetType() == TypeMetadata }

// TotalSize returns the cumulative payload size of this node.
func (n *Node) TotalSize() uint64 {
	switch n.GetType() {
	case TypeFile:
		if sz := n.GetFileSize(); sz > 0 {
			return sz
		}
		var total uint64
		for _, b := range n.BlocksValue() {
			total += b.Size
		}
		return total
	case TypeDir:
		var total uint64
		for _, e := range n.EntriesValue() {
			total += e.Size
		}
		return total
	default:
		return uint64(len(n.GetData()))
	}
}

// BlockCount returns the number of direct block references.
func (n *Node) BlockCount() int { return len(n.GetBlocks()) }

// EntryCount returns the number of direct entries in a DIR node.
func (n *Node) EntryCount() int { return len(n.GetEntries()) }

// BlocksValue returns the FILE node's direct block references
// as a slice of value-type MemFSBlock entries.
func (n *Node) BlocksValue() []MemFSBlock {
	src := n.GetBlocks()
	out := make([]MemFSBlock, len(src))
	for i, b := range src {
		if b == nil {
			continue
		}
		out[i] = MemFSBlock{
			Mid:  midFromBytes(b.GetMid()),
			Size: b.GetSize(),
		}
	}
	return out
}

// EntriesValue returns the DIR node's entries as a slice of
// value-type DirEntry.
func (n *Node) EntriesValue() []DirEntry {
	src := n.GetEntries()
	out := make([]DirEntry, len(src))
	for i, e := range src {
		if e == nil {
			continue
		}
		out[i] = DirEntry{
			Name: e.GetName(),
			Mid:  midFromBytes(e.GetMid()),
			Type: e.GetType(),
			Size: e.GetSize(),
		}
	}
	return out
}

// MetaValue returns the optional metadata envelope as a
// value-type MemFSMeta.
func (n *Node) MetaValue() MemFSMeta {
	m := n.GetMeta()
	if m == nil {
		return MemFSMeta{}
	}
	return MemFSMeta{
		MimeType: m.GetMimeType(),
		Attrs:    m.GetAttrs(),
	}
}

// Serialize returns the canonical protobuf form of this node.
func (n *Node) Serialize() ([]byte, error) {
	return proto.Marshal(n.MemFSNode)
}

// Deserialize populates n from its canonical protobuf form.
func (n *Node) Deserialize(data []byte) error {
	if len(data) == 0 {
		return errors.New("memfs: empty node bytes")
	}
	return proto.Unmarshal(data, n.MemFSNode)
}

// MID returns the content identifier of this node.
func (n *Node) MID() (mid.MID, error) {
	raw, err := n.Serialize()
	if err != nil {
		return mid.MID{}, fmt.Errorf("memfs: marshal node: %w", err)
	}
	return mid.FromBytesWithCodec(raw, mid.CodecMemFS), nil
}

// MustMID is the panicking form of MID.
func (n *Node) MustMID() mid.MID {
	m, err := n.MID()
	if err != nil {
		panic(err)
	}
	return m
}

// EntriesSorted returns a lexicographically-sorted copy of
// the DIR entries.
func (n *Node) EntriesSorted() []DirEntry {
	out := n.EntriesValue()
	sortDirEntries(out)
	return out
}

// Mode returns the unix mode bits stored in the node.
func (n *Node) Mode() fs.FileMode {
	if n.GetMode() == 0 {
		switch n.GetType() {
		case TypeDir:
			return fs.ModeDir | 0o755
		case TypeSymlink:
			return fs.ModeSymlink | 0o777
		default:
			return 0o644
		}
	}
	return fs.FileMode(n.GetMode())
}

// MTime returns the modification time.
func (n *Node) MTime() time.Time {
	if n.GetMtime() == 0 {
		return time.Time{}
	}
	return time.Unix(0, n.GetMtime())
}

// MimeType returns the MIME type recorded in the optional
// metadata envelope, or "" if none is set.
func (n *Node) MimeType() string {
	if n.GetMeta() == nil {
		return ""
	}
	return n.GetMeta().GetMimeType()
}

// Bytes returns the inline data payload, or nil when none.
func (n *Node) Bytes() []byte { return n.GetData() }

// SymlinkTarget returns the symlink target path.
func (n *Node) SymlinkTarget() string { return n.GetSymlinkTarget() }

// ParseNode deserializes a MemFSNode from its canonical form.
func ParseNode(data []byte) (*Node, error) {
	n := &Node{MemFSNode: &membusspb.MemFSNode{}}
	if err := n.Deserialize(data); err != nil {
		return nil, fmt.Errorf("memfs: parse node: %w", err)
	}
	return n, nil
}

// midFromBytes builds a mid.MID from a raw multihash envelope
// stored as []byte. An empty input yields the zero MID.
//
// We try to parse the public "mem…" string form first so the
// codec round-trips correctly; if that fails we fall back to
// embedding the raw envelope with the 0x72 MemFS codec.
func midFromBytes(b []byte) mid.MID {
	if len(b) == 0 {
		return mid.MID{}
	}
	if s := string(b); len(s) > 3 && s[:3] == "mem" {
		if m, err := mid.Parse(s); err == nil {
			return m
		}
	}
	mh := make([]byte, len(b))
	copy(mh, b)
	m, err := mid.FromMultihash(mid.CodecMemFS, mh)
	if err != nil {
		return mid.MID{Hash: mh}
	}
	return m
}

// sortDirEntries sorts in place by name (lexicographic, byte-wise).
func sortDirEntries(entries []DirEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})
}
