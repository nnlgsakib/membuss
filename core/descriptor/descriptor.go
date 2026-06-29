package descriptor

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

var (
	magic    = [4]byte{'M', 'E', 'M', 'B'}
	version  uint8 = 1
	errBadMagic    = errors.New("descriptor: invalid magic bytes")
	errBadVersion  = errors.New("descriptor: unsupported version")
	errBadChecksum = errors.New("descriptor: checksum mismatch")
)

// BlockEntry describes one leaf block in the DAG.
type BlockEntry struct {
	MID   mid.MID
	Size  uint64
	Index uint32
}

// ErasureInfo captures the erasure-coding parameters applied
// to the content (if any).
type ErasureInfo struct {
	DataShards   int
	ParityShards int
	OriginalMIDs []string
	ShardMIDs    []string
}

// Descriptor is the in-memory representation of a .mbuss file.
type Descriptor struct {
	RootMID        mid.MID
	TotalSize      uint64
	BlockCount     uint32
	Name           string
	MimeType       string
	CreatedAt      time.Time
	Chunker        string
	ChunkSize      uint32
	Blocks         []BlockEntry
	Erasure        *ErasureInfo
	BootstrapPeers []string
	MemNSName      string
	Signature      []byte
}

// Option configures the Build process.
type Option func(*buildOptions)

type buildOptions struct {
	chunker      string
	chunkSize    uint32
	bootstrap    []string
	memnsName    string
	signatureFn  func([]byte) ([]byte, error)
}

// WithChunker sets the chunker algorithm name.
func WithChunker(name string) Option {
	return func(o *buildOptions) { o.chunker = name }
}

// WithChunkSize sets the fixed chunk size.
func WithChunkSize(size uint32) Option {
	return func(o *buildOptions) { o.chunkSize = size }
}

// WithBootstrapPeers sets the bootstrap peer list.
func WithBootstrapPeers(peers []string) Option {
	return func(o *buildOptions) { o.bootstrap = peers }
}

// WithMemNSName sets the MemNS name.
func WithMemNSName(name string) Option {
	return func(o *buildOptions) { o.memnsName = name }
}

// WithSignature sets a function that signs the serialized
// payload (used by the keyring).
func WithSignatureFn(fn func([]byte) ([]byte, error)) Option {
	return func(o *buildOptions) { o.signatureFn = fn }
}

// Build creates a Descriptor by walking the DAG rooted at root.
// It collects all leaf block MIDs, sizes, and metadata from the
// store. Erasure manifests are included when present.
func Build(s store.Store, root mid.MID, opts ...Option) (*Descriptor, error) {
	if s == nil {
		return nil, errors.New("descriptor: nil store")
	}
	if root.IsZero() {
		return nil, errors.New("descriptor: zero root MID")
	}

	var cfg buildOptions
	for _, o := range opts {
		o(&cfg)
	}

	has, err := s.Has(root)
	if err != nil {
		return nil, fmt.Errorf("descriptor: check root: %w", err)
	}
	if !has {
		return nil, fmt.Errorf("descriptor: root MID not found in store")
	}

	var (
		blocks []BlockEntry
		total  uint64
		index  uint32
	)

	err = store.Walk(s, root, func(m mid.MID, leaf bool) error {
		if !leaf {
			return nil
		}
		data, derr := s.Get(m)
		if derr != nil {
			return fmt.Errorf("descriptor: get block %s: %w", m.String(), derr)
		}
		blocks = append(blocks, BlockEntry{
			MID:   m,
			Size:  uint64(len(data)),
			Index: index,
		})
		total += uint64(len(data))
		index++
		return nil
	})
	if err != nil {
		return nil, err
	}

	// ObjectInfo metadata.
	var name, mimeType string
	if oi, oerr := store.GetObjectInfo(s, root); oerr == nil {
		name = oi.Name
		mimeType = oi.MimeType
	}

	// Erasure manifest (root-level only for now).
	var erasure *ErasureInfo
	if data, derr := s.Get(root); derr == nil {
		var manifest membusspb.ErasureManifest
		if uerr := proto.Unmarshal(data, &manifest); uerr == nil && manifest.OriginalMid != "" {
			erasure = &ErasureInfo{
				DataShards:   int(manifest.DataShards),
				ParityShards: int(manifest.ParityShards),
				OriginalMIDs: []string{manifest.OriginalMid},
				ShardMIDs:    manifest.ShardMids,
			}
		}
	}

	d := &Descriptor{
		RootMID:        root,
		TotalSize:      total,
		BlockCount:     uint32(len(blocks)),
		Name:           name,
		MimeType:       mimeType,
		CreatedAt:      time.Now(),
		Chunker:        cfg.chunker,
		ChunkSize:      cfg.chunkSize,
		Blocks:         blocks,
		Erasure:        erasure,
		BootstrapPeers: cfg.bootstrap,
		MemNSName:      cfg.memnsName,
	}

	return d, nil
}

// Parse decodes a .mbuss file from bytes.
func Parse(data []byte) (*Descriptor, error) {
	if len(data) < 4+1+32 {
		return nil, errors.New("descriptor: file too short")
	}
	if !bytes.Equal(data[:4], magic[:]) {
		return nil, errBadMagic
	}
	ver := data[4]
	if ver != version {
		return nil, errBadVersion
	}
	payload := data[5 : len(data)-32]
	checksum := data[len(data)-32:]

	h := sha256.Sum256(payload)
	if !bytes.Equal(h[:], checksum) {
		return nil, errBadChecksum
	}

	var pb membusspb.DescriptorPayload
	if err := proto.Unmarshal(payload, &pb); err != nil {
		return nil, fmt.Errorf("descriptor: unmarshal: %w", err)
	}

	root, err := mid.Parse(pb.RootMid)
	if err != nil {
		return nil, fmt.Errorf("descriptor: parse root mid: %w", err)
	}

	d := &Descriptor{
		RootMID:        root,
		TotalSize:      pb.TotalSize,
		BlockCount:     pb.BlockCount,
		Name:           pb.Name,
		MimeType:       pb.MimeType,
		CreatedAt:      time.Unix(pb.CreatedAt, 0),
		Chunker:        pb.Chunker,
		ChunkSize:      pb.ChunkSize,
		BootstrapPeers: pb.BootstrapPeers,
		MemNSName:      pb.MemnsName,
		Signature:      pb.Signature,
	}

	d.Blocks = make([]BlockEntry, len(pb.Blocks))
	for i, b := range pb.Blocks {
		m, merr := mid.Parse(b.Mid)
		if merr != nil {
			return nil, fmt.Errorf("descriptor: parse block[%d] mid: %w", i, merr)
		}
		d.Blocks[i] = BlockEntry{MID: m, Size: b.Size, Index: b.Index}
	}

	if pb.Erasure != nil {
		d.Erasure = &ErasureInfo{
			DataShards:   int(pb.Erasure.DataShards),
			ParityShards: int(pb.Erasure.ParityShards),
			OriginalMIDs: pb.Erasure.OriginalMids,
			ShardMIDs:    pb.Erasure.ShardMids,
		}
	}

	return d, nil
}

// Serialize encodes the descriptor into the .mbuss binary format.
func (d *Descriptor) Serialize() ([]byte, error) {
	pb := &membusspb.DescriptorPayload{
		RootMid:        d.RootMID.String(),
		TotalSize:      d.TotalSize,
		BlockCount:     d.BlockCount,
		Name:           d.Name,
		MimeType:       d.MimeType,
		CreatedAt:      d.CreatedAt.Unix(),
		Chunker:        d.Chunker,
		ChunkSize:      d.ChunkSize,
		BootstrapPeers: d.BootstrapPeers,
		MemnsName:      d.MemNSName,
		Signature:      d.Signature,
	}

	pb.Blocks = make([]*membusspb.DescriptorBlock, len(d.Blocks))
	for i, b := range d.Blocks {
		pb.Blocks[i] = &membusspb.DescriptorBlock{
			Mid:   b.MID.String(),
			Size:  b.Size,
			Index: b.Index,
		}
	}

	if d.Erasure != nil {
		pb.Erasure = &membusspb.DescriptorErasure{
			DataShards:   uint32(d.Erasure.DataShards),
			ParityShards: uint32(d.Erasure.ParityShards),
			OriginalMids: d.Erasure.OriginalMIDs,
			ShardMids:    d.Erasure.ShardMIDs,
		}
	}

	payload, err := proto.Marshal(pb)
	if err != nil {
		return nil, fmt.Errorf("descriptor: marshal: %w", err)
	}

	checksum := sha256.Sum256(payload)

	var buf bytes.Buffer
	buf.Grow(4 + 1 + len(payload) + 32)
	buf.Write(magic[:])
	buf.WriteByte(version)
	buf.Write(payload)
	buf.Write(checksum[:])

	return buf.Bytes(), nil
}

// Export writes the descriptor to a .mbuss file at path.
func (d *Descriptor) Export(path string) error {
	data, err := d.Serialize()
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// Import reads a .mbuss file from path.
func Import(path string) (*Descriptor, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("descriptor: read: %w", err)
	}
	return Parse(data)
}

// Verify checks that all blocks described exist in the store.
// Returns the list of missing MIDs.
func (d *Descriptor) Verify(s store.Store) ([]mid.MID, error) {
	if s == nil {
		return nil, errors.New("descriptor: nil store")
	}
	var missing []mid.MID
	for _, b := range d.Blocks {
		has, err := s.Has(b.MID)
		if err != nil {
			return nil, fmt.Errorf("descriptor: verify block %s: %w", b.MID.String(), err)
		}
		if !has {
			missing = append(missing, b.MID)
		}
	}
	return missing, nil
}

// ToMagnetLink returns a magnet-style URI for this descriptor.
//
//	magnet:?x=mem:<rootMID>&dn=<name>&bs=<peer1,peer2>&ec=<k,m>
func (d *Descriptor) ToMagnetLink() string {
	var parts []string
	parts = append(parts, "x=mem:"+d.RootMID.String())
	if d.Name != "" {
		parts = append(parts, "dn="+url.QueryEscape(d.Name))
	}
	if len(d.BootstrapPeers) > 0 {
		parts = append(parts, "bs="+url.QueryEscape(strings.Join(d.BootstrapPeers, ",")))
	}
	if d.Erasure != nil && d.Erasure.DataShards > 0 {
		parts = append(parts, "ec="+strconv.Itoa(d.Erasure.DataShards)+","+strconv.Itoa(d.Erasure.ParityShards))
	}
	if d.MemNSName != "" {
		parts = append(parts, "mn="+url.QueryEscape(d.MemNSName))
	}
	return "magnet:?" + strings.Join(parts, "&")
}

// FromMagnetLink parses a magnet URI into a partial Descriptor.
// Only the root MID and optional metadata are populated; blocks
// must be fetched from peers.
func FromMagnetLink(uri string) (*Descriptor, error) {
	if !strings.HasPrefix(uri, "magnet:?") {
		return nil, errors.New("descriptor: invalid magnet URI prefix")
	}
	params, err := url.ParseQuery(uri[len("magnet:?"):])
	if err != nil {
		return nil, fmt.Errorf("descriptor: parse magnet: %w", err)
	}

	x := params.Get("x")
	if x == "" {
		return nil, errors.New("descriptor: magnet missing x= parameter")
	}
	x = strings.TrimPrefix(x, "mem:")
	root, err := mid.Parse(x)
	if err != nil {
		return nil, fmt.Errorf("descriptor: parse magnet mid: %w", err)
	}

	d := &Descriptor{
		RootMID:   root,
		CreatedAt: time.Now(),
	}

	if dn := params.Get("dn"); dn != "" {
		d.Name, _ = url.QueryUnescape(dn)
	}
	if bs := params.Get("bs"); bs != "" {
		unescaped, _ := url.QueryUnescape(bs)
		d.BootstrapPeers = strings.Split(unescaped, ",")
	}
	if ec := params.Get("ec"); ec != "" {
		parts := strings.Split(ec, ",")
		if len(parts) == 2 {
			k, _ := strconv.Atoi(parts[0])
			m, _ := strconv.Atoi(parts[1])
			if k > 0 && m > 0 {
				d.Erasure = &ErasureInfo{DataShards: k, ParityShards: m}
			}
		}
	}
	if mn := params.Get("mn"); mn != "" {
		d.MemNSName, _ = url.QueryUnescape(mn)
	}

	return d, nil
}

// MemFSWalk is like store.Walk but understands MemFS DIR/FILE
// nodes. It calls visit for every leaf block.
func MemFSWalk(s store.Store, root mid.MID, visit func(m mid.MID, size uint64) error) error {
	return store.Walk(s, root, func(m mid.MID, leaf bool) error {
		if !leaf {
			return nil
		}
		data, err := s.Get(m)
		if err != nil {
			return err
		}
		return visit(m, uint64(len(data)))
	})
}

// CollectFileBlocks collects all leaf blocks from a MemFS FILE
// node, returning them in order. This is useful for descriptors
// of individual files within a directory tree.
func CollectFileBlocks(s store.Store, fileMID mid.MID) ([]BlockEntry, error) {
	data, err := s.Get(fileMID)
	if err != nil {
		return nil, err
	}

	var node membusspb.MemFSNode
	if err := proto.Unmarshal(data, &node); err != nil {
		return nil, err
	}

	var blocks []BlockEntry
	var index uint32

	for _, b := range node.Blocks {
		if b == nil || len(b.Mid) == 0 {
			continue
		}
		child, err := mid.FromMultihash(mid.CodecRaw, b.Mid)
		if err != nil {
			child, err = mid.FromMultihash(mid.CodecMemFS, b.Mid)
			if err != nil {
				continue
			}
		}
		blocks = append(blocks, BlockEntry{
			MID:   child,
			Size:  b.Size,
			Index: index,
		})
		index++
	}

	return blocks, nil
}

// SortBlocks sorts the block entries by index for consistent output.
func SortBlocks(blocks []BlockEntry) {
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Index < blocks[j].Index
	})
}
