// memgateAdapter is the production implementation of
// memgate.Backend, backed by the daemonBackend. Resolve
// falls back to Memex when the requested MID is not local.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/dag"
	"github.com/nnlgsakib/membuss/core/memfs"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
	"github.com/nnlgsakib/membuss/net/memex"
	memgate "github.com/nnlgsakib/membuss/gateway/memgate"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

var _ memgate.Backend = (*memgateAdapter)(nil)

// memgateAdapter wraps daemonBackend to satisfy
// memgate.Backend. The two backends have similarly-named
// methods that take different parameter types, so we keep
// them on a separate type.
type memgateAdapter struct {
	b *daemonBackend
}

func newMemgateAdapter(b *daemonBackend) *memgateAdapter { return &memgateAdapter{b: b} }

// Resolve returns the bytes of a MID. If the MID is not
// local, the adapter asks the DHT for providers and runs a
// Memex session to fetch it.
func (a *memgateAdapter) Resolve(ctx context.Context, m mid.MID) (io.ReadCloser, memgate.ContentInfo, error) {
	b := a.b
	has, err := b.store.Has(m)
	if err != nil {
		return nil, memgate.ContentInfo{}, err
	}
	if has {
		if complete, cerr := isDAGComplete(b.store, m); cerr != nil || !complete {
			has = false
		}
	}
	if !has && b.memex != nil && b.dht != nil {
		provCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		provs, perr := b.dht.FindProviders(provCtx, m)
		cancel()
		if perr != nil || len(provs) == 0 {
			// Fallback: use currently connected swarm peers
			for _, pid := range b.host.Network().Peers() {
				provs = append(provs, b.host.Peerstore().PeerInfo(pid))
			}
		}
		if len(provs) > 0 {
			sess, serr := memex.NewSession(memex.SessionConfig{
				Engine:    b.memex,
				Root:      m,
				Providers: provs,
				Timeout:   30 * time.Second,
			})
			if serr == nil {
				if rc, ferr := sess.FetchWithBackoff(ctx, memex.DefaultRetryConfig()); ferr == nil && rc != nil {
					has = true
					if c, ok := rc.(io.Closer); ok {
						_ = c.Close()
					}
				}
			}
		}
	}
	if !has {
		return nil, memgate.ContentInfo{}, errMGNotFound
	}
	var (
		rc     io.ReadCloser
		size   uint64
		blocks uint64
		nodeMime string
	)
	if m.Codec() == mid.CodecMemFS {
		mr := memfs.NewResolver(b.store)
		node, err := mr.Resolve(ctx, m)
		if err != nil {
			return nil, memgate.ContentInfo{}, err
		}
		if node.IsDir() {
			raw, err := b.store.Get(m)
			if err != nil {
				return nil, memgate.ContentInfo{}, err
			}
			size = uint64(len(raw))
			blocks = 1
			rc = io.NopCloser(bytes.NewReader(raw))
			nodeMime = "inode/directory"
		} else {
			size = node.TotalSize()
			blocks = uint64(1 + node.BlockCount())
			openRc, err := mr.Open(ctx, m)
			if err != nil {
				return nil, memgate.ContentInfo{}, err
			}
			rc = openRc
			nodeMime = node.MimeType()
		}
	} else {
		var err error
		blocks, size, err = countDAG(b.store, m)
		if err != nil {
			return nil, memgate.ContentInfo{}, err
		}
		resolver := dag.NewResolver(b.store)
		rawRc, err := resolver.Resolve(m, nil)
		if err != nil {
			return nil, memgate.ContentInfo{}, err
		}
		rc = io.NopCloser(rawRc)
	}
	sealed, _ := b.store.IsSealed(m)
	// Phase 19: surface the uploader-supplied name and
	// MIME type so the gateway can serve with the right
	// Content-Type and Content-Disposition.
	oi, _ := store.GetObjectInfo(b.store, m)
	mimeType := oi.MimeType
	if mimeType == "" && m.Codec() == mid.CodecMemFS {
		mimeType = nodeMime
	}
	return rc, memgate.ContentInfo{
		MID:        m.String(),
		Size:       size,
		Blocks:     blocks,
		Sealed:     sealed,
		Name:       oi.Name,
		MimeType:   mimeType,
		ContentType: memgate.DetectContentType(m.String(), nil, mimeType),
	}, nil
}

// RawBlock returns the raw bytes of a single block (no DAG
// walk).
func (a *memgateAdapter) RawBlock(ctx context.Context, m mid.MID) ([]byte, error) {
	b := a.b
	has, err := b.store.Has(m)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, errMGNotFound
	}
	return b.store.Get(m)
}

// DAGNodeJSON returns the DAG node at m serialized as JSON.
func (a *memgateAdapter) DAGNodeJSON(ctx context.Context, m mid.MID) ([]byte, error) {
	b := a.b
	has, err := b.store.Has(m)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, errMGNotFound
	}
	raw, err := b.store.Get(m)
	if err != nil {
		return nil, err
	}
	var links []string
	if m.Codec() == mid.CodecMemFS {
		var node membusspb.MemFSNode
		if uerr := proto.Unmarshal(raw, &node); uerr == nil {
			switch node.Type {
			case membusspb.MemFSType_FILE:
				for _, blk := range node.Blocks {
					if blk != nil && len(blk.Mid) > 0 {
						codec := mid.CodecMemFS
						if blk.Size > 0 {
							codec = mid.CodecRaw
						}
						if child, err := mid.FromMultihash(uint64(codec), blk.Mid); err == nil {
							links = append(links, child.String())
						}
					}
				}
			case membusspb.MemFSType_DIR:
				for _, entry := range node.Entries {
					if entry != nil && len(entry.Mid) > 0 {
						codec := mid.CodecMemFS
						if entry.Type == membusspb.MemFSType_RAW {
							codec = mid.CodecRaw
						}
						if child, err := mid.FromMultihash(uint64(codec), entry.Mid); err == nil {
							links = append(links, child.String())
						}
					}
				}
			}
		}
	} else {
		var node membusspb.DAGNode
		if uerr := proto.Unmarshal(raw, &node); uerr == nil {
			links = node.Links
		}
	}
	size := uint64(len(raw))
	view := map[string]any{
		"mid":   m.String(),
		"size":  size,
		"links": links,
	}
	return json.Marshal(view)
}

// Stat returns a quick metadata snapshot.
func (a *memgateAdapter) Stat(ctx context.Context, m mid.MID) (memgate.ContentInfo, error) {
	b := a.b
	has, err := b.store.Has(m)
	if err != nil {
		return memgate.ContentInfo{}, err
	}
	if !has {
		return memgate.ContentInfo{}, errMGNotFound
	}
	if complete, cerr := isDAGComplete(b.store, m); cerr != nil || !complete {
		return memgate.ContentInfo{}, errMGNotFound
	}
	var (
		blocks uint64
		size   uint64
		nodeMime string
	)
	if m.Codec() == mid.CodecMemFS {
		mr := memfs.NewResolver(b.store)
		node, err := mr.Resolve(ctx, m)
		if err != nil {
			return memgate.ContentInfo{}, err
		}
		size = node.TotalSize()
		blocks = uint64(1 + node.BlockCount())
		if node.IsDir() {
			nodeMime = "inode/directory"
		} else {
			nodeMime = node.MimeType()
		}
	} else {
		var err error
		blocks, size, err = countDAG(b.store, m)
		if err != nil {
			return memgate.ContentInfo{}, err
		}
	}
	sealed, _ := b.store.IsSealed(m)
	// Phase 19: surface Name + MimeType from the
	// per-MID ObjectInfo (see core/store.ObjectInfo).
	oi, _ := store.GetObjectInfo(b.store, m)
	mimeType := oi.MimeType
	if mimeType == "" && m.Codec() == mid.CodecMemFS {
		mimeType = nodeMime
	}
	return memgate.ContentInfo{
		MID:         m.String(),
		Size:        size,
		Blocks:      blocks,
		Sealed:      sealed,
		Name:        oi.Name,
		MimeType:    mimeType,
		ContentType: memgate.DetectContentType(m.String(), nil, mimeType),
	}, nil
}

// Ping is a no-op health check.
func (a *memgateAdapter) Ping(ctx context.Context) error {
	if a.b.store == nil {
		return errors.New("no store")
	}
	return nil
}

// --- Phase 17: MemFS methods on memgateAdapter ---

// memfsResolver returns a *memfs.Resolver that reads from
// the daemon's local store, wrapping it with fetchingBlockstore to resolve missing blocks from the network.
func (a *memgateAdapter) memfsResolver(ctx context.Context) *memfs.Resolver {
	return memfs.NewResolver(&fetchingBlockstore{
		Blockstore: a.b.store,
		b:          a.b,
		ctx:        ctx,
	})
}

// MemFSInfo returns the metadata for a MemFS node. Returns
// errMGNotFound when the node is not a MemFS node or is
// absent from the local store.
func (a *memgateAdapter) MemFSInfo(ctx context.Context, m mid.MID) (memgate.MemFSInfo, error) {
	r := a.memfsResolver(ctx)
	st, err := r.Stat(ctx, m)
	if err != nil {
		return memgate.MemFSInfo{}, errMGNotFound
	}
	return memgate.MemFSInfo{
		MID:   m.String(),
		Type:  memFSTypeString(st.Type),
		Size:  st.Size,
		Mode:  uint32(st.Mode),
		MTime: st.MTime.Unix(),
		Mime:  st.MimeType,
	}, nil
}

// MemFSPathGet returns a streaming reader for a file at
// m/path. The returned io.ReadSeekCloser streams the
// resolved file content; the size and MIME type are
// returned alongside.
func (a *memgateAdapter) MemFSPathGet(ctx context.Context, m mid.MID, path string) (io.ReadSeekCloser, uint64, string, error) {
	r := a.memfsResolver(ctx)
	node, err := r.ResolvePath(ctx, m, path)
	if err != nil {
		return nil, 0, "", errMGNotFound
	}
	if !node.IsFile() {
		return nil, 0, "", errMGNotFound
	}
	// Resolve the file's own MID so the Open call works
	// even if the root is a deep directory.
	fileMID := node.MustMID()
	rc, err := r.Open(ctx, fileMID)
	if err != nil {
		return nil, 0, "", err
	}
	return rc, node.TotalSize(), node.MimeType(), nil
}

// MemFSList returns the entries of a MemFS directory.
func (a *memgateAdapter) MemFSList(ctx context.Context, m mid.MID) ([]memgate.MemFSEntry, error) {
	r := a.memfsResolver(ctx)
	st, err := r.Stat(ctx, m)
	if err != nil {
		return nil, errMGNotFound
	}
	if st.Type != memfs.TypeDir {
		return nil, errMGNotFound
	}
	out := make([]memgate.MemFSEntry, 0, len(st.Entries))
	for _, e := range st.Entries {
		out = append(out, memgate.MemFSEntry{
			Name: e.Name,
			MID:  e.Mid.String(),
			Type: memFSTypeString(e.Type),
			Size: e.Size,
		})
	}
	return out, nil
}

// MemFSPathInfo returns metadata about a path under the given root.
func (a *memgateAdapter) MemFSPathInfo(ctx context.Context, m mid.MID, subPath string) (memgate.MemFSInfo, error) {
	r := a.memfsResolver(ctx)
	node, err := r.ResolvePath(ctx, m, subPath)
	if err != nil {
		return memgate.MemFSInfo{}, errMGNotFound
	}
	return memgate.MemFSInfo{
		MID:   node.MustMID().String(),
		Type:  memFSTypeString(node.GetType()),
		Size:  node.TotalSize(),
		Mode:  uint32(node.Mode()),
		MTime: node.MTime().Unix(),
		Mime:  node.MimeType(),
	}, nil
}

// MemFSPathList returns entries of a directory under root at subPath.
func (a *memgateAdapter) MemFSPathList(ctx context.Context, m mid.MID, subPath string) ([]memgate.MemFSEntry, error) {
	r := a.memfsResolver(ctx)
	node, err := r.ResolvePath(ctx, m, subPath)
	if err != nil {
		return nil, errMGNotFound
	}
	if !node.IsDir() {
		return nil, errMGNotFound
	}
	out := make([]memgate.MemFSEntry, 0, node.EntryCount())
	for _, e := range node.EntriesValue() {
		out = append(out, memgate.MemFSEntry{
			Name: e.Name,
			MID:  e.Mid.String(),
			Type: memFSTypeString(e.Type),
			Size: e.Size,
		})
	}
	return out, nil
}

// memFSTypeString returns a short label for a MemFSType.
func memFSTypeString(t memfs.MemFSType) string {
	switch t {
	case memfs.TypeFile:
		return "file"
	case memfs.TypeDir:
		return "dir"
	case memfs.TypeSymlink:
		return "symlink"
	case memfs.TypeMetadata:
		return "metadata"
	default:
		return "raw"
	}
}

var errMGNotFound = errors.New("not found")


