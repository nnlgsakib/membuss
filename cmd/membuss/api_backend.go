// apiAdapter is the production implementation of api.Backend,
// backed by the daemonBackend. The HTTP Node API uses JSON
// envelopes and mid.MID values, while the gRPC Backend uses
// string IDs and a different Stat shape. The two surfaces
// stay on separate types to avoid signature collisions.
package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/nnlgsakib/membuss/api"
	"github.com/nnlgsakib/membuss/core/memfs"
	"github.com/nnlgsakib/membuss/core/mid"
)

// apiAdapter wraps daemonBackend to satisfy api.Backend.
type apiAdapter struct {
	b *daemonBackend
}

func newAPIAdapter(b *daemonBackend) *apiAdapter { return &apiAdapter{b: b} }

// Add ingests a stream into the daemon. We persist the
// bytes to a temp file because daemonBackend.Add takes a
// path. The temp file is removed on return.
func (a *apiAdapter) Add(ctx context.Context, name string, r io.Reader) (api.AddResult, error) {
	b := a.b
	if b == nil || b.store == nil {
		return api.AddResult{}, errors.New("api: no backend")
	}
	if r == nil {
		return api.AddResult{}, errors.New("api: nil reader")
	}
	f, err := os.CreateTemp("", "membuss-api-add-*")
	if err != nil {
		return api.AddResult{}, err
	}
	tmpPath := f.Name()
	defer os.Remove(tmpPath)
	// Stream the upload to disk. The HTTP handler caps the
	// body with MaxBytesReader; here we just copy.
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return api.AddResult{}, err
	}
	if err := f.Close(); err != nil {
		return api.AddResult{}, err
	}
	// Phase 19: forward the caller-supplied name so the
	// daemon can persist it as the per-MID ObjectInfo.
	res, err := b.Add(ctx, tmpPath, "", 0, true, name, "")
	if err != nil {
		return api.AddResult{}, err
	}
	return api.AddResult{
		MID:      res.MID,
		Size:     res.Size,
		Blocks:   res.Blocks,
		Name:     res.Name,
		MimeType: res.MimeType,
	}, nil
}

// Resolve streams the bytes of m. The Node API contract
// returns the size alongside the reader; we re-walk the
// DAG via Stat to fill it in. For large content this is
// cheap because the DAG is small relative to the data.
func (a *apiAdapter) Resolve(ctx context.Context, m mid.MID) (io.ReadCloser, uint64, error) {
	b := a.b
	rc, err := b.Get(ctx, m.String(), 0, 0)
	if err != nil {
		return nil, 0, err
	}
	info, err := b.Stat(ctx, m.String())
	if err != nil || !info.Present {
		// Return the reader anyway; the caller can read
		// to EOF. Size is unknown in that case.
		return rc, 0, nil
	}
	return rc, info.Size, nil
}

// Seal pins m. We always do a recursive seal: the Node
// API operator almost always wants the whole DAG pinned.
func (a *apiAdapter) Seal(ctx context.Context, m mid.MID) (api.SealResult, error) {
	b := a.b
	res, err := b.Seal(ctx, m.String(), true)
	if err != nil {
		return api.SealResult{}, err
	}
	return api.SealResult{Pinned: res.Pinned, Already: res.Already}, nil
}

// Unseal removes the pin.
func (a *apiAdapter) Unseal(ctx context.Context, m mid.MID) (uint64, error) {
	b := a.b
	return b.Unseal(ctx, m.String())
}

// Stat returns a metadata snapshot.
func (a *apiAdapter) Stat(ctx context.Context, m mid.MID) (api.StatInfo, error) {
	b := a.b
	res, err := b.Stat(ctx, m.String())
	if err != nil {
		return api.StatInfo{Present: false}, err
	}
	return api.StatInfo{
		Present: res.Present,
		Size:    res.Size,
		Blocks:  res.Blocks,
		Sealed:  res.Sealed,
	}, nil
}

// Peers returns the local peer table.
func (a *apiAdapter) Peers(limit int) ([]api.PeerInfo, error) {
	b := a.b
	if b.pex == nil {
		return nil, nil
	}
	infos, _, err := b.Peers(uint32(limit))
	if err != nil {
		return nil, err
	}
	out := make([]api.PeerInfo, 0, len(infos))
	for _, p := range infos {
		out = append(out, api.PeerInfo{
			PeerID: p.PeerID,
			Addrs:  p.Addrs,
		})
	}
	return out, nil
}

// GC runs garbage collection. The Node API surface does
// not expose the "all" toggle; we always pass false so we
// only collect truly unreachable data.
func (a *apiAdapter) GC(ctx context.Context) (api.GCInfo, error) {
	b := a.b
	res, err := b.GC(ctx, false)
	if err != nil {
		return api.GCInfo{}, err
	}
	return api.GCInfo{
		BytesFreed: res.BytesFreed,
		BlocksKept: res.BlocksKept,
	}, nil
}

// NodeInfo returns the local node's identity.
func (a *apiAdapter) NodeInfo() api.NodeInfo {
	b := a.b
	var addrs []string
	if b.host != nil {
		for _, ma := range b.host.Addrs() {
			addrs = append(addrs, ma.String())
		}
	}
	return api.NodeInfo{
		PeerID:  peerIDString(b.host),
		Addrs:   addrs,
		Version: "",
		Build:   "",
	}
}

// --- Phase 17: MemFS methods on apiAdapter ---

// memFSBuilder returns a *memfs.Builder that writes into the
// daemon's local store.
func (a *apiAdapter) memFSBuilder() *memfs.Builder {
	return memfs.NewBuilder(a.b.store)
}

// memFSResolver returns a *memfs.Resolver that reads from
// the daemon's local store.
func (a *apiAdapter) memFSResolver() *memfs.Resolver {
	return memfs.NewResolver(a.b.store)
}

// AddFile ingests a file as a MemFS FILE node. When wrapDir
// is true the result is wrapped in a single-entry DIR so
// the returned MID is a directory MID addressable through
// the gateway.
func (a *apiAdapter) AddFile(ctx context.Context, name string, r io.Reader, wrapDir bool) (api.AddResult, error) {
	if a == nil || a.b == nil || a.b.store == nil {
		return api.AddResult{}, errors.New("api: no backend")
	}
	if r == nil {
		return api.AddResult{}, errors.New("api: nil reader")
	}
	// Read the body fully so we can both MemFS-add and
	// (when needed) fall through to the legacy store.
	data, err := io.ReadAll(r)
	if err != nil {
		return api.AddResult{}, err
	}
	b := a.memFSBuilder()
	res, err := b.AddFile(name, bytes.NewReader(data), 0o644, time.Time{}, "")
	if err != nil {
		return api.AddResult{}, err
	}
	if wrapDir {
		dirRes, err := b.AddDir(name, []memfs.DirEntry{
			{Name: name, Mid: res.MID, Type: memfs.TypeFile, Size: res.Size},
		}, 0o755, time.Time{})
		if err != nil {
			return api.AddResult{}, err
		}
		res = dirRes
	}
	// Seal and announce, mirroring the legacy Add path.
	_ = a.b.store.Seal(res.MID, true)
	if a.b.dht != nil {
		announceCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_ = a.b.dht.Provide(announceCtx, res.MID)
		cancel()
	}
	return api.AddResult{
		MID:    res.MID.String(),
		Size:   res.Size,
		Blocks: res.Block,
		Name:   name,
	}, nil
}

// AddDirectory ingests a multipart directory upload. Each
// part is written to a temp file so memfs.AddDirectoryFromFS
// can walk it with os.DirFS.
func (a *apiAdapter) AddDirectory(ctx context.Context, parts []api.DirectoryPart) (api.AddResult, error) {
	if a == nil || a.b == nil || a.b.store == nil {
		return api.AddResult{}, errors.New("api: no backend")
	}
	if len(parts) == 0 {
		return api.AddResult{}, errors.New("api: no parts")
	}
	tmp, err := os.MkdirTemp("", "membuss-api-add-dir-*")
	if err != nil {
		return api.AddResult{}, err
	}
	defer os.RemoveAll(tmp)

	for _, p := range parts {
		// Normalize separators: accept both "/" and the
		// OS-specific separator. fs.FS uses "/".
		rel := strings.ReplaceAll(p.Path, string(filepath.Separator), "/")
		rel = path.Clean("/" + rel)
		rel = strings.TrimPrefix(rel, "/")
		if rel == "" || rel == "." {
			continue
		}
		full := filepath.Join(tmp, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return api.AddResult{}, err
		}
		f, err := os.Create(full)
		if err != nil {
			return api.AddResult{}, err
		}
		if _, err := f.Write(p.Data); err != nil {
			f.Close()
			return api.AddResult{}, err
		}
		if err := f.Close(); err != nil {
			return api.AddResult{}, err
		}
	}
	b := a.memFSBuilder()
	res, err := b.AddDirectoryFromFS(os.DirFS(tmp), ".")
	if err != nil {
		return api.AddResult{}, err
	}
	_ = a.b.store.Seal(res.MID, true)
	if a.b.dht != nil {
		announceCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_ = a.b.dht.Provide(announceCtx, res.MID)
		cancel()
	}
	return api.AddResult{
		MID:    res.MID.String(),
		Size:   res.Size,
		Blocks: res.Block,
	}, nil
}

// Ls returns the entries of a MemFS directory.
func (a *apiAdapter) Ls(ctx context.Context, m mid.MID) ([]api.LsEntry, error) {
	if a == nil || a.b == nil || a.b.store == nil {
		return nil, errors.New("api: no backend")
	}
	r := a.memFSResolver()
	st, err := r.Stat(ctx, m)
	if err != nil {
		return nil, err
	}
	if st.Type != memfs.TypeDir {
		return nil, errors.New("api: not a directory")
	}
	out := make([]api.LsEntry, 0, len(st.Entries))
	for _, e := range st.Entries {
		out = append(out, api.LsEntry{
			Name: e.Name,
			MID:  e.Mid.String(),
			Type: memFSTypeString(e.Type),
			Size: e.Size,
		})
	}
	return out, nil
}

// GetPath returns a streaming reader for a file at m/path.
func (a *apiAdapter) GetPath(ctx context.Context, m mid.MID, p string) (io.ReadSeekCloser, uint64, string, error) {
	if a == nil || a.b == nil || a.b.store == nil {
		return nil, 0, "", errors.New("api: no backend")
	}
	r := a.memFSResolver()
	node, err := r.ResolvePath(ctx, m, p)
	if err != nil {
		return nil, 0, "", err
	}
	if !node.IsFile() {
		return nil, 0, "", errors.New("api: not a file")
	}
	rc, err := r.Open(ctx, m)
	if err != nil {
		// Try the resolved node MID.
		rc, err = r.Open(ctx, node.MustMID())
		if err != nil {
			return nil, 0, "", err
		}
	}
	return rc, node.TotalSize(), node.MimeType(), nil
}

// fsFile is a small adapter that lets memfs.AddDirectoryFromFS
// consume a list of parts. It is intentionally unused — we
// always materialize to a temp directory first because fs.FS
// has no streaming variant and the parts are already
// buffered by the multipart reader.
var _ fs.File = (*fsFile)(nil)

type fsFile struct {
	io.ReadSeeker
	info fs.FileInfo
}

func (f *fsFile) Stat() (fs.FileInfo, error) { return f.info, nil }
func (f *fsFile) Close() error               { return nil }
