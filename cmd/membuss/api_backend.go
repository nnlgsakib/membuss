// apiAdapter is the production implementation of api.Backend,
// backed by the daemonBackend. The HTTP Node API uses JSON
// envelopes and mid.MID values, while the gRPC Backend uses
// string IDs and a different Stat shape. The two surfaces
// stay on separate types to avoid signature collisions.
package main

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/nnlgsakib/membuss/api"
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