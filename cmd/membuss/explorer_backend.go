// explorerAdapter is the production implementation of
// explorer.Backend, backed by the daemonBackend. It glues
// together the live subsystems (store, PEX, DHT, anchor
// engine, host identity, herald, store size) into the
// read-only surface the explorer needs.
package main

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/nnlgsakib/membuss/core/mid"
	explorer "github.com/nnlgsakib/membuss/gateway/explorer"
	"github.com/nnlgsakib/membuss/gateway/memgate"
	"github.com/nnlgsakib/membuss/net/memex"
)

var _ explorer.Backend = (*explorerAdapter)(nil)

// explorerAdapter wraps daemonBackend to satisfy
// explorer.Backend.
type explorerAdapter struct {
	b *daemonBackend
	// started is when the daemon started; used for
	// Uptime. Cached because Time.Now() at process start
	// is the only sensible answer.
	started time.Time
	// anchorMode is the immutable config value the
	// daemon was started with.
	anchorMode bool
}

func newExplorerAdapter(b *daemonBackend, anchorMode bool) *explorerAdapter {
	return &explorerAdapter{b: b, started: time.Now(), anchorMode: anchorMode}
}

// Stat returns a metadata snapshot.
func (a *explorerAdapter) Stat(ctx context.Context, m mid.MID) (bool, uint64, uint64, bool, uint64, error) {
	b := a.b
	if b.store == nil {
		return false, 0, 0, false, 0, errors.New("explorer: no store")
	}
	has, err := b.store.Has(m)
	if err != nil {
		return false, 0, 0, false, 0, err
	}
	if !has {
		return false, 0, 0, false, 0, nil
	}
	sealed, _ := b.store.IsSealed(m)
	blocks, size, err := countDAG(b.store, m)
	if err != nil {
		return false, 0, 0, false, 0, err
	}
	return true, size, blocks, sealed, m.Codec(), nil
}

// Seal pins m recursively. We delegate to daemonBackend.
func (a *explorerAdapter) Seal(ctx context.Context, m mid.MID) error {
	_, err := a.b.Seal(ctx, m.String(), true)
	return err
}

// Unseal removes the pin.
func (a *explorerAdapter) Unseal(ctx context.Context, m mid.MID) error {
	_, err := a.b.Unseal(ctx, m.String())
	return err
}

// Providers returns DHT-known providers for m.
func (a *explorerAdapter) Providers(ctx context.Context, m mid.MID, limit int) ([]string, error) {
	b := a.b
	if b.dht == nil {
		return nil, nil
	}
	var lim uint32
	if limit > 0 {
		lim = uint32(limit)
	}
	provCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	provs, err := b.dht.FindProviders(provCtx, m)
	if err != nil {
		return nil, err
	}
	if lim > 0 && uint32(len(provs)) > lim {
		provs = provs[:lim]
	}
	out := make([]string, 0, len(provs))
	for _, p := range provs {
		addrs := make([]string, 0, len(p.Addrs))
		for _, a := range p.Addrs {
			addrs = append(addrs, a.String())
		}
		// Format: peer_id\taddr1,addr2
		if len(addrs) == 0 {
			out = append(out, p.ID.String())
			continue
		}
		out = append(out, p.ID.String()+"\t"+joinStrings(addrs, ","))
	}
	return out, nil
}

// Resolve mirrors memgateAdapter.Resolve: when the MID is
// not local it asks the DHT for providers and runs a
// Memex session to fetch the missing blocks. The returned
// reader streams the reassembled DAG; the explorer closes
// it after draining.
//
// explorer.ErrNotFound is returned when the local store
// is empty AND the DHT has no provider records. The
// explorer package uses this to distinguish "not found"
// from "DHT had providers but Memex failed" so the
// template can show a "try again later" message instead
// of a hard 404.
func (a *explorerAdapter) Resolve(ctx context.Context, m mid.MID) (io.ReadCloser, explorer.ContentInfo, error) {
	b := a.b
	if b.store == nil {
		return nil, explorer.ContentInfo{}, errors.New("explorer: no store")
	}
	has, err := b.store.Has(m)
	if err != nil {
		return nil, explorer.ContentInfo{}, err
	}
	if !has && b.dht != nil && b.memex != nil {
		provCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		provs, perr := b.dht.FindProviders(provCtx, m)
		cancel()
		if perr != nil || len(provs) == 0 {
			// No DHT providers -> explorer will render
			// "not found". Returning a typed error
			// keeps the explorer template free of
			// string matching on transport errors.
			return nil, explorer.ContentInfo{}, explorer.ErrNotFound
		}
		sess, serr := memex.NewSession(memex.SessionConfig{
			Engine:    b.memex,
			Root:      m,
			Providers: provs,
			Timeout:   30 * time.Second,
		})
		if serr == nil {
			if _, ferr := sess.Fetch(ctx); ferr == nil {
			has = true
		} else {
			// Providers existed but the Memex
			// session failed. The caller
			// (explorer.handleMID) re-checks
			// Providers() to distinguish
			// ResolveAttempted from
			// ResolveNoProviders, so we
			// return ErrNotFound here and let
			// that classification happen.
			return nil, explorer.ContentInfo{}, explorer.ErrNotFound
		}
		}
	}
	if !has {
		return nil, explorer.ContentInfo{}, explorer.ErrNotFound
	}
	// Reuse the memgate adapter's Resolve so the size /
	// blocks / sealed numbers are computed exactly the
	// same way the public gateway would compute them.
	mg := &memgateAdapter{b: b}
	rc, info, err := mg.Resolve(ctx, m)
	if err != nil {
		if errors.Is(err, errMGNotFound) {
			return nil, explorer.ContentInfo{}, explorer.ErrNotFound
		}
		return nil, explorer.ContentInfo{}, err
	}
	return rc, explorer.ContentInfo{
		MID:    info.MID,
		Size:   info.Size,
		Blocks: info.Blocks,
		Sealed: info.Sealed,
	}, nil
}

// memgate.ContentInfo is referenced via the embedded
// memgateAdapter call; keep an unused import guard so
// the file compiles even if the type is removed.
var _ memgate.ContentInfo

// Peers returns the local PEX peer table.
func (a *explorerAdapter) Peers(ctx context.Context, limit int) ([]explorer.PeerInfo, error) {
	b := a.b
	if b.pex == nil {
		return nil, nil
	}
	infos := b.pex.Peers()
	if limit > 0 && len(infos) > limit {
		infos = infos[:limit]
	}
	out := make([]explorer.PeerInfo, 0, len(infos))
	for _, p := range infos {
		addrs := make([]string, 0, len(p.Addrs))
		for _, a := range p.Addrs {
			addrs = append(addrs, a)
		}
		out = append(out, explorer.PeerInfo{
			PeerID:    p.PeerId,
			Addrs:     addrs,
			Connected: false, // explorer does not have a direct "connected" view
		})
	}
	return out, nil
}

// SealedMIDs lists all sealed MIDs in the local store.
func (a *explorerAdapter) SealedMIDs(ctx context.Context) ([]mid.MID, error) {
	b := a.b
	if b.store == nil {
		return nil, nil
	}
	return b.store.AllSealed()
}

// SealedCount returns the count of sealed MIDs.
func (a *explorerAdapter) SealedCount(ctx context.Context) (int, error) {
	mids, err := a.SealedMIDs(ctx)
	if err != nil {
		return 0, err
	}
	return len(mids), nil
}

// BlockCount returns the count of all blocks in the
// local store. Only meaningful for the BadgerDB-backed
// store; returns 0 for the in-memory store.
func (a *explorerAdapter) BlockCount(ctx context.Context) (uint64, error) {
	if a.b.store == nil {
		return 0, nil
	}
	if s, ok := a.b.store.(interface {
		AllBlocks() ([]mid.MID, error)
	}); ok {
		mids, err := s.AllBlocks()
		if err != nil {
			return 0, err
		}
		return uint64(len(mids)), nil
	}
	return 0, nil
}

// StoreBytes returns the total bytes used by the store.
func (a *explorerAdapter) StoreBytes(ctx context.Context) (uint64, error) {
	if a.b.store == nil {
		return 0, nil
	}
	return a.b.store.Size()
}

// AnchorPeers returns the registered anchor peers.
func (a *explorerAdapter) AnchorPeers(ctx context.Context) ([]explorer.AnchorRow, error) {
	if a.b.anchor == nil {
		return nil, nil
	}
	anchors := a.b.anchor.AnchorPeers()
	out := make([]explorer.AnchorRow, 0, len(anchors))
	for _, ai := range anchors {
		addrs := make([]string, 0, len(ai.Addrs))
		for _, m := range ai.Addrs {
			addrs = append(addrs, m.String())
		}
		out = append(out, explorer.AnchorRow{
			PeerID: ai.ID.String(),
			Addrs:  addrs,
		})
	}
	return out, nil
}

// AnchorStatus returns the local anchor engine stats.
func (a *explorerAdapter) AnchorStatus(ctx context.Context) explorer.AnchorInfo {
	if a.b.anchor == nil {
		return explorer.AnchorInfo{
			PeerID: peerIDString(a.b.host),
		}
	}
	st := a.b.anchor.Status()
	return explorer.AnchorInfo{
		PeerID:     st.PeerID,
		UptimeSecs: int64(st.Uptime.Seconds()),
		BlocksHeld: st.BlocksHeld,
		Anchors:    int32(st.Anchors),
		Backlog:    int32(st.Backlog),
		Synced:     st.Synced,
	}
}

// LocalPeerID returns the local node's peer ID.
func (a *explorerAdapter) LocalPeerID(ctx context.Context) string {
	return peerIDString(a.b.host)
}

// LocalAddrs returns the local node's listen addrs.
func (a *explorerAdapter) LocalAddrs(ctx context.Context) []string {
	if a.b.host == nil {
		return nil
	}
	addrs := make([]string, 0, len(a.b.host.Addrs()))
	for _, ma := range a.b.host.Addrs() {
		addrs = append(addrs, ma.String())
	}
	return addrs
}

// NodeVersion returns the version + build string for the
// local node. Build is the value passed via --build.
func (a *explorerAdapter) NodeVersion(ctx context.Context) (string, string) {
	build := ""
	if a.b.herald != nil {
		// The herald holds no build string, but the gRPC
		// server does. We expose a free-form "dev" label
		// here; the daemon can plumb a real value through
		// later if needed.
		_ = peer.ID("") // silence unused import when a.b.anchor is nil
	}
	return "0.1.0", build
}

// Uptime returns the time since the daemon started.
func (a *explorerAdapter) Uptime(ctx context.Context) time.Duration {
	return time.Since(a.started)
}

// AnchorMode reports whether the daemon was started with
// anchor mode enabled.
func (a *explorerAdapter) AnchorMode(ctx context.Context) bool {
	return a.anchorMode
}

// joinStrings is a tiny helper to format a peer addr list.
func joinStrings(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	out := parts[0]
	for i := 1; i < len(parts); i++ {
		out += sep + parts[i]
	}
	return out
}