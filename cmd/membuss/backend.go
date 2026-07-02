// Backend is the production implementation of server.Backend
// used by cmd/membuss. It wires the gRPC service to the
// live subsystems: Mem-Store, Memex, the libp2p host, the
// DHT, PEX, the herald, and the anchor engine.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/host"

	"github.com/nnlgsakib/membuss/anchor"
	"github.com/nnlgsakib/membuss/config"
	"github.com/nnlgsakib/membuss/core/chunk"
	"github.com/nnlgsakib/membuss/core/dag"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
	"github.com/nnlgsakib/membuss/net/dht"
	"github.com/nnlgsakib/membuss/net/herald"
	"github.com/nnlgsakib/membuss/net/memex"
	"github.com/nnlgsakib/membuss/net/pex"
	"github.com/nnlgsakib/membuss/obs/metrics"
	serverpkg "github.com/nnlgsakib/membuss/rpc/server"
)

// daemonBackend is the live, production implementation of
// server.Backend. All RPCs dispatch into the local subsystems.
type daemonBackend struct {
	dataDir string

	// host is the local libp2p host. It owns the DHT, PEX,
	// and Memex protocols.
	host host.Host

	// store is the local BadgerDB block store.
	store store.Store

	// dht, pex, memex are the local networking subsystems.
	dht   *dht.MemDHT
	pex   *pex.PEX
	memex *memex.Engine

	// herald is the reprovide loop. May be nil when the
	// anchor engine is the only announcer.
	herald *herald.MemHerald

	// anchor is the Anchor Node engine. nil if AnchorMode is
	// disabled in config.
	anchor *anchor.AnchorEngine

	// metrics is the optional Prometheus facade. nil = no-op.
	metrics *metrics.Metrics

	// retryBackoff configures the Memex session retry schedule.
	retryBackoff config.RetryBackoffConfig

	// logger is the structured-logging handle. nil = no-op.
	logger *slog.Logger
}

// slogAnchorLogger adapts *slog.Logger to anchor.Logger.
type slogAnchorLogger struct {
	l *slog.Logger
}

func (a *slogAnchorLogger) Infof(format string, args ...any) {
	a.l.Info(fmt.Sprintf(format, args...))
}

func (a *slogAnchorLogger) Errorf(format string, args ...any) {
	a.l.Error(fmt.Sprintf(format, args...))
}

// Compile-time check that daemonBackend satisfies server.Backend.
var _ serverpkg.Backend = (*daemonBackend)(nil)

// Add reads the file, builds the DAG, seals the root, and
// announces it to the DHT. chunker/chunkSize come from the
// gRPC request; if empty/zero, fixed 256 KiB is used.
func (b *daemonBackend) Add(ctx context.Context, path, chunker string, chunkSize uint32, sealRoot bool, name, mimeType string) (serverpkg.AddResult, error) {
	if path == "" {
		return serverpkg.AddResult{}, errors.New("add: empty path")
	}
	if !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return serverpkg.AddResult{}, err
		}
		path = abs
	}
	f, err := os.Open(path)
	if err != nil {
		return serverpkg.AddResult{}, err
	}
	defer f.Close()

	// Pick a chunker. Default is fixed 256 KiB.
	var factory chunk.ChunkerFactory
	switch chunker {
	case "rabin":
		factory = chunk.NewRabin()
	default:
		size := int(chunkSize)
		if size <= 0 {
			size = 256 * 1024
		}
		factory = chunk.NewFixed(size)
	}
	c, err := factory(f)
	if err != nil {
		return serverpkg.AddResult{}, fmt.Errorf("add: chunker: %w", err)
	}

	// Build DAG.
	root, err := dag.NewBuilder(b.store).Build(c)
	if err != nil {
		return serverpkg.AddResult{}, fmt.Errorf("add: dag: %w", err)
	}

	// Count leaves by re-walking the DAG (cheap).
	blocks, size, err := countDAG(b.store, root)
	if err != nil {
		return serverpkg.AddResult{}, fmt.Errorf("add: count: %w", err)
	}

	// Phase 19: persist the per-MID ObjectInfo so the
	// gateway can reproduce the user-facing metadata
	// on download. Name defaults to the file's
	// basename; MimeType defaults to an extension
	// sniff (see core/store.SniffMime).
	if name == "" {
		name = filepath.Base(path)
	}
	if mimeType == "" {
		mimeType = store.SniffMime(name)
	}
	if err := store.SetObjectInfo(b.store, root, store.ObjectInfo{
		Name:     name,
		MimeType: mimeType,
		Size:     size,
		IsRoot:   true,
	}); err != nil {
		return serverpkg.AddResult{}, fmt.Errorf("add: objectinfo: %w", err)
	}

	if sealRoot {
		if err := b.store.Seal(root, true); err != nil {
			return serverpkg.AddResult{}, fmt.Errorf("add: seal: %w", err)
		}
		// Announce to the DHT so other nodes can find this MID.
		if b.dht != nil {
			announceCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			provideRecursive(announceCtx, b.dht, b.store, root)
			cancel()
		}
	}

	return serverpkg.AddResult{
		MID:      root.String(),
		Size:     size,
		Blocks:   blocks,
		Sealed:   sealRoot,
		Name:     name,
		MimeType: mimeType,
	}, nil
}

// Get returns the content of midStr. If the MID is not local,
// the backend falls back to a Memex fetch using the DHT's
// provider list. The returned reader is the raw DAG-resolved
// bytes.
func (b *daemonBackend) Get(ctx context.Context, midStr string, offset, limit uint64) (io.ReadCloser, error) {
	root, err := mid.Parse(midStr)
	if err != nil {
		return nil, fmt.Errorf("get: parse mid: %w", err)
	}
	has, err := b.store.Has(root)
	if err != nil {
		return nil, err
	}
	if has {
		if complete, cerr := isDAGComplete(b.store, root); cerr != nil || !complete {
			has = false
		}
	}
	if !has && b.memex != nil {
		// Try DHT to find a provider, then Memex-fetch.
		if b.dht != nil {
			provCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			provs, perr := b.dht.FindProviders(provCtx, root)
			cancel()
			if perr == nil && len(provs) > 0 {
				sess, serr := memex.NewSession(memex.SessionConfig{
					Engine:         b.memex,
					Root:           root,
					Providers:      provs,
					Timeout:        memex.DefaultSessionTimeout,
					ProviderFinder: b.dht.FindProviders,
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
			return nil, fmt.Errorf("get: mid not found locally and no provider available")
		}
	}

	resolver := dag.NewResolver(b.store)
	rc, err := resolver.Resolve(root, nil)
	if err != nil {
		return nil, err
	}
	if offset == 0 && limit == 0 {
		return io.NopCloser(rc), nil
	}
	return io.NopCloser(sectionReader(rc, offset, limit)), nil
}

// GetWithProgress returns the content of midStr with progress
// reporting. If the MID is not local, the backend falls back
// to a Memex fetch using the DHT's provider list. progressFn
// is called as blocks arrive with the running total of bytes
// received and total bytes (total may be 0 until all blocks
// are known).
func (b *daemonBackend) GetWithProgress(ctx context.Context, midStr string, offset, limit uint64, progressFn func(update memex.ProgressUpdate)) (io.ReadCloser, error) {
	root, err := mid.Parse(midStr)
	if err != nil {
		return nil, fmt.Errorf("get: parse mid: %w", err)
	}
	has, err := b.store.Has(root)
	if err != nil {
		return nil, err
	}
	if has {
		if complete, cerr := isDAGComplete(b.store, root); cerr != nil || !complete {
			has = false
		}
	}
	if !has && b.memex != nil {
		// Try DHT to find a provider, then Memex-fetch.
		if b.dht != nil {
			provCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			provs, perr := b.dht.FindProviders(provCtx, root)
			cancel()
			if perr == nil && len(provs) > 0 {
				sess, serr := memex.NewSession(memex.SessionConfig{
					Engine:         b.memex,
					Root:           root,
					Providers:      provs,
					Timeout:        memex.DefaultSessionTimeout,
					ProgressFn:     progressFn,
					ProviderFinder: b.dht.FindProviders,
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
			return nil, fmt.Errorf("get: mid not found locally and no provider available")
		}
	}

	resolver := dag.NewResolver(b.store)
	rc, err := resolver.Resolve(root, nil)
	if err != nil {
		return nil, err
	}
	if offset == 0 && limit == 0 {
		return io.NopCloser(rc), nil
	}
	return io.NopCloser(sectionReader(rc, offset, limit)), nil
}

// Seal pins midStr. If recursive is true, the daemon walks the
// DAG and seals every reachable block.
//
// A Seal is a forward-looking pin: the seal record is written
// even when the recursive walk does not reach every block
// (e.g. the operator pins a MID they have not fetched yet).
// Missing blocks are surfaced as a soft warning through the
// daemon's logger when one is configured; the RPC still
// succeeds so the CLI / explorer can complete the action.
func (b *daemonBackend) Seal(ctx context.Context, midStr string, recursive bool) (serverpkg.SealResult, error) {
	root, err := mid.Parse(midStr)
	if err != nil {
		return serverpkg.SealResult{}, fmt.Errorf("seal: parse mid: %w", err)
	}
	// Idempotency check.
	if sealed, _ := b.store.IsSealed(root); sealed {
		return serverpkg.SealResult{Pinned: 0, Already: true}, nil
	}
	if err := b.store.Seal(root, recursive); err != nil {
		// A walk-incomplete error is informational: the
		// pin record is already on disk and the missing
		// blocks will be filled in by a later fetch.
		// Log it and continue with the success path.
		if errors.Is(err, store.ErrSealWalkIncomplete) {
			if b.logger != nil {
				b.logger.Warn("seal: walk incomplete; missing blocks will be filled in on first fetch",
					"mid", midStr, "err", err.Error())
			}
		} else {
			return serverpkg.SealResult{}, fmt.Errorf("seal: %w", err)
		}
	}
	// Count newly pinned blocks. The walk is best-effort;
	// missing blocks simply do not contribute to the count.
	blocks := uint64(0)
	if recursive {
		seen := map[string]struct{}{}
		_ = walkDAG(b.store, root, func(m mid.MID) error {
			if _, ok := seen[m.String()]; !ok {
				seen[m.String()] = struct{}{}
				blocks++
			}
			return nil
		})
	} else {
		blocks = 1
	}
	// Announce.
	if b.dht != nil {
		announceCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		provideRecursive(announceCtx, b.dht, b.store, root)
		cancel()
	}
	return serverpkg.SealResult{Pinned: blocks, Already: false}, nil
}
// Unseal removes the pin on midStr.
func (b *daemonBackend) Unseal(ctx context.Context, midStr string) (uint64, error) {
	root, err := mid.Parse(midStr)
	if err != nil {
		return 0, fmt.Errorf("unseal: parse mid: %w", err)
	}
	if err := b.store.Unseal(root); err != nil {
		return 0, err
	}
	return 1, nil
}

// Stat returns a snapshot describing midStr.
func (b *daemonBackend) Stat(ctx context.Context, midStr string) (serverpkg.StatInfo, error) {
	root, err := mid.Parse(midStr)
	if err != nil {
		return serverpkg.StatInfo{Present: false}, nil
	}
	has, err := b.store.Has(root)
	if err != nil {
		return serverpkg.StatInfo{}, err
	}
	if !has {
		return serverpkg.StatInfo{Present: false}, nil
	}
	complete, err := isDAGComplete(b.store, root)
	if err != nil {
		return serverpkg.StatInfo{}, err
	}
	if !complete {
		return serverpkg.StatInfo{Present: false}, nil
	}
	sealed, _ := b.store.IsSealed(root)
	blocks, size, err := countDAG(b.store, root)
	if err != nil {
		return serverpkg.StatInfo{}, err
	}
	// Phase 19: attach the per-MID ObjectInfo so the
	// CLI and the explorer can show / render the
	// upload name and the sniffed MIME type.
	oi, _ := store.GetObjectInfo(b.store, root)
	info := serverpkg.StatInfo{
		Present:  true,
		Size:     size,
		Blocks:   blocks,
		Sealed:   sealed,
		Codec:    root.Codec(),
		Name:     oi.Name,
		MimeType: oi.MimeType,
	}

	if b.dht != nil {
		provCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		provs, _ := b.dht.FindProviders(provCtx, root)
		cancel()
		info.Sealers = len(provs)

		anchors := b.getKnownAnchors(ctx)
		for _, p := range provs {
			if _, ok := anchors[p.ID.String()]; ok {
				info.AnchorSealers++
			}
		}
	}

	return info, nil
}

func (b *daemonBackend) getKnownAnchors(ctx context.Context) map[string]struct{} {
	anchors := make(map[string]struct{})
	if b.anchor != nil {
		for _, a := range b.anchor.AnchorPeers() {
			anchors[a.ID.String()] = struct{}{}
		}
	}
	if b.dht != nil {
		sCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		ch, err := b.dht.SearchValue(sCtx, anchor.AnchorRegistryKey)
		if err == nil {
			for val := range ch {
				ai, err := anchor.DecodeAnchorValue(val)
				if err == nil && ai.ID != "" {
					anchors[ai.ID.String()] = struct{}{}
				}
			}
		}
	}
	return anchors
}

// Peers returns the local PEX peer table.
func (b *daemonBackend) Peers(limit uint32) ([]serverpkg.NodePeerInfo, uint32, error) {
	if b.pex == nil {
		return nil, 0, nil
	}
	anchors := b.getKnownAnchors(context.Background())
	infos := b.pex.Peers()
	out := make([]serverpkg.NodePeerInfo, 0, len(infos))
	for _, p := range infos {
		addrs := make([]string, 0, len(p.Addrs))
		for _, a := range p.Addrs {
			addrs = append(addrs, a)
		}
		_, isAnchor := anchors[p.PeerId]
		out = append(out, serverpkg.NodePeerInfo{
			PeerID:   p.PeerId,
			Addrs:    addrs,
			IsAnchor: isAnchor,
		})
	}
	if limit > 0 && uint32(len(out)) > limit {
		out = out[:limit]
	}
	return out, uint32(len(infos)), nil
}

// DHTPeek asks the DHT who provides midStr.
func (b *daemonBackend) DHTPeek(ctx context.Context, midStr string, limit uint32) ([]serverpkg.NodePeerInfo, error) {
	if b.dht == nil {
		return nil, nil
	}
	root, err := mid.Parse(midStr)
	if err != nil {
		return nil, err
	}
	provCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	provs, err := b.dht.FindProviders(provCtx, root)
	if err != nil {
		return nil, err
	}
	anchors := b.getKnownAnchors(ctx)
	out := make([]serverpkg.NodePeerInfo, 0, len(provs))
	for _, p := range provs {
		addrs := make([]string, 0, len(p.Addrs))
		for _, a := range p.Addrs {
			addrs = append(addrs, a.String())
		}
		_, isAnchor := anchors[p.ID.String()]
		out = append(out, serverpkg.NodePeerInfo{
			PeerID:   p.ID.String(),
			Addrs:    addrs,
			IsAnchor: isAnchor,
		})
	}
	if limit > 0 && uint32(len(out)) > limit {
		out = out[:limit]
	}
	return out, nil
}

// GC runs garbage collection on the local store.
func (b *daemonBackend) GC(ctx context.Context, all bool) (serverpkg.GCInfo, error) {
	if b.store == nil {
		return serverpkg.GCInfo{}, errors.New("gc: no store")
	}
	freed, err := b.store.GC(ctx)
	if err != nil {
		return serverpkg.GCInfo{}, err
	}
	// Count kept blocks. The Store interface does not
	// expose a direct kept count post-GC, but we can use
	// AllBlocks on the BADGER store. If the in-memory
	// store is in use (tests), this returns 0.
	if s, ok := b.store.(interface {
		AllBlocks() ([]mid.MID, error)
	}); ok {
		mids, err := s.AllBlocks()
		if err == nil {
			return serverpkg.GCInfo{BytesFreed: freed, BlocksKept: uint64(len(mids))}, nil
		}
	}
	return serverpkg.GCInfo{BytesFreed: freed, BlocksKept: 0}, nil
}

// Delete recursively removes the given MID and its children from the store.
func (b *daemonBackend) Delete(ctx context.Context, midStr string) (serverpkg.DeleteResult, error) {
	if b.store == nil {
		return serverpkg.DeleteResult{}, errors.New("delete: no store")
	}
	m, err := mid.Parse(midStr)
	if err != nil {
		return serverpkg.DeleteResult{}, fmt.Errorf("delete: parse mid: %w", err)
	}

	type recursiveDeleter interface {
		DeleteRecursive(m mid.MID) (uint64, uint64, error)
	}

	if rd, ok := b.store.(recursiveDeleter); ok {
		deleted, freed, err := rd.DeleteRecursive(m)
		if err != nil {
			return serverpkg.DeleteResult{}, err
		}
		if b.dht != nil {
			_ = b.dht.RemoveProviderRecord(m)
		}
		return serverpkg.DeleteResult{
			BlocksDeleted: deleted,
			BytesFreed:    freed,
		}, nil
	}

	return serverpkg.DeleteResult{}, errors.New("delete: store does not support recursive deletion")
}


// AnchorStatus returns the anchor engine's stats. If the
// anchor engine is not running, returns zero-valued info
// with the host's PeerID.
func (b *daemonBackend) AnchorStatus() serverpkg.AnchorInfo {
	if b.anchor == nil {
		return serverpkg.AnchorInfo{
			PeerID: peerIDString(b.host),
		}
	}
	st := b.anchor.Status()
	return serverpkg.AnchorInfo{
		PeerID:     st.PeerID,
		UptimeSecs: int64(st.Uptime.Seconds()),
		BlocksHeld: st.BlocksHeld,
		Anchors:    int32(st.Anchors),
		Backlog:    int32(st.Backlog),
		Synced:     st.Synced,
	}
}

func peerIDString(h host.Host) string {
	if h == nil {
		return ""
	}
	return h.ID().String()
}

// countDAG walks a DAG rooted at root and returns the number
// of nodes and the total bytes (sum of leaf payload sizes).
func countDAG(bs store.Store, root mid.MID) (uint64, uint64, error) {
	var (
		nodes uint64
		bytes uint64
	)
	err := walkDAG(bs, root, func(m mid.MID) error {
		nodes++
		raw, err := bs.Get(m)
		if err != nil {
			return err
		}
		bytes += uint64(len(raw))
		return nil
	})
	return nodes, bytes, err
}

// walkDAG performs a depth-first walk of the DAG and calls
// visit for every MID encountered (the root plus all
// descendants).
func walkDAG(bs store.Store, root mid.MID, visit func(mid.MID) error) error {
	return store.Walk(bs, root, func(m mid.MID, leaf bool) error {
		return visit(m)
	})
}

// sectionReader returns an io.ReadCloser that yields up to
// limit bytes from rc starting at offset.
func sectionReader(rc io.Reader, offset, limit uint64) io.Reader {
	if offset > 0 {
		// Discard offset bytes.
		if _, err := io.CopyN(io.Discard, rc, int64(offset)); err != nil {
			return io.NopCloser(bytes.NewReader(nil))
		}
	}
	if limit == 0 {
		return io.NopCloser(rc)
	}
	return io.NopCloser(io.LimitReader(rc, int64(limit)))
}

// provideRecursive walks the DAG starting at root, announcing the root MID
// and all discovered MemFS node MIDs to the DHT.
func provideRecursive(ctx context.Context, dht *dht.MemDHT, s store.Store, root mid.MID) {
	if dht == nil || s == nil || root.IsZero() {
		return
	}
	_ = dht.Provide(ctx, root)
	_ = store.Walk(s, root, func(m mid.MID, leaf bool) error {
		if m.Codec() == mid.CodecMemFS {
			_ = dht.Provide(ctx, m)
		}
		return nil
	})
}

// isDAGComplete checks if all blocks in the Merkle DAG rooted at root are
// present in the store.
func isDAGComplete(s store.Store, root mid.MID) (bool, error) {
	if s == nil || root.IsZero() {
		return false, nil
	}
	err := store.Walk(s, root, func(m mid.MID, leaf bool) error {
		return nil
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) || strings.Contains(err.Error(), "block not found") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

