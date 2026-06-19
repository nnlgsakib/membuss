// explorerAdapter is the production implementation of
// explorer.Backend, backed by the daemonBackend. It glues
// together the live subsystems (store, PEX, DHT, anchor
// engine, host identity, herald, store size) into the
// read-only surface the explorer needs.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/nnlgsakib/membuss/anchor"
	hostpkg "github.com/nnlgsakib/membuss/net/host"
	"github.com/nnlgsakib/membuss/core/keyring"
	"github.com/nnlgsakib/membuss/core/memfs"
	"github.com/nnlgsakib/membuss/core/memlink"
	"github.com/nnlgsakib/membuss/core/memns"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
	explorer "github.com/nnlgsakib/membuss/gateway/explorer"
	"github.com/nnlgsakib/membuss/gateway/memgate"
	"github.com/nnlgsakib/membuss/net/memex"
	membusspb "github.com/nnlgsakib/membuss/proto"
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
	keyring    *keyring.KeyRing
	memnsRes   *memns.Resolver
	// allRoots tracks all root MIDs known to this node
	// (both sealed and unsealed). Populated from sealed
	// list on startup, extended as content is added or
	// fetched.
	allRoots map[string]struct{}
}

func newExplorerAdapter(b *daemonBackend, anchorMode bool, keyring *keyring.KeyRing, memnsRes *memns.Resolver) *explorerAdapter {
	a := &explorerAdapter{b: b, started: time.Now(), anchorMode: anchorMode, keyring: keyring, memnsRes: memnsRes, allRoots: make(map[string]struct{})}
	// Populate allRoots from sealed MIDs on startup.
	if b.store != nil {
		if sealed, err := b.store.AllSealed(); err == nil {
			for _, m := range sealed {
				a.allRoots[m.String()] = struct{}{}
			}
		}
	}
	return a
}

// Stat returns a metadata snapshot.
func (a *explorerAdapter) Stat(ctx context.Context, m mid.MID) (explorer.ContentInfo, error) {
	st, err := a.b.Stat(ctx, m.String())
	if err != nil {
		return explorer.ContentInfo{}, err
	}
	return explorer.ContentInfo{
		MID:           m.String(),
		Size:          st.Size,
		Blocks:        st.Blocks,
		Sealed:        st.Sealed,
		Present:       st.Present,
		Codec:         st.Codec,
		Name:          st.Name,
		MimeType:      st.MimeType,
		Sealers:       st.Sealers,
		AnchorSealers: st.AnchorSealers,
	}, nil
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
	return a.ResolveWithProgress(ctx, m, nil)
}

// ResolveWithProgress resolves a MID with progress reporting.
// progressFn is called as blocks arrive with the running total
// of bytes received and total bytes (total may be 0 until all
// blocks are known).
func (a *explorerAdapter) ResolveWithProgress(ctx context.Context, m mid.MID, progressFn func(blocksResolved, blocksTotal uint64)) (io.ReadCloser, explorer.ContentInfo, error) {
	b := a.b
	if b.store == nil {
		return nil, explorer.ContentInfo{}, errors.New("explorer: no store")
	}
	has, err := b.store.Has(m)
	if err != nil {
		return nil, explorer.ContentInfo{}, err
	}
	if has {
		if complete, cerr := isDAGComplete(b.store, m); cerr != nil || !complete {
			has = false
		}
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
			Engine:     b.memex,
			Root:       m,
			Providers:  provs,
			Timeout:    30 * time.Second,
			ProgressFn: progressFn,
		})
		if serr == nil {
			if _, ferr := sess.Fetch(ctx); ferr == nil {
				has = true
			} else {
				// The Memex session reported progress (blocks
				// downloaded) but the final reassembly or
				// verification step failed. Re-check the store
				// because individual blocks may have been stored
				// even though the session-level Fetch errored.
				if h2, herr := b.store.Has(m); herr == nil && h2 {
					has = true
				}
			}
		}
	}
	if !has {
		return nil, explorer.ContentInfo{}, explorer.ErrNotFound
	}
	// Track this MID as a known root so it appears in the file list.
	a.allRoots[m.String()] = struct{}{}
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
	st, _ := a.b.Stat(ctx, m.String())
	return rc, explorer.ContentInfo{
		MID:           info.MID,
		Size:          info.Size,
		Blocks:        info.Blocks,
		Sealed:        info.Sealed,
		Name:          info.Name,
		MimeType:      info.MimeType,
		Sealers:       st.Sealers,
		AnchorSealers: st.AnchorSealers,
	}, nil
}

// memgate.ContentInfo is referenced via the embedded
// memgateAdapter call; keep an unused import guard so
// the file compiles even if the type is removed.
var _ memgate.ContentInfo

// Peers returns the local PEX peer table.
func (a *explorerAdapter) Peers(ctx context.Context, limit int) ([]explorer.PeerInfo, error) {
	infos, _, err := a.b.Peers(uint32(limit))
	if err != nil {
		return nil, err
	}
	out := make([]explorer.PeerInfo, 0, len(infos))
	for _, p := range infos {
		out = append(out, explorer.PeerInfo{
			PeerID:    p.PeerID,
			Addrs:     p.Addrs,
			IsAnchor:  p.IsAnchor,
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

// AllStoredMIDs lists every root MID in the local store,
// regardless of seal status, with its sealed flag.
func (a *explorerAdapter) AllStoredMIDs(ctx context.Context) ([]explorer.StoredMIDView, error) {
	b := a.b
	if b.store == nil {
		return nil, nil
	}
	sealed, err := b.store.AllSealed()
	if err != nil {
		return nil, err
	}
	sealedSet := make(map[string]struct{}, len(sealed))
	for _, m := range sealed {
		sealedSet[m.String()] = struct{}{}
	}
	out := make([]explorer.StoredMIDView, 0, len(a.allRoots))
	for key := range a.allRoots {
		m, err := mid.Parse(key)
		if err != nil {
			continue
		}
		name := ""
		if info, serr := store.GetObjectInfo(b.store, m); serr == nil && info.Name != "" {
			name = info.Name
		}
		out = append(out, explorer.StoredMIDView{
			MID:    key,
			Name:   name,
			Sealed: func() bool { _, ok := sealedSet[key]; return ok }(),
		})
	}
	return out, nil
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
	var out []explorer.AnchorRow
	if a.b.anchor != nil {
		for _, ai := range a.b.anchor.AnchorPeers() {
			addrs := make([]string, 0, len(ai.Addrs))
			for _, m := range ai.Addrs {
				addrs = append(addrs, m.String())
			}
			out = append(out, explorer.AnchorRow{
				PeerID: ai.ID.String(),
				Addrs:  addrs,
			})
		}
	} else if a.b.dht != nil {
		sCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		ch, err := a.b.dht.SearchValue(sCtx, "/membuss/anchors/v1")
		if err == nil {
			for val := range ch {
				ai, err := anchor.DecodeAnchorValue(val)
				if err == nil && ai.ID != "" {
					addrs := make([]string, 0, len(ai.Addrs))
					for _, m := range ai.Addrs {
						addrs = append(addrs, m.String())
					}
					out = append(out, explorer.AnchorRow{
						PeerID: ai.ID.String(),
						Addrs:  addrs,
					})
				}
			}
		}
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

// BandwidthStats returns the real-time bandwidth totals and rates.
func (a *explorerAdapter) BandwidthStats(ctx context.Context) (totalIn, totalOut int64, rateIn, rateOut float64, err error) {
	if wh, ok := a.b.host.(*hostpkg.Host); ok && wh != nil {
		totIn, totOut, rIn, rOut := wh.BandwidthTotals()
		return totIn, totOut, rIn, rOut, nil
	}
	return 0, 0, 0, 0, nil
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

// Add ingests a stream from the explorer upload form. The
// implementation writes to a temp file, calls daemonBackend.Add,
// and removes the temp file.
func (a *explorerAdapter) Add(ctx context.Context, name string, r io.Reader) (explorer.ContentInfo, error) {
	b := a.b
	if b == nil || b.store == nil {
		return explorer.ContentInfo{}, errors.New("explorer: no backend")
	}
	if r == nil {
		return explorer.ContentInfo{}, errors.New("explorer: nil reader")
	}
	f, err := os.CreateTemp("", "membuss-explorer-add-*")
	if err != nil {
		return explorer.ContentInfo{}, err
	}
	tmpPath := f.Name()
	defer os.Remove(tmpPath)
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return explorer.ContentInfo{}, err
	}
	if err := f.Close(); err != nil {
		return explorer.ContentInfo{}, err
	}
	res, err := b.Add(ctx, tmpPath, "", 0, true, name, "")
	if err != nil {
		return explorer.ContentInfo{}, err
	}
	a.allRoots[res.MID] = struct{}{}
	return explorer.ContentInfo{
		MID:           res.MID,
		Size:          res.Size,
		Blocks:        res.Blocks,
		Sealed:        res.Sealed,
		Name:          res.Name,
		MimeType:      res.MimeType,
		Present:       true,
	}, nil
}

// AddDirectory ingests a directory as MemFS from a set of files with relative paths.
func (a *explorerAdapter) AddDirectory(ctx context.Context, name string, files []explorer.DirectoryFile) (explorer.ContentInfo, error) {
	b := a.b
	if b == nil || b.store == nil {
		return explorer.ContentInfo{}, errors.New("explorer: no backend")
	}
	if len(files) == 0 {
		return explorer.ContentInfo{}, errors.New("explorer: no files")
	}

	tmp, err := os.MkdirTemp("", "membuss-explorer-add-dir-*")
	if err != nil {
		return explorer.ContentInfo{}, err
	}
	defer os.RemoveAll(tmp)

	for _, f := range files {
		rel := strings.ReplaceAll(f.Path, "\\", "/")
		rel = path.Clean("/" + rel)
		rel = strings.TrimPrefix(rel, "/")
		if rel == "" || rel == "." {
			continue
		}
		full := filepath.Join(tmp, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return explorer.ContentInfo{}, err
		}
		outFile, err := os.Create(full)
		if err != nil {
			return explorer.ContentInfo{}, err
		}
		if _, err := io.Copy(outFile, f.R); err != nil {
			outFile.Close()
			return explorer.ContentInfo{}, err
		}
		if err := outFile.Close(); err != nil {
			return explorer.ContentInfo{}, err
		}
	}

	memBuilder := memfs.NewBuilder(b.store)
	res, err := memBuilder.AddDirectoryFromFS(os.DirFS(tmp), ".")
	if err != nil {
		return explorer.ContentInfo{}, err
	}

	_ = b.store.Seal(res.MID, true)
	if b.dht != nil {
		announceCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		provideRecursive(announceCtx, b.dht, b.store, res.MID)
		cancel()
	}

	// Use uploader-supplied folder name, or fallback.
	name = filepath.Clean(name)
	if name == "." || name == "/" || name == "\\" {
		name = ""
	}
	dirName := name
	if dirName == "" {
		dirName = "upload"
		if len(files) > 0 && files[0].Path != "" {
			parts := strings.Split(strings.ReplaceAll(files[0].Path, "\\", "/"), "/")
			if len(parts) > 0 && parts[0] != "" {
				dirName = parts[0]
			}
		}
	}

	_ = store.SetObjectInfo(b.store, res.MID, store.ObjectInfo{
		Name:     dirName,
		MimeType: "inode/directory",
		Size:     res.Size,
	})

	a.allRoots[res.MID.String()] = struct{}{}

	return explorer.ContentInfo{
		MID:           res.MID.String(),
		Size:          res.Size,
		Blocks:        res.Block,
		Sealed:        true,
		Present:       true,
		Name:          name,
		MimeType:      "inode/directory",
	}, nil
}

func (a *explorerAdapter) Rename(ctx context.Context, m mid.MID, name string) error {
	b := a.b
	if b == nil || b.store == nil {
		return errors.New("explorer: no backend")
	}
	info, err := store.GetObjectInfo(b.store, m)
	if err != nil {
		return err
	}
	info.Name = name
	return store.SetObjectInfo(b.store, m, info)
}

// --- Phase 17: MemFS methods on explorerAdapter ---

// MemFSInfo returns the metadata for a MemFS node.
func (a *explorerAdapter) MemFSInfo(ctx context.Context, m mid.MID) (explorer.MemFSInfo, error) {
	r := memfs.NewResolver(&fetchingBlockstore{
		Blockstore: a.b.store,
		b:          a.b,
		ctx:        ctx,
	})
	st, err := r.Stat(ctx, m)
	if err != nil {
		return explorer.MemFSInfo{}, err
	}
	return explorer.MemFSInfo{
		MID:   m.String(),
		Type:  memFSTypeString(st.Type),
		Size:  st.Size,
		Mode:  uint32(st.Mode),
		MTime: st.MTime.Unix(),
		Mime:  st.MimeType,
	}, nil
}

// MemFSList returns the entries of a MemFS directory.
func (a *explorerAdapter) MemFSList(ctx context.Context, m mid.MID) ([]explorer.MemFSEntry, error) {
	r := memfs.NewResolver(&fetchingBlockstore{
		Blockstore: a.b.store,
		b:          a.b,
		ctx:        ctx,
	})
	st, err := r.Stat(ctx, m)
	if err != nil {
		return nil, err
	}
	if st.Type != memfs.TypeDir {
		return nil, errors.New("not a directory")
	}
	out := make([]explorer.MemFSEntry, 0, len(st.Entries))
	for _, e := range st.Entries {
		out = append(out, explorer.MemFSEntry{
			Name: e.Name,
			MID:  e.Mid.String(),
			Type: memFSTypeString(e.Type),
			Size: e.Size,
		})
	}
	return out, nil
}

// MemFSPathGet returns a streaming reader for the file at
// m/path. Used by the explorer's preview pane.
func (a *explorerAdapter) MemFSPathGet(ctx context.Context, m mid.MID, path string) (io.ReadSeekCloser, uint64, string, error) {
	r := memfs.NewResolver(&fetchingBlockstore{
		Blockstore: a.b.store,
		b:          a.b,
		ctx:        ctx,
	})
	node, err := r.ResolvePath(ctx, m, path)
	if err != nil {
		return nil, 0, "", err
	}
	if !node.IsFile() {
		return nil, 0, "", errors.New("not a file")
	}
	rc, err := r.Open(ctx, node.MustMID())
	if err != nil {
		return nil, 0, "", err
	}
	return rc, node.TotalSize(), node.MimeType(), nil
}

// KeyringKeys lists the keyring keys.
func (a *explorerAdapter) KeyringKeys(ctx context.Context) ([]explorer.KeyringKeyInfo, error) {
	if a.keyring == nil {
		return nil, errors.New("keyring not configured")
	}
	keys, err := a.keyring.List()
	if err != nil {
		return nil, err
	}
	out := make([]explorer.KeyringKeyInfo, 0, len(keys))
	for _, k := range keys {
		kType := "ed25519"
		if key, err := a.keyring.Get(k.Name); err == nil {
			kType = strings.ToLower(key.PubKey.Type().String())
		}
		out = append(out, explorer.KeyringKeyInfo{
			Name:      k.Name,
			MemNSName: k.MemNSName,
			Type:      kType,
			CreatedAt: k.CreatedAt,
		})
	}
	return out, nil
}

// ResolveMemNSRecord resolves a MemNS record.
func (a *explorerAdapter) ResolveMemNSRecord(ctx context.Context, name string) (explorer.MemNSRecordInfo, error) {
	if a.memnsRes == nil {
		return explorer.MemNSRecordInfo{}, errors.New("memns resolver not configured")
	}

	cleanName := name
	if strings.HasPrefix(cleanName, "/memns/") {
		cleanName = cleanName[7:]
	}

	var rec *membusspb.MemNSRecord
	var err error

	// If we own the key, try loading the record locally first
	if a.keyring != nil {
		keys, _ := a.keyring.List()
		for _, k := range keys {
			kMemNS := k.MemNSName
			if strings.HasPrefix(kMemNS, "/memns/") {
				kMemNS = kMemNS[7:]
			}
			if kMemNS == cleanName {
				rec, err = a.keyring.LoadRecord(k.Name)
				break
			}
		}
	}

	// If not owned or not found locally, fetch from DHT
	if rec == nil {
		rec, err = memns.ResolveDHT(ctx, a.memnsRes.DHTClient(), cleanName)
		if err != nil {
			return explorer.MemNSRecordInfo{}, err
		}
	}

	// Map routes
	routes := make([]explorer.MemRouteInfo, 0, len(rec.Routes))
	for _, r := range rec.Routes {
		routes = append(routes, explorer.MemRouteInfo{
			Target: string(r.Target),
			Weight: r.Weight,
			Label:  r.Label,
		})
	}

	// Map delegates
	delegates := make([]string, 0, len(rec.Delegates))
	for _, d := range rec.Delegates {
		delegates = append(delegates, string(d))
	}

	// Map changelog
	changelog := make([]explorer.MemLogEntryInfo, 0)
	if rec.Changelog != nil {
		for _, e := range rec.Changelog.Entries {
			changelog = append(changelog, explorer.MemLogEntryInfo{
				Sequence:  e.Sequence,
				Value:     string(e.Value),
				Timestamp: time.Unix(0, e.Timestamp),
				Message:   e.Message,
			})
		}
	}

	return explorer.MemNSRecordInfo{
		Name:      "/memns/" + cleanName,
		Value:     string(rec.Value),
		Sequence:  rec.Sequence,
		ExpiresAt: time.Unix(0, rec.Validity),
		TTL:       time.Duration(rec.Ttl),
		Routes:    routes,
		Delegates: delegates,
		Changelog: changelog,
	}, nil
}

// ResolveMemLink resolves a MemLink domain and returns its resolution details.
func (a *explorerAdapter) ResolveMemLink(ctx context.Context, domain string) (explorer.MemLinkInfo, error) {
	if a.memnsRes == nil {
		return explorer.MemLinkInfo{}, errors.New("memns resolver not configured")
	}
	dnsResAPI := a.memnsRes.DNS()
	if dnsResAPI == nil {
		return explorer.MemLinkInfo{}, errors.New("dns resolver not configured")
	}

	dnsRes, ok := dnsResAPI.(*memlink.DNSResolver)
	if !ok {
		return explorer.MemLinkInfo{}, errors.New("unexpected DNS resolver type")
	}

	rawTXT, err := dnsRes.LookupTXTRecord(domain)
	if err != nil {
		return explorer.MemLinkInfo{}, fmt.Errorf("lookup txt record failed: %w", err)
	}

	parsed, err := memlink.ParseTXTRecord(rawTXT)
	if err != nil {
		return explorer.MemLinkInfo{}, fmt.Errorf("failed to parse TXT record: %w", err)
	}

	resolved, err := dnsRes.Resolve(ctx, domain)
	if err != nil {
		return explorer.MemLinkInfo{}, fmt.Errorf("dns resolve failed: %w", err)
	}

	ttl := 300
	if parsed.TTL > 0 {
		ttl = parsed.TTL
	}

	return explorer.MemLinkInfo{
		Domain:            domain,
		RawTXT:            rawTXT,
		ResolvedMemNSName: parsed.MemNSName,
		ResolvedMID:       resolved,
		TTLRemaining:      ttl,
	}, nil
}

// ConnectPeer parses a multiaddr and dials the peer.
func (a *explorerAdapter) ConnectPeer(ctx context.Context, multiaddr string) error {
	ai, err := peer.AddrInfoFromString(multiaddr)
	if err != nil {
		return fmt.Errorf("parse multiaddr: %w", err)
	}
	if a.b.host == nil {
		return errors.New("host not ready")
	}
	return a.b.host.Connect(ctx, *ai)
}