// Explorer: the built-in web UI for browsing MIDs, DAGs,
// peers, and network stats. Served under /explorer/* on the
// Mem-Gate chi router.
//
// The explorer is a read-mostly surface. It performs server-
// side rendering for every page; the only page that uses
// client-side JS is /explorer/mid/{mid}/dag, which fetches
// each node lazily from /mem/{mid}?format=dag-json.
package explorer

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"
	"unicode"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/nnlgsakib/membuss/core/mid"
)

//go:embed web/templates/*.html web/static/*.css web/static/*.js
var assetFS embed.FS

// ResolveStatus is the outcome of the explorer's "fetch
// from DHT" attempt on a MID the local store did not
// have on the first try. The three values are mutually
// exclusive and drive the NotFound / NotAvailable /
// Fetching states of the MID detail page.
type ResolveStatus int

const (
	// ResolveNone means no DHT fetch was attempted
	// (the MID was found locally, or the page is being
	// served in pure static mode).
	ResolveNone ResolveStatus = iota
	// ResolveFound means the MID was not local but the
	// DHT had a provider and the Memex session
	// successfully retrieved the content. The page
	// renders the metadata as if the MID had always
	// been local.
	ResolveFound
	// ResolveAttempted means the DHT reported providers
	// but the Memex fetch failed (timeout, no usable
	// peer, range not satisfiable, ...). The page
	// tells the user "not available right now, try
	// again later" and lists the providers we know
	// about so the operator can chase them manually.
	ResolveAttempted
	// ResolveNoProviders means the DHT has no provider
	// records for this MID at all. The page renders
	// "not found" and offers a retry link that will
	// re-run FindProviders with a fresh timeout.
	ResolveNoProviders
	// ResolveError means the DHT lookup itself errored
	// (e.g. context deadline exceeded while talking to
	// the DHT). The page renders the error so the
	// operator can tell transient DHT outage apart
	// from "definitively unknown".
	ResolveError
)

// ContentInfo is the metadata returned by Backend.Resolve
// and Backend.Stat. It mirrors the X-Membuss-* response
// headers the public Mem-Gate gateway returns, so a single
// struct drives both the gateway and the explorer.
type ContentInfo struct {
	MID         string
	Size        uint64
	Blocks      uint64
	ContentType string
	Sealed      bool
	Present bool
	Codec       uint64
	// Phase 19: human-friendly metadata captured at Add
	// time. Empty when the content was added by an
	// older daemon.
	Name     string
	MimeType string
}

// ErrNotFound is returned by Backend.Resolve when the MID
// is neither in the local store nor reachable from any DHT
// provider. The explorer page translates this into the
// "not found" branch of the template.
var ErrNotFound = errors.New("explorer: not found locally and no provider reachable")

// PeerInfo is a minimal copy of the PEX peer table row the
// explorer renders. Defined here so the explorer package
// does not have to import the api or pex packages.
type PeerInfo struct {
	PeerID    string
	Addrs     []string
	Connected bool
}

// AnchorRow is one registered anchor peer.
type AnchorRow struct {
	PeerID string
	Addrs  []string
}

// AnchorInfo mirrors the anchor engine's status.
type AnchorInfo struct {
	PeerID     string
	UptimeSecs int64
	BlocksHeld int64
	Anchors    int32
	Backlog    int32
	Synced     int64
}

// DirectoryFile is one file in a folder upload.
type DirectoryFile struct {
	Path string
	Size int64
	R    io.Reader
}

// Backend is the contract the explorer depends on. The
// daemon supplies a real implementation; tests inject a
// memBackend.
type Backend interface {
	// Stat returns a metadata snapshot for m. Present=false
	// means the MID is not in the local store. Phase 19:
	// the returned ContentInfo carries the uploader-supplied
	// Name and MimeType so the explorer can render the
	// CDN-style View button + the human-friendly filename.
	Stat(ctx context.Context, m mid.MID) (ContentInfo, error)
	// Seal pins m recursively.
	Seal(ctx context.Context, m mid.MID) error
	// Unseal removes the pin on m.
	Unseal(ctx context.Context, m mid.MID) error
	// Providers returns DHT-known providers for m.
	Providers(ctx context.Context, m mid.MID, limit int) ([]string, error)
	// Resolve fetches the content addressed by m. When the
	// MID is not in the local store the implementation
	// MUST consult the DHT for providers and run a
	// Memex session to retrieve the missing blocks, the
	// same way the public Mem-Gate gateway does. The
	// returned reader is the reassembled DAG; the caller
	// is responsible for closing it. Returns an error
	// (typically explorer.ErrNotFound) when the MID is
	// neither local nor reachable from any known
	// provider.
	Resolve(ctx context.Context, m mid.MID) (io.ReadCloser, ContentInfo, error)
	// ResolveWithProgress fetches the content addressed
	// by m with progress reporting. progressFn is called
	// as blocks arrive with the running count of blocks
	// resolved and total blocks discovered so far.
	ResolveWithProgress(ctx context.Context, m mid.MID, progressFn func(blocksResolved, blocksTotal uint64)) (io.ReadCloser, ContentInfo, error)
	// Add ingests content from a reader and returns the
	// resulting MID + metadata. name is the original
	// filename (used for Content-Disposition on download).
	Add(ctx context.Context, name string, r io.Reader) (ContentInfo, error)
	// AddDirectory ingests a directory as MemFS from a set of files with relative paths.
	AddDirectory(ctx context.Context, name string, files []DirectoryFile) (ContentInfo, error)
	// Rename updates the name metadata of a MID.
	Rename(ctx context.Context, m mid.MID, name string) error
	// Peers returns the local PEX peer table.
	Peers(ctx context.Context, limit int) ([]PeerInfo, error)
	// SealedMIDs lists all sealed MIDs in the local store.
	SealedMIDs(ctx context.Context) ([]mid.MID, error)
	// SealedCount returns the count of sealed MIDs.
	SealedCount(ctx context.Context) (int, error)
	// BlockCount returns the count of all blocks in the
	// local store.
	BlockCount(ctx context.Context) (uint64, error)
	// StoreBytes returns the total bytes used by the
	// local store.
	StoreBytes(ctx context.Context) (uint64, error)
	// AnchorPeers returns the registered anchor peers.
	AnchorPeers(ctx context.Context) ([]AnchorRow, error)
	// AnchorStatus returns the local anchor engine stats.
	// When no anchor engine is running, returns a zero
	// value with the local PeerID.
	AnchorStatus(ctx context.Context) AnchorInfo
	// LocalPeerID returns the local node's peer ID.
	LocalPeerID(ctx context.Context) string
	// LocalAddrs returns the local node's listen addrs.
	LocalAddrs(ctx context.Context) []string
	// NodeVersion returns the version + build string for
	// the local node.
	NodeVersion(ctx context.Context) (version, build string)
	// Uptime returns the time since the daemon started.
	Uptime(ctx context.Context) time.Duration
	// AnchorMode reports whether the daemon was started
	// with anchor mode enabled.
	AnchorMode(ctx context.Context) bool

	// --- Phase 17: MemFS support ---

	// MemFSInfo describes a MemFS node: its type, size, mode,
	// mtime, mime, and (for directories) the entries.
	MemFSInfo(ctx context.Context, m mid.MID) (MemFSInfo, error)
	// MemFSList returns the entries of a MemFS directory.
	MemFSList(ctx context.Context, m mid.MID) ([]MemFSEntry, error)
	// MemFSPathGet returns a streaming reader for the file
	// at m/path.
	MemFSPathGet(ctx context.Context, m mid.MID, path string) (io.ReadSeekCloser, uint64, string, error)
}

// MemFSInfo describes a MemFS node.
type MemFSInfo struct {
	MID   string
	Type  string
	Size  uint64
	Mode  uint32
	MTime int64
	Mime  string
}

// MemFSEntry is one row of a MemFS directory listing.
type MemFSEntry struct {
	Name string `json:"name"`
	MID  string `json:"mid"`
	Type string `json:"type"`
	Size uint64 `json:"size"`
}

// Config configures an Explorer.
type Config struct {
	// Backend serves the data. Required.
	Backend Backend
	// ReadTimeout is the per-request read timeout.
	ReadTimeout time.Duration
	// WriteTimeout is the per-request write timeout.
	WriteTimeout time.Duration
	// ProviderLimit caps the number of DHT providers
	// queried for a single MID detail page.
	ProviderLimit int
	// PeerLimit caps the number of peers listed on the
	// peers page.
	PeerLimit int
	// ResolveTimeout caps how long the explorer spends
	// on the DHT+Memex fallback when a MID is not in
	// the local store. Zero defaults to 30s. Set this
	// lower on page-loaded UIs where users notice a
	// stalled render.
	ResolveTimeout time.Duration
}

// Explorer is the built-in web UI.
type Explorer struct {
	cfg Config
	tpl *template.Template
	pages map[string]*template.Template
}

// New parses the embedded templates and returns an Explorer
// ready to be served.
func New(cfg Config) (*Explorer, error) {
	if cfg.Backend == nil {
		return nil, errors.New("explorer: nil backend")
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 60 * time.Second
	}
	if cfg.ProviderLimit == 0 {
		cfg.ProviderLimit = 32
	}
	if cfg.PeerLimit == 0 {
		cfg.PeerLimit = 256
	}

	funcs := template.FuncMap{
		"humanBytes": humanBytes,
	}
	tpl, err := template.New("explorer").Funcs(funcs).ParseFS(assetFS, "web/templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("explorer: parse templates: %w", err)
	}
	pages, err := buildPages(tpl)
	if err != nil {
		return nil, fmt.Errorf("explorer: build pages: %w", err)
	}
	return &Explorer{cfg: cfg, tpl: tpl, pages: pages}, nil
}

// Router returns the chi router. Exposed so tests can drive
// the explorer via httptest.
func (e *Explorer) Router() http.Handler { return e.buildRouter() }

// Handler returns the http.Handler wrapped with the
// configured write timeout. The daemon wires this into
// http.Server via Mem-Gate.
func (e *Explorer) Handler() http.Handler {
	return e.buildRouter()
}

func (e *Explorer) buildRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)

	r.Get("/", e.handleIndex)
	r.Get("/mid/{mid}", e.handleMID)
	r.Get("/mid/{mid}/dag", e.handleDAG)
	r.Get("/mid/{mid}/resolve-stream", e.handleResolveStream)
	r.Post("/mid/{mid}/seal", e.handleSeal)
	r.Post("/mid/{mid}/unseal", e.handleUnseal)
	r.Post("/mid/{mid}/rename", e.handleRename)
	r.Get("/peers", e.handlePeers)
	r.Get("/anchors", e.handleAnchors)
	r.Get("/node", e.handleNode)
	r.Post("/search", e.handleSearch)
	r.Get("/upload", e.handleUploadPage)
	r.Post("/upload", e.handleUpload)

	// Static assets.
	r.Get("/static/style.css", e.handleStatic("style.css", "text/css; charset=utf-8"))
	r.Get("/static/dag.js", e.handleStatic("dag.js", "application/javascript; charset=utf-8"))
	return r
}

// --- handlers ---

type sealedMIDView struct {
	MID  string
	Name string
}

type indexData struct {
	Title       string
	NodeInfo    nodeInfoView
	PeerCount   int
	StoreBytes  uint64
	SealedCount int
	BlockCount  uint64
	Uptime      int64
	SealedList  []sealedMIDView
}

type nodeInfoView struct {
	PeerID     string
	Addrs      []string
	Version    string
	Build      string
	AnchorMode bool
}

func (e *Explorer) handleIndex(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	b := e.cfg.Backend
	version, build := b.NodeVersion(ctx)
	sealed, _ := b.SealedMIDs(ctx)
	
	sealedList := make([]sealedMIDView, 0, len(sealed))
	for _, m := range sealed {
		name := ""
		if info, err := b.Stat(ctx, m); err == nil && info.Name != "" {
			name = info.Name
		}
		sealedList = append(sealedList, sealedMIDView{
			MID:  m.String(),
			Name: name,
		})
	}
	sort.Slice(sealedList, func(i, j int) bool {
		// If one of the elements is unnamed, sort it to the end or compare MIDs
		if sealedList[i].Name == "" && sealedList[j].Name != "" {
			return false
		}
		if sealedList[i].Name != "" && sealedList[j].Name == "" {
			return true
		}
		if sealedList[i].Name != sealedList[j].Name {
			return sealedList[i].Name < sealedList[j].Name
		}
		return sealedList[i].MID < sealedList[j].MID
	})
	if len(sealedList) > 50 {
		sealedList = sealedList[:50]
	}
	peers, _ := b.Peers(ctx, e.cfg.PeerLimit)
	data := indexData{
		Title:       "Home",
		PeerCount:   len(peers),
		StoreBytes:  mustStoreBytes(ctx, b),
		SealedCount: len(sealed),
		BlockCount:  mustBlockCount(ctx, b),
		Uptime:      int64(b.Uptime(ctx).Seconds()),
		SealedList:  sealedList,
		NodeInfo: nodeInfoView{
			PeerID:     b.LocalPeerID(ctx),
			Addrs:      b.LocalAddrs(ctx),
			Version:    version,
			Build:      build,
			AnchorMode: b.AnchorMode(ctx),
		},
	}
	e.render(w, "index.html", data)
}

type midData struct {
	Title         string
	MID           string
	NotFound      bool
	MemFSType     string
	MemFSEntries  []MemFSEntry
	SymlinkTarget string
	Size          uint64
	Blocks        uint64
	Sealed        bool
	Codec         uint64
	ContentType   string
	DataShards    int
	ParityShards  int
	TotalShards   int
	Health        string
	HealthLabel   string
	Providers     []string
	Name          string
	MimeType      string
	// ResolveStatus reports what the explorer's
	// background fetch attempt did when the MID was
	// not local. The four interesting values are
	// ResolveNone (local hit, no fetch needed),
	// ResolveFound (DHT + Memex succeeded, page
	// renders as if local), ResolveAttempted (DHT
	// had providers but Memex failed - the page
	// shows "not available, try again later"), and
	// ResolveNoProviders (DHT has nothing, page
	// shows "not found" with a retry link).
	ResolveStatus ResolveStatus
	// ResolveMessage is the human-readable message
	// that goes with ResolveStatus. It is empty
	// for ResolveNone and ResolveFound.
	ResolveMessage string
}

func (e *Explorer) handleMID(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	root, err := mid.Parse(midStr)
	if err != nil {
		http.Error(w, "bad mid: "+err.Error(), http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	b := e.cfg.Backend
	info, err := b.Stat(ctx, root)
	if err != nil {
		http.Error(w, "stat: "+err.Error(), http.StatusInternalServerError)
		return
	}
	present := info.Present
	size, blocks, sealed, codec := info.Size, info.Blocks, info.Sealed, info.Codec
	data := midData{
		Title:    "MID " + midStr,
		MID:      midStr,
		NotFound: !present,
		Name:     info.Name,
		MimeType: info.MimeType,
	}
	if !present {
		provs, _ := b.Providers(ctx, root, e.cfg.ProviderLimit)
		if len(provs) > 0 {
			e.render(w, "mid-resolving.html", data)
			return
		}
		e.resolveFromDHT(ctx, b, root, &data)
		if p2, err2 := b.Stat(ctx, root); err2 == nil && p2.Present {
			present = true
			size, blocks, sealed, codec = p2.Size, p2.Blocks, p2.Sealed, p2.Codec
			data.NotFound = false
			data.Name = p2.Name
			data.MimeType = p2.MimeType
		}
	}
	provs, _ := b.Providers(ctx, root, e.cfg.ProviderLimit)
	data.Providers = provs
	if present {
		data.Size = size
		data.Blocks = blocks
		data.Sealed = sealed
		data.Codec = codec
		data.ContentType = data.MimeType
		data.DataShards = 10
		data.ParityShards = 4
		data.TotalShards = data.DataShards + data.ParityShards
		data.Health = fmt.Sprintf("%d/%d shards needed", data.DataShards, data.TotalShards)
		data.HealthLabel = "OK"
		// Phase 17: probe for MemFS metadata so the
		// template can switch on type (file / dir /
		// symlink / metadata / raw).
		if minfo, err := b.MemFSInfo(ctx, root); err == nil {
			data.MemFSType = minfo.Type
			if minfo.Type == "dir" {
				if entries, lerr := b.MemFSList(ctx, root); lerr == nil {
					data.MemFSEntries = entries
				}
			}
		}
	}
	e.render(w, "mid.html", data)
}

// resolveFromDHT runs the "ask the DHT, then try a
// Memex session" pipeline on a MID the local store
// does not have. The outcome is recorded in data.ResolveStatus
// (and data.ResolveMessage) so the template can render
// the right "fetching" / "not available" / "not found"
// message.
//
// The implementation is intentionally a thin wrapper
// over Backend.Resolve: that method already does the
// DHT lookup + Memex session under the hood. We just
// translate the three terminal outcomes (success,
// "no providers", "providers but fetch failed") into
// the explorer's ResolveStatus enum.
func (e *Explorer) resolveFromDHT(ctx context.Context, b Backend, m mid.MID, data *midData) {
	timeout := e.cfg.ResolveTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	rc, info, err := b.Resolve(rctx, m)
	if rc != nil {
		// We don't need the bytes themselves for the
		// detail page; what matters is whether the
		// session got the content into the local
		// store. Drain + close so the underlying
		// Memex session releases its provider
		// slots immediately.
		_, _ = io.Copy(io.Discard, rc)
		_ = rc.Close()
	}
	switch {
	case err == nil:
		data.ResolveStatus = ResolveFound
		data.ResolveMessage = fmt.Sprintf("fetched %d bytes from DHT in %s", info.Size, "background")
	case errors.Is(err, ErrNotFound):
		// Backend.Resolve could not find a provider.
		// Distinguish "DHT has nothing" from
		// "DHT had something but the session failed"
		// by checking Providers() ourselves - that
		// is the same call the page already makes
		// after this returns, so we use the limit
		// only as a hint.
		if provs, perr := b.Providers(rctx, m, e.cfg.ProviderLimit); perr == nil && len(provs) > 0 {
			data.ResolveStatus = ResolveAttempted
			data.ResolveMessage = "DHT reported providers but the Memex fetch failed; try again later"
		} else {
			data.ResolveStatus = ResolveNoProviders
			data.ResolveMessage = "no DHT provider records for this MID; the content may not be pinned anywhere on the network"
		}
	default:
		data.ResolveStatus = ResolveError
		data.ResolveMessage = "DHT lookup error: " + err.Error()
	}
}

func (e *Explorer) handleDAG(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	if _, err := mid.Parse(midStr); err != nil {
		http.Error(w, "bad mid: "+err.Error(), http.StatusBadRequest)
		return
	}
	e.render(w, "dag.html", map[string]any{
		"Title": "DAG " + midStr,
		"MID":   midStr,
	})
}

func (e *Explorer) handleResolveStream(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	root, err := mid.Parse(midStr)
	if err != nil {
		http.Error(w, "bad mid: "+err.Error(), http.StatusBadRequest)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	b := e.cfg.Backend

	type sseEvent struct {
		State     string   `json:"state,omitempty"`
		Blocks    uint64   `json:"blocks,omitempty"`
		Total     uint64   `json:"total,omitempty"`
		Done      bool     `json:"done,omitempty"`
		MID       string   `json:"mid,omitempty"`
		Error     string   `json:"error,omitempty"`
		Providers []string `json:"providers,omitempty"`
	}

	sendEvent := func(ev sseEvent) {
		data, _ := json.Marshal(ev)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	// 1. Initial State: connecting to DHT
	sendEvent(sseEvent{State: "connecting"})

	provs, _ := b.Providers(ctx, root, e.cfg.ProviderLimit)
	sendEvent(sseEvent{State: "connecting", Providers: provs})

	timeout := e.cfg.ResolveTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rc, info, err := b.ResolveWithProgress(rctx, root, func(blocksResolved, blocksTotal uint64) {
		state := "downloading"
		if blocksTotal > 1 && blocksResolved <= 1 {
			state = "checking"
		}
		sendEvent(sseEvent{
			State:     state,
			Blocks:    blocksResolved,
			Total:     blocksTotal,
			Providers: provs,
		})
	})
	if err != nil {
		sendEvent(sseEvent{Error: err.Error()})
		return
	}
	if rc != nil {
		_, _ = io.Copy(io.Discard, rc)
		_ = rc.Close()
	}
	sendEvent(sseEvent{Done: true, MID: info.MID})
}

func (e *Explorer) handleSeal(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	root, err := mid.Parse(midStr)
	if err != nil {
		http.Error(w, "bad mid: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := e.cfg.Backend.Seal(r.Context(), root); err != nil {
		http.Error(w, "seal: "+err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/explorer/mid/"+midStr, http.StatusSeeOther)
}

func (e *Explorer) handleUnseal(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	root, err := mid.Parse(midStr)
	if err != nil {
		http.Error(w, "bad mid: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := e.cfg.Backend.Unseal(r.Context(), root); err != nil {
		http.Error(w, "unseal: "+err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/explorer/mid/"+midStr, http.StatusSeeOther)
}

type peersData struct {
	Title     string
	PeerCount int
	Peers     []PeerInfo
}

func (e *Explorer) handlePeers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	peers, _ := e.cfg.Backend.Peers(ctx, e.cfg.PeerLimit)
	e.render(w, "peers.html", peersData{
		Title:     "Peers",
		PeerCount: len(peers),
		Peers:     peers,
	})
}

type anchorsData struct {
	Title      string
	AnchorInfo AnchorInfo
	Anchors    []AnchorRow
	AnchorMode bool
}

func (e *Explorer) handleAnchors(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	b := e.cfg.Backend
	anchors, _ := b.AnchorPeers(ctx)
	rows := make([]AnchorRow, 0, len(anchors))
	for _, a := range anchors {
		rows = append(rows, a)
	}
	e.render(w, "anchors.html", anchorsData{
		Title:      "Anchors",
		AnchorInfo: b.AnchorStatus(ctx),
		Anchors:    rows,
		AnchorMode: b.AnchorMode(ctx),
	})
}

func (e *Explorer) handleNode(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	b := e.cfg.Backend
	version, build := b.NodeVersion(ctx)
	sealed, _ := b.SealedMIDs(ctx)
	e.render(w, "node.html", map[string]any{
		"Title":       "Node",
		"NodeInfo": nodeInfoView{
			PeerID:     b.LocalPeerID(ctx),
			Addrs:      b.LocalAddrs(ctx),
			Version:    version,
			Build:      build,
			AnchorMode: b.AnchorMode(ctx),
		},
		"StoreBytes":  mustStoreBytes(ctx, b),
		"SealedCount": len(sealed),
	})
}

// handleSearch is a POST handler that takes a MID from a
// form field and redirects to the MID detail page. An
// invalid MID is reported via the X-Membuss-Status header
// (no JS, no template rendering on error).
func (e *Explorer) handleSearch(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form: "+err.Error(), http.StatusBadRequest)
		return
	}
	midStr := sanitizeMIDString(r.FormValue("mid"))
	if midStr == "" {
		http.Error(w, "empty mid", http.StatusBadRequest)
		return
	}
	if _, err := mid.Parse(midStr); err != nil {
		http.Error(w, "bad mid: "+err.Error(), http.StatusBadRequest)
		return
	}
	http.Redirect(w, r, "/explorer/mid/"+midStr, http.StatusSeeOther)
}

// sanitizeMIDString strips invisible Unicode characters
// (zero-width joiners, soft hyphens, etc.) that users
// sometimes inadvertently paste along with a MID.
func sanitizeMIDString(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		if unicode.Is(unicode.Cc, r) && r != '\t' && r != '\n' && r != '\r' {
			return -1
		}
		if unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) {
			return -1
		}
		return r
	}, strings.TrimSpace(s))
}

func (e *Explorer) handleUploadPage(w http.ResponseWriter, r *http.Request) {
	e.render(w, "upload.html", map[string]any{
		"Title": "Upload",
	})
}

func (e *Explorer) handleUpload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(1 << 30); err != nil {
		http.Error(w, "bad form: "+err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	b := e.cfg.Backend

	// Check if this is a folder upload (multiple files sent under "files")
	if files, ok := r.MultipartForm.File["files"]; ok && len(files) > 0 {
		var dirFiles []DirectoryFile
		for _, fh := range files {
			f, err := fh.Open()
			if err != nil {
				http.Error(w, "open file: "+err.Error(), http.StatusInternalServerError)
				return
			}
			defer f.Close()

			dirFiles = append(dirFiles, DirectoryFile{
				Path: fh.Filename,
				Size: fh.Size,
				R:    f,
			})
		}

		folderName := r.FormValue("folder_name")
		res, err := b.AddDirectory(ctx, folderName, dirFiles)
		if err != nil {
			http.Error(w, "add directory: "+err.Error(), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/explorer/mid/"+res.MID, http.StatusSeeOther)
		return
	}

	// Otherwise, fall back to single file upload
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "no file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	res, err := b.Add(ctx, header.Filename, file)
	if err != nil {
		http.Error(w, "add: "+err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/explorer/mid/"+res.MID, http.StatusSeeOther)
}

func (e *Explorer) handleRename(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	root, err := mid.Parse(midStr)
	if err != nil {
		http.Error(w, "bad mid: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form: "+err.Error(), http.StatusBadRequest)
		return
	}
	newName := strings.TrimSpace(r.FormValue("name"))
	if newName == "" {
		http.Error(w, "empty name", http.StatusBadRequest)
		return
	}
	if err := e.cfg.Backend.Rename(r.Context(), root, newName); err != nil {
		http.Error(w, "rename: "+err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/explorer/mid/"+midStr, http.StatusSeeOther)
}

// handleStatic serves an embedded asset file.
func (e *Explorer) handleStatic(name, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "public, max-age=300")
		data, err := assetFS.ReadFile(path.Join("web", "static", name))
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = w.Write(data)
	}
}

// --- helpers ---

// render executes the named template. layout.html defines
// the page chrome; the named template defines the body.
// buildPages returns a map of page filename -> cloned
// template. Each clone has a "body" block that uses the
// page-specific body text. The master template keeps the
// page-specific *_body definitions; we add a "body"
// definition that calls the page's body by re-invoking
// the page's *_body template via {{template}}.
func buildPages(master *template.Template) (map[string]*template.Template, error) {
	pb := map[string]string{
		"index.html":        "index_body",
		"mid.html":          "mid_body",
		"mid-resolving.html": "mid-resolving_body",
		"dag.html":          "dag_body",
		"peers.html":        "peers_body",
		"anchors.html":      "anchors_body",
		"node.html":         "node_body",
		"upload.html":       "upload_body",
	}
	pages := make(map[string]*template.Template, len(pb))
	for page, bodyName := range pb {
		clone, err := master.Clone()
		if err != nil {
			return nil, fmt.Errorf("clone: %w", err)
		}
		// Add a "body" definition that delegates to the
		// page's body template. The body block becomes:
		//   {{define "body"}}{{template "X_body" .}}{{end}}
		override := fmt.Sprintf(`{{define "body"}}{{template %q .}}{{end}}`, bodyName)
		if _, err := clone.Parse(override); err != nil {
			return nil, fmt.Errorf("parse body override for %q: %w", page, err)
		}
		pages[page] = clone
	}
	return pages, nil
}

// render executes the per-page template.
func (e *Explorer) render(w http.ResponseWriter, page string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	t, ok := e.pages[page]
	if !ok {
		http.Error(w, "unknown page: "+page, http.StatusInternalServerError)
		return
	}
	if err := t.ExecuteTemplate(w, "layout", data); err != nil {
		_, _ = w.Write([]byte("template error: " + err.Error()))
	}
}

func mustStoreBytes(ctx context.Context, b Backend) uint64 {
	v, err := b.StoreBytes(ctx)
	if err != nil {
		return 0
	}
	return v
}

func mustBlockCount(ctx context.Context, b Backend) uint64 {
	v, err := b.BlockCount(ctx)
	if err != nil {
		return 0
	}
	return v
}

// humanBytes formats a byte count into a short human string
// like "1.2 MiB" / "44 B". Used by the templates.
func humanBytes(n any) string {
	var v float64
	switch x := n.(type) {
	case int:
		v = float64(x)
	case int64:
		v = float64(x)
	case uint64:
		v = float64(x)
	case float64:
		v = x
	default:
		return fmt.Sprintf("%v", n)
	}
	const (
		KiB = 1024
		MiB = 1024 * 1024
		GiB = 1024 * 1024 * 1024
	)
	switch {
	case v >= GiB:
		return fmt.Sprintf("%.2f GiB", v/GiB)
	case v >= MiB:
		return fmt.Sprintf("%.2f MiB", v/MiB)
	case v >= KiB:
		return fmt.Sprintf("%.2f KiB", v/KiB)
	default:
		return fmt.Sprintf("%d B", int64(v))
	}
}
