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
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/nnlgsakib/membuss/core/mid"
)

//go:embed assets/*.html assets/*.css assets/*.js
var assetFS embed.FS

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

// Backend is the contract the explorer depends on. The
// daemon supplies a real implementation; tests inject a
// memBackend.
type Backend interface {
	// Stat returns a metadata snapshot for m. ok=false
	// means the MID is not present in the local store.
	Stat(ctx context.Context, m mid.MID) (present bool, size, blocks uint64, sealed bool, codec uint64, err error)
	// Seal pins m recursively.
	Seal(ctx context.Context, m mid.MID) error
	// Unseal removes the pin on m.
	Unseal(ctx context.Context, m mid.MID) error
	// Providers returns DHT-known providers for m.
	Providers(ctx context.Context, m mid.MID, limit int) ([]string, error)
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
	tpl, err := template.New("explorer").Funcs(funcs).ParseFS(assetFS, "assets/*.html")
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
	return http.TimeoutHandler(e.buildRouter(), e.cfg.WriteTimeout, `{"ok":false,"error":"timeout"}`)
}

func (e *Explorer) buildRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(e.cfg.ReadTimeout))

	r.Get("/", e.handleIndex)
	r.Get("/mid/{mid}", e.handleMID)
	r.Get("/mid/{mid}/dag", e.handleDAG)
	r.Post("/mid/{mid}/seal", e.handleSeal)
	r.Post("/mid/{mid}/unseal", e.handleUnseal)
	r.Get("/peers", e.handlePeers)
	r.Get("/anchors", e.handleAnchors)
	r.Get("/node", e.handleNode)
	r.Post("/search", e.handleSearch)

	// Static assets.
	r.Get("/static/style.css", e.handleStatic("style.css", "text/css; charset=utf-8"))
	r.Get("/static/dag.js", e.handleStatic("dag.js", "application/javascript; charset=utf-8"))
	return r
}

// --- handlers ---

type indexData struct {
	Title       string
	NodeInfo    nodeInfoView
	PeerCount   int
	StoreBytes  uint64
	SealedCount int
	BlockCount  uint64
	Uptime      int64
	SealedList  []string
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
	sealedStrs := make([]string, 0, len(sealed))
	for _, m := range sealed {
		sealedStrs = append(sealedStrs, m.String())
	}
	sort.Strings(sealedStrs)
	if len(sealedStrs) > 50 {
		sealedStrs = sealedStrs[:50]
	}
	peers, _ := b.Peers(ctx, e.cfg.PeerLimit)
	data := indexData{
		Title:    "Home",
		PeerCount: len(peers),
		StoreBytes:  mustStoreBytes(ctx, b),
		SealedCount: len(sealed),
		BlockCount:  mustBlockCount(ctx, b),
		Uptime:      int64(b.Uptime(ctx).Seconds()),
		SealedList:  sealedStrs,
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
	present, size, blocks, sealed, codec, err := b.Stat(ctx, root)
	if err != nil {
		http.Error(w, "stat: "+err.Error(), http.StatusInternalServerError)
		return
	}
	provs, _ := b.Providers(ctx, root, e.cfg.ProviderLimit)
	data := midData{
		Title:    "MID " + midStr,
		MID:      midStr,
		NotFound: !present,
		Providers: provs,
	}
	if present {
		data.Size = size
		data.Blocks = blocks
		data.Sealed = sealed
		data.Codec = codec
		data.ContentType = "" // TODO: surface from gateway if available
		// Default RS layout per constitution (10+4).
		data.DataShards = 10
		data.ParityShards = 4
		data.TotalShards = data.DataShards + data.ParityShards
		data.Health = fmt.Sprintf("%d/%d shards needed", data.DataShards, data.TotalShards)
		data.HealthLabel = "OK"
	}
	e.render(w, "mid.html", data)
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
	midStr := strings.TrimSpace(r.FormValue("mid"))
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

// handleStatic serves an embedded asset file.
func (e *Explorer) handleStatic(name, contentType string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "public, max-age=300")
		data, err := assetFS.ReadFile(path.Join("assets", name))
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
		"index.html":   "index_body",
		"mid.html":     "mid_body",
		"dag.html":     "dag_body",
		"peers.html":   "peers_body",
		"anchors.html": "anchors_body",
		"node.html":    "node_body",
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