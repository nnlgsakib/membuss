// Mem-Gate: HTTP gateway that serves Membuss content by MID.
//
// The gateway is a read-only CDN edge. It supports:
//   - GET /mem/{mid}             resolved content (default)
//   - GET /mem/{mid}?format=raw  raw block bytes (no DAG walk)
//   - GET /mem/{mid}?format=dag-json  DAGNode as JSON
//   - HEAD /mem/{mid}            existence + size
//   - GET /mem/{mid}/{path}      DAG path traversal
//   - HTTP Range requests        206 Partial Content
//   - ETag + Cache-Control: immutable
package memgate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/nnlgsakib/membuss/core/memns"
	"github.com/nnlgsakib/membuss/core/mid"
)

// Backend is the contract Mem-Gate depends on. The daemon
// supplies a real implementation; tests inject a memBackend.
type Backend interface {
	// Resolve returns a streaming reader of the content
	// addressed by m. The caller is responsible for closing
	// the reader. The size is the total content size in
	// bytes; it is used for Content-Length / Range math.
	Resolve(ctx context.Context, m mid.MID) (io.ReadCloser, ContentInfo, error)
	// RawBlock returns the bytes of a single block (no DAG
	// walk). Used by ?format=raw.
	RawBlock(ctx context.Context, m mid.MID) ([]byte, error)
	// DAGNodeJSON returns the DAG node at m serialized as
	// JSON: {"mid":..., "links":[...], "size":...}.
	DAGNodeJSON(ctx context.Context, m mid.MID) ([]byte, error)
	// Stat returns a quick metadata snapshot for HEAD.
	Stat(ctx context.Context, m mid.MID) (ContentInfo, error)
	// Ping returns nil if the backend is healthy.
	Ping(ctx context.Context) error

	// --- Phase 17: MemFS support ---

	// MemFSInfo describes a MemFS node. Type is one of
	// "file", "dir", "symlink", "metadata", "raw".
	MemFSInfo(ctx context.Context, m mid.MID) (MemFSInfo, error)
	// MemFSPathGet returns a streaming reader for the file
	// at m/path, with its size and MIME type.
	MemFSPathGet(ctx context.Context, m mid.MID, path string) (io.ReadSeekCloser, uint64, string, error)
	// MemFSList returns the entries of a MemFS directory,
	// sorted lexicographically by name.
	MemFSList(ctx context.Context, m mid.MID) ([]MemFSEntry, error)
}

// MemFSInfo is the metadata returned by Backend.MemFSInfo.
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

// ContentInfo is the metadata returned by Backend.Resolve and
// Backend.Stat. It mirrors the X-Membuss-* response headers.
type ContentInfo struct {
	// MID is the canonical string form of the content's
	// root identifier.
	MID string
	// Size is the total content size in bytes.
	Size uint64
	// Blocks is the number of DAG nodes (or raw blocks)
	// the content consists of.
	Blocks uint64
	// ContentType, if non-empty, is the MIME type returned
	// in the Content-Type response header. Phase 19:
	// preferred over the extension-based sniff when the
	// uploader supplied an explicit MimeType.
	ContentType string
	// Sealed is true if the content is currently pinned in
	// the local store. Surfaced as X-Membuss-Sealed.
	Sealed bool
	// Name is the file name the uploader supplied (or the
	// basename of the source file when nothing was set).
	// Surfaced as the filename= parameter of
	// Content-Disposition and as X-Membuss-Name.
	Name string
	// MimeType is the explicit MIME type the uploader
	// supplied (or empty, in which case Mem-Gate falls
	// back to a filepath-extension sniff and then
	// application/octet-stream). Surfaced as
	// X-Membuss-MimeType.
	MimeType string
}

// Config configures a MemGate.
type Config struct {
	// Backend serves the actual content. Required.
	Backend Backend
	// MaxCacheBytes caps the in-memory LRU cache. Zero
	// disables caching. Defaults to 64 MiB.
	MaxCacheBytes uint64
	// ReadTimeout is the per-request read timeout. Defaults
	// to 30s.
	ReadTimeout time.Duration
	// WriteTimeout is the per-request write timeout.
	// Defaults to 60s.
	WriteTimeout time.Duration
	// ExplorerHandler, if non-nil, is mounted under
	// /explorer/* on the same router. The Mem-Gate
	// caller is responsible for constructing the
	// explorer with the appropriate backend.
	ExplorerHandler http.Handler
	// RateLimitPerMin is the per-source-IP request budget
	// enforced on the public Mem-Gate server, evaluated
	// per minute. Zero disables rate limiting. Default 100.
	RateLimitPerMin int

	// Phase 18: MemNS resolver
	MemNSResolver *memns.Resolver
}

// MemGate is the public HTTP gateway.
type MemGate struct {
	cfg    Config
	router chi.Router

	// lru is a small in-memory cache for hot MIDs. The
	// implementation is a map + doubly-linked list kept
	// under a single mutex; it does not need to scale
	// beyond the configured MaxCacheBytes.
	lru *lru

	// ipLimiter enforces a per-source-IP request budget on
	// public routes. nil disables rate limiting.
	ipLimiter *ipLimiter
}

// New returns a MemGate ready to be served. The returned
// router exposes /mem/{mid}/... and a /healthz endpoint.
func New(cfg Config) (*MemGate, error) {
	if cfg.Backend == nil {
		return nil, errors.New("memgate: nil backend")
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 60 * time.Second
	}
	if cfg.MaxCacheBytes == 0 {
		cfg.MaxCacheBytes = 64 * 1024 * 1024
	}

	mg := &MemGate{cfg: cfg, lru: newLRU(cfg.MaxCacheBytes)}
	mg.ipLimiter = newIPLimiter(cfg.RateLimitPerMin, 10*time.Minute)
	mg.router = mg.buildRouter()
	return mg, nil
}

// Router returns the chi router. Exposed so tests can drive
// the gateway via httptest.
func (m *MemGate) Router() http.Handler { return m.router }

// Handler returns an http.Handler wrapping the router with
// the gateway's timeouts applied. The daemon wires this into
// http.Server.
func (m *MemGate) Handler() http.Handler {
	return m.router
}

func (m *MemGate) buildRouter() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthz" || strings.HasPrefix(r.URL.Path, "/explorer") {
				next.ServeHTTP(w, r)
				return
			}
			middleware.Logger(next).ServeHTTP(w, r)
		})
	})
	r.Use(middleware.Recoverer)
	// Rate limit BEFORE route dispatch so a flood cannot pin
	// a single request handler. /healthz is intentionally not
	// exempted; operators who want it open can disable the
	// limiter by setting RateLimitPerMin=0 in config.
	r.Use(m.ipLimiter.Middleware)

	// Phase 18: custom domain serving middleware
	r.Use(m.customDomainMiddleware)

	r.Get("/healthz", m.handleHealth)
	r.Get("/mem/{mid}", m.handleGet)
	r.Head("/mem/{mid}", m.handleHead)
	// Directory listing (HTML or JSON) when the path
	// component is empty.
	r.Get("/mem/{mid}/", m.handleDirList)
	r.Get("/mem/{mid}/*", m.handlePathGet)

	// Phase 18: MemNS and MemLink routes
	r.Get("/memns/{name}", m.handleMemNSResolve)
	r.Get("/memns/{name}/*", m.handleMemNSResolve)
	r.Get("/memlink/{domain}", m.handleMemLinkGet)
	r.Get("/memlink/{domain}/*", m.handleMemLinkGet)

	if m.cfg.ExplorerHandler != nil {
		r.Mount("/explorer", m.cfg.ExplorerHandler)
	}
	return r
}

// --- handlers ---

func (m *MemGate) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := m.cfg.Backend.Ping(r.Context()); err != nil {
		http.Error(w, "backend not ready: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (m *MemGate) handleHead(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	root, err := mid.Parse(midStr)
	if err != nil {
		http.Error(w, "bad mid: "+err.Error(), http.StatusBadRequest)
		return
	}
	info, err := m.cfg.Backend.Stat(r.Context(), root)
	if err != nil {
		http.Error(w, "stat: "+err.Error(), http.StatusNotFound)
		return
	}
	h := w.Header()
	h.Set("X-Membuss-MID", info.MID)
	h.Set("X-Membuss-Size", strconv.FormatUint(info.Size, 10))
	h.Set("X-Membuss-Blocks", strconv.FormatUint(info.Blocks, 10))
	h.Set("X-Membuss-Sealed", strconv.FormatBool(info.Sealed))
	h.Set("ETag", `"`+info.MID+`"`)
	h.Set("Accept-Ranges", "bytes")
	h.Set("Content-Length", strconv.FormatUint(info.Size, 10))
	// Content-Type: prefer the uploader-supplied MimeType,
	// fall back to the path-based ContentType the backend
	// filled in. Both are surfaced so the test suite and
	// direct API consumers can tell them apart.
	ct := info.MimeType
	if ct == "" {
		ct = info.ContentType
	}
	if ct != "" {
		h.Set("Content-Type", ct)
	}
	h.Set("X-Membuss-Name", info.Name)
	h.Set("X-Membuss-MimeType", info.MimeType)
	h.Set("Cache-Control", "public, immutable, max-age=31536000")
	w.WriteHeader(http.StatusOK)
}

func (m *MemGate) handleGet(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	root, err := mid.Parse(midStr)
	if err != nil {
		http.Error(w, "bad mid: "+err.Error(), http.StatusBadRequest)
		return
	}
	q := r.URL.Query()

	// Phase 19: resolve the content info FIRST so we can
	// set the right Content-Type and Content-Disposition
	// headers before the body handler writes a single byte.
	// The CDN-style behavior is: explicit ?download=1
	// forces attachment, ?view=1 (or absence of either)
	// defaults to inline so the browser renders images,
	// text, video, etc. directly.
	preInfo, preErr := m.cfg.Backend.Stat(r.Context(), root)
	if preErr == nil {
		if (preInfo.MimeType == "inode/directory" || preInfo.ContentType == "inode/directory") && q.Get("format") == "" {
			http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
			return
		}
		if preInfo.MimeType != "" && w.Header().Get("Content-Type") == "" {
			w.Header().Set("Content-Type", preInfo.MimeType)
		}
		// Default filename for Content-Disposition. The
		// uploader-supplied name wins; falls back to
		// <mid>.bin. The disposition is set below.
		name := preInfo.Name
		if name == "" {
			name = midStr + ".bin"
		}
		disp := "inline"
		if q.Get("download") == "1" {
			disp = "attachment"
			if fn := q.Get("filename"); fn != "" {
				name = fn
			}
		}
		// Once-only: do not overwrite a header the
		// caller set explicitly (e.g. tests).
		if w.Header().Get("Content-Disposition") == "" {
			w.Header().Set("Content-Disposition",
				mime.FormatMediaType(disp, map[string]string{"filename": sanitizeFilename(name)}))
		}
		// X-Membuss-* mirrors for tooling that cannot
		// read Content-Disposition reliably.
		w.Header().Set("X-Membuss-Name", preInfo.Name)
		w.Header().Set("X-Membuss-MimeType", preInfo.MimeType)
	}

	switch q.Get("format") {
	case "dag-json":
		m.handleDAGJSON(w, r, root)
	case "raw":
		m.handleRawBlock(w, r, root)
	default:
		m.handleResolved(w, r, root, midStr)
	}
}

// handleDirList renders a directory listing as HTML or JSON,
// depending on the ?format query parameter. The handler is
// mounted at /mem/{mid}/ (trailing slash, no path component).
func (m *MemGate) handleDirList(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	root, err := mid.Parse(midStr)
	if err != nil {
		http.Error(w, "bad mid: "+err.Error(), http.StatusBadRequest)
		return
	}
	info, err := m.cfg.Backend.MemFSInfo(r.Context(), root)
	if err != nil {
		http.Error(w, "memfs: "+err.Error(), http.StatusNotFound)
		return
	}
	if info.Type != "dir" {
		// Non-directory MIDs at the bare /mem/{mid}/ URL
		// fall back to the legacy resolved-content handler.
		m.handleResolved(w, r, root, midStr)
		return
	}
	entries, err := m.cfg.Backend.MemFSList(r.Context(), root)
	if err != nil {
		http.Error(w, "memfs: "+err.Error(), http.StatusNotFound)
		return
	}
	// Auto-serve index.html if it exists and ?format is not set
	if r.URL.Query().Get("format") == "" {
		hasIndex := false
		for _, e := range entries {
			if e.Name == "index.html" {
				hasIndex = true
				break
			}
		}
		if hasIndex {
			m.serveMemFSPath(w, r, midStr, "index.html")
			return
		}
	}
	if r.URL.Query().Get("format") == "json" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"mid":     root.String(),
			"type":    info.Type,
			"size":    info.Size,
			"entries": entries,
		})
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `<!doctype html><html><head><title>%s</title>`+
		`<style>body{font-family:system-ui;max-width:60rem;margin:2rem auto;padding:0 1rem}`+
		`table{border-collapse:collapse;width:100%%}td,th{padding:.5rem;text-align:left;border-bottom:1px solid #eee}`+
		`a{color:#06c;text-decoration:none}a:hover{text-decoration:underline}`+
		`.muted{color:#888;font-size:.9em}</style></head><body>`+
		`<h1>Directory <code>%s</code></h1>`+
		`<table><tr><th>Name</th><th>Type</th><th>Size</th><th>MID</th></tr>`,
		html.EscapeString(root.String()), html.EscapeString(root.String()))
	for _, e := range entries {
		fmt.Fprintf(w, `<tr><td><a href="/mem/%s/%s">%s</a></td>`+
			`<td>%s</td><td>%d</td>`+
			`<td class="muted"><a href="/explorer/mid/%s">%s…</a></td></tr>`,
			url.PathEscape(root.String()), url.PathEscape(e.Name), html.EscapeString(e.Name), html.EscapeString(e.Type), e.Size, url.PathEscape(e.MID), html.EscapeString(shortMID(e.MID)))
	}
	fmt.Fprintf(w, `</table></body></html>`)
}

// shortMID returns the first 16 characters of a public MID,
// suitable for display in a table. The remainder is hidden
// so a directory listing does not become a wall of text.
func shortMID(s string) string {
	if len(s) <= 16 {
		return s
	}
	return s[:16]
}

func (m *MemGate) handlePathGet(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	innerPath := chi.URLParam(r, "*")
	m.serveMemFSPath(w, r, midStr, innerPath)
}

func (m *MemGate) handleDAGJSON(w http.ResponseWriter, r *http.Request, root mid.MID) {
	body, err := m.cfg.Backend.DAGNodeJSON(r.Context(), root)
	if err != nil {
		http.Error(w, "dag-json: "+err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Membuss-MID", root.String())
	w.Header().Set("ETag", `"`+root.String()+`"`)
	w.Header().Set("Cache-Control", "public, immutable, max-age=31536000")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (m *MemGate) handleRawBlock(w http.ResponseWriter, r *http.Request, root mid.MID) {
	data, err := m.cfg.Backend.RawBlock(r.Context(), root)
	if err != nil {
		http.Error(w, "raw: "+err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("X-Membuss-MID", root.String())
	w.Header().Set("ETag", `"`+root.String()+`"`)
	w.Header().Set("Cache-Control", "public, immutable, max-age=31536000")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// handleResolved serves the resolved content. The hot path
// is: cache hit -> write directly. Miss -> Backend.Resolve ->
// copy with optional range slicing.
func (m *MemGate) handleResolved(w http.ResponseWriter, r *http.Request, root mid.MID, midStr string) {
	// Cache lookup. LRU is keyed on the public MID string
	// so that a malicious or buggy caller cannot trigger
	// cache growth by spamming raw-block requests.
	if data, ok := m.lru.get(midStr); ok {
		m.writeBytes(w, r, midStr, data, detectContentType(midStr, data, ""))
		return
	}

	rc, info, err := m.cfg.Backend.Resolve(r.Context(), root)
	if err != nil {
		http.Error(w, "resolve: "+err.Error(), http.StatusNotFound)
		return
	}
	defer rc.Close()

	// If we know the size and the reader is just a chunk
	// stream, we can stream the response without buffering
	// the entire content. Cache eligibility: the content
	// must fit under MaxCacheBytes.
	stream := true
	var buf []byte
	if info.Size > 0 && info.Size <= m.cfg.MaxCacheBytes {
		// Read fully and cache. The whole content fits in
		// the configured cache envelope.
		buf = make([]byte, info.Size)
		if _, err := io.ReadFull(rc, buf); err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			http.Error(w, "read: "+err.Error(), http.StatusBadGateway)
			return
		}
		m.lru.put(midStr, buf)
		stream = false
	} else {
		// Streamed path. Don't cache.
		w.Header().Set("Content-Type", chooseContentType(info.ContentType, "application/octet-stream"))
		w.Header().Set("X-Membuss-MID", info.MID)
		w.Header().Set("X-Membuss-Size", strconv.FormatUint(info.Size, 10))
		w.Header().Set("X-Membuss-Blocks", strconv.FormatUint(info.Blocks, 10))
		w.Header().Set("X-Membuss-Sealed", strconv.FormatBool(info.Sealed))
		w.Header().Set("ETag", `"`+info.MID+`"`)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Cache-Control", "public, immutable, max-age=31536000")
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, rc)
		return
	}

	_ = stream
	m.writeBytes(w, r, midStr, buf, chooseContentType(info.ContentType, detectContentType(midStr, buf, "")))
}

// writeBytes writes data to w honoring an optional Range
// header. It sets the X-Membuss-* and caching headers
// expected by clients.
func (m *MemGate) writeBytes(w http.ResponseWriter, r *http.Request, midStr string, data []byte, contentType string) {
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("X-Membuss-MID", midStr)
	h.Set("X-Membuss-Size", strconv.Itoa(len(data)))
	h.Set("ETag", `"`+midStr+`"`)
	h.Set("Accept-Ranges", "bytes")
	h.Set("Cache-Control", "public, immutable, max-age=31536000")

	if rng := r.Header.Get("Range"); rng != "" {
		start, end, err := parseRange(rng, int64(len(data)))
		if err != nil {
			http.Error(w, "bad range: "+err.Error(), http.StatusRequestedRangeNotSatisfiable)
			return
		}
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, len(data)))
		h.Set("Content-Length", strconv.FormatInt(end-start, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start:end])
		return
	}
	h.Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

// parseRange parses a single-range "bytes=start-end" header
// and returns the half-open [start,end) bounds. Multi-range
// requests are not supported (yet) and return an error.
func parseRange(s string, size int64) (int64, int64, error) {
	const prefix = "bytes="
	if !strings.HasPrefix(s, prefix) {
		return 0, 0, fmt.Errorf("range unit must be bytes")
	}
	spec := strings.TrimPrefix(s, prefix)
	if strings.Contains(spec, ",") {
		return 0, 0, fmt.Errorf("multi-range not supported")
	}
	dash := strings.IndexByte(spec, '-')
	if dash < 0 {
		return 0, 0, fmt.Errorf("range missing dash")
	}
	startStr := strings.TrimSpace(spec[:dash])
	endStr := strings.TrimSpace(spec[dash+1:])

	if size < 0 {
		return 0, 0, fmt.Errorf("negative size")
	}
	var start, end uint64
	var err error
	if startStr == "" {
		// Suffix range: last N bytes.
		n, perr := strconv.ParseUint(endStr, 10, 64)
		if perr != nil || n == 0 {
			return 0, 0, fmt.Errorf("bad suffix length")
		}
		if n > uint64(size) {
			n = uint64(size)
		}
		start = uint64(size) - n
		end = uint64(size)
		return int64(start), int64(end), nil
	}
	start, err = strconv.ParseUint(startStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("bad start: %w", err)
	}
	if endStr == "" {
		end = uint64(size)
	} else {
		end, err = strconv.ParseUint(endStr, 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("bad end: %w", err)
		}
		if size > 0 && end >= uint64(size-1) {
			end = uint64(size)
		} else {
			end++ // bytes=N-M is inclusive of M
		}
	}
	if start >= uint64(size) || end <= start || end > uint64(size) {
		return 0, 0, fmt.Errorf("range out of bounds")
	}
	return int64(start), int64(end), nil
}

// detectContentType picks a MIME type using (in order):
//   1. The Mid's suffix if the MID looks like a path with an
//      extension. MIDs do not normally carry extensions, so
//      this is rarely useful.
//   2. http.DetectContentType on the first 512 bytes.
//   3. application/octet-stream as a fallback.
func detectContentType(midStr string, data []byte, override string) string {
	if override != "" {
		return override
	}
	if ext := filepath.Ext(midStr); ext != "" {
		if ct := mime.TypeByExtension(ext); ct != "" {
			return ct
		}
	}
	if len(data) > 0 {
		return http.DetectContentType(data)
	}
	return "application/octet-stream"
}

func chooseContentType(info, fallback string) string {
	if info != "" {
		return info
	}
	return fallback
}

// sanitizeFilename strips control characters, quotes, and invalid
// characters to prevent header and HTML injection.
func sanitizeFilename(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '_'
		}
		switch r {
		case '"', '\\', '/', '<', '>', '|', ':', '*', '?':
			return '_'
		}
		return r
	}, s)
}

// --- LRU ---

// lru is a small bounded-byte LRU. The data structure is
// hand-rolled so the package does not pull in a heavy
// dependency. For a CDN edge, a simple list + map is more
// than adequate.
type lru struct {
	mu       sync.Mutex
	maxBytes uint64
	curBytes uint64
	// items is ordered most-recent-first.
	items map[string]*listEntry
	head  *listEntry
	tail  *listEntry
}

type listEntry struct {
	key  string
	data []byte
	prev *listEntry
	next *listEntry
}

func newLRU(maxBytes uint64) *lru {
	return &lru{maxBytes: maxBytes, items: make(map[string]*listEntry)}
}

func (l *lru) get(key string) ([]byte, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.items[key]
	if !ok {
		return nil, false
	}
	l.moveToFront(e)
	return e.data, true
}

func (l *lru) put(key string, data []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if e, ok := l.items[key]; ok {
		l.curBytes -= uint64(len(e.data))
		e.data = data
		l.curBytes += uint64(len(data))
		l.moveToFront(e)
	} else {
		e := &listEntry{key: key, data: data}
		l.items[key] = e
		l.curBytes += uint64(len(data))
		l.pushFront(e)
	}
	for l.curBytes > l.maxBytes && l.tail != nil {
		old := l.tail
		l.remove(old)
		delete(l.items, old.key)
		l.curBytes -= uint64(len(old.data))
	}
}

func (l *lru) moveToFront(e *listEntry) {
	if l.head == e {
		return
	}
	l.remove(e)
	l.pushFront(e)
}

func (l *lru) pushFront(e *listEntry) {
	e.prev = nil
	e.next = l.head
	if l.head != nil {
		l.head.prev = e
	}
	l.head = e
	if l.tail == nil {
		l.tail = e
	}
}

func (l *lru) remove(e *listEntry) {
	if e.prev != nil {
		e.prev.next = e.next
	} else {
		l.head = e.next
	}
	if e.next != nil {
		e.next.prev = e.prev
	} else {
		l.tail = e.prev
	}
	e.prev, e.next = nil, nil
}

// len returns the current number of entries.
func (l *lru) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.items)
}

// Bytes returns the current cache size in bytes.
func (l *lru) bytes() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.curBytes
}

// MaxBytes returns the configured cache cap.
func (l *lru) max() uint64 { return l.maxBytes }

// MarshalJSON renders the cache as a small JSON object.
func (l *lru) MarshalJSON() ([]byte, error) {
	type view struct {
		Entries int    `json:"entries"`
		Bytes   uint64 `json:"bytes"`
		Max     uint64 `json:"max_bytes"`
	}
	v := view{Entries: l.len(), Bytes: l.bytes(), Max: l.max()}
	return json.Marshal(v)
}

// Phase 18: custom domain serving middleware.
func (m *MemGate) customDomainMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if idx := strings.Index(host, ":"); idx != -1 {
			host = host[:idx]
		}

		// Don't intercept localhost or local IPs
		if host == "localhost" || host == "127.0.0.1" || host == "::1" {
			next.ServeHTTP(w, r)
			return
		}

		path := r.URL.Path
		// Don't intercept system paths
		if strings.HasPrefix(path, "/mem/") ||
			strings.HasPrefix(path, "/healthz") ||
			strings.HasPrefix(path, "/explorer") ||
			strings.HasPrefix(path, "/memns/") ||
			strings.HasPrefix(path, "/memlink/") {
			next.ServeHTTP(w, r)
			return
		}

		if m.cfg.MemNSResolver == nil {
			http.Error(w, "MemNS resolver not configured", http.StatusInternalServerError)
			return
		}

		// Resolve domain to MID
		resolved, err := m.cfg.MemNSResolver.Resolve(r.Context(), host)
		if err != nil {
			http.Error(w, "Domain not found or resolve failed: "+err.Error(), http.StatusNotFound)
			return
		}

		midStr := resolved
		if strings.HasPrefix(midStr, "/mem/") {
			midStr = midStr[5:]
		}

		innerPath := strings.TrimPrefix(path, "/")
		m.serveMemFSPath(w, r, midStr, innerPath)
	})
}

func (m *MemGate) handleMemNSResolve(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	innerPath := chi.URLParam(r, "*")

	if m.cfg.MemNSResolver == nil {
		http.Error(w, "MemNS resolver not configured", http.StatusInternalServerError)
		return
	}

	resolved, err := m.cfg.MemNSResolver.Resolve(r.Context(), name)
	if err != nil {
		http.Error(w, "memns resolve failed: "+err.Error(), http.StatusNotFound)
		return
	}

	midStr := resolved
	if strings.HasPrefix(midStr, "/mem/") {
		midStr = midStr[5:]
	}

	if strings.HasPrefix(resolved, "https://") || strings.HasPrefix(resolved, "/ipfs/") {
		http.Redirect(w, r, resolved, http.StatusTemporaryRedirect)
		return
	}

	m.serveMemFSPath(w, r, midStr, innerPath)
}

func (m *MemGate) handleMemLinkGet(w http.ResponseWriter, r *http.Request) {
	domain := chi.URLParam(r, "domain")
	innerPath := chi.URLParam(r, "*")

	if m.cfg.MemNSResolver == nil {
		http.Error(w, "MemNS resolver not configured", http.StatusInternalServerError)
		return
	}

	resolved, err := m.cfg.MemNSResolver.Resolve(r.Context(), domain)
	if err != nil {
		http.Error(w, "memlink resolve failed: "+err.Error(), http.StatusNotFound)
		return
	}

	midStr := resolved
	if strings.HasPrefix(midStr, "/mem/") {
		midStr = midStr[5:]
	}

	m.serveMemFSPath(w, r, midStr, innerPath)
}

func (m *MemGate) serveMemFSPath(w http.ResponseWriter, r *http.Request, midStr, innerPath string) {
	root, err := mid.Parse(midStr)
	if err != nil {
		http.Error(w, "bad mid: "+err.Error(), http.StatusBadRequest)
		return
	}

	info, err := m.cfg.Backend.MemFSInfo(r.Context(), root)
	isDir := err == nil && info.Type == "dir"

	if isDir && innerPath == "" && !strings.HasSuffix(r.URL.Path, "/") {
		http.Redirect(w, r, r.URL.Path+"/", http.StatusMovedPermanently)
		return
	}

	if isDir && innerPath == "" {
		entries, err := m.cfg.Backend.MemFSList(r.Context(), root)
		if err == nil {
			hasIndex := false
			for _, e := range entries {
				if e.Name == "index.html" {
					hasIndex = true
					break
				}
			}
			if hasIndex {
				innerPath = "index.html"
			}
		}
	}

	if innerPath == "" {
		m.handleResolved(w, r, root, midStr)
		return
	}

	rc, size, mimeType, err := m.cfg.Backend.MemFSPathGet(r.Context(), root, innerPath)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			http.Error(w, "not found: "+err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, "memfs: "+err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer rc.Close()

	ct := mimeType
	if ct == "" {
		ct = "application/octet-stream"
	}
	filename := filepath.Base(innerPath)
	w.Header().Set("Content-Type", ct)
	w.Header().Set("X-Membuss-MID", midStr)
	w.Header().Set("X-Membuss-Path", "/"+innerPath)
	w.Header().Set("X-Membuss-Name", filename)
	w.Header().Set("X-Membuss-MimeType", ct)
	if w.Header().Get("Content-Disposition") == "" {
		w.Header().Set("Content-Disposition", mime.FormatMediaType("inline", map[string]string{"filename": sanitizeFilename(filename)}))
	}
	if size > 0 {
		w.Header().Set("Content-Length", strconv.FormatUint(size, 10))
	}
	w.Header().Set("ETag", `"`+midStr+"/"+innerPath+`"`)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "public, immutable, max-age=31536000")
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}
