// Package api implements the local Node control API. The
// Node API is intended for operator scripts and CI jobs that
// need to drive the daemon programmatically without going
// through the gRPC or the public Mem-Gate gateway.
//
// All routes return a JSON envelope of the form:
//
//	{"ok": true,  "data": {...}}
//	{"ok": false, "error": "..."}
//
// The API is mounted under /api/v1 by the daemon.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/obs/metrics"
)

// Backend is the contract the Node API depends on. The
// daemon supplies a real implementation; tests inject a
// memBackend.
type Backend interface {
	// Add ingests a reader and returns the resulting MID +
	// size. The optional name hints at the original
	// filename (used for Content-Type sniffing).
	Add(ctx context.Context, name string, r io.Reader) (AddResult, error)
	// Resolve returns the bytes of a MID.
	Resolve(ctx context.Context, m mid.MID) (io.ReadCloser, uint64, error)
	// Seal pins a MID.
	Seal(ctx context.Context, m mid.MID) (SealResult, error)
	// Unseal removes the pin.
	Unseal(ctx context.Context, m mid.MID) (uint64, error)
	// Stat returns a metadata snapshot.
	Stat(ctx context.Context, m mid.MID) (StatInfo, error)
	// Peers returns the local peer table.
	Peers(limit int) ([]PeerInfo, error)
	// GC runs garbage collection.
	GC(ctx context.Context) (GCInfo, error)
	// NodeInfo returns the local node's identity.
	NodeInfo() NodeInfo

	// --- Phase 17: MemFS support ---

	// AddFile ingests a file and returns the MID of the
	// MemFS FILE node wrapping it. When wrapDir is true
	// the FILE node is wrapped in a single-entry DIR node
	// and the DIR MID is returned.
	AddFile(ctx context.Context, name string, r io.Reader, wrapDir bool) (AddResult, error)
	// AddDirectory ingests a directory as MemFS. The
	// multipart parts are passed as (path, reader) pairs;
	// the implementation reconstructs the tree.
	AddDirectory(ctx context.Context, parts []DirectoryPart) (AddResult, error)
	// Ls returns the entries of a MemFS DIR node, sorted
	// lexicographically by name.
	Ls(ctx context.Context, m mid.MID) ([]LsEntry, error)
	// GetPath returns a reader for the file at m/path.
	// Path uses "/" separators and may be empty (returns
	// the file at m itself).
	GetPath(ctx context.Context, m mid.MID, path string) (io.ReadSeekCloser, uint64, string, error)
}

// DirectoryPart is one file in a multipart directory upload.
type DirectoryPart struct {
	// Path is the slash-separated path of the file
	// relative to the directory root (e.g. "src/main.go").
	Path string
	// Size is the content length in bytes, when known.
	Size int64
	// Name is the original filename (basename of Path).
	Name string
	// Data is the file content, fully buffered in memory.
	// We read it from the multipart part in the handler
	// because the request body is closed by the time
	// the Backend.AddDirectory method runs, which would
	// otherwise yield an empty stream.
	Data []byte
}

// LsEntry is one row of /api/v1/ls.
type LsEntry struct {
	Name string `json:"name"`
	MID  string `json:"mid"`
	Type string `json:"type"`
	Size uint64 `json:"size"`
}

// AddResult is the return value of Backend.Add.
type AddResult struct {
	MID      string
	Size     uint64
	Blocks   uint64
	// Name and MimeType are the per-MID ObjectInfo
	// that the HTTP API captured at upload time.
	Name     string
	MimeType string
}

// SealResult is the return value of Backend.Seal.
type SealResult struct {
	Pinned  uint64
	Already bool
}

// StatInfo is the return value of Backend.Stat.
type StatInfo struct {
	Present  bool
	Size     uint64
	Blocks   uint64
	Sealed   bool
	// Name and MimeType are the per-MID ObjectInfo
	// captured at Add time, or empty for content
	// added by an older daemon.
	Name     string
	MimeType string
}

// PeerInfo is one row of the local peer table.
type PeerInfo struct {
	PeerID string
	Addrs  []string
}

// GCInfo is the return value of Backend.GC.
type GCInfo struct {
	BytesFreed uint64
	BlocksKept uint64
}

// NodeInfo is the local node's identity.
type NodeInfo struct {
	PeerID  string
	Addrs   []string
	Version string
	Build   string
}

// Config configures a NodeAPI.
type Config struct {
	Backend Backend
	// MaxUploadBytes caps POST /api/v1/add bodies. Default
	// is 1 GiB.
	MaxUploadBytes int64
	// ReadTimeout, WriteTimeout bound each request.
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	// APIKey, if non-empty, is the shared secret required in
	// the X-Membuss-Key header for every request. When empty
	// the API is unauthenticated.
	APIKey string
	// Metrics, if non-nil, is exposed at GET /metrics.
	Metrics *metrics.Metrics
}

// NodeAPI is the local HTTP control API.
type NodeAPI struct {
	cfg    Config
	router chi.Router
}

// New returns a NodeAPI. The returned router is mounted
// under /api/v1 by Handler().
func New(cfg Config) (*NodeAPI, error) {
	if cfg.Backend == nil {
		return nil, errors.New("api: nil backend")
	}
	if cfg.MaxUploadBytes == 0 {
		cfg.MaxUploadBytes = 1 << 30
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 30 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 5 * time.Minute
	}
	a := &NodeAPI{cfg: cfg}
	a.router = a.buildRouter()
	return a, nil
}

// Handler returns an http.Handler that wraps the router with
// the configured timeouts.
func (a *NodeAPI) Handler() http.Handler {
	return http.TimeoutHandler(a.router, a.cfg.WriteTimeout, `{"ok":false,"error":"timeout"}`)
}

// Router returns the bare chi router (used by tests).
func (a *NodeAPI) Router() http.Handler { return a.router }

func (a *NodeAPI) buildRouter() chi.Router {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(a.cfg.ReadTimeout))

	// Prometheus scrape endpoint, exempt from API-key auth.
	if a.cfg.Metrics != nil {
		r.Method("GET", "/metrics", a.cfg.Metrics.Handler())
	}
	r.Route("/api/v1", func(r chi.Router) {
		// Enforce API-key auth on every /api/v1 request when
		// configured. /metrics and /healthz remain open.
		r.Use(apiKeyAuth(a.cfg.APIKey))
		r.Post("/add", a.handleAdd)
		r.Post("/add/dir", a.handleAddDir)
		r.Get("/get/{mid}", a.handleGet)
		r.Get("/get/{mid}/{path:*}", a.handleGet)
		r.Get("/ls/{mid}", a.handleLs)
		r.Post("/seal/{mid}", a.handleSeal)
		r.Delete("/seal/{mid}", a.handleUnseal)
		r.Get("/stat/{mid}", a.handleStat)
		r.Get("/peers", a.handlePeers)
		r.Get("/node/info", a.handleNodeInfo)
		r.Post("/gc", a.handleGC)
		r.Get("/healthz", a.handleHealthz)
	})
	return r
}

// --- response envelope ---

// envelope is the standard JSON wrapper.
type envelope struct {
	OK    bool   `json:"ok"`
	Data  any    `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, env envelope) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(env)
}

func ok(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusOK, envelope{OK: true, Data: data})
}

func created(w http.ResponseWriter, data any) {
	writeJSON(w, http.StatusCreated, envelope{OK: true, Data: data})
}

func fail(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, envelope{OK: false, Error: err.Error()})
}

// --- handlers ---

func (a *NodeAPI) handleHealthz(w http.ResponseWriter, r *http.Request) {
	ok(w, map[string]bool{"ok": true})
}

func (a *NodeAPI) handleAdd(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength > a.cfg.MaxUploadBytes {
		fail(w, http.StatusRequestEntityTooLarge, fmt.Errorf("upload too large: %d > %d", r.ContentLength, a.cfg.MaxUploadBytes))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, a.cfg.MaxUploadBytes)
	defer r.Body.Close()

	var (
		reader io.Reader = r.Body
		name             = "upload"
	)
	// Multipart upload? Pull the first file part. Otherwise
	// treat the raw body as the payload.
	ct := r.Header.Get("Content-Type")
	if len(ct) >= 19 && ct[:19] == "multipart/form-data" {
		mr, err := r.MultipartReader()
		if err != nil {
			fail(w, http.StatusBadRequest, fmt.Errorf("multipart: %w", err))
			return
		}
		part, err := mr.NextPart()
		if err != nil {
			fail(w, http.StatusBadRequest, fmt.Errorf("multipart: no part: %w", err))
			return
		}
		defer part.Close()
		if part.FileName() != "" {
			name = part.FileName()
		}
		reader = part
	}
	// Phase 17: ?wrap=dir wraps the file in a single-entry
	// DIR node so the returned MID is addressable via the
	// /mem/{mid}/ gateway path.
	wrap := r.URL.Query().Get("wrap") == "dir"
	name = sanitizePath(name)
	res, err := a.cfg.Backend.AddFile(r.Context(), name, reader, wrap)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	created(w, map[string]any{
		"mid":    res.MID,
		"size":   res.Size,
		"blocks": res.Blocks,
		"name":   res.Name,
		"mime":   res.MimeType,
	})
}

// handleAddDir ingests a directory via multipart upload.
// Each part must have a "X-Membuss-Path" header set to the
// relative path of the file. Parts without that header are
// silently skipped.
func (a *NodeAPI) handleAddDir(w http.ResponseWriter, r *http.Request) {
	if r.ContentLength > a.cfg.MaxUploadBytes {
		fail(w, http.StatusRequestEntityTooLarge, fmt.Errorf("upload too large: %d > %d", r.ContentLength, a.cfg.MaxUploadBytes))
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, a.cfg.MaxUploadBytes)
	defer r.Body.Close()

	ct := r.Header.Get("Content-Type")
	if len(ct) < 19 || ct[:19] != "multipart/form-data" {
		fail(w, http.StatusBadRequest, fmt.Errorf("directory upload requires multipart/form-data"))
		return
	}
	mr, err := r.MultipartReader()
	if err != nil {
		fail(w, http.StatusBadRequest, fmt.Errorf("multipart: %w", err))
		return
	}
	var parts []DirectoryPart
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			fail(w, http.StatusBadRequest, fmt.Errorf("multipart: %w", err))
			return
		}
		path := part.FileName()
		if p := part.Header.Get("X-Membuss-Path"); p != "" {
			path = p
		}
		if path == "" {
			part.Close()
			continue
		}
		path = sanitizePath(path)
		// Read the part body fully while the request is still
		// open. The request body is closed by the defer above
		// as soon as this handler returns, so any consumer
		// downstream that reads p.Data lazily would see 0
		// bytes. Buffering the bytes here keeps the Backend
		// interface simple (no Close hooks, no time-of-check
		// races) at the cost of a transient memory copy per
		// file.
		data, err := io.ReadAll(part)
		_ = part.Close()
		if err != nil {
			fail(w, http.StatusBadRequest, fmt.Errorf("multipart: read %q: %w", path, err))
			return
		}
		parts = append(parts, DirectoryPart{
			Path: path,
			Name: sanitizePath(part.FileName()),
			Data: data,
		})
	}
	if len(parts) == 0 {
		fail(w, http.StatusBadRequest, fmt.Errorf("no files in directory upload"))
		return
	}
	res, err := a.cfg.Backend.AddDirectory(r.Context(), parts)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	created(w, map[string]any{
		"mid":    res.MID,
		"size":   res.Size,
		"blocks": res.Blocks,
	})
}

func (a *NodeAPI) handleGet(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	m, err := mid.Parse(midStr)
	if err != nil {
		fail(w, http.StatusBadRequest, fmt.Errorf("bad mid: %w", err))
		return
	}
	// Phase 17: /api/v1/get/{mid}/{path...} streams a
	// specific file inside a MemFS directory. When the
	// route carries additional path segments, we resolve
	// through the MemFS resolver.
	if path := r.URL.Path; strings.Contains(path[len("/api/v1/get/"):], "/") {
		// path is e.g. /api/v1/get/{mid}/a/b.txt. Strip
		// the prefix to extract the inner path.
		rel := strings.TrimPrefix(path, "/api/v1/get/")
		// rel now begins with the MID, followed by / and
		// the inner path. Split on the first slash after
		// the MID.
		idx := strings.Index(rel, "/")
		if idx < 0 {
			// No inner path; fall through to plain Get.
			idx = len(rel)
		}
		inner := rel[idx:]
		if inner != "" {
			rc, size, mime, err := a.cfg.Backend.GetPath(r.Context(), m, inner)
			if err != nil {
				fail(w, http.StatusNotFound, err)
				return
			}
			defer rc.Close()
			ct := mime
			if ct == "" {
				ct = "application/octet-stream"
			}
			w.Header().Set("Content-Type", ct)
			w.Header().Set("X-Membuss-MID", m.String())
			if size > 0 {
				w.Header().Set("Content-Length", strconv.FormatUint(size, 10))
			}
			w.WriteHeader(http.StatusOK)
			_, _ = io.Copy(w, rc)
			return
		}
	}
	rc, size, err := a.cfg.Backend.Resolve(r.Context(), m)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Membuss-MID", m.String())
	w.Header().Set("Content-Length", strconv.FormatUint(size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

// handleLs returns the entries of a MemFS directory.
func (a *NodeAPI) handleLs(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	m, err := mid.Parse(midStr)
	if err != nil {
		fail(w, http.StatusBadRequest, fmt.Errorf("bad mid: %w", err))
		return
	}
	entries, err := a.cfg.Backend.Ls(r.Context(), m)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	ok(w, map[string]any{
		"mid":     m.String(),
		"entries": entries,
		"count":   len(entries),
	})
}

func (a *NodeAPI) handleSeal(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	m, err := mid.Parse(midStr)
	if err != nil {
		fail(w, http.StatusBadRequest, fmt.Errorf("bad mid: %w", err))
		return
	}
	res, err := a.cfg.Backend.Seal(r.Context(), m)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	ok(w, map[string]any{
		"mid":     m.String(),
		"pinned":  res.Pinned,
		"already": res.Already,
	})
}

func (a *NodeAPI) handleUnseal(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	m, err := mid.Parse(midStr)
	if err != nil {
		fail(w, http.StatusBadRequest, fmt.Errorf("bad mid: %w", err))
		return
	}
	n, err := a.cfg.Backend.Unseal(r.Context(), m)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	ok(w, map[string]any{
		"mid":     m.String(),
		"removed": n,
	})
}

func (a *NodeAPI) handleStat(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	m, err := mid.Parse(midStr)
	if err != nil {
		fail(w, http.StatusBadRequest, fmt.Errorf("bad mid: %w", err))
		return
	}
	info, err := a.cfg.Backend.Stat(r.Context(), m)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}
	if !info.Present {
		fail(w, http.StatusNotFound, fmt.Errorf("not present"))
		return
	}
	ok(w, map[string]any{
		"mid":    m.String(),
		"size":   info.Size,
		"blocks": info.Blocks,
		"sealed": info.Sealed,
	})
}

func (a *NodeAPI) handlePeers(w http.ResponseWriter, r *http.Request) {
	limit := 0
	if s := r.URL.Query().Get("limit"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			limit = n
		}
	}
	peers, err := a.cfg.Backend.Peers(limit)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	ok(w, map[string]any{
		"peers": peers,
		"total": len(peers),
	})
}

func (a *NodeAPI) handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	info := a.cfg.Backend.NodeInfo()
	ok(w, map[string]any{
		"peer_id": info.PeerID,
		"addrs":   info.Addrs,
		"version": info.Version,
		"build":   info.Build,
	})
}

func (a *NodeAPI) handleGC(w http.ResponseWriter, r *http.Request) {
	info, err := a.cfg.Backend.GC(r.Context())
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	ok(w, map[string]any{
		"bytes_freed": info.BytesFreed,
		"blocks_kept": info.BlocksKept,
	})
}

// --- helpers ---

// newRequestID is a tiny helper used by the middleware stack
// to attach a request id if none is present.
func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// sanitizePath strips control characters, quotes, and invalid
// characters to prevent HTML injection in downstream consumers.
// It permits slashes (/) so directory structures are preserved.
func sanitizePath(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '_'
		}
		switch r {
		case '"', '\\', '<', '>', '|', ':', '*', '?':
			return '_'
		}
		return r
	}, s)
}