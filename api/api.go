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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
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

	"github.com/nnlgsakib/membuss/core/keyring"
	"github.com/nnlgsakib/membuss/core/memns"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/obs/metrics"
	membusspb "github.com/nnlgsakib/membuss/proto"
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
	AddDirectory(ctx context.Context, name string, parts []DirectoryPart) (AddResult, error)
	// Ls returns the entries of a MemFS DIR node, sorted
	// lexicographically by name.
	Ls(ctx context.Context, m mid.MID) ([]LsEntry, error)
	// GetPath returns a reader for the file at m/path.
	// Path uses "/" separators and may be empty (returns
	// the file at m itself).
	GetPath(ctx context.Context, m mid.MID, path string) (io.ReadSeekCloser, uint64, string, error)
	// Delete recursively removes the given MID and its children from the store.
	Delete(ctx context.Context, midStr string) (DeleteResult, error)
}

// DeleteResult is the return value of Backend.Delete.
type DeleteResult struct {
	BlocksDeleted uint64
	BytesFreed    uint64
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
	Sealers       int
	AnchorSealers int
}

// PeerInfo is one row of the local peer table.
type PeerInfo struct {
	PeerID   string
	Addrs    []string
	IsAnchor bool
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

	// Phase 18: MemNS and KeyRing fields
	KeyRing       *keyring.KeyRing
	MemNSResolver *memns.Resolver
	LogLevel      string
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
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/api/v1/node/info" || r.URL.Path == "/healthz" {
				next.ServeHTTP(w, r)
				return
			}
			if strings.ToLower(a.cfg.LogLevel) == "debug" {
				middleware.Logger(next).ServeHTTP(w, r)
			} else {
				next.ServeHTTP(w, r)
			}
		})
	})
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
		r.Delete("/delete/{mid}", a.handleDelete)
		r.Get("/healthz", a.handleHealthz)

		// Phase 18: MemNS and KeyRing API routes
		r.Post("/memns/publish", a.handleMemNSPublish)
		r.Get("/memns/resolve/{name}", a.handleMemNSResolve)
		r.Get("/memns/log/{name}", a.handleMemNSLog)
		r.Get("/memlink/resolve/{domain}", a.handleMemLinkResolve)
		r.Post("/keyring/gen", a.handleKeyRingGen)
		r.Get("/keyring/list", a.handleKeyRingList)
		r.Post("/keyring/import", a.handleKeyRingImport)
		r.Get("/keyring/export/{name}", a.handleKeyRingExport)
		r.Delete("/keyring/rm/{name}", a.handleKeyRingRm)
		r.Post("/memns/delegate/add", a.handleMemNSDelegateAdd)
		r.Post("/memns/delegate/rm", a.handleMemNSDelegateRm)
		r.Get("/memns/delegate/list/{keyname}", a.handleMemNSDelegateList)
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
	name := r.URL.Query().Get("name")
	res, err := a.cfg.Backend.AddDirectory(r.Context(), name, parts)
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

func (a *NodeAPI) handleDelete(w http.ResponseWriter, r *http.Request) {
	midStr := chi.URLParam(r, "mid")
	if midStr == "" {
		fail(w, http.StatusBadRequest, fmt.Errorf("delete: mid required"))
		return
	}
	res, err := a.cfg.Backend.Delete(r.Context(), midStr)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}
	ok(w, map[string]any{
		"blocks_deleted": res.BlocksDeleted,
		"bytes_freed":    res.BytesFreed,
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

// APIMemRoute represents the API's route payload mapping.
type APIMemRoute struct {
	Target     string            `json:"target"`
	Weight     uint32            `json:"weight"`
	Label      string            `json:"label"`
	Conditions map[string]string `json:"conditions"`
}

func (a *NodeAPI) handleMemNSPublish(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Key     string        `json:"key"`
		Value   string        `json:"value"`
		TTL     uint64        `json:"ttl"` // in seconds
		Message string        `json:"message"`
		Routes  []APIMemRoute `json:"routes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}

	if a.cfg.KeyRing == nil || a.cfg.MemNSResolver == nil {
		fail(w, http.StatusInternalServerError, errors.New("keyring or resolver not configured"))
		return
	}

	key, err := a.cfg.KeyRing.Get(req.Key)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}

	var seq uint64 = 1
	current, err := memns.ResolveDHT(r.Context(), a.cfg.MemNSResolver.DHTClient(), key.MemNSName)
	if err == nil && current != nil {
		seq = current.Sequence + 1
	}

	ttl := 24 * time.Hour
	if req.TTL > 0 {
		ttl = time.Duration(req.TTL) * time.Second
	}

	var pbRoutes []*membusspb.MemRoute
	for _, rt := range req.Routes {
		pbRoutes = append(pbRoutes, &membusspb.MemRoute{
			Target:     []byte(rt.Target),
			Weight:     rt.Weight,
			Label:      rt.Label,
			Conditions: rt.Conditions,
		})
	}

	var prevLog *membusspb.MemLog
	var prevDelegates [][]byte
	var prevMeta map[string]string
	if current != nil {
		prevLog = current.Changelog
		prevDelegates = current.Delegates
		prevMeta = current.Meta
	}

	record, err := memns.BuildRecord(key, req.Value, seq, ttl, pbRoutes, req.Message)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}

	if prevLog != nil {
		record.Changelog.Entries = append(prevLog.Entries, record.Changelog.Entries...)
	}
	if len(prevDelegates) > 0 {
		record.Delegates = prevDelegates
	}
	if len(prevMeta) > 0 {
		for k, v := range prevMeta {
			if k != "owner_key" {
				record.Meta[k] = v
			}
		}
	}

	errDHT := memns.PublishDHT(r.Context(), a.cfg.MemNSResolver.DHTClient(), key, record)
	if a.cfg.MemNSResolver.PubSub() != nil {
		_ = a.cfg.MemNSResolver.PubSub().PublishPub(r.Context(), key, record)
	}

	if errDHT != nil {
		fail(w, http.StatusInternalServerError, fmt.Errorf("dht publish failed: %w", errDHT))
		return
	}

	_ = a.cfg.KeyRing.SaveRecord(req.Key, record)

	ok(w, map[string]any{
		"name":     key.MemNSName,
		"sequence": record.Sequence,
	})
}

func (a *NodeAPI) handleMemNSResolve(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if a.cfg.MemNSResolver == nil {
		fail(w, http.StatusInternalServerError, errors.New("memns resolver not configured"))
		return
	}

	rec, err := memns.ResolveDHT(r.Context(), a.cfg.MemNSResolver.DHTClient(), name)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}

	routes := []map[string]any{}
	for _, rt := range rec.Routes {
		routes = append(routes, map[string]any{
			"target":     string(rt.Target),
			"weight":     rt.Weight,
			"label":      rt.Label,
			"conditions": rt.Conditions,
		})
	}

	ok(w, map[string]any{
		"value":    string(rec.Value),
		"sequence": rec.Sequence,
		"expires":  time.Unix(0, rec.Validity).UTC().Format(time.RFC3339),
		"routes":   routes,
	})
}

func (a *NodeAPI) handleMemNSLog(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if a.cfg.MemNSResolver == nil {
		fail(w, http.StatusInternalServerError, errors.New("memns resolver not configured"))
		return
	}

	rec, err := memns.ResolveDHT(r.Context(), a.cfg.MemNSResolver.DHTClient(), name)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}

	var entries []map[string]any
	if rec.Changelog != nil {
		for _, e := range rec.Changelog.Entries {
			entries = append(entries, map[string]any{
				"sequence":  e.Sequence,
				"mid":       string(e.Value),
				"timestamp": e.Timestamp,
				"message":   e.Message,
			})
		}
	}

	ok(w, map[string]any{
		"entries": entries,
	})
}

func (a *NodeAPI) handleMemLinkResolve(w http.ResponseWriter, r *http.Request) {
	domain := chi.URLParam(r, "domain")
	if a.cfg.MemNSResolver == nil {
		fail(w, http.StatusInternalServerError, errors.New("memns resolver not configured"))
		return
	}

	dnsRaw := a.cfg.MemNSResolver.DNS()
	if dnsRaw == nil {
		fail(w, http.StatusInternalServerError, errors.New("dns resolver not configured"))
		return
	}

	dns, okResolver := dnsRaw.(interface {
		LookupTXTRecord(domain string) (string, error)
	})
	if !okResolver {
		fail(w, http.StatusInternalServerError, errors.New("dns resolver does not support LookupTXTRecord"))
		return
	}

	rawTxt, err := dns.LookupTXTRecord(domain)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}

	resolved, err := a.cfg.MemNSResolver.Resolve(r.Context(), domain)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}

	ok(w, map[string]any{
		"raw_txt":      rawTxt,
		"resolved_mid": resolved,
	})
}

func (a *NodeAPI) handleKeyRingGen(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}

	if a.cfg.KeyRing == nil {
		fail(w, http.StatusInternalServerError, errors.New("keyring not configured"))
		return
	}

	key, err := a.cfg.KeyRing.Generate(req.Name, req.Type)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}

	ok(w, map[string]any{
		"name":       key.Name,
		"memns_name": key.MemNSName,
	})
}

func (a *NodeAPI) handleKeyRingList(w http.ResponseWriter, r *http.Request) {
	if a.cfg.KeyRing == nil {
		fail(w, http.StatusInternalServerError, errors.New("keyring not configured"))
		return
	}

	list, err := a.cfg.KeyRing.List()
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}

	ok(w, list)
}

func (a *NodeAPI) handleKeyRingImport(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
		PEM  string `json:"pem"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}

	if a.cfg.KeyRing == nil {
		fail(w, http.StatusInternalServerError, errors.New("keyring not configured"))
		return
	}

	err := a.cfg.KeyRing.Import(req.Name, []byte(req.PEM))
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}

	ok(w, map[string]any{"ok": true})
}

func (a *NodeAPI) handleKeyRingExport(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if a.cfg.KeyRing == nil {
		fail(w, http.StatusInternalServerError, errors.New("keyring not configured"))
		return
	}

	pemBytes, err := a.cfg.KeyRing.Export(name)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}

	ok(w, map[string]any{
		"pem": string(pemBytes),
	})
}

func (a *NodeAPI) handleKeyRingRm(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if a.cfg.KeyRing == nil {
		fail(w, http.StatusInternalServerError, errors.New("keyring not configured"))
		return
	}

	err := a.cfg.KeyRing.Delete(name)
	if err != nil {
		fail(w, http.StatusInternalServerError, err)
		return
	}

	ok(w, map[string]any{"ok": true})
}

func (a *NodeAPI) handleMemNSDelegateAdd(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string `json:"name"`
		Delegate string `json:"delegate"` // base64 encoded public key
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}

	delegateBytes, err := base64.StdEncoding.DecodeString(req.Delegate)
	if err != nil {
		fail(w, http.StatusBadRequest, fmt.Errorf("invalid base64 public key: %w", err))
		return
	}

	if a.cfg.KeyRing == nil || a.cfg.MemNSResolver == nil {
		fail(w, http.StatusInternalServerError, errors.New("keyring or resolver not configured"))
		return
	}

	key, err := a.cfg.KeyRing.Get(req.Name)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}

	current, err := memns.ResolveDHT(r.Context(), a.cfg.MemNSResolver.DHTClient(), key.MemNSName)
	if err != nil {
		fail(w, http.StatusInternalServerError, fmt.Errorf("failed to get latest record from DHT: %w", err))
		return
	}

	found := false
	for _, d := range current.Delegates {
		if bytes.Equal(d, delegateBytes) {
			found = true
			break
		}
	}

	if found {
		ok(w, map[string]any{"ok": true, "message": "delegate already exists"})
		return
	}

	current.Delegates = append(current.Delegates, delegateBytes)
	current.Sequence++
	current.Validity = time.Now().Add(24 * time.Hour).UnixNano()

	canonical := memns.CanonicalBytes(current)
	sig, err := key.PrivKey.Sign(canonical)
	if err != nil {
		fail(w, http.StatusInternalServerError, fmt.Errorf("failed to sign record: %w", err))
		return
	}
	current.Signature = sig

	err = memns.PublishDHT(r.Context(), a.cfg.MemNSResolver.DHTClient(), key, current)
	if err != nil {
		fail(w, http.StatusInternalServerError, fmt.Errorf("failed to publish to DHT: %w", err))
		return
	}

	if a.cfg.MemNSResolver.PubSub() != nil {
		_ = a.cfg.MemNSResolver.PubSub().PublishPub(r.Context(), key, current)
	}

	_ = a.cfg.KeyRing.SaveRecord(req.Name, current)

	ok(w, map[string]any{"ok": true})
}

func (a *NodeAPI) handleMemNSDelegateRm(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string `json:"name"`
		Delegate string `json:"delegate"` // base64 encoded public key
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fail(w, http.StatusBadRequest, err)
		return
	}

	delegateBytes, err := base64.StdEncoding.DecodeString(req.Delegate)
	if err != nil {
		fail(w, http.StatusBadRequest, fmt.Errorf("invalid base64 public key: %w", err))
		return
	}

	if a.cfg.KeyRing == nil || a.cfg.MemNSResolver == nil {
		fail(w, http.StatusInternalServerError, errors.New("keyring or resolver not configured"))
		return
	}

	key, err := a.cfg.KeyRing.Get(req.Name)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}

	current, err := memns.ResolveDHT(r.Context(), a.cfg.MemNSResolver.DHTClient(), key.MemNSName)
	if err != nil {
		fail(w, http.StatusInternalServerError, fmt.Errorf("failed to get latest record from DHT: %w", err))
		return
	}

	var newDelegates [][]byte
	found := false
	for _, d := range current.Delegates {
		if bytes.Equal(d, delegateBytes) {
			found = true
		} else {
			newDelegates = append(newDelegates, d)
		}
	}

	if !found {
		fail(w, http.StatusNotFound, errors.New("delegate not found in delegates list"))
		return
	}

	current.Delegates = newDelegates
	current.Sequence++
	current.Validity = time.Now().Add(24 * time.Hour).UnixNano()

	canonical := memns.CanonicalBytes(current)
	sig, err := key.PrivKey.Sign(canonical)
	if err != nil {
		fail(w, http.StatusInternalServerError, fmt.Errorf("failed to sign record: %w", err))
		return
	}
	current.Signature = sig

	err = memns.PublishDHT(r.Context(), a.cfg.MemNSResolver.DHTClient(), key, current)
	if err != nil {
		fail(w, http.StatusInternalServerError, fmt.Errorf("failed to publish to DHT: %w", err))
		return
	}

	if a.cfg.MemNSResolver.PubSub() != nil {
		_ = a.cfg.MemNSResolver.PubSub().PublishPub(r.Context(), key, current)
	}

	_ = a.cfg.KeyRing.SaveRecord(req.Name, current)

	ok(w, map[string]any{"ok": true})
}

func (a *NodeAPI) handleMemNSDelegateList(w http.ResponseWriter, r *http.Request) {
	keyname := chi.URLParam(r, "keyname")
	if a.cfg.KeyRing == nil || a.cfg.MemNSResolver == nil {
		fail(w, http.StatusInternalServerError, errors.New("keyring or resolver not configured"))
		return
	}

	key, err := a.cfg.KeyRing.Get(keyname)
	if err != nil {
		fail(w, http.StatusNotFound, err)
		return
	}

	current, err := memns.ResolveDHT(r.Context(), a.cfg.MemNSResolver.DHTClient(), key.MemNSName)
	if err != nil {
		fail(w, http.StatusInternalServerError, fmt.Errorf("failed to get latest record from DHT: %w", err))
		return
	}

	var delegates []string
	for _, d := range current.Delegates {
		delegates = append(delegates, base64.StdEncoding.EncodeToString(d))
	}

	ok(w, map[string]any{
		"delegates": delegates,
	})
}