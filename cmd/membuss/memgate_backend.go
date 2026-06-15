// memgateAdapter is the production implementation of
// memgate.Backend, backed by the daemonBackend. Resolve
// falls back to Memex when the requested MID is not local.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/nnlgsakib/membuss/core/dag"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
	"github.com/nnlgsakib/membuss/net/memex"
	memgate "github.com/nnlgsakib/membuss/gateway/memgate"
)

var _ memgate.Backend = (*memgateAdapter)(nil)

// memgateAdapter wraps daemonBackend to satisfy
// memgate.Backend. The two backends have similarly-named
// methods that take different parameter types, so we keep
// them on a separate type.
type memgateAdapter struct {
	b *daemonBackend
}

func newMemgateAdapter(b *daemonBackend) *memgateAdapter { return &memgateAdapter{b: b} }

// Resolve returns the bytes of a MID. If the MID is not
// local, the adapter asks the DHT for providers and runs a
// Memex session to fetch it.
func (a *memgateAdapter) Resolve(ctx context.Context, m mid.MID) (io.ReadCloser, memgate.ContentInfo, error) {
	b := a.b
	has, err := b.store.Has(m)
	if err != nil {
		return nil, memgate.ContentInfo{}, err
	}
	if !has && b.memex != nil && b.dht != nil {
		provCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		provs, perr := b.dht.FindProviders(provCtx, m)
		cancel()
		if perr == nil && len(provs) > 0 {
			sess, serr := memex.NewSession(memex.SessionConfig{
				Engine:    b.memex,
				Root:      m,
				Providers: provs,
				Timeout:   30 * time.Second,
			})
			if serr == nil {
				if _, ferr := sess.Fetch(ctx); ferr == nil {
					has = true
				}
			}
		}
	}
	if !has {
		return nil, memgate.ContentInfo{}, errMGNotFound
	}
	blocks, size, err := countDAG(b.store, m)
	if err != nil {
		return nil, memgate.ContentInfo{}, err
	}
	sealed, _ := b.store.IsSealed(m)
	resolver := dag.NewResolver(b.store)
	rc, err := resolver.Resolve(m, nil)
	if err != nil {
		return nil, memgate.ContentInfo{}, err
	}
	// Phase 19: surface the uploader-supplied name and
	// MIME type so the gateway can serve with the right
	// Content-Type and Content-Disposition.
	oi, _ := store.GetObjectInfo(b.store, m)
	return io.NopCloser(rc), memgate.ContentInfo{
		MID:        m.String(),
		Size:       size,
		Blocks:     blocks,
		Sealed:     sealed,
		Name:       oi.Name,
		MimeType:   oi.MimeType,
		// ContentType is the legacy / extension-based
		// fallback. The handleGet handler prefers
		// MimeType when both are set.
		ContentType: detectContentType(m.String(), nil, oi.MimeType),
	}, nil
}

// RawBlock returns the raw bytes of a single block (no DAG
// walk).
func (a *memgateAdapter) RawBlock(ctx context.Context, m mid.MID) ([]byte, error) {
	b := a.b
	has, err := b.store.Has(m)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, errMGNotFound
	}
	return b.store.Get(m)
}

// DAGNodeJSON returns the DAG node at m serialized as JSON.
func (a *memgateAdapter) DAGNodeJSON(ctx context.Context, m mid.MID) ([]byte, error) {
	b := a.b
	has, err := b.store.Has(m)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, errMGNotFound
	}
	raw, err := b.store.Get(m)
	if err != nil {
		return nil, err
	}
	// Try to decode as a DAGNode. Leaf nodes are raw bytes
	// and have no link list; the JSON view reflects that.
	links, _ := parseDAGLinks(raw)
	size := uint64(len(raw))
	view := map[string]any{
		"mid":   m.String(),
		"size":  size,
		"links": links,
	}
	return json.Marshal(view)
}

// Stat returns a quick metadata snapshot.
func (a *memgateAdapter) Stat(ctx context.Context, m mid.MID) (memgate.ContentInfo, error) {
	b := a.b
	has, err := b.store.Has(m)
	if err != nil {
		return memgate.ContentInfo{}, err
	}
	if !has {
		return memgate.ContentInfo{}, errMGNotFound
	}
	blocks, size, err := countDAG(b.store, m)
	if err != nil {
		return memgate.ContentInfo{}, err
	}
	sealed, _ := b.store.IsSealed(m)
	// Phase 19: surface Name + MimeType from the
	// per-MID ObjectInfo (see core/store.ObjectInfo).
	oi, _ := store.GetObjectInfo(b.store, m)
	return memgate.ContentInfo{
		MID:         m.String(),
		Size:        size,
		Blocks:      blocks,
		Sealed:      sealed,
		Name:        oi.Name,
		MimeType:    oi.MimeType,
		ContentType: detectContentType(m.String(), nil, oi.MimeType),
	}, nil
}

// Ping is a no-op health check.
func (a *memgateAdapter) Ping(ctx context.Context) error {
	if a.b.store == nil {
		return errors.New("no store")
	}
	return nil
}

var errMGNotFound = errors.New("not found")

// detectContentType is exported so the memgate handler can
// use it. It prefers the ContentType the client provided as
// a hint, then sniffs the bytes, and finally falls back to
// "application/octet-stream".
func detectContentType(midStr string, data []byte, hint string) string {
	if hint != "" {
		return hint
	}
	if ext := filepath.Ext(midStr); ext != "" {
		if ct := mimeByExt(ext); ct != "" {
			return ct
		}
	}
	if len(data) > 0 {
		return http.DetectContentType(data)
	}
	return "application/octet-stream"
}

func mimeByExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".html", ".htm":
		return "text/html; charset=utf-8"
	case ".json":
		return "application/json"
	case ".txt":
		return "text/plain; charset=utf-8"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".pdf":
		return "application/pdf"
	default:
		return ""
	}
}
