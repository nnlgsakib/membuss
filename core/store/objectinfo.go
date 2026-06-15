// Phase 19: per-MID object metadata.
//
// Each sealed MID can carry a small JSON-encoded object
// descriptor alongside its blocks. The descriptor captures
// the user-facing metadata that content-addressed stores
// usually lose: the original filename, the MIME type, and
// any other operator hint. The descriptor is stored under
// the /m/ namespace (see keys.go) keyed by the MID string
// so the same Blockstore write path (PutMeta / GetMeta)
// that already powers GC timestamps and similar side-band
// state can persist it.
//
// The wire format is a tiny JSON document so operators can
// read the descriptor with bbolt / BadgerDB tooling without
// going through Membuss.
package store

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/nnlgsakib/membuss/core/mid"
)

// ObjectInfo is the per-MID metadata captured at Add time
// and replayed on download. It is intentionally small:
// name (used for Content-Disposition), MIME type (used
// for Content-Type / browser render), and a small
// extension point for future fields.
type ObjectInfo struct {
	// Name is the file name the caller supplied at Add
	// time. Empty when the upload did not carry a name
	// (e.g. raw stdin). Used by Mem-Gate as the
	// filename= parameter for Content-Disposition.
	Name string `json:"name,omitempty"`
	// MimeType is the MIME type the caller supplied
	// (e.g. "text/html; charset=utf-8"). When empty,
	// Mem-Gate falls back to a filepath-extension
	// sniff and then to application/octet-stream.
	MimeType string `json:"mime_type,omitempty"`
	// Size is the total content size in bytes.
	// Duplicated from the DAG walk for convenience;
	// the canonical size still comes from
	// countDAG. Stored so the descriptor is
	// self-describing when extracted with DB tooling.
	Size uint64 `json:"size,omitempty"`
}

// objectInfoKey returns the meta key for a MID's
// descriptor. The key shape is "<mid>" so the value lives
// at /m/<mid>, keeping it on the same prefix as other
// per-block metadata.
func objectInfoKey(m mid.MID) string { return "obj/" + m.String() }

// SetObjectInfo writes the descriptor to the store.
func SetObjectInfo(s Blockstore, m mid.MID, info ObjectInfo) error {
	if s == nil {
		return errors.New("store: nil blockstore")
	}
	if m.IsZero() {
		return errors.New("store: zero mid")
	}
	if info.Name == "" && info.MimeType == "" {
		// Nothing worth persisting; an empty
		// descriptor would only burn a row in the
		// meta table.
		return nil
	}
	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return s.PutMeta(objectInfoKey(m), buf)
}

// GetObjectInfo reads the descriptor back. Returns a
// zero ObjectInfo and nil when no descriptor has ever
// been written (the common case for content added by an
// older daemon).
func GetObjectInfo(s Blockstore, m mid.MID) (ObjectInfo, error) {
	if s == nil {
		return ObjectInfo{}, errors.New("store: nil blockstore")
	}
	if m.IsZero() {
		return ObjectInfo{}, errors.New("store: zero mid")
	}
	raw, err := s.GetMeta(objectInfoKey(m))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return ObjectInfo{}, nil
		}
		return ObjectInfo{}, err
	}
	var info ObjectInfo
	if err := json.Unmarshal(raw, &info); err != nil {
		// Corrupt descriptor. Don't fail the
		// caller (Stat must stay best-effort);
		// return a zero value.
		return ObjectInfo{}, nil
	}
	return info, nil
}

// SniffMime returns a best-effort Content-Type for a
// filename. Falls back to application/octet-stream when
// the extension is unknown. The returned string includes
// the charset parameter for text/* so browsers render
// UTF-8 correctly.
func SniffMime(name string) string {
	if name == "" {
		return "application/octet-stream"
	}
	ext := strings.ToLower(name)
	if i := strings.LastIndex(ext, "."); i >= 0 {
		ext = ext[i:]
	} else {
		ext = ""
	}
	switch ext {
	case ".html", ".htm":
		return "text/html; charset=utf-8"
	case ".css":
		return "text/css; charset=utf-8"
	case ".js", ".mjs":
		return "application/javascript; charset=utf-8"
	case ".json":
		return "application/json; charset=utf-8"
	case ".txt", ".log", ".md":
		return "text/plain; charset=utf-8"
	case ".svg":
		return "image/svg+xml"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".pdf":
		return "application/pdf"
	case ".zip":
		return "application/zip"
	case ".tar":
		return "application/x-tar"
	case ".gz":
		return "application/gzip"
	case ".mp3":
		return "audio/mpeg"
	case ".mp4":
		return "video/mp4"
	case ".webm":
		return "video/webm"
	case "":
		return "application/octet-stream"
	}
	return "application/octet-stream"
}
