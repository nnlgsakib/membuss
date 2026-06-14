// Package logging is the Phase 10 structured-logging facade for
// Membuss. It wraps the standard library's log/slog with a
// Membuss-shaped constructor that maps a human-friendly level
// string ("debug", "info", "warn", "error") onto a slog.Level.
//
// Callers obtain a *slog.Logger via New and use it directly; no
// Membuss-specific wrappers are needed.
package logging

import (
	"io"
	"log/slog"
	"strings"
)

// ParseLevel maps a level name to a slog.Level. Empty and
// unknown values fall back to LevelInfo.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "info", "":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error", "err":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New returns a *slog.Logger that writes JSON to w at the level
// indicated by levelStr. The "membuss" attribute is attached to
// every record.
func New(w io.Writer, levelStr string) *slog.Logger {
	if w == nil {
		w = io.Discard
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: ParseLevel(levelStr)})
	return slog.New(h).With("membuss", "daemon")
}

// NewDiscard returns a logger that drops every record. Useful in
// tests that do not care about log output.
func NewDiscard() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
