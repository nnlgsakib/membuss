package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := []struct {
		in   string
		want slog.Level
	}{
		{"", slog.LevelInfo},
		{"info", slog.LevelInfo},
		{"INFO", slog.LevelInfo},
		{"debug", slog.LevelDebug},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"err", slog.LevelError},
		{"nonsense", slog.LevelInfo},
	}
	for _, c := range cases {
		if got := ParseLevel(c.in); got != c.want {
			t.Errorf("ParseLevel(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestNew_EmitsJSON(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "debug")
	log.Info("hello", "k", "v")
	out := buf.String()
	if !strings.Contains(out, `"msg":"hello"`) {
		t.Errorf("missing msg field in %q", out)
	}
	if !strings.Contains(out, `"k":"v"`) {
		t.Errorf("missing kv in %q", out)
	}
	// JSON must be valid.
	var rec map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &rec); err != nil {
		t.Errorf("non-JSON log line: %v (%q)", err, out)
	}
}

func TestNew_RespectsLevel(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "warn")
	log.Info("hello") // dropped
	log.Warn("warned")
	out := buf.String()
	if strings.Contains(out, "hello") {
		t.Errorf("info line should be dropped at warn level: %q", out)
	}
	if !strings.Contains(out, "warned") {
		t.Errorf("warn line should pass: %q", out)
	}
}

func TestNewDiscard(t *testing.T) {
	log := NewDiscard()
	if log == nil {
		t.Fatal("NewDiscard returned nil")
	}
	log.Info("nothing") // must not panic
}

func TestNew_FiltersMdnsWarning(t *testing.T) {
	var buf bytes.Buffer
	log := New(&buf, "info")
	
	// This log message should be dropped by the filterHandler
	log.Warn("[WARN] mdns: Failed to set multicast interface: setsockopt: An invalid argument was supplied.")
	
	// This normal log message should not be dropped
	log.Info("normal log message")

	out := buf.String()
	if strings.Contains(out, "Failed to set multicast interface") {
		t.Errorf("mDNS setsockopt warning should be filtered out: %q", out)
	}
	if !strings.Contains(out, "normal log message") {
		t.Errorf("normal log message should not be filtered out: %q", out)
	}
}
