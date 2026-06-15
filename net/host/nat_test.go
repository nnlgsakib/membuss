package host

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestHost_NATStatus_DefaultUnknown checks that a freshly
// constructed in-process host reports "unknown" reachability
// when no AutoNAT probe has been run. We compare case-
// insensitively because the underlying libp2p String()
// method capitalises its verdict ("Unknown").
func TestHost_NATStatus_DefaultUnknown(t *testing.T) {
	h, err := NewHost(Config{InProcess: true})
	if err != nil {
		t.Fatalf("in-process host: %v", err)
	}
	defer h.Close()
	got := strings.ToLower(h.NATStatus())
	if got != "unknown" {
		t.Fatalf("NATStatus = %q, want %q", got, "unknown")
	}
	if h.IsPublic() {
		t.Fatal("IsPublic = true on in-process host")
	}
	if h.IsPrivate() {
		t.Fatal("IsPrivate = true on in-process host")
	}
}

// TestHost_WaitForNAT_ImmediateNoWait asserts that a 0/negative
// timeout returns immediately with the current status.
func TestHost_WaitForNAT_ImmediateNoWait(t *testing.T) {
	h, err := NewHost(Config{InProcess: true})
	if err != nil {
		t.Fatalf("in-process host: %v", err)
	}
	defer h.Close()

	start := time.Now()
	got, err := h.WaitForNAT(context.Background(), 0)
	if err != nil {
		t.Fatalf("WaitForNAT: %v", err)
	}
	if strings.ToLower(got) != "unknown" {
		t.Fatalf("got %q, want unknown", got)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("WaitForNAT blocked %s on 0 timeout", elapsed)
	}
}

// TestHost_WaitForNAT_TimeoutFires checks that a short
// timeout on a host that never gets an AutoNAT verdict
// returns ctx.DeadlineExceeded (or context-deadline-exceeded
// wrapped) without hanging.
func TestHost_WaitForNAT_TimeoutFires(t *testing.T) {
	h, err := NewHost(Config{InProcess: true})
	if err != nil {
		t.Fatalf("in-process host: %v", err)
	}
	defer h.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	got, err := h.WaitForNAT(ctx, 200*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatalf("WaitForNAT returned nil err, want timeout; got=%q", got)
	}
	if elapsed < 150*time.Millisecond {
		t.Fatalf("returned too early: %s", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("took too long: %s", elapsed)
	}
}

// TestHost_PersistentIdentity_NATFieldsDontBreak verifies
// that the new Phase 11 fields in Config do not break the
// existing persistent-identity smoke test (the existing
// TestNewHost_PersistentIdentity already exercises this;
// this test is a more explicit regression guard).
func TestHost_PersistentIdentity_NATFieldsDontBreak(t *testing.T) {
	dir := t.TempDir()
	h, err := NewHost(Config{
		DataDir:             dir,
		RelayService:        false,
		RelayMaxConns:       128,
		RelayMaxReservations: 128,
		RelayBandwidthMB:    16,
		ForceRelay:          false,
	})
	if err != nil {
		t.Fatalf("NewHost: %v", err)
	}
	defer h.Close()
	if h.ID().String() == "" {
		t.Fatal("empty peer id")
	}
}
