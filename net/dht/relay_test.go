package dht

import (
	"context"
	"testing"
	"time"
)

// TestRelaysKey_Stable guards the DHT key from accidental
// renames. Renaming the key is a wire-format change: any
// in-flight relay announcement under the old key becomes
// invisible to FindRelays.
func TestRelaysKey_Stable(t *testing.T) {
	want := "/membuss/relays/v1"
	if RelaysKey != want {
		t.Fatalf("RelaysKey = %q, want %q", RelaysKey, want)
	}
}

// TestFindRelays_Empty exercises the no-published-record
// path: FindRelays must return (nil-or-empty, nil) on a DHT
// that has no record yet, so the caller can fall back to a
// static config list.
func TestFindRelays_Empty(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newTestHost(t)
	t.Cleanup(func() { _ = h.Close() })

	md, err := New(ctx, Config{Host: h})
	if err != nil {
		t.Fatalf("dht: %v", err)
	}
	t.Cleanup(func() { _ = md.Close() })

	// Fresh DHT: no Put has happened, so the lookup misses.
	got, err := md.FindRelays(ctx, 8)
	if err != nil {
		t.Fatalf("FindRelays on empty dht: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("FindRelays returned %d entries, want 0", len(got))
	}
}

// TestPublishAsRelay_NoAddrs is the in-process host case: a
// host that has no addrs cannot publish itself, so
// PublishAsRelay must be a silent no-op (returning nil).
func TestPublishAsRelay_NoAddrs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Build a host with NoListenAddrs so Addrs() returns
	// an empty slice. We reuse newTestHost's identity
	// plumbing but force no listen addrs.
	host, _, err := genInProcessHost()
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	defer host.Close()

	md, err := New(ctx, Config{Host: host})
	if err != nil {
		t.Fatalf("dht: %v", err)
	}
	defer md.Close()

	if err := md.PublishAsRelay(ctx); err != nil {
		t.Fatalf("PublishAsRelay with no addrs: %v", err)
	}
}
