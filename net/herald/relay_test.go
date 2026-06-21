package herald

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fakePublisher records every PublishAsRelay call so the
// tests can assert on the cadence.
type fakePublisher struct {
	calls            atomic.Int64
	err              error
	routingTableSize int
}

func (f *fakePublisher) PublishAsRelay(_ context.Context) error {
	f.calls.Add(1)
	return f.err
}

func (f *fakePublisher) RoutingTableSize() int {
	if f.routingTableSize == -1 {
		return 0
	}
	if f.routingTableSize == 0 {
		return 1 // default to 1 so tests run immediately
	}
	return f.routingTableSize
}

// TestNewRelayAnnouncer_NilDHT confirms the constructor
// rejects a zero DHT.
func TestNewRelayAnnouncer_NilDHT(t *testing.T) {
	if _, err := NewRelayAnnouncer(RelayAnnouncer{}); err == nil {
		t.Fatal("NewRelayAnnouncer with nil DHT: want error, got nil")
	}
}

// TestNewRelayAnnouncer_AppliesDefaults confirms the
// zero-value config gets a non-zero Interval and a non-nil
// Now function.
func TestNewRelayAnnouncer_AppliesDefaults(t *testing.T) {
	pub := &fakePublisher{}
	r, err := NewRelayAnnouncer(RelayAnnouncer{DHT: pub})
	if err != nil {
		t.Fatalf("NewRelayAnnouncer: %v", err)
	}
	if r.Interval <= 0 {
		t.Fatalf("Interval = %s, want > 0", r.Interval)
	}
	if r.Now == nil {
		t.Fatal("Now not defaulted")
	}
}

// TestRelayAnnouncer_StartFiresImmediate asserts that Start
// fires one publish asynchronously after routing table is populated.
func TestRelayAnnouncer_StartFiresImmediate(t *testing.T) {
	pub := &fakePublisher{}
	r, err := NewRelayAnnouncer(RelayAnnouncer{
		DHT:               pub,
		Interval:          200 * time.Millisecond,
		BootCheckInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRelayAnnouncer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.Start(ctx)

	// Poll until the asynchronous publish happens
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if pub.calls.Load() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := pub.calls.Load(); got < 1 {
		t.Fatalf("Start did not fire an immediate publish (calls=%d)", got)
	}
}

// TestRelayAnnouncer_LoopTicks asserts the ticker fires a
// second publish within a small multiple of the configured
// interval.
func TestRelayAnnouncer_LoopTicks(t *testing.T) {
	pub := &fakePublisher{}
	r, err := NewRelayAnnouncer(RelayAnnouncer{
		DHT:               pub,
		Interval:          30 * time.Millisecond,
		BootCheckInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRelayAnnouncer: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	r.Start(ctx)
	// Poll for >=2 calls. We expect 1 immediate + >=1 ticker
	// within the 500ms window.
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if pub.calls.Load() >= 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("loop fired %d times, want >= 2", pub.calls.Load())
}

// TestRelayAnnouncer_PropagatesError ensures the error from
// PublishAsRelay is returned by RunOnce (the daemon does
// not use the return value, but tests do).
func TestRelayAnnouncer_PropagatesError(t *testing.T) {
	want := errors.New("dht down")
	pub := &fakePublisher{err: want}
	r, err := NewRelayAnnouncer(RelayAnnouncer{DHT: pub})
	if err != nil {
		t.Fatalf("NewRelayAnnouncer: %v", err)
	}
	if err := r.RunOnce(context.Background()); !errors.Is(err, want) {
		t.Fatalf("RunOnce err = %v, want %v", err, want)
	}
}

// TestRelayAnnouncer_Start_DelayedRoutingTable asserts that Start does not
// announce until the routing table has at least 1 peer.
func TestRelayAnnouncer_Start_DelayedRoutingTable(t *testing.T) {
	pub := &fakePublisher{
		routingTableSize: -1, // initially empty (returns 0)
	}
	r, err := NewRelayAnnouncer(RelayAnnouncer{
		DHT:               pub,
		Interval:          10 * time.Second,
		BootCheckInterval: 5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewRelayAnnouncer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.Start(ctx)

	// Wait a bit and check that no calls were made because RoutingTableSize == 0
	time.Sleep(50 * time.Millisecond)
	if got := pub.calls.Load(); got != 0 {
		t.Fatalf("expected 0 calls when routing table is empty, got %d", got)
	}

	// Now populate the routing table
	pub.routingTableSize = 1

	// Wait and check that the announce was made
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if pub.calls.Load() >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if got := pub.calls.Load(); got < 1 {
		t.Fatalf("expected call to be made after routing table populated, got %d", got)
	}
}
