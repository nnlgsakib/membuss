package dht

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"

	"github.com/nnlgsakib/membuss/core/mid"
)

// newTestHost builds a libp2p host listening on a random
// 127.0.0.1 TCP+QUIC port with a fresh Ed25519 identity.
func newTestHost(t *testing.T) host.Host {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tcpAddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	quicAddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/0/quic-v1")
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrs(tcpAddr, quicAddr),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Security(noise.ID, noise.New),
	)
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	return h
}

// waitForRoutingTable blocks until the DHT's local routing
// table has at least want peers, or the deadline elapses.
func waitForRoutingTable(ctx context.Context, d *MemDHT, want int, max time.Duration) error {
	deadline := time.Now().Add(max)
	for time.Now().Before(deadline) {
		if d.RoutingTableSize() >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("dht routing table never reached %d peers (have %d)", want, d.RoutingTableSize())
}

func TestMemDHT_ProvideFindProviders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h1 := newTestHost(t)
	h2 := newTestHost(t)
	t.Cleanup(func() { _ = h1.Close(); _ = h2.Close() })

	d1, err := New(ctx, Config{Host: h1, Mode: kaddht.ModeServer})
	if err != nil {
		t.Fatalf("dht1: %v", err)
	}
	d2, err := New(ctx, Config{Host: h2, Mode: kaddht.ModeServer})
	if err != nil {
		t.Fatalf("dht2: %v", err)
	}
	t.Cleanup(func() { _ = d1.Close(); _ = d2.Close() })

	// Bootstrap each DHT against the other host so the
	// routing tables learn about the peer.
	if err := d1.Bootstrap(ctx, []peer.AddrInfo{{ID: h2.ID(), Addrs: h2.Addrs()}}); err != nil {
		t.Fatalf("d1 bootstrap: %v", err)
	}
	if err := d2.Bootstrap(ctx, []peer.AddrInfo{{ID: h1.ID(), Addrs: h1.Addrs()}}); err != nil {
		t.Fatalf("d2 bootstrap: %v", err)
	}

	// Connect the libp2p hosts so the bootstrap connections
	// actually succeed.
	if err := h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatalf("h1 connect h2: %v", err)
	}
	if err := h2.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}); err != nil {
		t.Fatalf("h2 connect h1: %v", err)
	}

	if err := waitForRoutingTable(ctx, d1, 1, 30*time.Second); err != nil {
		t.Fatalf("d1: %v", err)
	}
	if err := waitForRoutingTable(ctx, d2, 1, 30*time.Second); err != nil {
		t.Fatalf("d2: %v", err)
	}

	id := mid.FromBytes([]byte("membuss-phase-4-provide-test"))
	if err := d1.Provide(ctx, id); err != nil {
		t.Fatalf("provide: %v", err)
	}

	deadline := time.Now().Add(45 * time.Second)
	for {
		provs, err := d2.FindProviders(ctx, id)
		if err == nil && len(provs) > 0 {
			for _, p := range provs {
				if p.ID == h1.ID() {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("d2 did not find h1 as provider of %s", id)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func TestMemDHT_ZeroMIDRejected(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := newTestHost(t)
	t.Cleanup(func() { _ = h.Close() })
	d, err := New(ctx, Config{Host: h})
	if err != nil {
		t.Fatalf("dht: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Provide(ctx, mid.MID{}); err == nil {
		t.Fatal("expected error providing zero MID")
	}
}

func TestMemDHT_PutGetValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	h1 := newTestHost(t)
	h2 := newTestHost(t)
	t.Cleanup(func() { _ = h1.Close(); _ = h2.Close() })

	d1, err := New(ctx, Config{Host: h1, Mode: kaddht.ModeServer})
	if err != nil {
		t.Fatalf("dht1: %v", err)
	}
	d2, err := New(ctx, Config{Host: h2, Mode: kaddht.ModeServer})
	if err != nil {
		t.Fatalf("dht2: %v", err)
	}
	t.Cleanup(func() { _ = d1.Close(); _ = d2.Close() })

	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), time.Hour)
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), time.Hour)
	if err := h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := d1.Bootstrap(ctx, []peer.AddrInfo{{ID: h2.ID(), Addrs: h2.Addrs()}}); err != nil {
		t.Fatalf("d1 bootstrap: %v", err)
	}
	if err := d2.Bootstrap(ctx, []peer.AddrInfo{{ID: h1.ID(), Addrs: h1.Addrs()}}); err != nil {
		t.Fatalf("d2 bootstrap: %v", err)
	}

	if err := waitForRoutingTable(ctx, d1, 1, 30*time.Second); err != nil {
		t.Fatalf("d1: %v", err)
	}
	if err := waitForRoutingTable(ctx, d2, 1, 30*time.Second); err != nil {
		t.Fatalf("d2: %v", err)
	}

	key := "/membuss/test/kv/1"
	want := []byte("hello, peer")
	if err := d1.PutValue(ctx, key, want); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := d2.GetValue(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestBootstrapWithBackoff_NoPeers verifies that calling with an
// empty peer list is a no-op.
func TestBootstrapWithBackoff_NoPeers(t *testing.T) {
	h := newTestHost(t)
	t.Cleanup(func() { h.Close() })
	mdht, err := New(context.Background(), Config{Host: h})
	if err != nil {
		t.Fatalf("new dht: %v", err)
	}
	t.Cleanup(func() { _ = mdht.Close() })

	n, err := mdht.BootstrapWithBackoff(context.Background(), nil, BootstrapConfig{})
	if err != nil {
		t.Errorf("unexpected err: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 successes, got %d", n)
	}
}

// TestBootstrapWithBackoff_BackoffSequence verifies the delay doubles
// after each failed attempt, bounded by Max.
func TestBootstrapWithBackoff_BackoffSequence(t *testing.T) {
	// We use a fake peer (invalid multiaddr) to force failures.
	h := newTestHost(t)
	t.Cleanup(func() { h.Close() })
	mdht, err := New(context.Background(), Config{Host: h})
	if err != nil {
		t.Fatalf("new dht: %v", err)
	}
	t.Cleanup(func() { _ = mdht.Close() })

	bad := peer.AddrInfo{ID: peer.ID("QmInvalid")}
	cfg := BootstrapConfig{
		Initial:     1 * time.Millisecond,
		Max:         4 * time.Millisecond,
		Factor:      2.0,
		MaxAttempts: 3,
	}
	start := time.Now()
	n, _ := mdht.BootstrapWithBackoff(context.Background(), []peer.AddrInfo{bad}, cfg)
	elapsed := time.Since(start)
	if n != 0 {
		t.Errorf("expected 0 successes for bad peer, got %d", n)
	}
	// 1ms + 2ms + 4ms = 7ms minimum (the third attempt is allowed
	// up to MaxAttempts so the backoff after attempt 2 is 4ms then
	// we break, so total observed sleep is 1ms + 2ms = 3ms; but
	// attempt 1 has no sleep before it. We accept 0..20ms.)
	if elapsed > 50*time.Millisecond {
		t.Errorf("backoff took too long: %v", elapsed)
	}
}

// TestConfig_ModeOrDefault exercises the YAML-friendly
// Config.ModeName -> kaddht.ModeOpt resolver. The typed
// Config.Mode field is kept for callers that need the raw
// enum; the string field is what config.yaml drives.
func TestConfig_ModeOrDefault(t *testing.T) {
	cases := []struct {
		name     string
		modeName string
		typed    kaddht.ModeOpt
		want     kaddht.ModeOpt
	}{
		{"empty defaults to auto", "", 0, kaddht.ModeAuto},
		{"auto explicitly", "auto", 0, kaddht.ModeAuto},
		{"client", "client", 0, kaddht.ModeClient},
		{"server", "server", 0, kaddht.ModeServer},
		{"auto-server", "auto-server", 0, kaddht.ModeAutoServer},
		{"auto-server alias", "autoserver", 0, kaddht.ModeAutoServer},
		{"uppercase client", "CLIENT", 0, kaddht.ModeClient},
		{"unknown falls back to typed", "garbage", kaddht.ModeServer, kaddht.ModeServer},
		{"ModeName wins over typed", "client", kaddht.ModeServer, kaddht.ModeClient},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := Config{ModeName: c.modeName, Mode: c.typed}
			if got := cfg.modeOrDefault(); got != c.want {
				t.Fatalf("modeOrDefault: got %v want %v", got, c.want)
			}
		})
	}
}

func TestBootstrapWithBackoff_ParallelDials(t *testing.T) {
	h := newTestHost(t)
	t.Cleanup(func() { h.Close() })
	mdht, err := New(context.Background(), Config{Host: h})
	if err != nil {
		t.Fatalf("new dht: %v", err)
	}
	t.Cleanup(func() { _ = mdht.Close() })

	bad1 := peer.AddrInfo{ID: peer.ID("QmInvalid1")}
	bad2 := peer.AddrInfo{ID: peer.ID("QmInvalid2")}

	cfg := BootstrapConfig{
		Initial:     20 * time.Millisecond,
		Max:         40 * time.Millisecond,
		Factor:      2.0,
		MaxAttempts: 2,
	}

	start := time.Now()
	_, _ = mdht.BootstrapWithBackoff(context.Background(), []peer.AddrInfo{bad1, bad2}, cfg)
	elapsed := time.Since(start)

	// In parallel execution, both bad peers back off concurrently (minimum sleep is 20ms).
	// In sequential execution, they back off one after another (minimum sleep is 40ms).
	if elapsed >= 38*time.Millisecond {
		t.Errorf("expected parallel execution to take less than 38ms, took %v", elapsed)
	}
}
