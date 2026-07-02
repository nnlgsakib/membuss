package dht

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/nnlgsakib/membuss/net/host"
)

func TestDHT_BlacklistConnectionGating(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create host 1 listening on random local port
	h1, err := host.NewHost(host.Config{
		DataDir:     t.TempDir(),
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
	})
	if err != nil {
		t.Fatalf("failed to create host1: %v", err)
	}
	defer h1.Close()

	// Create host 2
	h2, err := host.NewHost(host.Config{
		DataDir:     t.TempDir(),
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
	})
	if err != nil {
		t.Fatalf("failed to create host2: %v", err)
	}
	defer h2.Close()

	// Initially we can connect
	err = h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()})
	if err != nil {
		t.Fatalf("initial connect failed: %v", err)
	}

	// Blacklist h2 on h1
	err = h1.BlockPeer(h2.ID())
	if err != nil {
		t.Fatalf("failed to block peer: %v", err)
	}

	// Verify h2 is blocked
	if !h1.IsPeerBlocked(h2.ID()) {
		t.Error("expected peer to be blocked")
	}

	// Try to connect again -> should fail
	err = h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()})
	if err == nil {
		t.Fatal("expected connection to fail after blacklisting, but got success")
	}
}

func TestDHT_BlacklistFilters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h1, err := host.NewHost(host.Config{
		DataDir:     t.TempDir(),
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
	})
	if err != nil {
		t.Fatalf("host1: %v", err)
	}
	defer h1.Close()

	h2, err := host.NewHost(host.Config{
		DataDir:     t.TempDir(),
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
	})
	if err != nil {
		t.Fatalf("host2: %v", err)
	}
	defer h2.Close()

	// Initialize d1 with connection gater
	d1, err := New(ctx, Config{
		Host:            h1,
		ModeName:        "server",
		ConnectionGater: h1.ConnectionGater(),
	})
	if err != nil {
		t.Fatalf("d1: %v", err)
	}
	defer d1.Close()

	// Initially, check that we can filter a non-blocked peer
	if h1.IsPeerBlocked(h2.ID()) {
		t.Fatal("h2 should not be blocked initially")
	}

	// Block h2 on h1
	_ = h1.BlockPeer(h2.ID())

	// Verify that the routing table filter would reject h2
	if d1.dht.RoutingTable().Find(h2.ID()) != "" {
		t.Fatal("blocked peer should not be found or kept in routing table")
	}
}
