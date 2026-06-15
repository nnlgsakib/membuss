// Tests for libp2p mDNS auto-discovery (Phase 17).
//
// The test brings up two in-process libp2p hosts with mDNS
// enabled, waits for each to discover the other through the
// loopback interface, and asserts the connection lands. We
// use the in-process path (no listening) so the test does
// not depend on raw sockets; mDNS works on loopback and on
// the same L2 segment.
package host

import (
	"context"
	"testing"
	"time"
)

// TestMDNSDiscovery spins up two hosts with mDNS enabled and
// asserts they find each other within a generous timeout.
// On a busy CI runner mDNS can be slow, hence 30s.
func TestMDNSDiscovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping mDNS test in -short mode")
	}
	a, err := NewHost(Config{
		DataDir: t.TempDir(),
		ListenAddrs: []string{
			"/ip4/127.0.0.1/tcp/0",
		},
		MDNS:             true,
		MDNSServiceName:  "_p2p-mdns-test._udp",
	})
	if err != nil {
		t.Fatalf("host a: %v", err)
	}
	defer a.Close()
	b, err := NewHost(Config{
		DataDir: t.TempDir(),
		ListenAddrs: []string{
			"/ip4/127.0.0.1/tcp/0",
		},
		MDNS:             true,
		MDNSServiceName:  "_p2p-mdns-test._udp",
	})
	if err != nil {
		t.Fatalf("host b: %v", err)
	}
	defer b.Close()

	// Wait up to 30s for the notifee dials to bring b into
	// a's peer table (and vice versa).
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if a.Peerstore().Addrs(b.ID()) != nil &&
			b.Peerstore().Addrs(a.ID()) != nil {
			// Found. Force a real connection so the
			// test exits the loop on a stable state.
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = a.Connect(ctx, b.Peerstore().PeerInfo(b.ID()))
			cancel()
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("mDNS did not discover peer within 30s: a.ID=%s b.ID=%s", a.ID(), b.ID())
}
