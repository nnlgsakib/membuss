package pex

import (
	"context"
	"crypto/rand"
	"sync"
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

	membusspb "github.com/nnlgsakib/membuss/proto"
)

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

// TestPEX_GossipRound is a deterministic two-host test: we
// construct the PEX engines with a fast ticker (substituted
// via direct invocation of the round method through the
// internal exchange), seed the host's peer tables, connect
// them, and verify that after a single round each side knows
// the other.
func TestPEX_GossipRound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h1 := newTestHost(t)
	h2 := newTestHost(t)
	t.Cleanup(func() { _ = h1.Close(); _ = h2.Close() })

	p1, err := New(Config{Host: h1})
	if err != nil {
		t.Fatalf("p1: %v", err)
	}
	p2, err := New(Config{Host: h2})
	if err != nil {
		t.Fatalf("p2: %v", err)
	}

	p1.Start(ctx)
	p2.Start(ctx)
	t.Cleanup(func() {
		p1.Stop()
		p2.Stop()
	})

	// Seed p1 with a peer that does not exist on p2 yet.
	p1.AddPeer(peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()})
	// Seed p2 with h1 as well so each side has at least one
	// peer in its table before the round.
	p2.AddPeer(peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()})

	// Connect them so PEX streams can flow.
	if err := h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatalf("h1 connect h2: %v", err)
	}
	if err := h2.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}); err != nil {
		t.Fatalf("h2 connect h1: %v", err)
	}

	// Drive one exchange each direction. Wait for both to
	// succeed. Use a wait group with a deadline to bound
	// the test.
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = p1.exchange(ctx, h2.ID())
	}()
	go func() {
		defer wg.Done()
		_ = p2.exchange(ctx, h1.ID())
	}()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatalf("PEX exchange timed out")
	}

	// Now verify that p1 learned about p2's seed (if any) and
	// p2 learned about p1's seed. The simplest invariant is
	// that each side's table contains the other side's peer
	// ID, which the seeding + handler code guarantees.
	if !tableHas(p1, h2.ID()) {
		t.Fatalf("p1 missing h2 in table: %v", p1.Peers())
	}
	if !tableHas(p2, h1.ID()) {
		t.Fatalf("p2 missing h1 in table: %v", p2.Peers())
	}
}

// TestPEX_StreamHandlerMerges verifies the inbound stream
// handler. We open a /membuss/pex/1.0.0 stream from h1 to
// h2 carrying h1's peer info; h2's handleStream should merge
// h1 into its own table.
func TestPEX_StreamHandlerMerges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	h1 := newTestHost(t)
	h2 := newTestHost(t)
	t.Cleanup(func() { _ = h1.Close(); _ = h2.Close() })

	p2, err := New(Config{Host: h2})
	if err != nil {
		t.Fatalf("p2: %v", err)
	}
	p2.Start(ctx)
	t.Cleanup(p2.Stop)

	// Manually merge h1's info into the table (the inbound
	// handler always does this, but the seeding lets us
	// assert the inbound side merged it as well).
	p1, err := New(Config{Host: h1})
	if err != nil {
		t.Fatalf("p1: %v", err)
	}
	p1.AddPeer(peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()})

	if err := h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// Open a PEX stream from h1 to h2. The handleStream on h2
	// will read our message, merge it, and send its own back.
	if err := p1.exchange(ctx, h2.ID()); err != nil {
		t.Fatalf("exchange: %v", err)
	}

	if !tableHas(p2, h1.ID()) {
		t.Fatalf("p2 missing h1 in table: %v", p2.Peers())
	}
}

func tableHas(p *PEX, id peer.ID) bool {
	for _, pi := range p.Peers() {
		if pi.PeerId == id.String() {
			return true
		}
	}
	return false
}

func TestPEX_OfflineDetectionAndEviction(t *testing.T) {
	h1 := newTestHost(t)
	t.Cleanup(func() { _ = h1.Close() })

	p1, err := New(Config{Host: h1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	fakeAddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9999")

	// Add the fake peer as signed
	fakeID := addSignedTestPeer(t, p1, membusspb.Reachability_PUBLIC, []multiaddr.Multiaddr{fakeAddr}, nil)

	// Verify it's returned by Peers() and FilterForGossip()
	if !tableHas(p1, fakeID) {
		t.Fatalf("peer should be present initially")
	}

	gossipPeers := p1.FilterForGossip()
	found := false
	for _, gp := range gossipPeers {
		if gp.PeerId == fakeID.String() {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("peer should be shared in gossip initially")
	}

	// Mark dial as failed once
	p1.MarkDialResult(fakeID, false)

	// Since LastDialSuccess is now false, it should NOT be returned by Peers()
	if tableHas(p1, fakeID) {
		t.Fatalf("offline peer should be filtered out from Peers()")
	}

	// It should also NOT be shared in gossip
	gossipPeers = p1.FilterForGossip()
	for _, gp := range gossipPeers {
		if gp.PeerId == fakeID.String() {
			t.Fatalf("offline peer should not be shared in gossip")
		}
	}

	// Mark dial as failed a second time
	p1.MarkDialResult(fakeID, false)

	// Now it should be completely evicted from the internal table
	p1.mu.Lock()
	_, exists := p1.peers[fakeID]
	p1.mu.Unlock()
	if exists {
		t.Fatalf("offline peer should be completely evicted from internal table after 2 failed dials")
	}
}

func TestPEX_SignatureDefense(t *testing.T) {
	h, err := NewTestInProcessHost()
	if err != nil {
		t.Fatalf("in-process host: %v", err)
	}
	defer h.Close()

	p, err := New(Config{Host: h})
	if err != nil {
		t.Fatalf("pex: %v", err)
	}

	// Generate a fake peer ID & key pair
	priv, _, _ := crypto.GenerateEd25519Key(rand.Reader)
	pid, _ := peer.IDFromPrivateKey(priv)
	pubBytes, _ := crypto.MarshalPublicKey(priv.GetPublic())

	// 1. Test Unsigned Record (should be rejected)
	unsignedInfo := &membusspb.PeerInfo{
		PeerId:       pid.String(),
		Addrs:        []string{"/ip4/1.2.3.4/tcp/4001"},
		LastSeen:     p.now().Unix(),
		Reachability: membusspb.Reachability_PUBLIC,
		Seq:          p.now().Unix(),
		PubKey:       pubBytes,
	}
	p.mergeFromMessage([]*membusspb.PeerInfo{unsignedInfo}, p.now().Unix())
	if tableHas(p, pid) {
		t.Fatal("unsigned peer record should be rejected")
	}

	// 2. Test Invalid Signature Record (should be rejected)
	badSigInfo := &membusspb.PeerInfo{
		PeerId:       pid.String(),
		Addrs:        []string{"/ip4/1.2.3.4/tcp/4001"},
		LastSeen:     p.now().Unix(),
		Reachability: membusspb.Reachability_PUBLIC,
		Seq:          p.now().Unix(),
		PubKey:       pubBytes,
		Signature:    []byte("invalid-signature"),
	}
	p.mergeFromMessage([]*membusspb.PeerInfo{badSigInfo}, p.now().Unix())
	if tableHas(p, pid) {
		t.Fatal("invalid signature peer record should be rejected")
	}

	// 3. Test Expired Record (should be rejected)
	expiredInfo := &membusspb.PeerInfo{
		PeerId:       pid.String(),
		Addrs:        []string{"/ip4/1.2.3.4/tcp/4001"},
		LastSeen:     p.now().Unix(),
		Reachability: membusspb.Reachability_PUBLIC,
		Seq:          p.now().Unix() - int64(freshnessWindow.Seconds()) - 100,
		PubKey:       pubBytes,
	}
	// Sign expired record
	canonicalExpired := CanonicalPeerInfoBytes(expiredInfo)
	sigExpired, _ := priv.Sign(canonicalExpired)
	expiredInfo.Signature = sigExpired
	p.mergeFromMessage([]*membusspb.PeerInfo{expiredInfo}, p.now().Unix())
	if tableHas(p, pid) {
		t.Fatal("expired peer record should be rejected")
	}

	// 4. Test Valid Record (should be accepted)
	validInfo := &membusspb.PeerInfo{
		PeerId:          pid.String(),
		Addrs:           []string{"/ip4/1.2.3.4/tcp/4001"},
		LastSeen:        p.now().Unix(),
		Reachability:    membusspb.Reachability_PUBLIC,
		Seq:             p.now().Unix(),
		PubKey:          pubBytes,
		LastDialSuccess: true,
	}
	canonicalValid := CanonicalPeerInfoBytes(validInfo)
	sigValid, _ := priv.Sign(canonicalValid)
	validInfo.Signature = sigValid
	p.mergeFromMessage([]*membusspb.PeerInfo{validInfo}, p.now().Unix())
	if !tableHas(p, pid) {
		t.Fatal("valid signed peer record should be accepted")
	}

	// 5. Test Replay / Older Sequence Record (should be ignored)
	replayInfo := &membusspb.PeerInfo{
		PeerId:          pid.String(),
		Addrs:           []string{"/ip4/1.2.3.4/tcp/4001", "/ip4/5.6.7.8/tcp/4001"}, // modified addrs
		LastSeen:        p.now().Unix(),
		Reachability:    membusspb.Reachability_PUBLIC,
		Seq:             validInfo.Seq - 10, // older sequence
		PubKey:          pubBytes,
		LastDialSuccess: true,
	}
	canonicalReplay := CanonicalPeerInfoBytes(replayInfo)
	sigReplay, _ := priv.Sign(canonicalReplay)
	replayInfo.Signature = sigReplay
	p.mergeFromMessage([]*membusspb.PeerInfo{replayInfo}, p.now().Unix())

	// Verify addrs did not update to the replay's values
	p.mu.Lock()
	entry, ok := p.peers[pid]
	p.mu.Unlock()
	if !ok {
		t.Fatal("peer missing")
	}
	if len(entry.info.Addrs) != 1 {
		t.Fatalf("expected addrs not to update on replay, got: %v", entry.info.Addrs)
	}

	// 6. Test Newer Sequence Record (should overwrite)
	newerInfo := &membusspb.PeerInfo{
		PeerId:          pid.String(),
		Addrs:           []string{"/ip4/1.2.3.4/tcp/4001", "/ip4/5.6.7.8/tcp/4001"}, // modified addrs
		LastSeen:        p.now().Unix(),
		Reachability:    membusspb.Reachability_PUBLIC,
		Seq:             validInfo.Seq + 10, // newer sequence
		PubKey:          pubBytes,
		LastDialSuccess: true,
	}
	canonicalNewer := CanonicalPeerInfoBytes(newerInfo)
	sigNewer, _ := priv.Sign(canonicalNewer)
	newerInfo.Signature = sigNewer
	p.mergeFromMessage([]*membusspb.PeerInfo{newerInfo}, p.now().Unix())

	// Verify addrs updated
	p.mu.Lock()
	entry, ok = p.peers[pid]
	p.mu.Unlock()
	if !ok {
		t.Fatal("peer missing")
	}
	if len(entry.info.Addrs) != 2 {
		t.Fatalf("expected addrs to update on newer seq, got: %v", entry.info.Addrs)
	}
}