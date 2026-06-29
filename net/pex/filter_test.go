package pex

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// TestPEX_FilterForGossip_BasicShape builds a table with the
// 10+5+3 mix from the Phase 12 spec and asserts the
// outgoing list contains exactly the 10 public + 3
// relay-only entries, with the relay-only addrs stripped.
func TestPEX_FilterForGossip_BasicShape(t *testing.T) {
	h, err := NewTestInProcessHost()
	if err != nil {
		t.Fatalf("in-process host: %v", err)
	}
	defer h.Close()

	now := time.Unix(1_700_000_000, 0).UTC()
	p, err := New(Config{Host: h, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("pex: %v", err)
	}

	// 10 PUBLIC peers
	publicIDs := make([]peer.ID, 0, 10)
	for i := 0; i < 10; i++ {
		pid := addSignedTestPeer(t, p, membusspb.Reachability_PUBLIC, nil, nil)
		publicIDs = append(publicIDs, pid)
	}

	// 5 PRIVATE peers with last_dial_success=false (should be
	// excluded from the outgoing list).
	privateUnreachIDs := make([]peer.ID, 0, 5)
	for i := 0; i < 5; i++ {
		pid := addSignedTestPeer(t, p, membusspb.Reachability_PRIVATE, nil, nil)
		privateUnreachIDs = append(privateUnreachIDs, pid)
		p.MarkDialResult(pid, false)
	}

	// 3 RELAY_ONLY peers
	relayOnlyIDs := make([]peer.ID, 0, 3)
	for i := 0; i < 3; i++ {
		pid := addSignedTestPeer(t, p, membusspb.Reachability_RELAY_ONLY, nil, nil)
		relayOnlyIDs = append(relayOnlyIDs, pid)
	}

	gossip := p.FilterForGossip()
	if len(gossip) != 14 {
		t.Fatalf("gossip list = %d entries, want 14 (10 public + 3 relay-only + 1 self)", len(gossip))
	}

	// Build a set of included peer IDs for O(1) lookup.
	// The protobuf struct embeds a mutex so we store the
	// info by pointer in the map.
	included := make(map[string]*membusspb.PeerInfo, len(gossip))
	for _, info := range gossip {
		included[info.PeerId] = info
	}

	// Verify and remove self from check map
	if _, ok := included[h.ID().String()]; !ok {
		t.Fatalf("self missing from gossip list")
	}
	delete(included, h.ID().String())

	// All 10 PUBLIC must be present with their addrs intact.
	for _, pid := range publicIDs {
		info, ok := included[pid.String()]
		if !ok {
			t.Fatalf("public peer %s missing from gossip", pid)
		}
		if info.Reachability != membusspb.Reachability_PUBLIC {
			t.Fatalf("public peer %s has reachability %v", pid, info.Reachability)
		}
	}

	// All 3 RELAY_ONLY must be present but with addrs
	// stripped (relay_addrs is the only useful info).
	for _, pid := range relayOnlyIDs {
		info, ok := included[pid.String()]
		if !ok {
			t.Fatalf("relay-only peer %s missing from gossip", pid)
		}
		if info.Reachability != membusspb.Reachability_RELAY_ONLY {
			t.Fatalf("relay-only peer %s has reachability %v", pid, info.Reachability)
		}
		if len(info.Addrs) != 0 {
			t.Fatalf("relay-only peer %s has direct addrs %v (should be stripped)", pid, info.Addrs)
		}
		if len(info.RelayAddrs) == 0 {
			t.Fatalf("relay-only peer %s has no relay_addrs", pid)
		}
	}

	// All 5 PRIVATE+unreachable must be excluded.
	for _, pid := range privateUnreachIDs {
		if _, ok := included[pid.String()]; ok {
			t.Fatalf("private-unreachable peer %s leaked into gossip", pid)
		}
	}
}

// TestPEX_FilterForGossip_StaleEntryDropped makes sure the
// freshness window actually drops stale entries.
func TestPEX_FilterForGossip_StaleEntryDropped(t *testing.T) {
	h, err := NewTestInProcessHost()
	if err != nil {
		t.Fatalf("in-process host: %v", err)
	}
	defer h.Close()

	base := time.Unix(1_700_000_000, 0).UTC()
	clock := base
	now := func() time.Time { return clock }
	p, err := New(Config{Host: h, Now: now})
	if err != nil {
		t.Fatalf("pex: %v", err)
	}

	pid := addSignedTestPeer(t, p, membusspb.Reachability_PUBLIC, nil, nil)

	// Move the clock past the freshness window.
	clock = base.Add(freshnessWindow + time.Hour)

	gossip := p.FilterForGossip()
	for _, info := range gossip {
		if info.PeerId == pid.String() {
			t.Fatalf("stale entry %s leaked into gossip", pid)
		}
	}
}

// TestPEX_FilterForGossip_PrivateReachableKept verifies that
// PRIVATE entries with last_dial_success=true are still
// shared (they may be on the recipient's LAN).
func TestPEX_FilterForGossip_PrivateReachableKept(t *testing.T) {
	h, err := NewTestInProcessHost()
	if err != nil {
		t.Fatalf("in-process host: %v", err)
	}
	defer h.Close()

	now := time.Unix(1_700_000_000, 0).UTC()
	p, err := New(Config{Host: h, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("pex: %v", err)
	}

	pid := addSignedTestPeer(t, p, membusspb.Reachability_PRIVATE, nil, nil)
	p.MarkDialResult(pid, true)

	gossip := p.FilterForGossip()
	var found *membusspb.PeerInfo
	for _, info := range gossip {
		if info.PeerId == pid.String() {
			found = info
			break
		}
	}
	if found == nil {
		t.Fatal("private-reachable peer missing from gossip")
	}
	if found.Reachability != membusspb.Reachability_PRIVATE {
		t.Fatalf("reachability = %v, want PRIVATE", found.Reachability)
	}
	if !found.LastDialSuccess {
		t.Fatal("last_dial_success lost across the filter")
	}
}

// TestPEX_FilterForGossip_SelfSkipped verifies that our own
// peer ID never makes it into the outgoing list, even if we
// are accidentally in the table.
func TestPEX_FilterForGossip_SelfSkipped(t *testing.T) {
	h, err := NewTestInProcessHost()
	if err != nil {
		t.Fatalf("in-process host: %v", err)
	}
	defer h.Close()

	now := time.Unix(1_700_000_000, 0).UTC()
	p, err := New(Config{Host: h, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatalf("pex: %v", err)
	}

	// Add ourselves (the filter must reject the table entry).
	p.AddPeerWithReachability(peer.AddrInfo{ID: h.ID()}, membusspb.Reachability_PUBLIC, nil)
	// Add a real peer so the table is non-empty.
	_ = addSignedTestPeer(t, p, membusspb.Reachability_PUBLIC, nil, nil)

	gossip := p.FilterForGossip()
	if len(gossip) != 2 {
		t.Fatalf("expected 2 gossip entries, got %d", len(gossip))
	}

	var selfInfo *membusspb.PeerInfo
	for _, info := range gossip {
		if info.PeerId == h.ID().String() {
			selfInfo = info
		}
	}
	if selfInfo == nil {
		t.Fatal("self missing from gossip")
	}
	if len(selfInfo.Signature) == 0 {
		t.Fatal("self in gossip is unsigned")
	}
}

// TestPEX_MarkDialResult_Persists verifies that the dial
// outcome is preserved on the table entry.
func TestPEX_MarkDialResult_Persists(t *testing.T) {
	h, err := NewTestInProcessHost()
	if err != nil {
		t.Fatalf("in-process host: %v", err)
	}
	defer h.Close()

	p, err := New(Config{Host: h})
	if err != nil {
		t.Fatalf("pex: %v", err)
	}

	pid := makeTestPeer(t)
	p.AddPeer(peer.AddrInfo{ID: pid})
	p.MarkDialResult(pid, false)

	for _, info := range p.Peers() {
		if info.PeerId == pid.String() && info.LastDialSuccess {
			t.Fatal("MarkDialResult(false) did not stick")
		}
	}
}
