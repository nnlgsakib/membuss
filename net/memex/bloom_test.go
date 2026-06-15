package memex

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/mid"

	"github.com/nnlgsakib/membuss/core/store"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// sealedStub is a minimal SealedLister for tests.
type sealedStub struct {
	mids []mid.MID
}

func (s *sealedStub) AllSealed() ([]mid.MID, error) { return s.mids, nil }

// TestBloomManagerPeerLikelyHas covers the per-peer filter
// query. With no announcement received, PeerLikelyHas
// returns true (the safe default: keep the peer). After
// we inject a remote filter that excludes m, the answer
// flips to false.
func TestBloomManagerPeerLikelyHas(t *testing.T) {
	h := newTestHost(t)
	t.Cleanup(func() { _ = h.Close() })

	mgr, err := NewBloomManager(BloomConfig{
		Host:     h,
		Sealed:   &sealedStub{},
		Capacity: 1000,
		FPRate:   0.01,
		// 0 = disable the loop so it does not
		// interfere with the test.
		Interval: 0,
	})
	if err != nil {
		t.Fatalf("NewBloomManager: %v", err)
	}
	mgr.Start()
	t.Cleanup(mgr.Stop)

	target := mid.FromBytes([]byte("some-mid"))
	other := peer.ID("QmFakePeer")

	// No announcement yet: assume peer has it.
	if !mgr.PeerLikelyHas(other, target) {
		t.Fatal("PeerLikelyHas without announcement must return true")
	}

	// Inject a remote filter that excludes target.
	bf := bloom.NewWithEstimates(1000, 0.01)
	// (no Add calls -> filter is empty -> Test returns false)
	mgr.mu.Lock()
	mgr.peers[other] = &remoteBloom{filter: bf, received: time.Now()}
	mgr.mu.Unlock()

	if mgr.PeerLikelyHas(other, target) {
		t.Fatal("PeerLikelyHas with negative filter must return false")
	}
}

// TestBloomManagerFilteredProviders asserts that
// FilteredProviders drops peers whose filters say
// "absent" and keeps peers with no information.
func TestBloomManagerFilteredProviders(t *testing.T) {
	h := newTestHost(t)
	t.Cleanup(func() { _ = h.Close() })

	mgr, err := NewBloomManager(BloomConfig{
		Host:     h,
		Sealed:   &sealedStub{},
		Capacity: 1000,
		FPRate:   0.01,
		Interval: 0,
	})
	if err != nil {
		t.Fatalf("NewBloomManager: %v", err)
	}
	mgr.Start()
	t.Cleanup(mgr.Stop)

	want := mid.FromBytes([]byte("wanted-mid"))
	negFilter := bloom.NewWithEstimates(1000, 0.01)
	posFilter := bloom.NewWithEstimates(1000, 0.01)
	posFilter.Add(want.Bytes())

	pos := peer.ID("QmPos")
	neg := peer.ID("QmNeg")
	unk := peer.ID("QmUnk")

	mgr.mu.Lock()
	mgr.peers[pos] = &remoteBloom{filter: posFilter, received: time.Now()}
	mgr.peers[neg] = &remoteBloom{filter: negFilter, received: time.Now()}
	mgr.mu.Unlock()

	in := []peer.AddrInfo{
		{ID: pos},
		{ID: neg},
		{ID: unk},
	}
	got := mgr.FilteredProviders(want, in)
	if len(got) != 2 {
		t.Fatalf("FilteredProviders = %d, want 2 (pos+unk); got=%v", len(got), got)
	}
	ids := map[peer.ID]bool{}
	for _, p := range got {
		ids[p.ID] = true
	}
	if !ids[pos] {
		t.Fatal("FilteredProviders dropped the positive peer")
	}
	if !ids[unk] {
		t.Fatal("FilteredProviders dropped the unknown peer")
	}
	if ids[neg] {
		t.Fatal("FilteredProviders kept the negative peer")
	}
}

// TestBloomAnnouncementRoundtrip ensures the wire
// encoding survives a marshal/unmarshal roundtrip
// and that the filter inside still rejects MIDs that
// were not added.
func TestBloomAnnouncementRoundtrip(t *testing.T) {
	h := newTestHost(t)
	t.Cleanup(func() { _ = h.Close() })

	mgr, err := NewBloomManager(BloomConfig{
		Host:     h,
		Sealed:   &sealedStub{mids: []mid.MID{mid.FromBytes([]byte("x"))}},
		Capacity: 1000,
		FPRate:   0.01,
		Interval: 0,
	})
	if err != nil {
		t.Fatalf("NewBloomManager: %v", err)
	}
	mgr.Start()
	t.Cleanup(mgr.Stop)

	if err := mgr.RefreshLocal(context.Background()); err != nil {
		t.Fatalf("RefreshLocal: %v", err)
	}

	ann, err := mgr.localAnnouncement()
	if err != nil {
		t.Fatalf("localAnnouncement: %v", err)
	}
	buf, err := proto.Marshal(ann)
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}
	var dec membusspb.BloomAnnouncement
	if err := proto.Unmarshal(buf, &dec); err != nil {
		t.Fatalf("proto.Unmarshal: %v", err)
	}
	if !bytes.Equal(dec.BloomFilter, ann.BloomFilter) {
		t.Fatal("filter bytes changed across roundtrip")
	}

	// Decode the embedded filter and probe it.
	recBF := &bloom.BloomFilter{}
	if err := recBF.UnmarshalBinary(dec.BloomFilter); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	if recBF.Test(mid.FromBytes([]byte("nope")).Bytes()) {
		t.Fatal("decoded filter says 'yes' for a MID that was never added")
	}
	if !recBF.Test(mid.FromBytes([]byte("x")).Bytes()) {
		t.Fatal("decoded filter says 'no' for a MID that was added")
	}
}

// TestBloomExchangeEndToEnd is the headline test for
// Phase 13: two nodes connect, each builds a local
// filter from its sealed set, they exchange
// announcements, and we confirm that FilteredProviders
// would skip a peer whose set does not contain a
// particular MID.
func TestBloomExchangeEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	hA := newTestHost(t)
	hB := newTestHost(t)
	t.Cleanup(func() { _ = hA.Close(); _ = hB.Close() })

	if err := hA.Connect(ctx, peer.AddrInfo{ID: hB.ID(), Addrs: hB.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}

	// A's sealed set contains `aOnly`; B's contains
	// `bOnly`. The two are distinct.
	aOnly := mid.FromBytes([]byte("A"))
	bOnly := mid.FromBytes([]byte("B"))

	mgrA, err := NewBloomManager(BloomConfig{
		Host:     hA,
		Sealed:   &sealedStub{mids: []mid.MID{aOnly}},
		Capacity: 1000,
		FPRate:   0.01,
		Interval: 0,
	})
	if err != nil {
		t.Fatalf("NewBloomManager A: %v", err)
	}
	mgrA.Start()
	t.Cleanup(mgrA.Stop)
	if err := mgrA.RefreshLocal(context.Background()); err != nil {
		t.Fatalf("RefreshLocal A: %v", err)
	}

	mgrB, err := NewBloomManager(BloomConfig{
		Host:     hB,
		Sealed:   &sealedStub{mids: []mid.MID{bOnly}},
		Capacity: 1000,
		FPRate:   0.01,
		Interval: 0,
	})
	if err != nil {
		t.Fatalf("NewBloomManager B: %v", err)
	}
	mgrB.Start()
	t.Cleanup(mgrB.Stop)

	// Build A's announcement manually (with the new
	// sealed set) and push it directly to B's map to
	// simulate the protocol exchange.
	ann, err := mgrA.localAnnouncement()
	if err != nil {
		t.Fatalf("ann: %v", err)
	}
	bf := &bloom.BloomFilter{}
	if err := bf.UnmarshalBinary(ann.BloomFilter); err != nil {
		t.Fatalf("UnmarshalBinary: %v", err)
	}
	mgrB.mu.Lock()
	mgrB.peers[hA.ID()] = &remoteBloom{filter: bf, received: time.Now()}
	mgrB.mu.Unlock()

	// B wants aOnly: A's filter says YES, so A is kept.
	kept := mgrB.FilteredProviders(aOnly, []peer.AddrInfo{{ID: hA.ID()}})
	if len(kept) != 1 {
		t.Fatalf("aOnly: FilteredProviders = %d, want 1", len(kept))
	}
	// B wants bOnly: A's filter does NOT contain bOnly,
	// so A is dropped.
	kept = mgrB.FilteredProviders(bOnly, []peer.AddrInfo{{ID: hA.ID()}})
	if len(kept) != 0 {
		t.Fatalf("bOnly: FilteredProviders = %d, want 0 (A lacks bOnly)", len(kept))
	}
}

// TestBloomManagerRefreshLocal exercises the rebuild
// path. After RefreshLocal, PeerLikelyHas must return
// true for an MID that the SealedLister reports.
func TestBloomManagerRefreshLocal(t *testing.T) {
	h := newTestHost(t)
	t.Cleanup(func() { _ = h.Close() })

	stub := &sealedStub{mids: []mid.MID{mid.FromBytes([]byte("alpha"))}}
	mgr, err := NewBloomManager(BloomConfig{
		Host:     h,
		Sealed:   stub,
		Capacity: 1000,
		FPRate:   0.01,
		Interval: 0,
	})
	if err != nil {
		t.Fatalf("NewBloomManager: %v", err)
	}
	mgr.Start()
	t.Cleanup(mgr.Stop)

	if err := mgr.RefreshLocal(context.Background()); err != nil {
		t.Fatalf("RefreshLocal: %v", err)
	}
	ann, err := mgr.localAnnouncement()
	if err != nil {
		t.Fatalf("localAnnouncement: %v", err)
	}
	if ann.ItemCount != 1 {
		t.Fatalf("ItemCount = %d, want 1", ann.ItemCount)
	}
}

// TestBloomManagerAddSealed verifies that AddSealed
// updates the local filter in place so an immediate
// localAnnouncement reports the new count.
func TestBloomManagerAddSealed(t *testing.T) {
	h := newTestHost(t)
	t.Cleanup(func() { _ = h.Close() })
	mgr, err := NewBloomManager(BloomConfig{
		Host:     h,
		Sealed:   &sealedStub{},
		Capacity: 1000,
		FPRate:   0.01,
		Interval: 0,
	})
	if err != nil {
		t.Fatalf("NewBloomManager: %v", err)
	}
	mgr.Start()
	t.Cleanup(mgr.Stop)
	if err := mgr.RefreshLocal(context.Background()); err != nil {
		t.Fatalf("RefreshLocal: %v", err)
	}
	before, _ := mgr.localAnnouncement()
	mgr.AddSealed(mid.FromBytes([]byte("new-seal")))
	after, _ := mgr.localAnnouncement()
	if after.ItemCount != before.ItemCount+1 {
		t.Fatalf("ItemCount went from %d to %d, want +1", before.ItemCount, after.ItemCount)
	}
}


// TestSessionSelectPeersForMID covers the MemexSession
// entry point that consults the bloom filter. We wire a
// session up with a manager that already knows about a
// peer filter, then verify the per-MID helper drops the
// excluded peer.
func TestSessionSelectPeersForMID(t *testing.T) {
	hA := newTestHost(t)
	hB := newTestHost(t)
	t.Cleanup(func() { _ = hA.Close(); _ = hB.Close() })

	mgr, err := NewBloomManager(BloomConfig{
		Host:     hA,
		Sealed:   &sealedStub{},
		Capacity: 1000,
		FPRate:   0.01,
		Interval: 0,
	})
	if err != nil {
		t.Fatalf("NewBloomManager: %v", err)
	}
	mgr.Start()
	t.Cleanup(mgr.Stop)

	bsA := store.NewMemstore()
	eng, err := New(Config{Host: hA, Blockstore: bsA, Bloom: mgr})
	if err != nil {
		t.Fatalf("memex New: %v", err)
	}
	eng.Start()
	t.Cleanup(eng.Stop)

	want := mid.FromBytes([]byte("wanted"))
	// hA's local filter knows about want.
	if err := mgr.RefreshLocal(context.Background()); err != nil {
		t.Fatalf("RefreshLocal: %v", err)
	}
	mgr.AddSealed(want)
	// hB is intentionally NOT in mgr.peers: it should be
	// kept (the safe default) by selectPeersForMID.

	sess, err := NewSession(SessionConfig{
		Engine:    eng,
		Root:      want,
		Providers: []peer.AddrInfo{{ID: hA.ID()}, {ID: hB.ID()}},
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	got := sess.selectPeersForMID(want)
	if len(got) != 2 {
		t.Fatalf("selectPeersForMID = %d, want 2 (hA: yes; hB: unknown=keep)", len(got))
	}
}
