package anchor

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/nnlgsakib/membuss/core/store"
)

func TestAnchor_MergeAnchors(t *testing.T) {
	p1, _ := peer.Decode("12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X")
	p2, _ := peer.Decode("12D3KooWMUPp3d6dbMVFc8JfwVkno3kHzjNtHZXc6pvd6hi8C2a8")
	direct := []peer.AddrInfo{{ID: p1}}
	anchors := []peer.AddrInfo{{ID: p2}, {ID: p1}}
	merged := mergeAnchors(direct, anchors)
	if len(merged) != 2 {
		t.Fatalf("merged length: got %d, want 2", len(merged))
	}
	if merged[0].ID != p1 {
		t.Fatal("direct provider should remain first")
	}
	if merged[1].ID != p2 {
		t.Fatal("anchor should be appended")
	}
}

func TestAnchor_EncodeDecodeAddrInfo(t *testing.T) {
	pid, _ := peer.Decode("12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X")
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	ai := peer.AddrInfo{ID: pid, Addrs: []multiaddr.Multiaddr{addr}}
	raw, err := encodeAddrInfo(ai)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := DecodeAnchorValue(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.ID != ai.ID {
		t.Fatalf("ID: got %s, want %s", got.ID, ai.ID)
	}
	if len(got.Addrs) != 1 || got.Addrs[0].String() != addr.String() {
		t.Fatalf("Addrs: got %v, want %v", got.Addrs, ai.Addrs)
	}
}

func TestAnchor_RegistryIsConsistent(t *testing.T) {
	h := newTestHost(t)
	t.Cleanup(func() { _ = h.Close() })

	bs := store.NewMemstore()
	eng := &AnchorEngine{
		cfg: Config{
			Host:  h,
			Store: bs,
		},
		anchors:  make(map[peer.ID]peer.AddrInfo),
		hostSeen: make(map[string]struct{}),
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	st := eng.Status()
	if st.PeerID != h.ID().String() {
		t.Fatalf("PeerID: got %s, want %s", st.PeerID, h.ID())
	}
}