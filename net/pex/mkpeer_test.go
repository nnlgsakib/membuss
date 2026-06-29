package pex

import (
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

// makeTestPeer returns a fresh, unconnected peer ID.
func makeTestPeer(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("id from key: %v", err)
	}
	return pid
}

// addSignedTestPeer generates a keypair, constructs a signed PeerInfo, and adds it to the PEX instance.
func addSignedTestPeer(t *testing.T, p *PEX, reach membusspb.Reachability, addrs []multiaddr.Multiaddr, relayAddrs []multiaddr.Multiaddr) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("id from key: %v", err)
	}

	if reach == membusspb.Reachability_RELAY_ONLY && len(relayAddrs) == 0 {
		relayMA, _ := multiaddr.NewMultiaddr("/ip4/1.2.3.4/tcp/4001/p2p/" + pid.String())
		relayAddrs = []multiaddr.Multiaddr{relayMA}
	}

	pubBytes, err := crypto.MarshalPublicKey(priv.GetPublic())
	if err != nil {
		t.Fatalf("marshal pubkey: %v", err)
	}

	addrStrs := make([]string, len(addrs))
	for i, a := range addrs {
		addrStrs[i] = a.String()
	}

	relayStrs := make([]string, len(relayAddrs))
	for i, r := range relayAddrs {
		relayStrs[i] = r.String()
	}

	info := &membusspb.PeerInfo{
		PeerId:       pid.String(),
		Addrs:        addrStrs,
		RelayAddrs:   relayStrs,
		LastSeen:     p.now().Unix(),
		Reachability: reach,
		Seq:          p.now().Unix(),
		PubKey:       pubBytes,
	}

	canonical := CanonicalPeerInfoBytes(info)
	sig, err := priv.Sign(canonical)
	if err != nil {
		t.Fatalf("sign error: %v", err)
	}
	info.Signature = sig

	p.mu.Lock()
	defer p.mu.Unlock()
	p.upsertLocked(peer.AddrInfo{ID: pid, Addrs: addrs}, relayAddrs, p.now().Unix(), false)
	if entry, ok := p.peers[pid]; ok {
		entry.info = info
		entry.info.LastDialSuccess = true
	}
	return pid
}

