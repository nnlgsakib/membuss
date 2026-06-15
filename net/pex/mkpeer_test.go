package pex

import (
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// makeTestPeer returns a fresh, unconnected peer ID. It
// fabricates a PeerInfo row in the table without going
// through libp2p's host plumbing so filter tests can
// exercise the table without binding any TCP port.
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
