package anchor

import (
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
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