package pex

import (
	"crypto/rand"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
)

// NewTestInProcessHost returns a libp2p host with no listen
// addrs (the same shape as net/host.newInProcessHost but
// exposed for tests in this package). The filter tests use
// it so they can build a PEX without binding a TCP port.
func NewTestInProcessHost() (host.Host, error) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	return libp2p.New(
		libp2p.Identity(priv),
		libp2p.NoListenAddrs,
	)
}
