package dht

import (
	"crypto/rand"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
)

// genInProcessHost returns an in-process libp2p host with no
// listen addrs. The unused identity is the one used by the
// genInProcessHost callers; we return it for symmetry with
// the public newTestHost helper.
func genInProcessHost() (host.Host, crypto.PrivKey, error) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.NoListenAddrs,
	)
	if err != nil {
		return nil, nil, err
	}
	return h, priv, nil
}
