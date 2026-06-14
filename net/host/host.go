// Package host constructs a libp2p host for a Membuss node.
//
// The host is the central libp2p object: it owns the network
// identity (PeerID), the transport stack, the connection
// manager, and the protocol multiplexer. Higher-level packages
// (net/dht, net/pex, net/memex, net/herald) attach their
// protocols to the host returned by NewHost.
//
// The Ed25519 identity is loaded from
// <DataDir>/identity.key so that the node has a stable PeerID
// across restarts. If the file is missing a fresh key is
// generated and saved with 0600 permissions.
package host

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
)

// IdentityFilename is the on-disk filename for the Ed25519
// private key. It lives directly under DataDir.
const IdentityFilename = "identity.key"

// Config configures a libp2p host construction.
type Config struct {
	// ListenAddrs is the set of libp2p multiaddrs the host
	// binds to. The defaults are TCP and QUIC on 0.0.0.0:4001.
	ListenAddrs []string

	// DataDir is the directory holding the persistent node
	// identity. Required unless InProcess is true.
	DataDir string

	// InProcess, if true, disables listening and the on-disk
	// identity. Used by tests that want a fully synthetic host.
	InProcess bool

	// UserAgent, if non-empty, identifies the node to peers.
	UserAgent string
}

// NewHost constructs a libp2p host according to cfg. The host
// uses TCP + QUIC for transport, Noise for channel security,
// and yamux for stream multiplexing. The Ed25519 identity is
// loaded from <DataDir>/identity.key (or generated and saved
// if absent).
//
// The returned host is ready to have stream handlers attached.
// The caller MUST call host.Close() when done.
func NewHost(cfg Config) (host.Host, error) {
	if cfg.InProcess {
		return newInProcessHost(cfg)
	}
	if cfg.DataDir == "" {
		return nil, errors.New("host: empty DataDir")
	}

	priv, err := loadOrCreateIdentity(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("host: identity: %w", err)
	}

	listen := cfg.ListenAddrs
	if len(listen) == 0 {
		listen = []string{
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/udp/4001/quic-v1",
		}
	}

	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(listen...),
		// Pass the transport CONSTRUCTORS; libp2p wires the
		// resource manager / connection manager itself.
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
	}
	if cfg.UserAgent != "" {
		opts = append(opts, libp2p.UserAgent(cfg.UserAgent))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("host: build libp2p host: %w", err)
	}
	return h, nil
}

// loadOrCreateIdentity loads the Ed25519 private key from
// <dir>/identity.key, or generates a new one and saves it with
// 0600 permissions. The returned key's PublicKey hashed to
// libp2p's PeerID is the node's stable network identity.
func loadOrCreateIdentity(dir string) (crypto.PrivKey, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir data dir: %w", err)
	}

	path := filepath.Join(dir, IdentityFilename)
	if data, err := os.ReadFile(path); err == nil {
		priv, err := crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, fmt.Errorf("unmarshal identity: %w", err)
		}
		return priv, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read identity: %w", err)
	}

	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate identity: %w", err)
	}
	raw, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("marshal identity: %w", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return nil, fmt.Errorf("write identity: %w", err)
	}
	return priv, nil
}

// PeerIDFromKey is a convenience that derives the libp2p
// PeerID from a private key.
func PeerIDFromKey(priv crypto.PrivKey) (peer.ID, error) {
	return peer.IDFromPrivateKey(priv)
}

// newInProcessHost builds a fully synthetic libp2p host that
// does not listen on any external address. It is intended for
// tests that wire two hosts together with the in-process
// transport.
func newInProcessHost(cfg Config) (host.Host, error) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate identity: %w", err)
	}
	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.NoListenAddrs,
	}
	if cfg.UserAgent != "" {
		opts = append(opts, libp2p.UserAgent(cfg.UserAgent))
	}
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("host: build in-process libp2p host: %w", err)
	}
	return h, nil
}
