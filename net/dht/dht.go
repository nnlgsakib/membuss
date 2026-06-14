// Package dht wraps go-libp2p-kad-dht into a Membuss-shaped API.
//
// Membuss uses the DHT to announce provider records ("I have
// this MID") and to discover providers of a given MID. Small
// arbitrary values can also be stored and retrieved. The
// underlying Kademlia protocol is identified by the prefix
// /membuss/dht/1.0.0 (the libp2p kad-dht library appends
// /kad/1.0.0 automatically).
package dht

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-cid"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/multiformats/go-multihash"

	"github.com/nnlgsakib/membuss/core/mid"
)

// ProtocolPrefix is the application-specific protocol prefix
// for the Membuss DHT. The kad-dht library appends /kad/1.0.0
// to it for the actual protocol ID.
const ProtocolPrefix = "/membuss/dht/1.0.0"

// DefaultBootstrapTimeout is the maximum time a Bootstrap call
// will wait for connections to bootstrap peers.
const DefaultBootstrapTimeout = 30 * time.Second

// MemDHT is the Membuss DHT facade. It is safe for concurrent
// use after construction.
type MemDHT struct {
	dht *kaddht.IpfsDHT
}

// Config configures a MemDHT.
type Config struct {
	Host host.Host
	BootstrapPeers []peer.AddrInfo
	// Mode overrides the kad-dht operating mode. The
	// default is kaddht.ModeAuto, which lets kad-dht pick
	// Client vs. Server based on reachability. Tests can
	// pass kaddht.ModeServer to force a server role.
	Mode kaddht.ModeOpt
}

// New constructs a MemDHT. The DHT is not yet connected to any
// peer; call Bootstrap to connect to the configured bootstrap
// set.
func modeOrDefault(m kaddht.ModeOpt) kaddht.ModeOpt {
	if m == 0 {
		return kaddht.ModeAuto
	}
	return m
}

func New(ctx context.Context, cfg Config) (*MemDHT, error) {
	if cfg.Host == nil {
		return nil, errors.New("dht: nil host")
	}
	opts := []kaddht.Option{
		kaddht.ProtocolPrefix(protocol.ID(ProtocolPrefix)),
		kaddht.Mode(modeOrDefault(cfg.Mode)),
		// Register a permissive validator for the "membuss"
		// namespace so that arbitrary app-level values can be
		// stored and retrieved. The kad-dht default validator
		// only allows "/pk/..." (public-key) records.
		kaddht.NamespacedValidator("membuss", permissiveValidator{}),
	}
	d, err := kaddht.New(ctx, cfg.Host, opts...)
	if err != nil {
		return nil, fmt.Errorf("dht: build kad-dht: %w", err)
	}
	return &MemDHT{dht: d}, nil
}

// Provide announces to the DHT that this node can serve the
// given MID.
func (m *MemDHT) Provide(ctx context.Context, id mid.MID) error {
	if m == nil || m.dht == nil {
		return errors.New("dht: nil")
	}
	if id.IsZero() {
		return errors.New("dht: zero MID")
	}
	c := midToCID(id)
	if !c.Defined() {
		return errors.New("dht: zero MID")
	}
	return m.dht.Provide(ctx, c, true)
}

// FindProviders returns the set of peers the DHT knows are
// providers of the given MID.
func (m *MemDHT) FindProviders(ctx context.Context, id mid.MID) ([]peer.AddrInfo, error) {
	if m == nil || m.dht == nil {
		return nil, errors.New("dht: nil")
	}
	if id.IsZero() {
		return nil, errors.New("dht: zero MID")
	}
	c := midToCID(id)
	if !c.Defined() {
		return nil, errors.New("dht: zero MID")
	}
	return m.dht.FindProviders(ctx, c)
}

// PutValue stores an arbitrary small value under the given
// key. The key must be in the form "/<namespace>/<path>".
// Membuss reserves the "membuss" namespace and registers a
// permissive validator for it.
func (m *MemDHT) PutValue(ctx context.Context, key string, value []byte) error {
	if m == nil || m.dht == nil {
		return errors.New("dht: nil")
	}
	if key == "" {
		return errors.New("dht: empty key")
	}
	if len(value) == 0 {
		return errors.New("dht: empty value")
	}
	return m.dht.PutValue(ctx, key, value)
}

// GetValue retrieves a value previously stored under key.
func (m *MemDHT) GetValue(ctx context.Context, key string) ([]byte, error) {
	if m == nil || m.dht == nil {
		return nil, errors.New("dht: nil")
	}
	if key == "" {
		return nil, errors.New("dht: empty key")
	}
	return m.dht.GetValue(ctx, key)
}

// Bootstrap connects to the configured bootstrap peers and
// refreshes the routing table.
func (m *MemDHT) Bootstrap(ctx context.Context, peers []peer.AddrInfo) error {
	if m == nil || m.dht == nil {
		return errors.New("dht: nil")
	}
	if err := m.dht.Bootstrap(ctx); err != nil {
		return fmt.Errorf("dht: bootstrap: %w", err)
	}
	for _, p := range peers {
		_ = m.dht.Host().Connect(ctx, p)
	}
	return nil
}

// Close releases the DHT's resources.
func (m *MemDHT) Close() error {
	if m == nil || m.dht == nil {
		return nil
	}
	return m.dht.Close()
}

// Host returns the underlying libp2p host.
func (m *MemDHT) Host() host.Host {
	if m == nil || m.dht == nil {
		return nil
	}
	return m.dht.Host()
}

// RoutingTableSize returns the number of peers in the DHT's
// local routing table. Tests use this to wait for the table
// to fill before exercising Provide / PutValue.
func (m *MemDHT) RoutingTableSize() int {
	if m == nil || m.dht == nil {
		return 0
	}
	return m.dht.RoutingTable().Size()
}

func midToCID(m mid.MID) cid.Cid {
	if m.IsZero() {
		return cid.Cid{}
	}
	return cid.NewCidV1(uint64(mid.CodecRaw), mhFromMID(m))
}

func mhFromMID(m mid.MID) multihash.Multihash {
	return multihash.Multihash(append([]byte(nil), m.HashBytes()...))
}

// permissiveValidator accepts any value stored under its
// namespace. It is used for the "membuss" app-level key/value
// records; the default kad-dht validator is preserved for
// "pk" public-key records.
type permissiveValidator struct{}

func (permissiveValidator) Validate(_ string, _ []byte) error { return nil }
func (permissiveValidator) Select(_ string, values [][]byte) (int, error) {
	if len(values) == 0 {
		return 0, errors.New("no values")
	}
	return 0, nil
}

// silence unused import
var _ = record.ErrInvalidRecordType