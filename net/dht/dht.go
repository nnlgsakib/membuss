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
	"log/slog"
	"strings"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
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
	// ModeName is the YAML-friendly version of Mode.
	// Allowed values: "auto" (default), "client",
	// "server", "auto-server". When set it overrides
	// the typed Mode field, so config.yaml can drive
	// the choice without forcing every caller to build
	// a kaddht.ModeOpt.
	ModeName string
	// Datastore is the on-disk store used by kad-dht to
	// persist provider records across Provide/Restart
	// cycles. When nil, kad-dht falls back to a private
	// in-memory store, which means FindProviders can
	// only see providers the local node has already
	// observed during this run. The Membuss daemon
	// always passes a MapDatastore-backed ds.Batching
	// here so the DHT propagates provider records
	// across a multi-node cluster the way IPFS does.
	Datastore ds.Batching
	// OptimisticProvide, when true, enables
	// kaddht.EnableOptimisticProvide. The optimisation
	// short-circuits the last few hops of the provide
	// walk: as soon as the local node has announced the
	// CID to its K closest peers, the Provide call
	// returns success. Cross-cluster propagation is
	// dramatically faster and is what IPFS ships with
	// by default. Default true.
	OptimisticProvide bool
}

// modeOrDefault resolves cfg.Mode vs cfg.ModeName to a
// concrete kaddht.ModeOpt. ModeName wins so config.yaml
// can drive the choice. Allowed values are "auto",
// "client", "server" and "auto-server". An empty
// ModeName plus a zero Mode falls back to ModeAuto.
func (c Config) modeOrDefault() kaddht.ModeOpt {
	switch strings.ToLower(strings.TrimSpace(c.ModeName)) {
	case "client":
		return kaddht.ModeClient
	case "server":
		return kaddht.ModeServer
	case "auto-server", "autoserver":
		return kaddht.ModeAutoServer
	case "auto", "":
		// fall through to the typed Mode below
	default:
		// unknown string: ignore and fall back
	}
	if c.Mode == 0 {
		return kaddht.ModeAuto
	}
	return c.Mode
}

// New constructs a MemDHT. The DHT is not yet connected to any
// peer; call Bootstrap to connect to the configured bootstrap
// set.
//
// Phase 17: New honours Config.ModeName (the YAML-friendly
// form), Config.Datastore (a ds.Batching the kad-dht
// ProviderManager persists into) and Config.OptimisticProvide
// (turns on the last-hop skip so cross-node provider records
// propagate like IPFS).
func New(ctx context.Context, cfg Config) (*MemDHT, error) {
	if cfg.Host == nil {
		return nil, errors.New("dht: nil host")
	}
	opts := []kaddht.Option{
		kaddht.ProtocolPrefix(protocol.ID(ProtocolPrefix)),
		kaddht.Mode(cfg.modeOrDefault()),
		// Register a validator for the "membuss" and "memns"
		// namespaces so that app-level values and MemNS records can be
		// securely stored, validated, and selected. The kad-dht default
		// validator only allows "/pk/..." (public-key) records.
		kaddht.NamespacedValidator("membuss", membussValidator{}),
		kaddht.NamespacedValidator("memns", membussValidator{}),
	}
	if cfg.Datastore != nil {
		// Provider-record persistence. Without this, the
		// DHT forgets every Provide() the moment the
		// Provide call returns, so FindProviders on
		// other nodes always returns an empty list on
		// a freshly-bootstrapped cluster.
		opts = append(opts, kaddht.Datastore(cfg.Datastore))
	}
	if cfg.OptimisticProvide {
		// IPFS default: skip the last hops of the
		// provide walk. Cuts the time before another
		// node can discover our content from minutes
		// (full DHT walk) to seconds (single hop).
		opts = append(opts, kaddht.EnableOptimisticProvide())
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

// SearchValue retrieves multiple values previously stored under key.
func (m *MemDHT) SearchValue(ctx context.Context, key string) (<-chan []byte, error) {
	if m == nil || m.dht == nil {
		return nil, errors.New("dht: nil")
	}
	if key == "" {
		return nil, errors.New("dht: empty key")
	}
	return m.dht.SearchValue(ctx, key)
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

// BootstrapConfig configures BootstrapWithBackoff. Zero values
// fall back to sane defaults.
type BootstrapConfig struct {
	// Initial is the first retry delay. Default 500ms.
	Initial time.Duration
	// Max caps a single backoff sleep. Default 60s.
	Max time.Duration
	// Factor multiplies the previous delay after each failure.
	// Default 2.0.
	Factor float64
	// MaxAttempts bounds the retries per peer. Zero = unlimited.
	MaxAttempts int
	// Logger, if non-nil, receives structured progress events.
	Logger *slog.Logger
}

// BootstrapWithBackoff attempts to connect to each bootstrap peer
// with an exponential backoff schedule. It is a best-effort loop:
// the first successful connect per peer terminates its retry, and
// the function returns the total number of successful connections
// plus the combined error of the last failure (if any). It is safe
// to call concurrently with Bootstrap.
//
// The loop is cancellable via ctx. On cancel it returns
// ctx.Err() alongside the success count.
func (m *MemDHT) BootstrapWithBackoff(ctx context.Context, peers []peer.AddrInfo, cfg BootstrapConfig) (int, error) {
	if m == nil || m.dht == nil {
		return 0, errors.New("dht: nil")
	}
	if cfg.Initial <= 0 {
		cfg.Initial = 500 * time.Millisecond
	}
	if cfg.Max <= 0 {
		cfg.Max = 60 * time.Second
	}
	if cfg.Factor < 1 {
		cfg.Factor = 2.0
	}
	// Background the DHT's own bootstrap so our retry loop
	// is the only thing the caller waits on.
	bgCtx, bgCancel := context.WithCancel(ctx)
	defer bgCancel()
	_ = m.dht.Bootstrap(bgCtx)

	h := m.dht.Host()
	hostCtx := func() context.Context { return bgCtx }

	var (
		mu        sync.Mutex
		lastErr   error
		successes int
		wg        sync.WaitGroup
	)

	for _, p := range peers {
		wg.Add(1)
		go func(p peer.AddrInfo) {
			defer wg.Done()
			delay := cfg.Initial
			for attempt := 1; ; attempt++ {
				if ctx.Err() != nil {
					return
				}
				connectCtx, cancel := context.WithTimeout(hostCtx(), 10*time.Second)
				err := h.Connect(connectCtx, p)
				cancel()
				if err == nil {
					mu.Lock()
					successes++
					mu.Unlock()
					if cfg.Logger != nil {
						cfg.Logger.Info("dht bootstrap peer connected",
							"peer", p.ID.String(),
							"attempt", attempt,
						)
					}
					break
				}
				mu.Lock()
				lastErr = err
				mu.Unlock()
				if cfg.Logger != nil {
					cfg.Logger.Warn("dht bootstrap peer connect failed",
						"peer", p.ID.String(),
						"attempt", attempt,
						"err", err.Error(),
					)
				}
				if cfg.MaxAttempts > 0 && attempt >= cfg.MaxAttempts {
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(delay):
				}
				delay = time.Duration(float64(delay) * cfg.Factor)
				if delay > cfg.Max {
					delay = cfg.Max
				}
			}
		}(p)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		mu.Lock()
		defer mu.Unlock()
		return successes, ctx.Err()
	case <-done:
		mu.Lock()
		defer mu.Unlock()
		return successes, lastErr
	}
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

// silence unused import
var _ = record.ErrInvalidRecordType
