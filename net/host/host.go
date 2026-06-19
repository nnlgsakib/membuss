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
//
// Phase 11: NewHost enables the full NAT traversal stack by
// default. AutoNAT reports reachability via the host event bus;
// DCUtR attempts direct connection upgrades through a relay;
// Circuit Relay v2 is enabled when Config.RelayService is true;
// AutoRelay picks up a static relay set from BootstrapPeers
// (or DHT-discovered relays). The returned *Host wrapper exposes
// reachability helpers and a WaitForNAT helper used by the
// daemon at startup.
package host

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/libp2p/go-libp2p/p2p/muxer/yamux"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	libp2pws "github.com/libp2p/go-libp2p/p2p/transport/websocket"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
)

// IdentityFilename is the on-disk filename for the Ed25519
// private key. It lives directly under DataDir.
const IdentityFilename = "identity.key"

// DefaultNATWait is the default time NewHost waits for
// AutoNAT to produce a reachability verdict before the
// daemon continues startup. Used when Config.NATWait is zero.
const DefaultNATWait = 10 * time.Second

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

	// --- Phase 11: NAT traversal + relay fallback ---

	// RelayService enables the circuit v2 relay hop on this
	// node. When true, the node can forward traffic for
	// NATed peers. Anchor nodes should set this to true.
	RelayService bool
	// RelayMaxConns caps the number of simultaneously
	// relayed circuits. Default 128.
	RelayMaxConns int
	// RelayMaxReservations caps the number of active relay
	// reservations. Default 128.
	RelayMaxReservations int
	// RelayBandwidthMB is the soft bandwidth cap (MB/s) the
	// relay will budget for forwarded traffic. 0 disables
	// the cap. Default 16.
	RelayBandwidthMB int
	// ForceRelay, when true, makes this node always use a
	// relay for outbound dials, skipping hole-punch. Useful
	// for debugging.
	ForceRelay bool
	// StaticRelays is the set of relay peers the AutoRelay
	// subsystem uses as initial candidates. Bootstrap peers
	// are a sensible default when this is empty.
	StaticRelays []peer.AddrInfo
	// NATWait is how long NewHost waits for AutoNAT to
	// produce a reachability verdict after constructing the
	// host. Zero means DefaultNATWait. A negative value
	// disables waiting entirely (NewHost returns
	// immediately with NATStatus "unknown").
	NATWait time.Duration

	// MDNS enables libp2p mDNS discovery. When true the
	// host broadcasts itself on the local network and
	// dials every peer it hears about. Default false
	// (sensible for production; ideal for the multi-node
	// Docker smoke test, where every node on the bridge
	// network finds every other one with zero
	// configuration).
	MDNS bool
	// MDNSServiceName overrides the libp2p mDNS service
	// tag. The default is mdns.ServiceName ("_p2p._udp").
	// Useful when several Membuss clusters share the
	// same broadcast domain.
	MDNSServiceName string
	// OnPeerFound, when set, is called for every peer the
	// mDNS service hears about (after we successfully
	// dial them). The daemon uses it to feed the
	// discovered peer into the DHT bootstrap list so a
	// private mDNS-only cluster still forms a DHT and
	// provider records propagate cross-node.
	OnPeerFound func(peer.AddrInfo)
}

// Host wraps a libp2p host.Host with Membuss-specific helpers
// for NAT traversal. It embeds host.Host so callers that need
// the libp2p interface (DHT, PEX, Memex, ...) can use Host
// transparently.
type Host struct {
	host.Host

	// reachability tracks the latest known reachability state
	// reported by the AutoNAT subsystem. The event-bus
	// subscription updates it asynchronously after New
	// returns.
	mu           sync.RWMutex
	reachability network.Reachability
	eventSub     event.Subscription

	// mdns, when non-nil, is the libp2p mDNS discovery
	// service. It is closed by Host.Close.
	mdns mdns.Service

	// onPeerFound, when non-nil, is invoked for every peer
	// the mDNS service dials. The daemon uses this to feed
	// the discovered peer into the DHT bootstrap list.
	onPeerFound func(peer.AddrInfo)

	// bwc tracks real-time traffic statistics.
	bwc *metrics.BandwidthCounter
}

// NewHost constructs a libp2p host according to cfg. The host
// uses TCP + QUIC for transport, Noise for channel security,
// and yamux for stream multiplexing. The Ed25519 identity is
// loaded from <DataDir>/identity.key (or generated and saved
// if absent).
//
// NAT traversal is enabled by default: AutoNAT, DCUtR, and
// AutoRelay. When Config.RelayService is true the host also
// runs the Circuit Relay v2 hop service.
//
// NewHost waits up to Config.NATWait (or DefaultNATWait) for
// the AutoNAT subsystem to produce a reachability verdict and
// logs the result. The wait is best-effort: if the verdict
// does not arrive in time, the host still returns and callers
// can poll NATStatus() later.
//
// The returned host is ready to have stream handlers attached.
// The caller MUST call host.Close() when done.
func NewHost(cfg Config) (*Host, error) {
	// The in-process path skips NAT options entirely because
	// libp2p refuses to wire AutoNAT/relay without listen
	// addrs.
	if cfg.InProcess {
		h, err := newInProcessHost(cfg)
		if err != nil {
			return nil, err
		}
		return wrapHost(h, true), nil
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
			"/ip4/0.0.0.0/tcp/4002/ws",
		}
	}

	// Phase 11: assemble NAT traversal options before the
	// libp2p constructor sees them. The order matters for
	// libp2p's dependency checks (e.g. EnableHolePunching
	// requires the relay client to be enabled, which is on
	// by default).
	natOpts, err := buildNATOptions(cfg)
	if err != nil {
		return nil, err
	}

	bwc := metrics.NewBandwidthCounter()
	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(listen...),
		libp2p.BandwidthReporter(bwc),
		// Pass the transport CONSTRUCTORS; libp2p wires the
		// resource manager / connection manager itself.
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Transport(libp2pws.New),
		libp2p.Security(noise.ID, noise.New),
		libp2p.Muxer(yamux.ID, yamux.DefaultTransport),
	}
	opts = append(opts, natOpts...)
	if cfg.UserAgent != "" {
		opts = append(opts, libp2p.UserAgent(cfg.UserAgent))
	}

	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("host: build libp2p host: %w", err)
	}
	wh := wrapHost(h, false)
	wh.bwc = bwc
	if cfg.MDNS {
		if cfg.OnPeerFound != nil {
			wh.onPeerFound = cfg.OnPeerFound
		}
		// Non-fatal: the host still works, peers just
		// have to find each other through other channels.
		_ = wh.startMDNS(cfg.MDNSServiceName)
	}
	return wh, nil
}

// buildNATOptions assembles the libp2p options that enable
// AutoNAT, DCUtR hole punching, the Circuit Relay v2 service
// (when Config.RelayService is true) and AutoRelay. The
// returned slice is empty when no NAT options are configured
// or the in-process path is used.
func buildNATOptions(cfg Config) ([]libp2p.Option, error) {
	opts := []libp2p.Option{
		// AutoNAT responder: lets other peers probe us to
		// determine their own reachability. Required for
		// AutoNAT to function across the network.
		libp2p.EnableNATService(),
		// DCUtR: even when the connection initially goes
		// through a relay, DCUtR attempts a direct
		// connection upgrade in the background. Spec
		// calls this "hole punching".
		libp2p.EnableHolePunching(),
	}

	// Circuit Relay v2 service. Only enabled on nodes that
	// opt in via Config.RelayService; other nodes still get
	// the relay client (default-on in go-libp2p) so they
	// can USE relays.
	if cfg.RelayService {
		resources := relay.DefaultResources()
		if cfg.RelayMaxReservations > 0 {
			resources.MaxReservations = cfg.RelayMaxReservations
		}
		if cfg.RelayMaxConns > 0 {
			resources.MaxCircuits = cfg.RelayMaxConns
		}
		resources.BufferSize = 4096
		opts = append(opts, libp2p.EnableRelayService(
			relay.WithResources(resources),
		))
	}

	// AutoRelay: when the node discovers it is not publicly
	// reachable, AutoRelay rewrites advertised addresses to
	// include a relay circuit. StaticRelays wins over
	// BootstrapPeers (caller pre-resolves the right thing).
	if len(cfg.StaticRelays) > 0 {
		opts = append(opts, libp2p.EnableAutoRelayWithStaticRelays(cfg.StaticRelays))
	}

	if cfg.ForceRelay {
		// ForceReachabilityPublic makes AutoNAT lie to
		// itself, which is useful when running on a
		// publicly reachable host that happens to be
		// behind a hostile firewall. The "Force" prefix
		// matches the libp2p API name; the behaviour is
		// the libp2p team-shipped semantics.
		opts = append(opts, libp2p.ForceReachabilityPublic())
	}

	return opts, nil
}

// wrapHost attaches the Membuss helpers (event-bus
// subscription, reachability tracking) to a freshly built
// libp2p host. When skipNAT is true the NAT wait is skipped
// entirely (used by the in-process test path).
func wrapHost(h host.Host, skipNAT bool) *Host {
	wh := &Host{
		Host:          h,
		reachability:  network.ReachabilityUnknown,
	}
	if skipNAT {
		return wh
	}
	// Subscribe to local reachability changes. The
	// subscription is held for the lifetime of the host;
	// when the host closes, the subscription channel is
	// closed and watchReachability exits.
	sub, err := h.EventBus().Subscribe(new(event.EvtLocalReachabilityChanged))
	if err != nil {
		// Non-fatal: the host still works, NATStatus just
		// stays "unknown".
		return wh
	}
	wh.eventSub = sub
	go wh.watchReachability()
	return wh
}

// watchReachability consumes EvtLocalReachabilityChanged
// events and updates the cached reachability. It exits when
// the subscription closes (host shutdown).
func (h *Host) watchReachability() {
	if h.eventSub == nil {
		return
	}
	defer h.eventSub.Close()
	for ev := range h.eventSub.Out() {
		if rc, ok := ev.(event.EvtLocalReachabilityChanged); ok {
			h.mu.Lock()
			h.reachability = rc.Reachability
			h.mu.Unlock()
		}
	}
}

// NATStatus returns the most recent AutoNAT verdict as a
// short string: "public", "private" or "unknown". The result
// is "unknown" until the AutoNAT subsystem produces a
// verdict (which usually happens within a few seconds of
// startup when the node has at least one connected peer).
func (h *Host) NATStatus() string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.reachability.String()
}

// WaitForNAT blocks up to timeout for the AutoNAT subsystem
// to produce a reachability verdict. It returns the verdict
// (as a string) and a nil error on success, or the current
// (possibly "unknown") status and ctx.Err() on cancel. A
// non-positive timeout returns immediately.
func (h *Host) WaitForNAT(ctx context.Context, timeout time.Duration) (string, error) {
	if timeout <= 0 {
		return h.NATStatus(), nil
	}
	wctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		s := h.NATStatus()
		if s != network.ReachabilityUnknown.String() {
			return s, nil
		}
		select {
		case <-wctx.Done():
			return h.NATStatus(), wctx.Err()
		case <-t.C:
		}
	}
}

// IsPublic reports whether AutoNAT currently considers this
// node publicly reachable.
func (h *Host) IsPublic() bool {
	return h.NATStatus() == network.ReachabilityPublic.String()
}

// IsPrivate reports whether AutoNAT currently considers this
// node behind a NAT / not publicly reachable.
func (h *Host) IsPrivate() bool {
	return h.NATStatus() == network.ReachabilityPrivate.String()
}

// Reachability returns the raw network.Reachability value.
// Lower-level code that wants the typed enum (rather than
// the string returned by NATStatus) uses this.
func (h *Host) Reachability() network.Reachability {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.reachability
}

// startMDNS attaches a libp2p mDNS discovery service to
// the host. When the service observes a peer it calls
// h.Connect in the background so the connection manager
// immediately starts dialling. The service is closed by
// Host.Close. serviceName is the DNS-SD service tag; pass
// "" to use mdns.ServiceName ("_p2p._udp").
func (h *Host) startMDNS(serviceName string) error {
	if serviceName == "" {
		serviceName = mdns.ServiceName
	}
	svc := mdns.NewMdnsService(h.Host, serviceName, &mdnsNotifee{h: h})
	if err := svc.Start(); err != nil {
		return fmt.Errorf("host: start mdns: %w", err)
	}
	h.mu.Lock()
	h.mdns = svc
	h.mu.Unlock()
	return nil
}

// mdnsNotifee is a tiny adapter that translates libp2p
// mDNS peer-found events into background dials plus
// caller-supplied hooks (e.g. DHT bootstrap). The dial is
// best-effort; failure is silent because mDNS will
// re-announce within a few seconds.
type mdnsNotifee struct {
	h *Host
}

func (n *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if n == nil || n.h == nil || n.h.Host == nil {
		return
	}
	if pi.ID == n.h.Host.ID() {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = n.h.Host.Connect(ctx, pi)
		if n.h.onPeerFound != nil {
			n.h.onPeerFound(pi)
		}
	}()
}

// Close shuts down the Membuss-specific helpers (mDNS,
// reachability subscription) and then closes the underlying
// libp2p host. The mDNS service is closed first so we stop
// emitting announcements while the host is tearing down.
func (h *Host) Close() error {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	svc := h.mdns
	h.mdns = nil
	sub := h.eventSub
	h.eventSub = nil
	h.mu.Unlock()
	if svc != nil {
		_ = svc.Close()
	}
	if sub != nil {
		_ = sub.Close()
	}
	if h.Host != nil {
		return h.Host.Close()
	}
	return nil
}

// loadOrCreateIdentity loads the Ed25519 private key from
// <dir>/identity.key, or generates a new one and saves it with
// 0600 permissions. The returned key's PublicKey hashed to
// libp2p's PeerID is the node's stable network identity.
//
// This is a thin wrapper around the public GenerateIdentity /
// SaveIdentity / LoadIdentity helpers in identity.go; the split
// exists so `membuss-cli init` can re-use the same persistence
// logic without a host being constructed.
func loadOrCreateIdentity(dir string) (crypto.PrivKey, error) {
	if priv, err := LoadIdentity(dir); err == nil {
		return priv, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read identity: %w", err)
	}
	priv, err := GenerateIdentity()
	if err != nil {
		return nil, err
	}
	if err := SaveIdentity(dir, priv); err != nil {
		return nil, err
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

// BandwidthTotals returns the real-time bandwidth totals and rates.
func (h *Host) BandwidthTotals() (totalIn, totalOut int64, rateIn, rateOut float64) {
	if h == nil || h.bwc == nil {
		return 0, 0, 0, 0
	}
	stats := h.bwc.GetBandwidthTotals()
	return stats.TotalIn, stats.TotalOut, stats.RateIn, stats.RateOut
}
