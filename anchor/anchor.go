// Package anchor implements the Anchor Node full-sync engine.
//
// Anchor nodes ensure content persists even when original
// providers go offline. A node configured with
// AnchorMode=true runs an AnchorEngine that:
//
//   - Discovers content announced on the DHT and pulls it
//     into the local store via Memex.
//   - Runs Mem-Herald with StrategyAll so every block in
//     the local store is announced to the DHT.
//   - Publishes itself as an anchor so other nodes can
//     discover it as a fallback provider.
//
// The engine is intentionally conservative: it never deletes
// content from the local store, it rate-limits its DHT
// queries, and it shuts down cleanly when the host context
// is cancelled.
package anchor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/multiformats/go-multiaddr"

	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/net/dht"
	"github.com/nnlgsakib/membuss/net/herald"
)

// AnchorRegistryKey is the local store meta key under which
// the anchor peer list is persisted as JSON.
const AnchorRegistryKey = "/membuss/anchors/v1"

// DefaultDiscoveryInterval is the time between the anchor
// engine's discovery + fetch rounds.
const DefaultDiscoveryInterval = 30 * time.Second

// MaxEnqueueBacklog caps the number of externally-queued
// MIDs the engine will process per round.
const MaxEnqueueBacklog = 1024

// AnchorStore is the subset of store.Store the anchor
// engine actually depends on. Splitting it out keeps tests
// free of BadgerDB and lets the in-memory store satisfy the
// engine without dragging the full Phase 2 surface into
// every test binary.
type AnchorStore interface {
	herald.SealedLister

	Size() (uint64, error)
	Put(m mid.MID, data []byte) error
	Has(m mid.MID) (bool, error)
	PutMeta(key string, value []byte) error
	GetMeta(key string) ([]byte, error)
	Seal(m mid.MID, recursive bool) error
	Close() error
}

// ProviderResolver returns the peers that should be asked to
// serve a given MID. The default implementation wraps the
// local DHT (see defaultProviderResolver); tests can inject
// a direct resolver that returns a known peer list without
// depending on DHT provider-record propagation.
type ProviderResolver interface {
	Resolve(ctx context.Context, m mid.MID) ([]peer.AddrInfo, error)
}

// defaultProviderResolver is the production implementation
// of ProviderResolver. It calls the local DHT and pads the
// result with registered anchor peers.
type defaultProviderResolver struct {
	dht     *dht.MemDHT
	anchors func() []peer.AddrInfo
}

func (r *defaultProviderResolver) Resolve(ctx context.Context, m mid.MID) ([]peer.AddrInfo, error) {
	provs, err := r.dht.FindProviders(ctx, m)
	if err != nil {
		return nil, err
	}
	return mergeAnchors(provs, r.anchors()), nil
}

// Fetcher is the contract the anchor engine uses to pull
// content from a peer. The default implementation is built
// on top of net/memex.
type Fetcher interface {
	Fetch(ctx context.Context, root mid.MID, providers []peer.AddrInfo) error
}

// Logger is the optional structured logger interface the
// engine uses. nil falls back to a no-op.
type Logger interface {
	Infof(format string, args ...any)
	Errorf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Infof(string, ...any)  {}
func (nopLogger) Errorf(string, ...any) {}

// Config configures an AnchorEngine.
type Config struct {
	// Host is the local libp2p host. Required.
	Host host.Host
	// DHT is the local DHT facade. Required.
	DHT *dht.MemDHT
	// Store is the local content store. Required.
	Store AnchorStore
	// Herald is the local Mem-Herald. Required.
	Herald *herald.MemHerald
	// Fetcher pulls content. Required.
	Fetcher Fetcher
	// ProviderResolver resolves providers for a given MID.
	// If nil, the engine builds a default that calls
	// DHT.FindProviders and pads with registered anchors.
	ProviderResolver ProviderResolver
	// DiscoveryInterval is the time between discovery +
	// fetch rounds. Default is DefaultDiscoveryInterval.
	DiscoveryInterval time.Duration
	// BootstrapAnchors is the initial set of anchor peers
	// the engine should trust on startup.
	BootstrapAnchors []peer.AddrInfo
	// Logger is optional; nil means silent.
	Logger Logger
}

// AnchorEngine is the Anchor Node full-sync engine.
type AnchorEngine struct {
	cfg    Config
	logger Logger

	mu       sync.Mutex
	anchors  map[peer.ID]peer.AddrInfo
	backlog  []mid.MID
	started  time.Time
	synced   int64
	hostSeen map[string]struct{}

	resolver ProviderResolver

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// New constructs an AnchorEngine. Call Start to begin the
// background loop.
func New(cfg Config) (*AnchorEngine, error) {
	if cfg.Host == nil {
		return nil, errors.New("anchor: nil host")
	}
	if cfg.DHT == nil {
		return nil, errors.New("anchor: nil dht")
	}
	if cfg.Store == nil {
		return nil, errors.New("anchor: nil store")
	}
	if cfg.Herald == nil {
		return nil, errors.New("anchor: nil herald")
	}
	if cfg.Fetcher == nil {
		return nil, errors.New("anchor: nil fetcher")
	}
	if cfg.DiscoveryInterval <= 0 {
		cfg.DiscoveryInterval = DefaultDiscoveryInterval
	}
	if cfg.Logger == nil {
		cfg.Logger = nopLogger{}
	}
	return &AnchorEngine{
		cfg:      cfg,
		logger:   cfg.Logger,
		anchors:  make(map[peer.ID]peer.AddrInfo),
		hostSeen: make(map[string]struct{}),
		resolver: cfg.ProviderResolver,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}, nil
}

// Start loads the persisted anchor registry, registers the
// bootstrap anchors, and launches the discovery loop.
func (e *AnchorEngine) Start(ctx context.Context) error {
	e.started = time.Now()
	if e.resolver == nil {
		dht := e.cfg.DHT
		e.resolver = &defaultProviderResolver{dht: dht, anchors: func() []peer.AddrInfo {
			return e.AnchorPeers()
		}}
	}
	e.loadRegistry()
	for _, ai := range e.cfg.BootstrapAnchors {
		e.AddAnchor(ai)
	}
	go e.loop(ctx)
	return nil
}

// Stop signals the loop to exit and waits for it.
func (e *AnchorEngine) Stop() {
	e.stopOnce.Do(func() { close(e.stopCh) })
	<-e.doneCh
}

// Enqueue asks the anchor engine to ensure root is locally
// stored. Safe to call from any goroutine.
func (e *AnchorEngine) Enqueue(root mid.MID) {
	if root.IsZero() {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.backlog) >= MaxEnqueueBacklog {
		e.backlog = e.backlog[1:]
	}
	e.backlog = append(e.backlog, root)
}

// AddAnchor adds ai to the local anchor registry.
func (e *AnchorEngine) AddAnchor(ai peer.AddrInfo) {
	if ai.ID == "" {
		return
	}
	e.mu.Lock()
	e.anchors[ai.ID] = ai
	e.mu.Unlock()
	e.cfg.Host.Peerstore().AddAddrs(ai.ID, ai.Addrs, peerstore.PermanentAddrTTL)
}

// RemoveAnchor removes a peer from the local anchor
// registry.
func (e *AnchorEngine) RemoveAnchor(id peer.ID) {
	e.mu.Lock()
	delete(e.anchors, id)
	e.mu.Unlock()
}

// AnchorPeers returns a snapshot of the current anchor
// registry.
func (e *AnchorEngine) AnchorPeers() []peer.AddrInfo {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]peer.AddrInfo, 0, len(e.anchors))
	for _, ai := range e.anchors {
		out = append(out, ai)
	}
	return out
}

// PublishSelf publishes this node's identity under
// AnchorRegistryKey. The wire format is JSON-encoded
// {id, addrs}.
func (e *AnchorEngine) PublishSelf(ctx context.Context) error {
	ai := peer.AddrInfo{
		ID:    e.cfg.Host.ID(),
		Addrs: e.cfg.Host.Addrs(),
	}
	payload, err := encodeAddrInfo(ai)
	if err != nil {
		return err
	}
	return e.cfg.DHT.PutValue(ctx, AnchorRegistryKey, payload)
}

// AnchorStatus is the JSON-shaped status the engine reports
// via its Status() method and the /anchor/status HTTP
// endpoint.
type AnchorStatus struct {
	PeerID     string        `json:"peer_id"`
	Uptime     time.Duration `json:"uptime"`
	BlocksHeld int64         `json:"blocks_held"`
	Anchors    int           `json:"anchors"`
	Backlog    int           `json:"backlog"`
	Synced     int64         `json:"synced"`
}

// Status returns a snapshot of the engine's stats.
func (e *AnchorEngine) Status() AnchorStatus {
	e.mu.Lock()
	backlog := len(e.backlog)
	anchors := len(e.anchors)
	e.mu.Unlock()
	var held int64
	if sz, err := e.cfg.Store.Size(); err == nil {
		held = int64(sz)
	}
	return AnchorStatus{
		PeerID:     e.cfg.Host.ID().String(),
		Uptime:     time.Since(e.started),
		BlocksHeld: held,
		Anchors:    anchors,
		Backlog:    backlog,
		Synced:     atomic.LoadInt64(&e.synced),
	}
}

func (e *AnchorEngine) loop(ctx context.Context) {
	defer close(e.doneCh)
	t := time.NewTicker(e.cfg.DiscoveryInterval)
	defer t.Stop()
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		case <-time.After(2 * time.Second):
			if err := e.PublishSelf(ctx); err != nil {
				e.logger.Errorf("anchor: publish self: %v", err)
			}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopCh:
			return
		case <-t.C:
			e.tick(ctx)
		}
	}
}

func (e *AnchorEngine) tick(ctx context.Context) {
	e.persistRegistry()

	// Discover content from connected peers via the
	// direct content-exchange stream.
	e.discoverFromPeers(ctx)

	e.mu.Lock()
	pending := e.backlog
	e.backlog = nil
	e.mu.Unlock()

	for _, m := range pending {
		e.fetchIfMissing(ctx, m)
	}

	sealed, err := e.cfg.Store.AllSealed()
	if err != nil || len(sealed) == 0 {
		return
	}
	maxSample := 4
	if len(sealed) < maxSample {
		maxSample = len(sealed)
	}
	for i := 0; i < maxSample; i++ {
		m := sealed[i]
		if !e.shouldFetch(m) {
			continue
		}
		provs, err := e.cfg.DHT.FindProviders(ctx, m)
		if err != nil {
			continue
		}
		provs = mergeAnchors(provs, e.AnchorPeers())
		if len(provs) == 0 {
			continue
		}
		if err := e.cfg.Fetcher.Fetch(ctx, m, provs); err != nil {
			e.logger.Errorf("anchor: fetch %s: %v", m.String()[:12], err)
			continue
		}
		e.sealFetched(ctx, m)
		atomic.AddInt64(&e.synced, 1)
		e.markSeen(m)
	}
}

// discoverFromPeers opens content-exchange streams to all
// connected peers and enqueues any sealed MIDs we don't
// already have.
func (e *AnchorEngine) discoverFromPeers(ctx context.Context) {
	known := make(map[string]struct{})
	sealed, err := e.cfg.Store.AllSealed()
	if err == nil {
		for _, m := range sealed {
			known[m.String()] = struct{}{}
		}
	}

	announcements, err := DiscoverContent(ctx, e.cfg.Host, known)
	if err != nil {
		e.logger.Errorf("anchor: discover from peers: %v", err)
		return
	}

	for _, a := range announcements {
		e.Enqueue(a.MID)
	}

	if len(announcements) > 0 {
		e.logger.Infof("anchor: discovered %d new MIDs from peers", len(announcements))
	}
}

// sealFetched seals a MID that was just fetched so GC
// never deletes it.
func (e *AnchorEngine) sealFetched(ctx context.Context, m mid.MID) {
	if err := e.cfg.Store.Seal(m, false); err != nil {
		e.logger.Errorf("anchor: seal fetched %s: %v", m.String()[:12], err)
	}
}

func (e *AnchorEngine) fetchIfMissing(ctx context.Context, m mid.MID) {
	if !e.shouldFetch(m) {
		return
	}
	provs, err := e.resolver.Resolve(ctx, m)
	if err != nil {
		return
	}
	if len(provs) == 0 {
		return
	}
	if err := e.cfg.Fetcher.Fetch(ctx, m, provs); err != nil {
		e.logger.Errorf("anchor: fetch %s: %v", m.String()[:12], err)
		return
	}
	e.sealFetched(ctx, m)
	atomic.AddInt64(&e.synced, 1)
	e.markSeen(m)
}

func (e *AnchorEngine) shouldFetch(m mid.MID) bool {
	if m.IsZero() {
		return false
	}
	has, err := e.cfg.Store.Has(m)
	if err != nil {
		return true
	}
	return !has
}

func (e *AnchorEngine) markSeen(m mid.MID) {
	e.mu.Lock()
	e.hostSeen[m.String()] = struct{}{}
	e.mu.Unlock()
}

func (e *AnchorEngine) persistRegistry() {
	e.mu.Lock()
	anchors := make([]peer.AddrInfo, 0, len(e.anchors))
	for _, ai := range e.anchors {
		anchors = append(anchors, ai)
	}
	e.mu.Unlock()
	payload, err := json.Marshal(anchors)
	if err != nil {
		return
	}
	_ = e.cfg.Store.PutMeta(AnchorRegistryKey, payload)
}

func (e *AnchorEngine) loadRegistry() {
	raw, err := e.cfg.Store.GetMeta(AnchorRegistryKey)
	if err != nil {
		return
	}
	var anchors []peer.AddrInfo
	if err := json.Unmarshal(raw, &anchors); err != nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, ai := range anchors {
		if ai.ID == "" {
			continue
		}
		e.anchors[ai.ID] = ai
		e.cfg.Host.Peerstore().AddAddrs(ai.ID, ai.Addrs, peerstore.PermanentAddrTTL)
	}
}

// FindProvidersWithAnchors wraps dht.FindProviders and pads
// the result with known anchor peers when fewer than `want`
// providers are returned. Direct providers keep their
// ordering; anchors are appended in registry order.
func FindProvidersWithAnchors(ctx context.Context, d *dht.MemDHT, m mid.MID, anchors []peer.AddrInfo, want int) ([]peer.AddrInfo, error) {
	provs, err := d.FindProviders(ctx, m)
	if err != nil {
		return nil, err
	}
	provs = mergeAnchors(provs, anchors)
	if want > 0 && len(provs) > want {
		provs = provs[:want]
	}
	return provs, nil
}

func mergeAnchors(direct, anchors []peer.AddrInfo) []peer.AddrInfo {
	if len(anchors) == 0 {
		return direct
	}
	seen := make(map[peer.ID]struct{}, len(direct))
	for _, p := range direct {
		seen[p.ID] = struct{}{}
	}
	for _, a := range anchors {
		if _, ok := seen[a.ID]; ok {
			continue
		}
		seen[a.ID] = struct{}{}
		direct = append(direct, a)
	}
	return direct
}

func encodeAddrInfo(ai peer.AddrInfo) ([]byte, error) {
	type wire struct {
		ID    string   `json:"id"`
		Addrs []string `json:"addrs"`
	}
	w := wire{ID: ai.ID.String()}
	for _, a := range ai.Addrs {
		w.Addrs = append(w.Addrs, a.String())
	}
	return json.Marshal(w)
}

// DecodeAnchorValue parses a value previously written by
// encodeAddrInfo. It is exported so other packages can
// reuse the same wire format.
func DecodeAnchorValue(raw []byte) (peer.AddrInfo, error) {
	type wire struct {
		ID    string   `json:"id"`
		Addrs []string `json:"addrs"`
	}
	var w wire
	if err := json.Unmarshal(raw, &w); err != nil {
		return peer.AddrInfo{}, err
	}
	id, err := peer.Decode(w.ID)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("anchor: bad peer id: %w", err)
	}
	addrs := make([]multiaddr.Multiaddr, 0, len(w.Addrs))
	for _, s := range w.Addrs {
		a, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			continue
		}
		addrs = append(addrs, a)
	}
	return peer.AddrInfo{ID: id, Addrs: addrs}, nil
}
