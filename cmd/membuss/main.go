// Command membuss is the Membuss daemon entry point.
//
// Phase 7: the daemon boots the libp2p host, DHT, PEX,
// Memex, Mem-Herald, and (optionally) the Anchor engine,
// then hosts the gRPC service that membuss-cli dials.
//
// Startup sequence:
//  1. Load config (config.Load).
//  2. Open the BadgerDB block store (core/store.NewMemStore).
//  3. Build the libp2p host with persistent Ed25519 identity.
//  4. Build the DHT and bootstrap to the configured peers.
//  5. Build PEX, Memex, and Mem-Herald; start their loops.
//  6. If AnchorMode is on, build the Anchor engine and start it.
//  7. Start the gRPC server with a daemonBackend that wires
//     every subsystem into the server.Backend interface.
//  8. Wait for SIGINT / SIGTERM and shut everything down.
package main

import (
	"context"
	cryptoTLS "crypto/tls"
	"path/filepath"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nnlgsakib/membuss/api"
	memgate "github.com/nnlgsakib/membuss/gateway/memgate"
	explorerPkg "github.com/nnlgsakib/membuss/gateway/explorer"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/nnlgsakib/membuss/anchor"
	"github.com/nnlgsakib/membuss/config"
	"github.com/nnlgsakib/membuss/core/store"
	"github.com/nnlgsakib/membuss/net/dht"
	"github.com/nnlgsakib/membuss/net/herald"
	"github.com/nnlgsakib/membuss/net/host"
	"github.com/nnlgsakib/membuss/net/memex"
	"github.com/nnlgsakib/membuss/net/pex"
	"github.com/nnlgsakib/membuss/obs/logging"
	"github.com/nnlgsakib/membuss/obs/metrics"
	serverpkg "github.com/nnlgsakib/membuss/rpc/server"
)

func main() {
	cfgPath := flag.String("config", "membuss.yaml", "path to YAML config file")
	build := flag.String("build", "dev", "build identifier reported by Ping")
	inMemory := flag.Bool("in-memory", false, "use an in-memory BadgerDB (no on-disk state)")
	noAnchor := flag.Bool("no-anchor", false, "disable the anchor engine even if config enables it")
	flag.Parse()

	// Build identifier flows into Ping responses.
	serverpkg.Build = *build

	// Construct a bootstrap logger at info level until we have
	// the real config. The real logger is built after Load.
	bootLogger := logging.New(os.Stdout, "info")
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		bootLogger.Error("config load failed", "err", err.Error())
		os.Exit(1)
	}
	logger := logging.New(os.Stdout, cfg.LogLevel)
	slog.SetDefault(logger)
	banner(cfg, *cfgPath, *inMemory, *noAnchor)

	// Optional Prometheus instrumentation. Disabled via
	// config (cfg.MetricsEnabled=false) for deployments that
	// do not want the /metrics endpoint.
	var mtrx *metrics.Metrics
	if cfg.MetricsEnabled {
		mtrx = metrics.New()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1) Block store.
	bs, err := openStore(cfg, *inMemory)
	if err != nil {
		logger.Error("open store", "err", err.Error()); os.Exit(1)
	}
	defer bs.Close()

	// Pre-parse bootstrap peers so the host can use them
	// as AutoRelay static candidates.
	bootstrapPeers, err := parsePeers(cfg.BootstrapPeers)
	if err != nil {
		logger.Error("parse bootstrap peers", "err", err.Error()); os.Exit(1)
	}

	// 2) libp2p host.
	hostCfg := host.Config{
		ListenAddrs:       cfg.ListenAddrs,
		DataDir:           cfg.DataDir,
		UserAgent:         "membuss/" + *build,
		StaticRelays:      bootstrapPeers,
		// --- Phase 11: NAT traversal ---
		RelayService:        cfg.RelayService,
		RelayMaxConns:       cfg.RelayMaxConns,
		RelayMaxReservations: cfg.RelayMaxReservations,
		RelayBandwidthMB:    cfg.RelayBandwidthMB,
		ForceRelay:          cfg.ForceRelay,
	}
	h, err := host.NewHost(hostCfg)
	if err != nil {
		logger.Error("host", "err", err.Error()); os.Exit(1)
	}
	defer h.Close()
	fmt.Fprintf(os.Stdout, "  peer_id:        %s\n", h.ID())
	fmt.Fprintf(os.Stdout, "  listen_addrs:   %v\n", h.Addrs())
	// Phase 11: wait for AutoNAT to resolve reachability.
	wait := time.Duration(cfg.NATWaitSeconds) * time.Second
	natStatus, natErr := h.WaitForNAT(ctx, wait)
	if natErr != nil && !errors.Is(natErr, context.DeadlineExceeded) {
		logger.Warn("nat wait", "err", natErr.Error())
	}
	fmt.Fprintf(os.Stdout, "  nat_status:     %s\n", natStatus)
	if natStatus == "private" {
		logger.Info("node is behind a NAT; relay addresses will be advertised")
	}

	// 3) DHT.
	mdht, err := dht.New(ctx, dht.Config{Host: h})
	if err != nil {
		logger.Error("dht", "err", err.Error()); os.Exit(1)
	}
	if err := mdht.Bootstrap(ctx, bootstrapPeers); err != nil {
		logger.Warn("dht bootstrap", "err", err.Error())
	}
	defer mdht.Close()

	// 4) PEX.
	px, err := pex.New(pex.Config{Host: h})
	if err != nil {
		logger.Error("pex", "err", err.Error()); os.Exit(1)
	}
	px.Start(ctx)
	defer px.Stop()

	// 5a) Memex bloom manager (peer filter exchange, Phase 13).
	// Constructed first so we can wire it into the engine.
	bloomMgr, err := memex.NewBloomManager(memex.BloomConfig{
		Host:     h,
		Sealed:   bs,
		Interval: cfg.MemexBloomAnnounceInterval,
	})
	if err != nil {
		logger.Error("memex bloom", "err", err.Error()); os.Exit(1)
	}
	bloomMgr.Start()
	defer bloomMgr.Stop()

	// 5b) Memex engine.
	mx, err := memex.New(memex.Config{Host: h, Blockstore: bs, Bloom: bloomMgr})
	if err != nil {
		logger.Error("memex", "err", err.Error()); os.Exit(1)
	}
	mx.Start()
	defer mx.Stop()

	// 6) Mem-Herald.
	hd, err := herald.New(herald.Config{
		Store:    bs,
		DHT:      mdht,
		Strategy: herald.StrategyRoots,
		Interval: cfg.ReprovideInterval,
		Rate:     100,
		Burst:    8,
	})
	if err != nil {
		logger.Error("herald", "err", err.Error()); os.Exit(1)
	}
	hd.Start(ctx)
	defer hd.Stop()

	// Phase 11: relay announcer. Only wired when the
	// node runs the relay service; other nodes just
	// consume the DHT relay list when they bootstrap.
	var relayAnnouncer *herald.RelayAnnouncer
	if cfg.RelayService {
		relayAnnouncer, err = herald.NewRelayAnnouncer(herald.RelayAnnouncer{
			DHT:      mdht,
			Interval: cfg.ReprovideInterval,
			Logger:   logger,
		})
		if err != nil {
			logger.Error("relay announcer", "err", err.Error()); os.Exit(1)
		}
		relayAnnouncer.Start(ctx)
		defer func() { _ = relayAnnouncer }()
		fmt.Fprintf(os.Stdout, "  relay_service:  enabled\n")
	}

	// 7) Optional Anchor engine.
	var anchorEng *anchor.AnchorEngine
	if cfg.AnchorMode && !*noAnchor {
		fetcher := &memexFetcher{eng: mx}
		anchorEng, err = anchor.New(anchor.Config{
			Host:              h,
			DHT:               mdht,
			Store:             bs,
			Herald:            hd,
			Fetcher:           fetcher,
			DiscoveryInterval: 30 * time.Second,
		})
		if err != nil {
			logger.Error("anchor", "err", err.Error()); os.Exit(1)
		}
		if err := anchorEng.Start(ctx); err != nil {
			logger.Error("anchor start", "err", err.Error()); os.Exit(1)
		}
		defer anchorEng.Stop()
		fmt.Fprintf(os.Stdout, "  anchor_mode:    enabled\n")
	} else {
		fmt.Fprintf(os.Stdout, "  anchor_mode:    disabled\n")
	}

	// 8) gRPC server.
	backend := &daemonBackend{
		dataDir:      cfg.DataDir,
		host:         h,
		store:        bs,
		dht:          mdht,
		pex:          px,
		memex:        mx,
		herald:       hd,
		anchor:       anchorEng,
		metrics:      mtrx,
		retryBackoff: cfg.MemexRetryBackoff,
		logger:       logger,
	}
	grpcSrv, err := startGRPC(cfg.GRPCAddr, backend)
	if err != nil {
		logger.Error("grpc", "err", err.Error()); os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "  grpc_addr:      %s\n", cfg.GRPCAddr)
	// 9) Mem-Gate: public HTTP gateway + CDN edge.
	gateSrv, err := startGateway(cfg.GatewayAddr, newMemgateAdapter(backend), newExplorerAdapter(backend, cfg.AnchorMode), cfg.GatewayRateLimitPerMin, cfg.GatewayTLS)
	if err != nil {
		logger.Error("gateway", "err", err.Error()); os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "  gateway_addr:   %s\n", gateSrv.Addr())

	// 10) Node API: local control plane over HTTP/JSON.
	apiSrv, err := startNodeAPI(cfg.APIAddr, newAPIAdapter(backend), mtrx, cfg.APIKey, cfg.APITLS)
	if err != nil {
		logger.Error("api", "err", err.Error()); os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "  api_addr:       %s\n", apiSrv.Addr())

	// Graceful shutdown: wait for SIGINT/SIGTERM, then
	// drain in the order: HTTP servers (gateway, api),
	// gRPC, memex engine, herald, anchor, pex, dht,
	// libp2p host, store.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("shutdown requested", "signal", sig.String())

	shutdownCtx, scancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer scancel()

	if err := gateSrv.ShutdownCtx(shutdownCtx); err != nil {
		logger.Warn("gateway shutdown", "err", err.Error())
	}
	if err := apiSrv.ShutdownCtx(shutdownCtx); err != nil {
		logger.Warn("api shutdown", "err", err.Error())
	}
	grpcSrv.GracefulStop()
	if err := mx.StopWait(shutdownCtx); err != nil {
		logger.Warn("memex stop", "err", err.Error())
	}
	logger.Info("shutdown complete")
}

// banner writes the startup banner to stdout.
func banner(cfg *config.Config, cfgPath string, inMemory, noAnchor bool) {
	fmt.Fprintf(os.Stdout,
		"membuss daemon starting\n"+
			"  config:           %s\n"+
			"  data_dir:         %s\n"+
			"  in_memory:        %t\n"+
			"  no_anchor:        %t\n"+
			"  bootstrap_peers:  %d\n"+
			"  http_cfg_addrs:   gateway=%s api=%s\n"+
			"  reprovide:        %s\n",
		cfgPath, cfg.DataDir, inMemory, noAnchor,
		len(cfg.BootstrapPeers),
		cfg.GatewayAddr, cfg.APIAddr,
		cfg.ReprovideInterval,
	)
}

// openStore opens the local BadgerDB block store. When
// inMemory is true, the store is backed by RAM and discards
// its contents on Close. The data dir is still passed through
// to subsystems that need it (host identity, etc.).
func openStore(cfg *config.Config, inMemory bool) (store.Store, error) {
	bloom := store.BloomConfig{
		Capacity: cfg.BloomCapacity,
		FPRate:   cfg.BloomFPRate,
		Disabled: cfg.BloomDisabled,
	}
	if !inMemory && !cfg.BloomDisabled {
		bloom.SnapshotPath = filepath.Join(cfg.DataDir, "bloom.bin")
	}
	if inMemory {
		return store.NewMemStore(store.Options{InMemory: true, Bloom: bloom})
	}
	return store.NewMemStore(store.Options{Path: cfg.DataDir, Bloom: bloom})
}

// parsePeers converts a list of "peerID+multiaddr" strings
// (as used in the config file) into peer.AddrInfo values.
func parsePeers(raw []string) ([]peer.AddrInfo, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]peer.AddrInfo, 0, len(raw))
	for _, s := range raw {
		ai, err := parsePeer(s)
		if err != nil {
			return nil, fmt.Errorf("parse peer %q: %w", s, err)
		}
		out = append(out, ai)
	}
	return out, nil
}

// parsePeer parses a single bootstrap peer string in either of:
//
//	/ip4/1.2.3.4/tcp/4001/p2p/<peerID>   (full multiaddr)
//	<peerID>                              (no addr, skip)
func parsePeer(s string) (peer.AddrInfo, error) {
	if s == "" {
		return peer.AddrInfo{}, fmt.Errorf("empty peer string")
	}
	// Try full multiaddr.
	ma, err := multiaddr.NewMultiaddr(s)
	if err == nil {
		ai, err := peer.AddrInfoFromP2pAddr(ma)
		if err == nil {
			return *ai, nil
		}
		// Fall through: maybe it's a multiaddr without /p2p/ suffix.
		return peer.AddrInfo{Addrs: []multiaddr.Multiaddr{ma}}, nil
	}
	// Try plain peer ID.
	id, err := peer.Decode(s)
	if err != nil {
		return peer.AddrInfo{}, fmt.Errorf("not a valid multiaddr or peer id: %w", err)
	}
	return peer.AddrInfo{ID: id}, nil
}

// startGRPC brings up the gRPC server on addr and returns it
// to the caller. The caller is responsible for GracefulStop.
func startGRPC(addr string, b serverpkg.Backend) (*serverGRPC, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	s := newServerGRPC()
	serverpkg.NewServer(b).Register(s.gsrv)
	go func() { _ = s.gsrv.Serve(lis) }()
	return s, nil
}

// serverGRPC is a small wrapper to make the gRPC server
// addressable from main() and to keep the construction in
// one place.
type serverGRPC struct {
	gsrv *serverGRPCServer
}

// newServerGRPC creates a fresh gRPC server with sensible
// defaults. Kept as a separate constructor so it can be
// replaced in tests with a bufconn-based listener.
func newServerGRPC() *serverGRPC {
	return &serverGRPC{gsrv: newGRPCServer()}
}

func (s *serverGRPC) GracefulStop() { s.gsrv.GracefulStop() }

// startGateway brings up the public Mem-Gate HTTP server.
// rateLimitPerMin is the per-IP request budget enforced on
// every public request. tls enables HTTPS when its
// CertFile/KeyFile are set.
func startGateway(addr string, b memgate.Backend, exp *explorerAdapter, rateLimitPerMin int, tlsCfg config.TLSConfig) (*httpServer, error) {
	mg, err := memgate.New(memgate.Config{
		Backend:         b,
		MaxCacheBytes:   64 << 20, // 64 MiB LRU
		ExplorerHandler: buildExplorer(exp),
		RateLimitPerMin: rateLimitPerMin,
	})
	if err != nil {
		return nil, fmt.Errorf("memgate: %w", err)
	}
	return startHTTP(addr, "membuss-gateway", mg.Handler(), tlsCfg)
}

// startNodeAPI brings up the local Node control API. mtrx
// exposes Prometheus at /metrics; apiKey enables X-Membuss-Key
// auth on every /api/v1 endpoint; tls enables HTTPS.
func startNodeAPI(addr string, b api.Backend, mtrx *metrics.Metrics, apiKey string, tlsCfg config.TLSConfig) (*httpServer, error) {
	nodeAPI, err := api.New(api.Config{
		Backend:        b,
		MaxUploadBytes: 1 << 30, // 1 GiB
		APIKey:         apiKey,
		Metrics:        mtrx,
	})
	if err != nil {
		return nil, fmt.Errorf("nodeapi: %w", err)
	}
	return startHTTP(addr, "membuss-api", nodeAPI.Handler(), tlsCfg)
}

// startHTTP binds an http.Handler to addr in a goroutine and
// returns a handle whose Close method does a graceful shutdown.
// A non-ErrServerClosed error from Serve is logged. When tls
// is non-empty, the server runs HTTPS with the supplied cert+key.
func startHTTP(addr, name string, h http.Handler, tlsCfg config.TLSConfig) (*httpServer, error) {
	srv := &http.Server{
		Addr:         addr,
		Handler:      h,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  2 * time.Minute,
	}
	if tlsCfg.Enabled() {
		cert, err := cryptoTLS.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("%s: load tls: %w", name, err)
		}
		srv.TLSConfig = &cryptoTLS.Config{Certificates: []cryptoTLS.Certificate{cert}, MinVersion: cryptoTLS.VersionTLS12}
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	hs := &httpServer{name: name, srv: srv, ln: ln, boundAddr: ln.Addr().String()}
	go func() {
		var err error
		if srv.TLSConfig != nil {
			// We need a TLS listener for ServeTLS; rebuild it.
			tlsLn := cryptoTLS.NewListener(ln, srv.TLSConfig)
			err = srv.Serve(tlsLn)
		} else {
			err = srv.Serve(ln)
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http serve", "name", name, "err", err.Error())
		}
	}()
	return hs, nil
}

// httpServer is a thin wrapper around http.Server that
// remembers its bound listener and a friendly name for
// logs. Close performs a 5s graceful shutdown.
type httpServer struct {
	name string
	srv  *http.Server
	ln   net.Listener

	// boundAddr is the address the OS assigned to the
	// listener. Captured at startHTTP time and stable
	// for the lifetime of the listener (useful when the
	// caller passed ":0" and needs the resolved port).
	boundAddr string
}

// Addr returns the address the server is listening on.
// When the configured addr was ":0" the returned
// string contains the OS-assigned port.
func (h *httpServer) Addr() string {
	if h == nil || h.ln == nil {
		return ""
	}
	return h.ln.Addr().String()
}

// buildExplorer constructs the explorer http.Handler.
// It returns nil when exp is nil so the gateway can be
// constructed without an explorer for tests.
func buildExplorer(exp *explorerAdapter) http.Handler {
	if exp == nil {
		return nil
	}
	h, err := explorerPkg.New(explorerPkg.Config{Backend: exp})
	if err != nil {
		slog.Warn("explorer", "err", err.Error())
		return nil
	}
	return h.Handler()
}

// Close performs a graceful shutdown with a 5s budget.
func (h *httpServer) Close() {
	if h == nil || h.srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.srv.Shutdown(ctx); err != nil {
		slog.Warn("http shutdown", "name", h.name, "err", err.Error())
	}
}

// ShutdownCtx performs a graceful shutdown bounded by the
// provided context's deadline. The daemon's main() calls this
// in sequence to drain HTTP traffic before tearing down the
// rest of the subsystems.
func (h *httpServer) ShutdownCtx(ctx context.Context) error {
	if h == nil || h.srv == nil {
		return nil
	}
	return h.srv.Shutdown(ctx)
}
