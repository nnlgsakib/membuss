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
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nnlgsakib/membuss/api"
	memgate "github.com/nnlgsakib/membuss/gateway/memgate"

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

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("membuss: %v", err)
	}
	banner(cfg, *cfgPath, *inMemory, *noAnchor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1) Block store.
	bs, err := openStore(cfg, *inMemory)
	if err != nil {
		log.Fatalf("membuss: open store: %v", err)
	}
	defer bs.Close()

	// 2) libp2p host.
	hostCfg := host.Config{
		ListenAddrs: cfg.ListenAddrs,
		DataDir:     cfg.DataDir,
		UserAgent:   "membuss/" + *build,
	}
	h, err := host.NewHost(hostCfg)
	if err != nil {
		log.Fatalf("membuss: host: %v", err)
	}
	defer h.Close()
	fmt.Fprintf(os.Stdout, "  peer_id:        %s\n", h.ID())
	fmt.Fprintf(os.Stdout, "  listen_addrs:   %v\n", h.Addrs())

	// 3) DHT.
	mdht, err := dht.New(ctx, dht.Config{Host: h})
	if err != nil {
		log.Fatalf("membuss: dht: %v", err)
	}
	bootstrapPeers, err := parsePeers(cfg.BootstrapPeers)
	if err != nil {
		log.Fatalf("membuss: parse bootstrap peers: %v", err)
	}
	if err := mdht.Bootstrap(ctx, bootstrapPeers); err != nil {
		log.Printf("membuss: dht bootstrap: %v (continuing)", err)
	}
	defer mdht.Close()

	// 4) PEX.
	px, err := pex.New(pex.Config{Host: h})
	if err != nil {
		log.Fatalf("membuss: pex: %v", err)
	}
	px.Start(ctx)
	defer px.Stop()

	// 5) Memex engine.
	mx, err := memex.New(memex.Config{Host: h, Blockstore: bs})
	if err != nil {
		log.Fatalf("membuss: memex: %v", err)
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
		log.Fatalf("membuss: herald: %v", err)
	}
	hd.Start(ctx)
	defer hd.Stop()

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
			log.Fatalf("membuss: anchor: %v", err)
		}
		if err := anchorEng.Start(ctx); err != nil {
			log.Fatalf("membuss: anchor start: %v", err)
		}
		defer anchorEng.Stop()
		fmt.Fprintf(os.Stdout, "  anchor_mode:    enabled\n")
	} else {
		fmt.Fprintf(os.Stdout, "  anchor_mode:    disabled\n")
	}

	// 8) gRPC server.
	backend := &daemonBackend{
		dataDir: cfg.DataDir,
		host:    h,
		store:   bs,
		dht:     mdht,
		pex:     px,
		memex:   mx,
		herald:  hd,
		anchor:  anchorEng,
	}
	grpcSrv, err := startGRPC(cfg.GRPCAddr, backend)
	if err != nil {
		log.Fatalf("membuss: grpc: %v", err)
	}
	defer grpcSrv.GracefulStop()
	fmt.Fprintf(os.Stdout, "  grpc_addr:      %s\n", cfg.GRPCAddr)
	// 9) Mem-Gate: public HTTP gateway + CDN edge.
	gateSrv, err := startGateway(cfg.GatewayAddr, newMemgateAdapter(backend))
	if err != nil {
		log.Fatalf("membuss: gateway: %v", err)
	}
	defer gateSrv.Close()
	fmt.Fprintf(os.Stdout, "  gateway_addr:   %s\n", gateSrv.Addr())

	// 10) Node API: local control plane over HTTP/JSON.
	apiSrv, err := startNodeAPI(cfg.APIAddr, newAPIAdapter(backend))
	if err != nil {
		log.Fatalf("membuss: api: %v", err)
	}
	defer apiSrv.Close()
	fmt.Fprintf(os.Stdout, "  api_addr:       %s\n", apiSrv.Addr())

	// Wait for SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	fmt.Fprintf(os.Stdout, "\nmembuss: received %s, shutting down...\n", sig)
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
	if inMemory {
		return store.NewMemStore(store.Options{InMemory: true})
	}
	return store.NewMemStore(store.Options{Path: cfg.DataDir})
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

// startGateway brings up the public Mem-Gate HTTP server
// and returns a handle the caller can Close to shut it
// down. The handler is mounted at "/".
func startGateway(addr string, b memgate.Backend) (*httpServer, error) {
	mg, err := memgate.New(memgate.Config{
		Backend:       b,
		MaxCacheBytes: 64 << 20, // 64 MiB LRU
	})
	if err != nil {
		return nil, fmt.Errorf("memgate: %w", err)
	}
	return startHTTP(addr, "membuss-gateway", mg.Handler())
}

// startNodeAPI brings up the local Node control API. The
// handler is mounted at "/api/v1" by api.Handler.
func startNodeAPI(addr string, b api.Backend) (*httpServer, error) {
	nodeAPI, err := api.New(api.Config{
		Backend:        b,
		MaxUploadBytes: 1 << 30, // 1 GiB
	})
	if err != nil {
		return nil, fmt.Errorf("nodeapi: %w", err)
	}
	return startHTTP(addr, "membuss-api", nodeAPI.Handler())
}

// startHTTP binds an http.Handler to addr in a goroutine
// and returns a handle whose Close method does a graceful
// shutdown. A non-ErrServerClosed error from Serve is
// logged.
func startHTTP(addr, name string, h http.Handler) (*httpServer, error) {
	srv := &http.Server{
		Addr:         addr,
		Handler:      h,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute,
		IdleTimeout:  2 * time.Minute,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	hs := &httpServer{name: name, srv: srv, ln: ln, boundAddr: ln.Addr().String()}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("%s: serve: %v", name, err)
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

// Close performs a graceful shutdown with a 5s budget.
func (h *httpServer) Close() {
	if h == nil || h.srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.srv.Shutdown(ctx); err != nil {
		log.Printf("%s: shutdown: %v", h.name, err)
	}
}
