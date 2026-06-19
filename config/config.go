// Package config defines the on-disk and in-memory configuration model for
// the Membuss daemon and loads it from a YAML file.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration object for a Membuss node.
//
// All fields are populated by Load. Defaults are applied by Default() before
// the YAML overlay is applied, so any field that the user omits from the
// config file gets a safe, sensible default rather than a zero value.
type Config struct {
	// ListenAddrs are the libp2p multiaddrs the host binds to.
	ListenAddrs []string `yaml:"listen_addrs"`

	// BootstrapPeers are the libp2p peer IDs (as multiaddrs) the DHT
	// attempts to connect to on startup. May be empty for a fresh
	// testnet or single-node run.
	BootstrapPeers []string `yaml:"bootstrap_peers"`

	// DataDir is the directory used by BadgerDB and the local block
	// store. The directory is created on startup if it does not exist.
	DataDir string `yaml:"data_dir"`

	// GatewayAddr is the HTTP listen address for the public Mem-Gate
	// gateway (CDN layer). Example: "127.0.0.1:8080".
	GatewayAddr string `yaml:"gateway_addr"`

	// APIAddr is the HTTP listen address for the local Node control
	// API. Example: "127.0.0.1:5001".
	APIAddr string `yaml:"api_addr"`

	// GRPCAddr is the listen address for the CLI <-> daemon gRPC
	// service. Example: "127.0.0.1:50051".
	GRPCAddr string `yaml:"grpc_addr"`

	// AnchorMode toggles the Anchor Node full-sync engine. When true,
	// the node will attempt to mirror all announced content so that
	// it remains available when original providers go offline.
	AnchorMode bool `yaml:"anchor_mode"`

	// AutoGCInterval controls how often the background garbage
	// collection loop runs. Zero disables auto-GC. Mutually
	// exclusive with AnchorMode (anchor nodes never GC).
	AutoGCInterval time.Duration `yaml:"auto_gc_interval"`

	// GCMinAge is the minimum age a block must have before it
	// can be garbage-collected. Recently-fetched content is
	// protected from GC for this duration. Zero means no age
	// restriction (original behavior).
	GCMinAge time.Duration `yaml:"gc_min_age"`

	// ReprovideInterval controls how often Mem-Herald re-announces
	// this node's provider records to the DHT.
	ReprovideInterval time.Duration `yaml:"reprovide_interval"`

	LogLevel string `yaml:"log_level"`
	GatewayTLS TLSConfig `yaml:"gateway_tls"`
	APITLS TLSConfig `yaml:"api_tls"`
	APIKey string `yaml:"api_key"`
	GatewayRateLimitPerMin int `yaml:"gateway_rate_limit_per_min"`
	MemexRetryBackoff RetryBackoffConfig `yaml:"memex_retry_backoff"`
	BootstrapBackoff RetryBackoffConfig `yaml:"bootstrap_backoff"`
	MetricsEnabled bool `yaml:"metrics_enabled"`

	// --- Phase 11: NAT traversal + relay fallback ---

	// RelayService enables the circuit v2 relay hop on this node so
	// it can forward traffic for NATed peers. Anchor nodes should
	// set this to true.
	RelayService bool `yaml:"relay_service"`
	// RelayMaxConns caps the number of simultaneously relayed
	// circuits. Default 128.
	RelayMaxConns int `yaml:"relay_max_conns"`
	// RelayMaxReservations caps the number of active relay
	// reservations. Default 128.
	RelayMaxReservations int `yaml:"relay_max_reservations"`
	// RelayBandwidthMB is the soft bandwidth cap (MB/s) the
	// relay will budget for forwarded traffic. 0 disables the
	// cap. Default 16.
	RelayBandwidthMB int `yaml:"relay_bandwidth_mb"`
	// ForceRelay, when true, makes this node always use a relay
	// for outbound dials, skipping hole-punch. Useful for
	// debugging.
	ForceRelay bool `yaml:"force_relay"`
	// NATWaitSeconds is how long the daemon waits on startup
	// for AutoNAT to resolve reachability before continuing.
	// Default 10s.
	NATWaitSeconds int `yaml:"nat_wait_seconds"`

	// --- Phase 13: Bloom filters ---

	// BloomCapacity is the expected number of MIDs the in-memory
	// block-existence filter will hold. Default 10_000_000.
	BloomCapacity uint `yaml:"bloom_capacity"`
	// BloomFPRate is the target false positive rate for the
	// block-existence filter. Default 0.001.
	BloomFPRate float64 `yaml:"bloom_fp_rate"`
	// BloomDisabled turns the in-memory filter off entirely.
	BloomDisabled bool `yaml:"bloom_disabled"`
	// MemexBloomAnnounceInterval controls how often the local
	// node broadcasts its sealed-MID bloom filter to directly
	// connected peers. Default 5m. Set to 0 to disable the
	// gossip.
	MemexBloomAnnounceInterval time.Duration `yaml:"memex_bloom_announce_interval"`

	// --- Phase 14: MID version ---

	// --- Phase 17: DHT server mode + provider persistence ---

	// DHTMode controls the kad-dht operating mode.
	// Allowed values: "auto" (default, lets kad-dht
	// pick Client vs. Server from AutoNAT),
	// "client", "server", "auto-server".
	//
	// In a private multi-node cluster (e.g. a Docker
	// bridge) AutoNAT may classify every node as
	// private and ModeAuto degrades to a pure-client
	// DHT that never answers queries. Operators who
	// want cross-node content resolution must set
	// this to "server".
	DHTMode string `yaml:"dht_mode"`
	// DHTOptimisticProvide, when true, enables the
	// kaddht.EnableOptimisticProvide shortcut: as
	// soon as the local node has announced the CID
	// to its K closest peers the Provide call
	// returns. Matches the IPFS default. Default
	// true.
	DHTOptimisticProvide bool `yaml:"dht_optimistic_provide"`

	// MIDVersion selects which MID string format the
	// daemon uses. v1 is the canonical CIDv1 +
	// base32lower form (default). legacy is the
	// pre-Phase-14 base58 form, supported for one
	// release cycle so operators can drain
	// pre-upgrade stores.
	MIDVersion string `yaml:"mid_version"`

	// --- Phase: geolocation ---

	// EnableGeolocation enables server-side IP geolocation
	// for peer addresses. When true, the explorer enriches
	// each peer with approximate Country, City, Lat, Lon
	// using a local MaxMind GeoLite2-City database.
	EnableGeolocation bool `yaml:"enable_geolocation"`
	// GeolocationDB is an optional path to a custom
	// GeoLite2-City.mmdb file. When empty the resolver
	// looks for GeoLite2-City.mmdb in DataDir.
	GeolocationDB string `yaml:"geolocation_db"`
}

// TLSConfig is a pair of PEM file paths enabling HTTPS on an HTTP
// server. Both fields must be set (or both empty).
type TLSConfig struct {
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// Enabled reports whether the TLS configuration is usable.
func (t TLSConfig) Enabled() bool { return t.CertFile != "" && t.KeyFile != "" }

// RetryBackoffConfig configures an exponential retry schedule.
type RetryBackoffConfig struct {
	Initial     time.Duration `yaml:"initial"`
	Max         time.Duration `yaml:"max"`
	Factor      float64       `yaml:"factor"`
	MaxAttempts int           `yaml:"max_attempts"`
}

// Default returns a Config populated with safe, sensible defaults
// suitable for running a local development node.
func Default() *Config {
	return &Config{
		ListenAddrs: []string{
			"/ip4/0.0.0.0/tcp/4001",
			"/ip4/0.0.0.0/udp/4001/quic-v1",
		},
		BootstrapPeers:    []string{},
		DataDir:           "./data",
		GatewayAddr:       "127.0.0.1:8080",
		APIAddr:           "127.0.0.1:5001",
		GRPCAddr:          "127.0.0.1:50051",
		AnchorMode:        false,
		AutoGCInterval:    24 * time.Hour,
		GCMinAge:          24 * time.Hour,
		ReprovideInterval: 12 * time.Hour,
		LogLevel:               "info",
		GatewayTLS:             TLSConfig{},
		APITLS:                 TLSConfig{},
		APIKey:                 "",
		GatewayRateLimitPerMin: 100,
		MemexRetryBackoff: RetryBackoffConfig{
			Initial:     100 * time.Millisecond,
			Max:         30 * time.Second,
			Factor:      2.0,
			MaxAttempts: 4,
		},
		BootstrapBackoff: RetryBackoffConfig{
			Initial:     500 * time.Millisecond,
			Max:         60 * time.Second,
			Factor:      2.0,
			MaxAttempts: 0,
		},
		MetricsEnabled: true,
		RelayService:         false,
		RelayMaxConns:        128,
		RelayMaxReservations: 128,
		RelayBandwidthMB:     16,
		ForceRelay:           false,
		NATWaitSeconds:       10,
		BloomCapacity:                  10_000_000,
		BloomFPRate:                    0.001,
		BloomDisabled:                  false,
		MemexBloomAnnounceInterval:     5 * time.Minute,
		MIDVersion:                    "v1",
		DHTMode:                       "server",
		DHTOptimisticProvide:          true,
		EnableGeolocation:             false,
	}
}

// Load reads a YAML config file from path, applies the defaults from
// Default() to any field the user did not set, and validates the result.
//
// The returned Config is always non-nil when err is nil.
func Load(path string) (*Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %q: %w", path, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("config: parse %q: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: invalid %q: %w", path, err)
	}

	return cfg, nil
}

// Validate returns an error if cfg is missing values that would make
// the daemon unstartable. Defaults do not bypass this check; a field
// explicitly set to the empty value will fail validation.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("nil config")
	}
	if strings.TrimSpace(c.DataDir) == "" {
		return errors.New("data_dir is required")
	}
	if len(c.ListenAddrs) == 0 {
		return errors.New("at least one listen_addrs entry is required")
	}
	for i, a := range c.ListenAddrs {
		if strings.TrimSpace(a) == "" {
			return fmt.Errorf("listen_addrs[%d] is empty", i)
		}
	}
	if c.ReprovideInterval <= 0 {
		return errors.New("reprovide_interval must be positive")
	}
	if c.GatewayRateLimitPerMin < 0 {
		return errors.New("gateway_rate_limit_per_min must be >= 0")
	}
	if (c.GatewayTLS.CertFile == "") != (c.GatewayTLS.KeyFile == "") {
		return errors.New("gateway_tls: cert_file and key_file must both be set or both empty")
	}
	if (c.APITLS.CertFile == "") != (c.APITLS.KeyFile == "") {
		return errors.New("api_tls: cert_file and key_file must both be set or both empty")
	}
	if c.MemexRetryBackoff.Initial < 0 || c.MemexRetryBackoff.Max < 0 {
		return errors.New("memex_retry_backoff: durations must be >= 0")
	}
	if c.MemexRetryBackoff.Factor < 1 {
		return errors.New("memex_retry_backoff: factor must be >= 1")
	}
	if c.BootstrapBackoff.Initial < 0 || c.BootstrapBackoff.Max < 0 {
		return errors.New("bootstrap_backoff: durations must be >= 0")
	}
	if c.BootstrapBackoff.Factor < 1 {
		return errors.New("bootstrap_backoff: factor must be >= 1")
	}
	if c.RelayMaxConns < 0 {
		return errors.New("relay_max_conns must be >= 0")
	}
	if c.RelayMaxReservations < 0 {
		return errors.New("relay_max_reservations must be >= 0")
	}
	if c.RelayBandwidthMB < 0 {
		return errors.New("relay_bandwidth_mb must be >= 0")
	}
	if c.NATWaitSeconds < 0 {
		return errors.New("nat_wait_seconds must be >= 0")
	}

	if c.BloomFPRate < 0 || c.BloomFPRate >= 1 {
		return errors.New("bloom_fp_rate must be in [0, 1)")
	}
	if c.MemexBloomAnnounceInterval < 0 {
		return errors.New("memex_bloom_announce_interval must be >= 0")
	}
	if c.MIDVersion != "v1" && c.MIDVersion != "legacy" && c.MIDVersion != "" {
		return errors.New("mid_version must be 'v1' or 'legacy'")
	}
	switch c.DHTMode {
	case "", "auto", "client", "server", "auto-server":
	default:
		return errors.New("dht_mode must be one of: auto, client, server, auto-server")
	}
	if c.AnchorMode {
		c.AutoGCInterval = 0
	}
	return nil
}
