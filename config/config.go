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

	// ReprovideInterval controls how often Mem-Herald re-announces
	// this node's provider records to the DHT.
	ReprovideInterval time.Duration `yaml:"reprovide_interval"`
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
		ReprovideInterval: 12 * time.Hour,
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
	return nil
}
