package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config must validate: %v", err)
	}
	if cfg.DataDir == "" {
		t.Fatal("default DataDir must be set")
	}
	if cfg.ReprovideInterval <= 0 {
		t.Fatal("default ReprovideInterval must be positive")
	}
	if len(cfg.ListenAddrs) == 0 {
		t.Fatal("default ListenAddrs must be non-empty")
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "does-not-exist.yaml"))
	if err == nil {
		t.Fatal("Load on missing file must return an error")
	}
}

func TestLoadOverridesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")

	yaml := `
data_dir: /tmp/membuss-data
gateway_addr: 0.0.0.0:9090
anchor_mode: true
reprovide_interval: 5m
listen_addrs:
  - /ip4/127.0.0.1/tcp/9999
bootstrap_peers:
  - /ip4/1.2.3.4/tcp/4001/p2p/QmExample
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.DataDir != "/tmp/membuss-data" {
		t.Errorf("DataDir = %q, want /tmp/membuss-data", cfg.DataDir)
	}
	if cfg.GatewayAddr != "0.0.0.0:9090" {
		t.Errorf("GatewayAddr = %q, want 0.0.0.0:9090", cfg.GatewayAddr)
	}
	if !cfg.AnchorMode {
		t.Error("AnchorMode = false, want true")
	}
	if cfg.ReprovideInterval != 5*time.Minute {
		t.Errorf("ReprovideInterval = %v, want 5m", cfg.ReprovideInterval)
	}
	if len(cfg.ListenAddrs) != 1 || cfg.ListenAddrs[0] != "/ip4/127.0.0.1/tcp/9999" {
		t.Errorf("ListenAddrs = %v, want single override", cfg.ListenAddrs)
	}
	if len(cfg.BootstrapPeers) != 1 {
		t.Errorf("BootstrapPeers len = %d, want 1", len(cfg.BootstrapPeers))
	}
}

func TestValidateRejectsEmptyDataDir(t *testing.T) {
	cfg := Default()
	cfg.DataDir = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("empty DataDir must fail validation")
	}
}

func TestValidateRejectsZeroInterval(t *testing.T) {
	cfg := Default()
	cfg.ReprovideInterval = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("zero ReprovideInterval must fail validation")
	}
}

func TestValidateRejectsEmptyListenAddrs(t *testing.T) {
	cfg := Default()
	cfg.ListenAddrs = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("nil ListenAddrs must fail validation")
	}
}
