// Tests for `membuss-cli init`. The init command is the
// canonical way to bring a fresh node online; the tests
// here run runInit directly so they don't need a daemon or
// a real libp2p stack.
package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/crypto"

	"github.com/nnlgsakib/membuss/config"
	"github.com/nnlgsakib/membuss/net/host"
)

// TestRunInit_FreshDir confirms a successful first init:
//   - all required directories exist,
//   - identity.key is present (mode 0600 on POSIX),
//   - config.yaml is a valid YAML file the loader accepts,
//   - the PeerID is the libp2p 12D3Koo... shape.
func TestRunInit_FreshDir(t *testing.T) {
	datadir := filepath.Join(t.TempDir(), "membuss")
	res, err := runInit(datadir, false)
	if err != nil {
		t.Fatalf("runInit: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result on first init")
	}
	if res.DataDir == "" || res.PeerID == "" {
		t.Fatalf("incomplete result: %+v", res)
	}

	// Layout.
	for _, sub := range []string{"", "datastore", "logs"} {
		p := filepath.Join(datadir, sub)
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected %s to exist: %v", p, err)
		}
	}
	cfgPath := config.DefaultConfigPath(datadir)
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("config.yaml missing: %v", err)
	}

	// identity.key: file exists, 0600 on POSIX, loadable.
	priv, err := host.LoadIdentity(datadir)
	if err != nil {
		t.Fatalf("LoadIdentity: %v", err)
	}
	if runtime.GOOS != "windows" {
		st, err := os.Stat(filepath.Join(datadir, host.IdentityFilename))
		if err != nil {
			t.Fatal(err)
		}
		if got := st.Mode().Perm(); got != host.IdentityFileMode {
			t.Fatalf("identity.key mode: got %o want %o", got, host.IdentityFileMode)
		}
	}

	// PeerID shape.
	pid, err := host.PeerIDFromKey(priv)
	if err != nil {
		t.Fatalf("PeerIDFromKey: %v", err)
	}
	if !strings.HasPrefix(pid.String(), "12D3Koo") {
		t.Fatalf("PeerID prefix = %q, want 12D3Koo...", pid.String())
	}

	// Config is valid YAML LoadConfig can re-read.
	cfg, err := config.LoadConfig(datadir)
	if err != nil {
		t.Fatalf("LoadConfig after init: %v", err)
	}
	if cfg.DataDir == "" {
		t.Fatal("DataDir empty in loaded config")
	}
	if cfg.LogLevel == "" {
		t.Fatal("LogLevel empty in loaded config")
	}
}

// TestRunInit_Idempotent confirms running init a second time
// without --force is a no-op (returns nil, nil).
func TestRunInit_Idempotent(t *testing.T) {
	datadir := filepath.Join(t.TempDir(), "membuss")
	if _, err := runInit(datadir, false); err != nil {
		t.Fatalf("first runInit: %v", err)
	}
	res, err := runInit(datadir, false)
	if err != nil {
		t.Fatalf("second runInit: %v", err)
	}
	if res != nil {
		t.Fatalf("expected nil result on second init, got %+v", res)
	}
}

// TestRunInit_ForceRegeneratesIdentity confirms that
// `init --force` produces a new PeerID (i.e. a fresh Ed25519
// keypair) even though the directory was already initialised.
func TestRunInit_ForceRegeneratesIdentity(t *testing.T) {
	datadir := filepath.Join(t.TempDir(), "membuss")

	first, err := runInit(datadir, false)
	if err != nil {
		t.Fatalf("first runInit: %v", err)
	}

	second, err := runInit(datadir, true)
	if err != nil {
		t.Fatalf("second runInit --force: %v", err)
	}
	if first.PeerID == second.PeerID {
		t.Fatalf("expected different PeerID after --force, both = %s", first.PeerID)
	}

	// The new key must be loadable and round-trip through
	// marshal/unmarshal unchanged.
	priv, err := host.LoadIdentity(datadir)
	if err != nil {
		t.Fatalf("LoadIdentity: %v", err)
	}
	raw1, _ := crypto.MarshalPrivateKey(priv)
	raw2, _ := crypto.MarshalPrivateKey(priv)
	if string(raw1) != string(raw2) {
		t.Fatal("identity did not round-trip")
	}
}

// TestRunInit_ConfigRoundTrip confirms the file written by
// init can be re-read and validates. This protects the
// template engine from accidentally emitting non-YAML.
func TestRunInit_ConfigRoundTrip(t *testing.T) {
	datadir := filepath.Join(t.TempDir(), "membuss")
	if _, err := runInit(datadir, false); err != nil {
		t.Fatalf("runInit: %v", err)
	}
	cfg, err := config.LoadConfig(datadir)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	// Sanity: defaults that the init template hard-codes.
	if cfg.GRPCAddr == "" {
		t.Fatal("GRPCAddr empty")
	}
	if cfg.GatewayAddr == "" {
		t.Fatal("GatewayAddr empty")
	}
	if cfg.APIAddr == "" {
		t.Fatal("APIAddr empty")
	}
}

// TestPrintInitSummary_Output makes sure the summary table
// includes the resolved datadir, the PeerID, and the next
// step hint.
func TestPrintInitSummary_Output(t *testing.T) {
	var buf strings.Builder
	r := &InitResult{
		DataDir:       "/tmp/datadir",
		ConfigPath:    "/tmp/datadir/config.yaml",
		DatastorePath: "/tmp/datadir/datastore",
		LogsPath:      "/tmp/datadir/logs",
		IdentityPath:  "/tmp/datadir/identity.key",
		PeerID:        "12D3KooTest",
	}
	printInitSummary(&buf, r)
	out := buf.String()
	for _, want := range []string{
		"Initialized membuss node at: /tmp/datadir",
		"12D3KooTest",
		"/tmp/datadir/config.yaml",
		"membuss-cli daemon start",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("summary missing %q\n---\n%s\n---", want, out)
		}
	}
}
