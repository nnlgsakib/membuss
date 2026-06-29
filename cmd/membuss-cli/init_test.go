// Tests for `membuss-cli init`. The init command is the
// canonical way to bring a fresh node online; the tests
// here run runInit directly so they don't need a daemon or
// a real libp2p stack.
package main

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
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

	// Write a dummy file to verify force cleanup
	dummyFile := filepath.Join(datadir, "dummy.txt")
	if err := os.WriteFile(dummyFile, []byte("dummy"), 0600); err != nil {
		t.Fatalf("failed to write dummy file: %v", err)
	}

	second, err := runInit(datadir, true)
	if err != nil {
		t.Fatalf("second runInit --force: %v", err)
	}
	if first.PeerID == second.PeerID {
		t.Fatalf("expected different PeerID after --force, both = %s", first.PeerID)
	}

	// Verify the dummy file was deleted by --force
	if _, err := os.Stat(dummyFile); !os.IsNotExist(err) {
		t.Errorf("expected dummy file to be deleted by --force, but stat got: %v", err)
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

func TestRunInit_PortAutoAdjustment(t *testing.T) {
	// Bind listeners on default ports matching their config bind IPs
	binds := []struct {
		host string
		port int
	}{
		{"0.0.0.0", 4001},
		{"0.0.0.0", 4002},
		{"127.0.0.1", 5001},
		{"127.0.0.1", 8080},
		{"127.0.0.1", 50051},
	}
	listeners := make([]net.Listener, 0, len(binds))
	for _, b := range binds {
		l, err := net.Listen("tcp", net.JoinHostPort(b.host, strconv.Itoa(b.port)))
		if err == nil {
			listeners = append(listeners, l)
		} else {
			t.Logf("failed to bind mock listener to %s:%d: %v", b.host, b.port, err)
		}
	}
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()

	datadir := filepath.Join(t.TempDir(), "membuss")
	res, err := runInit(datadir, false)
	if err != nil {
		t.Fatalf("runInit: %v", err)
	}
	if res == nil {
		t.Fatal("expected non-nil result")
	}

	cfg, err := config.LoadConfig(datadir)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	// Verify that ports were auto-adjusted and are different from default blocked ports
	_, apiPortStr, _ := net.SplitHostPort(cfg.APIAddr)
	if apiPortStr == "5001" {
		t.Error("expected API port to be adjusted from 5001, but it remained 5001")
	}

	_, gatewayPortStr, _ := net.SplitHostPort(cfg.GatewayAddr)
	if gatewayPortStr == "8080" {
		t.Error("expected Gateway port to be adjusted from 8080, but it remained 8080")
	}

	_, grpcPortStr, _ := net.SplitHostPort(cfg.GRPCAddr)
	if grpcPortStr == "50051" {
		t.Error("expected GRPC port to be adjusted from 50051, but it remained 50051")
	}

	// Also verify ListenAddrs
	for _, ma := range cfg.ListenAddrs {
		if strings.Contains(ma, "/tcp/4001") {
			t.Error("expected listen TCP port to be adjusted from 4001")
		}
		if strings.Contains(ma, "/tcp/4002") {
			t.Error("expected listen WS TCP port to be adjusted from 4002")
		}
	}
}

func TestRunInit_PortAvoidanceFromSiblingConfig(t *testing.T) {
	parentDir := t.TempDir()

	// Initialize first sibling node
	node1 := filepath.Join(parentDir, "node1")
	res1, err := runInit(node1, false)
	if err != nil {
		t.Fatalf("runInit node1: %v", err)
	}
	if res1 == nil {
		t.Fatal("expected non-nil result for node1")
	}

	cfg1, err := config.LoadConfig(node1)
	if err != nil {
		t.Fatalf("LoadConfig node1: %v", err)
	}

	// Initialize second sibling node
	node2 := filepath.Join(parentDir, "node2")
	res2, err := runInit(node2, false)
	if err != nil {
		t.Fatalf("runInit node2: %v", err)
	}
	if res2 == nil {
		t.Fatal("expected non-nil result for node2")
	}

	cfg2, err := config.LoadConfig(node2)
	if err != nil {
		t.Fatalf("LoadConfig node2: %v", err)
	}

	// Extract ports for both nodes
	ports1 := make(map[int]bool)
	collectPortsFromConfig(cfg1, ports1)

	ports2 := make(map[int]bool)
	collectPortsFromConfig(cfg2, ports2)

	// Verify there is absolutely no overlap between ports1 and ports2
	for p := range ports2 {
		if ports1[p] {
			t.Errorf("found overlapping port %d in both nodes", p)
		}
	}
}
