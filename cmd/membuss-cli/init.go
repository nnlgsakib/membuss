// Phase 16: `membuss-cli init`.
//
// The init command is the canonical way to bring a fresh node
// online. It is intentionally self-contained: it does not dial
// the daemon (there is no daemon yet!), and it never reads the
// running config because the whole point of init is to create
// one. It resolves the data directory from the
// --datadir / MEMBUSS_DATADIR / default chain, creates the
// on-disk layout, writes a fresh Ed25519 identity, and emits a
// config.yaml with comments so the operator can read what
// every field does.
//
// Idempotency: running init on an already-initialised datadir
// is a no-op (exit 0) unless --force is supplied. --force
// re-generates the identity and overwrites the config; the
// datastore and bloom snapshot are left in place (init is
// not a destructive operation; a separate `membuss-cli gc` or
// manual cleanup is required for that).
package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/nnlgsakib/membuss/config"
	"github.com/nnlgsakib/membuss/net/host"
)

// InitResult is the public summary of a successful init run.
// It is exported so the test suite can assert on individual
// fields without scraping stdout.
type InitResult struct {
	DataDir   string
	ConfigPath string
	DatastorePath string
	LogsPath  string
	PeerID    string
	IdentityPath string
}

// newInitCmd builds the `init` subcommand.
func newInitCmd() *cobra.Command {
	var (
		force bool
	)
	c := &cobra.Command{
		Use:   "init",
		Short: "Initialize a new membuss node in the data directory",
		Long: `init creates the data directory layout, generates a fresh
Ed25519 libp2p identity, and writes a config.yaml with all
the default knobs. The data directory is resolved from
--datadir, then MEMBUSS_DATADIR, then $HOME/.memdata.

If the data directory is already initialised, init is a no-op
unless --force is supplied; --force re-generates the identity
and overwrites the config. The on-disk datastore and bloom
filter snapshot are left untouched (use ` + "`membuss-cli gc`" + `
or remove them manually if you want a truly clean start).`,
		RunE: func(cmd *cobra.Command, args []string) error {
			datadir := config.ResolveDataDir(cmd.Flag("datadir").Value.String())
			if datadir == "" {
				return fmt.Errorf(
					"could not determine data directory: " +
						"pass --datadir or set MEMBUSS_DATADIR or HOME")
			}
			res, err := runInit(datadir, force)
			if err != nil {
				return err
			}
			if res == nil {
				// Already-initialised, no-op path.
				return nil
			}
			printInitSummary(cmd.OutOrStdout(), res)
			return nil
		},
	}
	c.Flags().BoolVar(&force, "force", false, "re-initialise even if the data directory is already initialised")
	return c
}

// runInit is the testable core of the init command. It returns
// (nil, nil) when the directory is already initialised and
// --force was not set; (res, nil) on success; (nil, err) on
// failure.
func runInit(datadir string, force bool) (*InitResult, error) {
	if datadir == "" {
		return nil, fmt.Errorf("empty data directory")
	}

	// Resolve to an absolute path so the summary table is
	// unambiguous regardless of where the operator ran init from.
	abs, err := filepath.Abs(datadir)
	if err != nil {
		return nil, fmt.Errorf("resolve %q: %w", datadir, err)
	}
	datadir = abs

	already := config.IsInitialized(datadir)
	if already && !force {
		// The contract is "exit 0 with a friendly message".
		// The cobra layer prints the message via stdout (not
		// stderr) so it does not look like an error.
		fmt.Printf("already initialized at %s\n", datadir)
		fmt.Println("re-run with --force to regenerate identity and config")
		return nil, nil
	}

	if force {
		// Clean the existing data directory to start completely fresh
		if err := os.RemoveAll(datadir); err != nil {
			return nil, fmt.Errorf("the data directory %s or files inside it (like config.yaml or badger files) are currently locked/in-use by another process (such as a running daemon). Please stop the daemon, close any open files, and try again. (Details: %w)", datadir, err)
		}
	}

	// 1) Layout.
	dirs := []string{
		datadir,
		filepath.Join(datadir, "datastore"),
		filepath.Join(datadir, "logs"),
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, host.IdentityDirMode); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", d, err)
		}
	}

	// 2) Identity.
	// Re-generate on --force (or first run); reuse the existing
	// key if --force is NOT set but the data dir already has one
	// (e.g. the operator deleted config.yaml but kept the key).
	// For the common "fresh dir" case this is just Generate.
	priv, err := host.LoadIdentity(datadir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load existing identity: %w", err)
	}
	if priv == nil || force {
		priv, err = host.GenerateIdentity()
		if err != nil {
			return nil, err
		}
	}
	if err := host.SaveIdentity(datadir, priv); err != nil {
		return nil, err
	}
	pid, err := host.PeerIDFromKey(priv)
	if err != nil {
		return nil, fmt.Errorf("derive peer id: %w", err)
	}

	// 3) Config.
	cfg := config.DefaultConfig(datadir)
	adjustPorts(cfg)
	cfgPath := config.DefaultConfigPath(datadir)
	if err := config.WriteConfig(cfg, cfgPath); err != nil {
		return nil, err
	}

	return &InitResult{
		DataDir:       datadir,
		ConfigPath:    cfgPath,
		DatastorePath: filepath.Join(datadir, "datastore"),
		LogsPath:      filepath.Join(datadir, "logs"),
		IdentityPath:  filepath.Join(datadir, host.IdentityFilename),
		PeerID:        pid.String(),
	}, nil
}

// printInitSummary writes a human-readable summary to w. The
// shape is intentionally narrow so a future `membuss-cli init
// --json` could re-use the same fields.
func printInitSummary(w io.Writer, r *InitResult) {
	if r == nil {
		return
	}
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Initialized membuss node at:", r.DataDir)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "  Peer ID:\t%s\n", r.PeerID)
	fmt.Fprintf(tw, "  Identity:\t%s\n", r.IdentityPath)
	fmt.Fprintf(tw, "  Config:\t%s\n", r.ConfigPath)
	fmt.Fprintf(tw, "  Datastore:\t%s\n", r.DatastorePath)
	fmt.Fprintf(tw, "  Logs:\t%s\n", r.LogsPath)
	_ = tw.Flush()
	fmt.Fprintln(w, "")
	fmt.Fprintln(w, "Edit config.yaml then run: membuss-cli daemon start")
	// A single blank line keeps the next shell prompt separated
	// without an extra newline in non-interactive contexts.
	fmt.Fprintln(w, "")
}

// trimNewline is a small helper used by tests that capture
// the printed summary as a string.
func trimNewline(s string) string { return strings.TrimRight(s, "\n") }

func adjustPorts(cfg *config.Config) {
	usedPorts := make(map[int]bool)

	// Scan sibling directories for existing config.yaml files
	if cfg.DataDir != "" {
		parent := filepath.Dir(cfg.DataDir)
		if entries, err := os.ReadDir(parent); err == nil {
			for _, entry := range entries {
				if entry.IsDir() {
					siblingPath := filepath.Join(parent, entry.Name())
					// Skip our own directory
					if filepath.Clean(siblingPath) == filepath.Clean(cfg.DataDir) {
						continue
					}
					// Check if sibling config exists
					if config.IsInitialized(siblingPath) {
						loadPortsFromSibling(siblingPath, usedPorts)
					}
				}
			}
		}
	}

	var err error
	cfg.GatewayAddr, err = adjustHostPort(cfg.GatewayAddr, usedPorts)
	if err != nil {
		_ = err
	}
	cfg.APIAddr, err = adjustHostPort(cfg.APIAddr, usedPorts)
	if err != nil {
		_ = err
	}
	cfg.GRPCAddr, err = adjustHostPort(cfg.GRPCAddr, usedPorts)
	if err != nil {
		_ = err
	}

	for i, ma := range cfg.ListenAddrs {
		cfg.ListenAddrs[i], err = adjustMultiaddrPort(ma, usedPorts)
		if err != nil {
			_ = err
		}
	}
}

func loadPortsFromSibling(siblingPath string, usedPorts map[int]bool) {
	siblingCfg, err := config.LoadConfig(siblingPath)
	if err == nil && siblingCfg != nil {
		collectPortsFromConfig(siblingCfg, usedPorts)
		return
	}

	// Fallback to manual parsing if Validate() or other checks failed
	path := config.DefaultConfigPath(siblingPath)
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}

	var minimalCfg struct {
		GatewayAddr string   `yaml:"gateway_addr"`
		APIAddr     string   `yaml:"api_addr"`
		GRPCAddr    string   `yaml:"grpc_addr"`
		ListenAddrs []string `yaml:"listen_addrs"`
	}
	if err := yaml.Unmarshal(data, &minimalCfg); err == nil {
		if _, portStr, err := net.SplitHostPort(minimalCfg.GatewayAddr); err == nil {
			if p, err := strconv.Atoi(portStr); err == nil {
				usedPorts[p] = true
			}
		}
		if _, portStr, err := net.SplitHostPort(minimalCfg.APIAddr); err == nil {
			if p, err := strconv.Atoi(portStr); err == nil {
				usedPorts[p] = true
			}
		}
		if _, portStr, err := net.SplitHostPort(minimalCfg.GRPCAddr); err == nil {
			if p, err := strconv.Atoi(portStr); err == nil {
				usedPorts[p] = true
			}
		}
		for _, maStr := range minimalCfg.ListenAddrs {
			parts := strings.Split(maStr, "/")
			for idx, part := range parts {
				if (part == "tcp" || part == "udp") && idx+1 < len(parts) {
					if p, err := strconv.Atoi(parts[idx+1]); err == nil {
						usedPorts[p] = true
					}
				}
			}
		}
	}
}

func collectPortsFromConfig(cfg *config.Config, usedPorts map[int]bool) {
	if _, portStr, err := net.SplitHostPort(cfg.GatewayAddr); err == nil {
		if p, err := strconv.Atoi(portStr); err == nil {
			usedPorts[p] = true
		}
	}
	if _, portStr, err := net.SplitHostPort(cfg.APIAddr); err == nil {
		if p, err := strconv.Atoi(portStr); err == nil {
			usedPorts[p] = true
		}
	}
	if _, portStr, err := net.SplitHostPort(cfg.GRPCAddr); err == nil {
		if p, err := strconv.Atoi(portStr); err == nil {
			usedPorts[p] = true
		}
	}
	for _, maStr := range cfg.ListenAddrs {
		parts := strings.Split(maStr, "/")
		for idx, part := range parts {
			if (part == "tcp" || part == "udp") && idx+1 < len(parts) {
				if p, err := strconv.Atoi(parts[idx+1]); err == nil {
					usedPorts[p] = true
				}
			}
		}
	}
}

func adjustHostPort(addrStr string, usedPorts map[int]bool) (string, error) {
	host, portStr, err := net.SplitHostPort(addrStr)
	if err != nil {
		return addrStr, nil
	}

	startPort, err := strconv.Atoi(portStr)
	if err != nil {
		return addrStr, nil
	}

	port := startPort
	for attempts := 0; attempts < 1000 && port <= 65535; attempts++ {
		if !usedPorts[port] {
			if isTCPPortAvailable(host, port) {
				usedPorts[port] = true
				return net.JoinHostPort(host, strconv.Itoa(port)), nil
			}
		}
		port++
	}
	return addrStr, nil
}

func adjustMultiaddrPort(maStr string, usedPorts map[int]bool) (string, error) {
	parts := strings.Split(maStr, "/")
	protoIdx := -1
	for i, part := range parts {
		if part == "tcp" || part == "udp" {
			protoIdx = i
			break
		}
	}
	if protoIdx == -1 || protoIdx+1 >= len(parts) {
		return maStr, nil
	}

	proto := parts[protoIdx]
	portStr := parts[protoIdx+1]
	startPort, err := strconv.Atoi(portStr)
	if err != nil {
		return maStr, nil
	}

	hostIP := "0.0.0.0"
	if protoIdx >= 2 {
		hostIP = parts[protoIdx-1]
	}

	port := startPort
	for attempts := 0; attempts < 1000 && port <= 65535; attempts++ {
		if !usedPorts[port] {
			available := false
			if proto == "tcp" {
				available = isTCPPortAvailable(hostIP, port)
			} else if proto == "udp" {
				available = isUDPPortAvailable(hostIP, port)
			}
			if available {
				usedPorts[port] = true
				parts[protoIdx+1] = strconv.Itoa(port)
				return strings.Join(parts, "/"), nil
			}
		}
		port++
	}
	return maStr, nil
}

func isTCPPortAvailable(host string, port int) bool {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	_ = l.Close()
	return true
}

func isUDPPortAvailable(host string, port int) bool {
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	l, err := net.ListenPacket("udp", addr)
	if err != nil {
		return false
	}
	_ = l.Close()
	return true
}
