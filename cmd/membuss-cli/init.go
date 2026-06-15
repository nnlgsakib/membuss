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
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

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
