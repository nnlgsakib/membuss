// Command membuss-cli is the user-facing CLI for the Membuss
// daemon. It dials the local gRPC endpoint (configurable via
// --addr or MEMBUSS_ADDR) and exposes the operator commands
// described in the Phase 7 spec.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nnlgsakib/membuss/config"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

var (
	// globalAddr is the gRPC endpoint the CLI dials. Resolved
	// from --addr, then MEMBUSS_ADDR, then the config file.
	globalAddr string
	// globalConfigPath is the YAML config used to discover
	// the gRPC endpoint when --addr is not given.
	globalConfigPath string
	// globalAPIAddr is the HTTP API endpoint the CLI uses
	// for MemFS commands (ls, get with path, add with
	// --wrap-dir, add <directory>). Resolved from --api-addr,
	// then $MEMBUSS_API_ADDR, then the config file's
	// APIAddr, then 127.0.0.1:5001.
	globalAPIAddr string
)

func main() {
	rootCmd := newRootCmd()
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "membuss-cli:", err)
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "membuss-cli",
		Short: "Membuss operator CLI",
		Long: `membuss-cli talks to a locally-running membuss daemon over gRPC.

It mirrors the MembussNode service:
  add, get, seal, unseal, stat, peers, dht, gc, anchor, daemon.

Run "membuss-cli init" first to set up the data directory.`,
		SilenceUsage:  true,
		SilenceErrors: false,
	}
	root.PersistentFlags().StringVar(&globalAddr, "addr", "", "daemon gRPC address (default: from config)")
	root.PersistentFlags().StringVar(&globalAPIAddr, "api-addr", "", "daemon HTTP API address for MemFS commands (default: 127.0.0.1:5001)")
	root.PersistentFlags().StringVar(&globalConfigPath, "config", "membuss.yaml", "config file used to locate the daemon")
	root.PersistentFlags().String("datadir", "", "data directory (default $HOME/.memdata; overrides MEMBUSS_DATADIR)")

	root.AddCommand(
		newAddCmd(),
		newGetCmd(),
		newSealCmd(),
		newUnsealCmd(),
		newStatCmd(),
		newLsCmd(),
		newPeersCmd(),
		newDHTCmd(),
		newGCCmd(),
		newAnchorCmd(),
		newPingCmd(),
		newDaemonCmd(),
		newInitCmd(),
	)
	return root
}

// --- connection helpers ---

// resolveAddr returns the gRPC endpoint the CLI should dial.
// Priority:
//  1. --addr flag
//  2. $MEMBUSS_ADDR
//  3. config.yaml in --datadir (or $MEMBUSS_DATADIR or $HOME/.memdata)
//  4. config.yaml at the legacy --config path
//  5. 127.0.0.1:50051
func resolveAddr() (string, error) {
	if globalAddr != "" {
		return globalAddr, nil
	}
	if v := os.Getenv("MEMBUSS_ADDR"); v != "" {
		return v, nil
	}
	if datadir := config.ResolveDataDir(""); datadir != "" {
		if cfg, err := config.LoadConfig(datadir); err == nil && cfg.GRPCAddr != "" {
			return cfg.GRPCAddr, nil
		}
	}
	if cfg, err := config.Load(globalConfigPath); err == nil && cfg.GRPCAddr != "" {
		return cfg.GRPCAddr, nil
	}
	return "127.0.0.1:50051", nil
}

// dial opens a gRPC connection to the daemon.
func dial() (membusspb.MembussNodeClient, membusspb.NodeClient, *grpc.ClientConn, error) {
	addr, err := resolveAddr()
	if err != nil {
		return nil, nil, nil, err
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return membusspb.NewMembussNodeClient(conn), membusspb.NewNodeClient(conn), conn, nil
}

// withConn runs fn with a connected client and closes it
// afterwards.
func withConn(fn func(m membusspb.MembussNodeClient, n membusspb.NodeClient) error) error {
	mc, nc, conn, err := dial()
	if err != nil {
		return err
	}
	defer conn.Close()
	return fn(mc, nc)
}

// --- add ---

func newAddCmd() *cobra.Command {
	var (
		chunker   string
		chunkSize uint32
		noSeal    bool
		wrapDir   bool
	)
	c := &cobra.Command{
		Use:   "add <file-or-dir>",
		Short: "Upload a file or directory, seal the root, return the MID",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			// Phase 17: when the path is a directory, route
			// to the MemFS /add/dir HTTP endpoint so the
			// result is a single DIR MID that resolves to
			// the whole tree.
			if fi, err := os.Stat(path); err == nil && fi.IsDir() {
				return addDirectoryHTTP(cmd, path)
			}
			if wrapDir {
				return addFileHTTP(cmd, path, true)
			}
			return withConn(func(mc membusspb.MembussNodeClient, _ membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Minute)
				defer cancel()
				resp, err := mc.Add(ctx, &membusspb.AddRequest{
					Path:      args[0],
					Chunker:   chunker,
					ChunkSize: chunkSize,
					NoSeal:    noSeal,
				})
				if err != nil {
					return err
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
				fmt.Fprintf(tw, "MID\t%s\n", resp.Mid)
				fmt.Fprintf(tw, "size\t%s (%d bytes)\n", formatBytes(resp.Size), resp.Size)
				fmt.Fprintf(tw, "blocks\t%d\n", resp.Blocks)
				fmt.Fprintf(tw, "sealed\t%t\n", resp.Sealed)
				return tw.Flush()
			})
		},
	}
	c.Flags().StringVar(&chunker, "chunker", "", "chunker: \"fixed\" (default) or \"rabin\"")
	c.Flags().Uint32Var(&chunkSize, "chunk-size", 0, "fixed chunk size in bytes (default 256 KiB)")
	c.Flags().BoolVar(&noSeal, "no-seal", false, "do not seal the root after ingest")
	c.Flags().BoolVar(&wrapDir, "wrap-dir", false, "wrap the file in a single-entry DIR node (MemFS)")
	return c
}

// --- get ---

func newGetCmd() *cobra.Command {
	var (
		outPath string
		offset  uint64
		limit   uint64
	)
	c := &cobra.Command{
		Use:   "get <MID> [-o file]",
		Short: "Fetch the content of a MID and write to a file (or stdout)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConn(func(mc membusspb.MembussNodeClient, _ membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Minute)
				defer cancel()
				stream, err := mc.Get(ctx, &membusspb.GetRequest{
					Mid:    args[0],
					Offset: offset,
					Limit:  limit,
				})
				if err != nil {
					return err
				}
				var out io.Writer
				if outPath == "" || outPath == "-" {
					out = cmd.OutOrStdout()
				} else {
					f, err := os.Create(outPath)
					if err != nil {
						return err
					}
					defer f.Close()
					out = f
				}
				var (
					total      uint64
					received   uint64
					startTime  = time.Now()
					blocksRecv uint64
				)
				for {
					frame, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return err
					}
					if _, err := out.Write(frame.Data); err != nil {
						return err
					}
					n := uint64(len(frame.Data))
					received += n
					blocksRecv++
					if frame.Total > 0 {
						total = frame.Total
					}
					if outPath == "-" || outPath == "" {
						elapsed := time.Since(startTime).Seconds()
						var pct int
						var sizeStr string
						if total > 0 {
							pct = int(received * 100 / total)
							sizeStr = fmt.Sprintf("%s / %s", formatBytes(received), formatBytes(total))
						} else {
							pct = 0
							sizeStr = fmt.Sprintf("%s / ?", formatBytes(received))
						}
						var rate string
						if elapsed > 0 {
							rate = fmt.Sprintf("%.0f blocks/s", float64(blocksRecv)/elapsed)
						}
						fmt.Fprintf(cmd.ErrOrStderr(), "\rFetching %s... [%-20s] %d%% (%s) %s", args[0], strings.Repeat("=", pct/5)+strings.Repeat(" ", 20-pct/5), pct, sizeStr, rate)
					}
				}
				if outPath == "-" || outPath == "" {
					fmt.Fprintf(cmd.ErrOrStderr(), "\n")
				}
				if outPath != "" && outPath != "-" {
					fmt.Fprintf(cmd.ErrOrStderr(), "wrote %d bytes to %s\n", received, outPath)
				}
				return nil
			})
		},
	}
	c.Flags().StringVarP(&outPath, "out", "o", "", "output file (default: stdout)")
	c.Flags().Uint64Var(&offset, "offset", 0, "byte offset to start at")
	c.Flags().Uint64Var(&limit, "limit", 0, "maximum bytes (0 = until EOF)")
	return c
}

// --- seal / unseal ---

func newSealCmd() *cobra.Command {
	var recursive bool
	c := &cobra.Command{
		Use:   "seal <MID>",
		Short: "Pin a MID (and optionally all reachable blocks)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConn(func(mc membusspb.MembussNodeClient, _ membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
				defer cancel()
				resp, err := mc.Seal(ctx, &membusspb.SealRequest{Mid: args[0], Recursive: recursive})
				if err != nil {
					return err
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
				fmt.Fprintf(tw, "pinned\t%d\n", resp.Pinned)
				fmt.Fprintf(tw, "already\t%t\n", resp.Already)
				return tw.Flush()
			})
		},
	}
	c.Flags().BoolVar(&recursive, "recursive", true, "seal every block reachable from this MID")
	return c
}

func newUnsealCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "unseal <MID>",
		Short: "Remove the pin on a MID",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConn(func(mc membusspb.MembussNodeClient, _ membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
				defer cancel()
				resp, err := mc.Unseal(ctx, &membusspb.UnsealRequest{Mid: args[0]})
				if err != nil {
					return err
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
				fmt.Fprintf(tw, "removed\t%d\n", resp.Removed)
				return tw.Flush()
			})
		},
	}
}

// --- stat ---

func newStatCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "stat <MID>",
		Short: "Show size, block count, and seal status for a MID",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConn(func(mc membusspb.MembussNodeClient, _ membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
				defer cancel()
				resp, err := mc.Stat(ctx, &membusspb.StatRequest{Mid: args[0]})
				if err != nil {
					return err
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
				fmt.Fprintf(tw, "present\t%t\n", resp.Present)
				fmt.Fprintf(tw, "size\t%s (%d bytes)\n", formatBytes(resp.Size), resp.Size)
				fmt.Fprintf(tw, "blocks\t%d\n", resp.Blocks)
				fmt.Fprintf(tw, "sealed\t%t\n", resp.Sealed)
				fmt.Fprintf(tw, "codec\t0x%x\n", resp.Codec)
				if resp.Erasure != nil {
					fmt.Fprintf(tw, "erasure\t%d+%d (%d shards)\n", resp.Erasure.DataShards, resp.Erasure.ParityShards, len(resp.Erasure.ShardMids))
				}
				return tw.Flush()
			})
		},
	}
}

// --- peers ---

func newPeersCmd() *cobra.Command {
	var limit uint32
	c := &cobra.Command{
		Use:   "peers",
		Short: "List peers known to the local PEX table",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConn(func(mc membusspb.MembussNodeClient, _ membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
				defer cancel()
				resp, err := mc.Peers(ctx, &membusspb.PeersRequest{Limit: limit})
				if err != nil {
					return err
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
				fmt.Fprintf(tw, "PEER ID\tADDRS\n")
				for _, p := range resp.Peers {
					fmt.Fprintf(tw, "%s\t%s\n", p.PeerId, strings.Join(p.Addrs, ","))
				}
				fmt.Fprintf(tw, "\n")
				fmt.Fprintf(tw, "total\t%d (showing %d)\n", resp.Total, len(resp.Peers))
				return tw.Flush()
			})
		},
	}
	c.Flags().Uint32Var(&limit, "limit", 0, "max entries to return (0 = all)")
	return c
}

// --- dht ---

func newDHTCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dht",
		Short: "DHT inspection commands",
	}
	cmd.AddCommand(newDHTPeekCmd())
	return cmd
}

func newDHTPeekCmd() *cobra.Command {
	var limit uint32
	c := &cobra.Command{
		Use:   "peek <MID>",
		Short: "Ask the local DHT who provides a MID",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConn(func(mc membusspb.MembussNodeClient, _ membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
				defer cancel()
				resp, err := mc.DHTPeek(ctx, &membusspb.DHTPeekRequest{Mid: args[0], Limit: limit})
				if err != nil {
					return err
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
				fmt.Fprintf(tw, "PEER ID\tADDRS\n")
				for _, p := range resp.Providers {
					fmt.Fprintf(tw, "%s\t%s\n", p.PeerId, strings.Join(p.Addrs, ","))
				}
				return tw.Flush()
			})
		},
	}
	c.Flags().Uint32Var(&limit, "limit", 0, "max entries to return (0 = all)")
	return c
}

// --- gc ---

func newGCCmd() *cobra.Command {
	var all bool
	c := &cobra.Command{
		Use:   "gc",
		Short: "Run garbage collection on the local store",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConn(func(mc membusspb.MembussNodeClient, _ membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Minute)
				defer cancel()
				resp, err := mc.GC(ctx, &membusspb.GCRequest{All: all})
				if err != nil {
					return err
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
				fmt.Fprintf(tw, "bytes_freed\t%s (%d bytes)\n", formatBytes(resp.BytesFreed), resp.BytesFreed)
				fmt.Fprintf(tw, "blocks_kept\t%d\n", resp.BlocksKept)
				return tw.Flush()
			})
		},
	}
	c.Flags().BoolVar(&all, "all", false, "reserved for future per-namespace GC flags")
	return c
}

// --- anchor ---

func newAnchorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "anchor",
		Short: "Anchor Node commands",
	}
	cmd.AddCommand(newAnchorStatusCmd())
	return cmd
}

func newAnchorStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show Anchor Node engine stats",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConn(func(mc membusspb.MembussNodeClient, _ membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
				defer cancel()
				resp, err := mc.AnchorStatus(ctx, &membusspb.AnchorStatusRequest{})
				if err != nil {
					return err
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
				fmt.Fprintf(tw, "peer_id\t%s\n", resp.PeerId)
				fmt.Fprintf(tw, "uptime\t%s\n", time.Duration(resp.UptimeSeconds)*time.Second)
				fmt.Fprintf(tw, "blocks_held\t%d\n", resp.BlocksHeld)
				fmt.Fprintf(tw, "anchors\t%d\n", resp.Anchors)
				fmt.Fprintf(tw, "backlog\t%d\n", resp.Backlog)
				fmt.Fprintf(tw, "synced\t%d\n", resp.Synced)
				return tw.Flush()
			})
		},
	}
}

// --- ping ---

func newPingCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ping [message]",
		Short: "Send a connectivity probe to the daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConn(func(_ membusspb.MembussNodeClient, nc membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
				defer cancel()
				msg := ""
				if len(args) > 0 {
					msg = strings.Join(args, " ")
				}
				resp, err := nc.Ping(ctx, &membusspb.PingRequest{Message: msg})
				if err != nil {
					return err
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
				fmt.Fprintf(tw, "build\t%s\n", resp.Build)
				if resp.Message != "" {
					fmt.Fprintf(tw, "echo\t%s\n", resp.Message)
				}
				return tw.Flush()
			})
		},
	}
}

// --- daemon ---

// newDaemonCmd exposes a CLI hook for the operator to launch
// the daemon in-process. This is convenient in development;
// production deployments typically run cmd/membuss as a
// separate service. The subcommand accepts `start` (foreground)
// and `status` (alias for `ping`).
func newDaemonCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Local daemon control",
	}
	cmd.AddCommand(&cobra.Command{
		Use:   "start",
		Short: "Run the daemon in the foreground (delegates to cmd/membuss)",
		RunE: func(cmd *cobra.Command, args []string) error {
			exe, err := os.Executable()
			if err != nil {
				return err
			}
			// Look for a sibling "membuss" binary; if absent, tell
			// the user to run cmd/membuss directly.
			dir := filepath.Dir(exe)
			daemon := filepath.Join(dir, "membuss")
			if runtime := os.Getenv("MEMBUSS_DAEMON"); runtime != "" {
				daemon = runtime
			}
			if _, err := os.Stat(daemon); err != nil {
				fmt.Fprintln(cmd.OutOrStdout(),
					"membuss-cli: no sibling 'membuss' binary found.\n"+
						"Build it with `go build -o bin/membuss ./cmd/membuss` and run it directly.")
				return nil
			}
			cmd.SilenceUsage = true
			return errors.New("delegation: not implemented in this build; run cmd/membuss directly")
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use:   "status",
		Short: "Alias for `membuss-cli ping`",
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConn(func(_ membusspb.MembussNodeClient, nc membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 5*time.Second)
				defer cancel()
				resp, err := nc.Ping(ctx, &membusspb.PingRequest{Message: "status"})
				if err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "ok\tbuild=%s\n", resp.Build)
				return nil
			})
		},
	})
	return cmd
}

// --- helpers ---

// formatBytes renders a byte count in a human-readable form.
// It mirrors the helper in rpc/server so the CLI output stays
// consistent with what stat/gc will report.
func formatBytes(n uint64) string {
	const (
		KiB = 1 << 10
		MiB = 1 << 20
		GiB = 1 << 30
	)
	switch {
	case n >= GiB:
		return fmt.Sprintf("%.2f GiB", float64(n)/float64(GiB))
	case n >= MiB:
		return fmt.Sprintf("%.2f MiB", float64(n)/float64(MiB))
	case n >= KiB:
		return fmt.Sprintf("%.2f KiB", float64(n)/float64(KiB))
	default:
		return strconv.FormatUint(n, 10) + " B"
	}
}

// --- Phase 17: MemFS commands (HTTP) ---

// resolveAPIAddr returns the HTTP API endpoint for MemFS
// commands. Priority:
//
//  1. --api-addr flag
//  2. $MEMBUSS_API_ADDR
//  3. config.yaml's APIAddr
//  4. 127.0.0.1:5001
func resolveAPIAddr() string {
	if globalAPIAddr != "" {
		return globalAPIAddr
	}
	if v := os.Getenv("MEMBUSS_API_ADDR"); v != "" {
		return v
	}
	if datadir := config.ResolveDataDir(""); datadir != "" {
		if cfg, err := config.LoadConfig(datadir); err == nil && cfg.APIAddr != "" {
			return cfg.APIAddr
		}
	}
	if cfg, err := config.Load(globalConfigPath); err == nil && cfg.APIAddr != "" {
		return cfg.APIAddr
	}
	return "127.0.0.1:5001"
}

// httpBase returns "http://<addr>" for the API host.
func httpBase() string {
	return "http://" + resolveAPIAddr()
}

// newLsCmd implements `membuss-cli ls <MID>`. It calls
// GET /api/v1/ls/{mid} and prints a tabwriter table.
func newLsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "ls <MID>",
		Short: "List the entries of a MemFS directory",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mid := args[0]
			resp, err := http.Get(httpBase() + "/api/v1/ls/" + mid)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("ls: %s: %s", resp.Status, string(body))
			}
			var env struct {
				OK   bool `json:"ok"`
				Data struct {
					Entries []struct {
						Name string `json:"name"`
						MID  string `json:"mid"`
						Type string `json:"type"`
						Size uint64 `json:"size"`
					} `json:"entries"`
				} `json:"data"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("ls: %s", env.Error)
			}
			tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tTYPE\tSIZE\tMID")
			for _, e := range env.Data.Entries {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", e.Name, e.Type, formatBytes(e.Size), e.MID)
			}
			return tw.Flush()
		},
	}
}

// addFileHTTP is the shared POST handler for single-file
// uploads. When wrapDir is true, the daemon returns a DIR
// MID that wraps the FILE node.
func addFileHTTP(cmd *cobra.Command, path string, wrapDir bool) error {
	url := httpBase() + "/api/v1/add"
	if wrapDir {
		url += "?wrap=dir"
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	req, err := http.NewRequest("POST", url, f)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add: %s: %s", resp.Status, string(body))
	}
	var env struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
		Data  struct {
			MID  string `json:"mid"`
			Size uint64 `json:"size"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return err
	}
	if !env.OK {
		return fmt.Errorf("add: %s", env.Error)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "%s\n", env.Data.MID)
	return nil
}

// addDirectoryHTTP uploads a directory as multipart/form-data
// to /api/v1/add/dir. Each part carries a X-Membuss-Path
// header with the file's relative path.
func addDirectoryHTTP(cmd *cobra.Command, root string) error {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	count := 0
	err := filepath.Walk(root, func(p string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = strings.ReplaceAll(rel, string(filepath.Separator), "/")
		fw, err := mw.CreateFormFile("file", rel)
		if err != nil {
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		if _, err := io.Copy(fw, f); err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		return err
	}
	if count == 0 {
		return errors.New("no files in directory")
	}
	if err := mw.Close(); err != nil {
		return err
	}
	req, err := http.NewRequest("POST", httpBase()+"/api/v1/add/dir", &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("add-dir: %s: %s", resp.Status, string(body))
	}
	var env struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
		Data  struct {
			MID string `json:"mid"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return err
	}
	if !env.OK {
		return fmt.Errorf("add-dir: %s", env.Error)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "%s\n", env.Data.MID)
	return nil
}
