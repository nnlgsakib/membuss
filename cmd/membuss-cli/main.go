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
	"net/textproto"
	"net/url"
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
	"github.com/nnlgsakib/membuss/core/memlink"
	"github.com/nnlgsakib/membuss/core/version"
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
		newDeleteCmd(),
		newStatCmd(),
		newLsCmd(),
		newPeersCmd(),
		newDHTCmd(),
		newGCCmd(),
		newAnchorCmd(),
		newPingCmd(),
		newDaemonCmd(),
		newInitCmd(),
		newMemNSCmd(),
		newKeyRingCmd(),
		newDescriptorCmd(),
		newVersionCmd(),
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
		dirName   string
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
				return addDirectoryHTTP(cmd, path, dirName)
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
	c.Flags().StringVar(&dirName, "name", "", "custom name for the uploaded directory (defaults to directory basename)")
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
				ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Minute)
				defer cancel()

				// Probe if it is a directory via HTTP ls API
				lsURL := httpBase() + "/api/v1/ls/" + args[0]
				var isDir bool
				var dirName string
				if resp, err := http.Get(lsURL); err == nil {
					defer resp.Body.Close()
					if resp.StatusCode == 200 {
						isDir = true
						if statResp, err := mc.Stat(ctx, &membusspb.StatRequest{Mid: args[0]}); err == nil {
							dirName = statResp.Name
						}
					}
				}

				if isDir {
					targetDir := outPath
					if targetDir == "" || targetDir == "-" {
						targetDir = dirName
						if targetDir == "" {
							targetDir = args[0]
						}
					}
					fmt.Fprintf(cmd.ErrOrStderr(), "Downloading directory structure to %s...\n", targetDir)
					return downloadDirRecursive(ctx, mc, args[0], targetDir, cmd.ErrOrStderr())
				}

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

func downloadDirRecursive(ctx context.Context, mc membusspb.MembussNodeClient, midStr, localPath string, errWriter io.Writer) error {
	if err := os.MkdirAll(localPath, 0o755); err != nil {
		return err
	}

	resp, err := http.Get(httpBase() + "/api/v1/ls/" + midStr)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ls %s: %s", midStr, string(body))
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
		return fmt.Errorf("ls %s: %s", midStr, env.Error)
	}

	for _, e := range env.Data.Entries {
		childPath := filepath.Join(localPath, e.Name)
		if e.Type == "dir" {
			if err := downloadDirRecursive(ctx, mc, e.MID, childPath, errWriter); err != nil {
				return err
			}
		} else {
			fmt.Fprintf(errWriter, "Downloading %s -> %s\n", e.Name, childPath)
			if err := downloadFile(ctx, mc, e.MID, childPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func downloadFile(ctx context.Context, mc membusspb.MembussNodeClient, midStr, localPath string) error {
	stream, err := mc.Get(ctx, &membusspb.GetRequest{Mid: midStr})
	if err != nil {
		return err
	}
	f, err := os.Create(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := f.Write(frame.Data); err != nil {
			return err
		}
	}
	return nil
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

func newDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "delete <MID>",
		Short: "Delete a MID and all its reachable blocks recursively from the local node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withConn(func(mc membusspb.MembussNodeClient, _ membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 30*time.Second)
				defer cancel()
				resp, err := mc.Delete(ctx, &membusspb.DeleteRequest{Mid: args[0]})
				if err != nil {
					return err
				}
				tw := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)
				fmt.Fprintf(tw, "deleted_blocks\t%d\n", resp.BlocksDeleted)
				fmt.Fprintf(tw, "bytes_freed\t%d\n", resp.BytesFreed)
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
				fmt.Fprintf(tw, "sealers\t%d (anchors: %d)\n", resp.Sealers, resp.AnchorSealers)
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
				fmt.Fprintf(tw, "PEER ID\tANCHOR\tADDRS\n")
				for _, p := range resp.Peers {
					fmt.Fprintf(tw, "%s\t%t\t%s\n", p.PeerId, p.IsAnchor, strings.Join(p.Addrs, ","))
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
				fmt.Fprintf(tw, "PEER ID\tANCHOR\tADDRS\n")
				for _, p := range resp.Providers {
					fmt.Fprintf(tw, "%s\t%t\t%s\n", p.PeerId, p.IsAnchor, strings.Join(p.Addrs, ","))
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

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print client and server version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintln(cmd.OutOrStdout(), "Client:")
			fmt.Fprintf(cmd.OutOrStdout(), "  %s\n", version.String())

			fmt.Fprintln(cmd.OutOrStdout(), "Server:")
			err := withConn(func(_ membusspb.MembussNodeClient, nc membusspb.NodeClient) error {
				ctx, cancel := context.WithTimeout(cmd.Context(), 2*time.Second)
				defer cancel()
				resp, err := nc.Ping(ctx, &membusspb.PingRequest{Message: ""})
				if err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "  membuss daemon version: %s\n", resp.Build)
				return nil
			})
			if err != nil {
				fmt.Fprintf(cmd.OutOrStdout(), "  error contacting daemon: %v\n", err)
			}
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
func addDirectoryHTTP(cmd *cobra.Command, root string, dirName string) error {
	name := dirName
	if name == "" {
		abs, err := filepath.Abs(root)
		if err == nil {
			name = filepath.Base(abs)
		} else {
			name = filepath.Base(root)
		}
		if name == "." || name == "/" || name == "\\" || name == "" {
			name = "dist"
		}
	}
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
		h := make(textproto.MIMEHeader)
		h.Set("Content-Disposition",
			fmt.Sprintf(`form-data; name="file"; filename="%s"`,
				escapeQuotes(filepath.Base(rel))))
		h.Set("Content-Type", "application/octet-stream")
		h.Set("X-Membuss-Path", rel)
		fw, err := mw.CreatePart(h)
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
	req, err := http.NewRequest("POST", httpBase()+"/api/v1/add/dir?name="+url.QueryEscape(name), &buf)
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
		return fmt.Errorf("failed to decode response: %w", err)
	}
	if !env.OK {
		return fmt.Errorf("add-dir: %s", env.Error)
	}
	fmt.Fprintf(cmd.OutOrStdout(), "%s\n", env.Data.MID)
	return nil
}

var quoteEscaper = strings.NewReplacer("\\", "\\\\", `"`, "\\\"")

func escapeQuotes(s string) string {
	return quoteEscaper.Replace(s)
}

// APIMemRoute represents the API's route payload mapping.
type APIMemRoute struct {
	Target     string            `json:"target"`
	Weight     uint32            `json:"weight"`
	Label      string            `json:"label"`
	Conditions map[string]string `json:"conditions"`
}

func newKeyRingCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keyring",
		Short: "Manage MemNS signing key pairs",
	}

	var keyType string
	genCmd := &cobra.Command{
		Use:   "gen <name>",
		Short: "Generate a new named key pair",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			name := args[0]
			body := map[string]any{
				"name": name,
				"type": keyType,
			}
			buf := &bytes.Buffer{}
			if err := json.NewEncoder(buf).Encode(body); err != nil {
				return err
			}
			resp, err := http.Post(httpBase()+"/api/v1/keyring/gen", "application/json", buf)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("gen key failed: %s: %s", resp.Status, string(bodyBytes))
			}
			var env struct {
				OK   bool `json:"ok"`
				Data struct {
					Name      string `json:"name"`
					MemNSName string `json:"memns_name"`
				} `json:"data"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("gen key: %s", env.Error)
			}
			fmt.Fprintf(c.OutOrStdout(), "Generated key %q -> %s\n", env.Data.Name, env.Data.MemNSName)
			return nil
		},
	}
	genCmd.Flags().StringVar(&keyType, "type", "ed25519", "key type: ed25519 or rsa")
	cmd.AddCommand(genCmd)

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List all keys + their /memns/ names",
		Args:  cobra.NoArgs,
		RunE: func(c *cobra.Command, args []string) error {
			resp, err := http.Get(httpBase() + "/api/v1/keyring/list")
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("list keys failed: %s: %s", resp.Status, string(bodyBytes))
			}
			var env struct {
				OK   bool `json:"ok"`
				Data []struct {
					Name      string    `json:"name"`
					MemNSName string    `json:"memns_name"`
					CreatedAt time.Time `json:"created_at"`
					PublicKey string    `json:"public_key"`
				} `json:"data"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("list keys: %s", env.Error)
			}

			tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "NAME\tMEMNS NAME\tCREATED AT")
			for _, k := range env.Data {
				fmt.Fprintf(tw, "%s\t%s\t%s\n", k.Name, k.MemNSName, k.CreatedAt.Local().Format(time.RFC3339))
			}
			return tw.Flush()
		},
	}
	cmd.AddCommand(listCmd)

	exportCmd := &cobra.Command{
		Use:   "export <name>",
		Short: "Export private key (PEM)",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			name := args[0]
			resp, err := http.Get(httpBase() + "/api/v1/keyring/export/" + name)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("export key failed: %s: %s", resp.Status, string(bodyBytes))
			}
			var env struct {
				OK   bool `json:"ok"`
				Data struct {
					PEM string `json:"pem"`
				} `json:"data"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("export key: %s", env.Error)
			}
			fmt.Fprint(c.OutOrStdout(), env.Data.PEM)
			return nil
		},
	}
	cmd.AddCommand(exportCmd)

	importCmd := &cobra.Command{
		Use:   "import <name> <file>",
		Short: "Import keypair",
		Args:  cobra.ExactArgs(2),
		RunE: func(c *cobra.Command, args []string) error {
			name := args[0]
			file := args[1]
			pemBytes, err := os.ReadFile(file)
			if err != nil {
				return err
			}
			body := map[string]any{
				"name": name,
				"pem":  string(pemBytes),
			}
			buf := &bytes.Buffer{}
			if err := json.NewEncoder(buf).Encode(body); err != nil {
				return err
			}
			resp, err := http.Post(httpBase()+"/api/v1/keyring/import", "application/json", buf)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("import key failed: %s: %s", resp.Status, string(bodyBytes))
			}
			var env struct {
				OK    bool   `json:"ok"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("import key: %s", env.Error)
			}
			fmt.Fprintf(c.OutOrStdout(), "Imported key %q successfully\n", name)
			return nil
		},
	}
	cmd.AddCommand(importCmd)

	rmCmd := &cobra.Command{
		Use:   "rm <name>",
		Short: "Delete key",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			name := args[0]
			req, err := http.NewRequest("DELETE", httpBase()+"/api/v1/keyring/rm/"+name, nil)
			if err != nil {
				return err
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("delete key failed: %s: %s", resp.Status, string(bodyBytes))
			}
			var env struct {
				OK    bool   `json:"ok"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("delete key: %s", env.Error)
			}
			fmt.Fprintf(c.OutOrStdout(), "Deleted key %q successfully\n", name)
			return nil
		},
	}
	cmd.AddCommand(rmCmd)

	return cmd
}

func newMemNSCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "memns",
		Short: "Manage MemNS record publishing and resolution",
	}

	var ttl uint64
	var msg string
	var routes []string

	publishCmd := &cobra.Command{
		Use:   "publish <keyname> <MID>",
		Short: "Publish a new MemNS record",
		Args:  cobra.ExactArgs(2),
		RunE: func(c *cobra.Command, args []string) error {
			key := args[0]
			val := args[1]

			var apiRoutes []APIMemRoute
			for _, rStr := range routes {
				parts := strings.SplitN(rStr, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid route format: %s (expected label=target:weight)", rStr)
				}
				label := parts[0]
				targetWeight := parts[1]
				twParts := strings.SplitN(targetWeight, ":", 2)
				if len(twParts) != 2 {
					return fmt.Errorf("invalid route target/weight format: %s (expected label=target:weight)", rStr)
				}
				target := twParts[0]
				var weight uint64
				if _, err := fmt.Sscan(twParts[1], &weight); err != nil {
					return fmt.Errorf("invalid weight in route: %s", rStr)
				}
				apiRoutes = append(apiRoutes, APIMemRoute{
					Target:     target,
					Weight:     uint32(weight),
					Label:      label,
					Conditions: make(map[string]string),
				})
			}

			body := map[string]any{
				"key":     key,
				"value":   val,
				"ttl":     ttl,
				"message": msg,
				"routes":  apiRoutes,
			}
			buf := &bytes.Buffer{}
			if err := json.NewEncoder(buf).Encode(body); err != nil {
				return err
			}

			resp, err := http.Post(httpBase()+"/api/v1/memns/publish", "application/json", buf)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("publish failed: %s: %s", resp.Status, string(bodyBytes))
			}

			var env struct {
				OK   bool `json:"ok"`
				Data struct {
					Name     string `json:"name"`
					Sequence uint64 `json:"sequence"`
				} `json:"data"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("publish: %s", env.Error)
			}

			fmt.Fprintf(c.OutOrStdout(), "Published %s -> %s at sequence %d\n", env.Data.Name, val, env.Data.Sequence)
			return nil
		},
	}
	publishCmd.Flags().Uint64Var(&ttl, "ttl", 3600, "TTL hints in seconds")
	publishCmd.Flags().StringVar(&msg, "message", "", "changelog message note")
	publishCmd.Flags().StringSliceVar(&routes, "route", nil, "routing targets in format: label=target:weight")
	cmd.AddCommand(publishCmd)

	var atSeq uint64

	resolveCmd := &cobra.Command{
		Use:   "resolve <name or domain>",
		Short: "Resolve a MemNS name or domain to MID",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			nameOrDomain := args[0]

			if atSeq > 0 {
				name := nameOrDomain
				if strings.Contains(name, ".") {
					resp, err := http.Get(httpBase() + "/api/v1/memlink/resolve/" + name)
					if err != nil {
						return err
					}
					defer resp.Body.Close()
					if resp.StatusCode != 200 {
						bodyBytes, _ := io.ReadAll(resp.Body)
						return fmt.Errorf("resolve memlink failed: %s: %s", resp.Status, string(bodyBytes))
					}
					var env struct {
						OK   bool `json:"ok"`
						Data struct {
							RawTxt      string `json:"raw_txt"`
							ResolvedMID string `json:"resolved_mid"`
						} `json:"data"`
						Error string `json:"error"`
					}
					if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
						return err
					}
					if !env.OK {
						return fmt.Errorf("resolve memlink: %s", env.Error)
					}
					rec, err := memlink.ParseTXTRecord(env.Data.RawTxt)
					if err != nil {
						return err
					}
					if rec.MemNSName == "" {
						return fmt.Errorf("historical resolve requires a mutable memns target, but domain resolved to static MID: %s", env.Data.ResolvedMID)
					}
					name = rec.MemNSName
				}

				if strings.HasPrefix(name, "/memns/") {
					name = name[7:]
				}

				resp, err := http.Get(httpBase() + "/api/v1/memns/log/" + name)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					bodyBytes, _ := io.ReadAll(resp.Body)
					return fmt.Errorf("resolve log failed: %s: %s", resp.Status, string(bodyBytes))
				}
				var env struct {
					OK   bool `json:"ok"`
					Data struct {
						Entries []struct {
							Sequence  uint64 `json:"sequence"`
							MID       string `json:"mid"`
							Timestamp int64  `json:"timestamp"`
							Message   string `json:"message"`
						} `json:"entries"`
					} `json:"data"`
					Error string `json:"error"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
					return err
				}
				if !env.OK {
					return fmt.Errorf("resolve log: %s", env.Error)
				}
				var found bool
				var val string
				for _, e := range env.Data.Entries {
					if e.Sequence == atSeq {
						val = e.MID
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("sequence %d not found in changelog history", atSeq)
				}
				fullName := nameOrDomain
				if !strings.HasPrefix(fullName, "/memns/") && !strings.Contains(fullName, ".") {
					fullName = "/memns/" + fullName
				}
				fmt.Fprintf(c.OutOrStdout(), "Name:     %s\n", fullName)
				fmt.Fprintf(c.OutOrStdout(), "Value:    %s\n", val)
				fmt.Fprintf(c.OutOrStdout(), "Sequence: %d\n", atSeq)
				return nil
			}

			var name string
			var isDomain bool
			if strings.Contains(nameOrDomain, ".") {
				isDomain = true
				resp, err := http.Get(httpBase() + "/api/v1/memlink/resolve/" + nameOrDomain)
				if err != nil {
					return err
				}
				defer resp.Body.Close()
				if resp.StatusCode != 200 {
					bodyBytes, _ := io.ReadAll(resp.Body)
					return fmt.Errorf("resolve memlink failed: %s: %s", resp.Status, string(bodyBytes))
				}
				var env struct {
					OK   bool `json:"ok"`
					Data struct {
						RawTxt      string `json:"raw_txt"`
						ResolvedMID string `json:"resolved_mid"`
					} `json:"data"`
					Error string `json:"error"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
					return err
				}
				if !env.OK {
					return fmt.Errorf("resolve memlink: %s", env.Error)
				}

				rec, err := memlink.ParseTXTRecord(env.Data.RawTxt)
				if err == nil && rec.MemNSName != "" {
					name = rec.MemNSName
				} else {
					fmt.Fprintf(c.OutOrStdout(), "Domain:   %s\n", nameOrDomain)
					fmt.Fprintf(c.OutOrStdout(), "Value:    %s\n", env.Data.ResolvedMID)
					return nil
				}
			} else {
				name = nameOrDomain
			}

			if strings.HasPrefix(name, "/memns/") {
				name = name[7:]
			}

			resp, err := http.Get(httpBase() + "/api/v1/memns/resolve/" + name)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("resolve failed: %s: %s", resp.Status, string(bodyBytes))
			}

			var env struct {
				OK   bool `json:"ok"`
				Data struct {
					Value    string `json:"value"`
					Sequence uint64 `json:"sequence"`
					Expires  string `json:"expires"`
					Routes   []struct {
						Target     string            `json:"target"`
						Weight     uint32            `json:"weight"`
						Label      string            `json:"label"`
						Conditions map[string]string `json:"conditions"`
					} `json:"routes"`
				} `json:"data"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("resolve: %s", env.Error)
			}

			fullName := "/memns/" + name
			if isDomain {
				fmt.Fprintf(c.OutOrStdout(), "Domain:   %s\n", nameOrDomain)
			}
			fmt.Fprintf(c.OutOrStdout(), "Name:     %s\n", fullName)
			fmt.Fprintf(c.OutOrStdout(), "Value:    %s\n", env.Data.Value)
			fmt.Fprintf(c.OutOrStdout(), "Sequence: %d\n", env.Data.Sequence)
			fmt.Fprintf(c.OutOrStdout(), "Expires:  %s\n", env.Data.Expires)
			fmt.Fprintf(c.OutOrStdout(), "TTL:      1h\n")

			if len(env.Data.Routes) > 0 {
				tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
				fmt.Fprintln(tw, "LABEL\tTARGET\tWEIGHT")
				for _, r := range env.Data.Routes {
					fmt.Fprintf(tw, "%s\t%s\t%d\n", r.Label, r.Target, r.Weight)
				}
				_ = tw.Flush()
			}
			return nil
		},
	}
	resolveCmd.Flags().Uint64Var(&atSeq, "at-sequence", 0, "historical sequence number to resolve")
	cmd.AddCommand(resolveCmd)

	logCmd := &cobra.Command{
		Use:   "log <name>",
		Short: "Show the publishing history of a MemNS name",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			name := args[0]
			if strings.HasPrefix(name, "/memns/") {
				name = name[7:]
			}

			resp, err := http.Get(httpBase() + "/api/v1/memns/log/" + name)
			if err != nil {
				return err
			}
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("log failed: %s: %s", resp.Status, string(bodyBytes))
			}

			var env struct {
				OK   bool `json:"ok"`
				Data struct {
					Entries []struct {
						Sequence  uint64 `json:"sequence"`
						MID       string `json:"mid"`
						Timestamp int64  `json:"timestamp"`
						Message   string `json:"message"`
					} `json:"entries"`
				} `json:"data"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("log: %s", env.Error)
			}

			tw := tabwriter.NewWriter(c.OutOrStdout(), 0, 0, 2, ' ', 0)
			fmt.Fprintln(tw, "SEQUENCE\tTIMESTAMP\tMID\tMESSAGE")
			for _, e := range env.Data.Entries {
				t := time.Unix(0, e.Timestamp).UTC().Format(time.RFC3339)
				fmt.Fprintf(tw, "%d\t%s\t%s\t%s\n", e.Sequence, t, e.MID, e.Message)
			}
			return tw.Flush()
		},
	}
	cmd.AddCommand(logCmd)

	delegateCmd := &cobra.Command{
		Use:   "delegate",
		Short: "Manage delegated keys authorized to publish to your MemNS name",
	}

	addDelCmd := &cobra.Command{
		Use:   "add <keyname> <pubkey-base64>",
		Short: "Authorize a delegate public key",
		Args:  cobra.ExactArgs(2),
		RunE: func(c *cobra.Command, args []string) error {
			name := args[0]
			pubkey := args[1]
			body := map[string]any{
				"name":     name,
				"delegate": pubkey,
			}
			buf := &bytes.Buffer{}
			if err := json.NewEncoder(buf).Encode(body); err != nil {
				return err
			}
			resp, err := http.Post(httpBase()+"/api/v1/memns/delegate/add", "application/json", buf)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("delegate add failed: %s: %s", resp.Status, string(bodyBytes))
			}
			var env struct {
				OK    bool   `json:"ok"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("delegate add: %s", env.Error)
			}
			fmt.Fprintf(c.OutOrStdout(), "Authorized delegate successfully\n")
			return nil
		},
	}
	delegateCmd.AddCommand(addDelCmd)

	rmDelCmd := &cobra.Command{
		Use:   "rm <keyname> <pubkey-base64>",
		Short: "Revoke authorization of a delegate public key",
		Args:  cobra.ExactArgs(2),
		RunE: func(c *cobra.Command, args []string) error {
			name := args[0]
			pubkey := args[1]
			body := map[string]any{
				"name":     name,
				"delegate": pubkey,
			}
			buf := &bytes.Buffer{}
			if err := json.NewEncoder(buf).Encode(body); err != nil {
				return err
			}
			resp, err := http.Post(httpBase()+"/api/v1/memns/delegate/rm", "application/json", buf)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("delegate rm failed: %s: %s", resp.Status, string(bodyBytes))
			}
			var env struct {
				OK    bool   `json:"ok"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("delegate rm: %s", env.Error)
			}
			fmt.Fprintf(c.OutOrStdout(), "Revoked delegate authorization successfully\n")
			return nil
		},
	}
	delegateCmd.AddCommand(rmDelCmd)

	listDelCmd := &cobra.Command{
		Use:   "list <keyname>",
		Short: "List all authorized delegates",
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			keyname := args[0]
			resp, err := http.Get(httpBase() + "/api/v1/memns/delegate/list/" + keyname)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				bodyBytes, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("delegate list failed: %s: %s", resp.Status, string(bodyBytes))
			}
			var env struct {
				OK   bool `json:"ok"`
				Data struct {
					Delegates []string `json:"delegates"`
				} `json:"data"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("delegate list: %s", env.Error)
			}
			fmt.Fprintf(c.OutOrStdout(), "Delegates for key %q:\n", keyname)
			for _, d := range env.Data.Delegates {
				fmt.Fprintf(c.OutOrStdout(), "  - %s\n", d)
			}
			return nil
		},
	}
	delegateCmd.AddCommand(listDelCmd)

	cmd.AddCommand(delegateCmd)

	return cmd
}

// --- descriptor ---

func newDescriptorCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "descriptor",
		Short: "BitTorrent-style .mbuss descriptor file management",
	}
	cmd.AddCommand(newDescriptorExportCmd())
	cmd.AddCommand(newDescriptorImportCmd())
	cmd.AddCommand(newDescriptorMetaCmd())
	return cmd
}

func newDescriptorExportCmd() *cobra.Command {
	var outPath string
	c := &cobra.Command{
		Use:   "export <MID> [-o file.mbuss]",
		Short: "Export a .mbuss descriptor file for a MID",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			midStr := args[0]
			url := httpBase() + "/api/v1/descriptor/" + midStr
			resp, err := http.Get(url)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("export: %s: %s", resp.Status, string(body))
			}
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				return err
			}
			if outPath == "" {
				outPath = midStr + ".mbuss"
			}
			if err := os.WriteFile(outPath, data, 0644); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Exported descriptor to %s (%d bytes)\n", outPath, len(data))
			return nil
		},
	}
	c.Flags().StringVarP(&outPath, "output", "o", "", "output file path (default: <mid>.mbuss)")
	return c
}

func newDescriptorImportCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "import <file.mbuss>",
		Short: "Import a .mbuss descriptor file and verify all blocks are present",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			url := httpBase() + "/api/v1/descriptor/import"
			resp, err := http.Post(url, "application/octet-stream", bytes.NewReader(data))
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("import: %s: %s", resp.Status, string(body))
			}
			var env struct {
				OK   bool `json:"ok"`
				Data struct {
					MID string `json:"mid"`
				} `json:"data"`
				Error string `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("import: %s", env.Error)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Imported descriptor: MID %s\n", env.Data.MID)
			return nil
		},
	}
}

func newDescriptorMetaCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "meta <MID>",
		Short: "Show descriptor metadata for a MID (without block list)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			midStr := args[0]
			url := httpBase() + "/api/v1/descriptor/" + midStr + "/meta"
			resp, err := http.Get(url)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("meta: %s: %s", resp.Status, string(body))
			}
			var env struct {
				OK   bool                   `json:"ok"`
				Data map[string]interface{} `json:"data"`
				Error string                 `json:"error"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
				return err
			}
			if !env.OK {
				return fmt.Errorf("meta: %s", env.Error)
			}
			enc := json.NewEncoder(cmd.OutOrStdout())
			enc.SetIndent("", "  ")
			return enc.Encode(env.Data)
		},
	}
}
