// Command membuss-entrypoint is the distroless-friendly
// entrypoint shim for the Membuss container.
//
// The distroless runtime image has no shell, so a shell
// script cannot be used as the container ENTRYPOINT. This
// tiny Go binary reads the same MEMBUSS_* env vars the
// shell script used to handle, renders them on top of
// /etc/membuss/config.yaml, writes a temp config file,
// and execs /usr/local/bin/membuss with -config pointing
// at it. It is built as a static PIE binary (~3 MB) and
// dropped into the distroless image.
package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

const baseConfigPath = "/etc/membuss/config.yaml"

// nonrootUID / nonrootGID match the `nonroot` user in the
// distroless image. We chown the datadir to this uid so the
// daemon can write its BadgerDB files.
const (
	nonrootUID = 65532
	nonrootGID = 65532
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "membuss-entrypoint:", err)
		os.Exit(1)
	}
}

func run() error {
	// Optional data-dir init: run `membuss-cli init` on the
	// datadir so the container has a fresh identity on first
	// boot. Idempotent; --force re-runs it.
	//
	// The named docker volume is created by the docker daemon
	// as root, which is unwritable by the distroless nonroot
	// user. We chown the datadir to nonroot (uid/gid 65532)
	// before running init so the daemon can later create the
	// BadgerDB files. The shim runs as root specifically to
	// perform this chown.
	if dir := os.Getenv("MEMBUSS_DATA_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
		// Best-effort chown. Ignore the error in the common
		// case where the directory is already owned by
		// nonroot (e.g. on restart, or when the operator
		// bind-mounts a host directory).
		_ = os.Chown(dir, nonrootUID, nonrootGID)
		initArgs := []string{"--datadir", dir, "init"}
		if os.Getenv("MEMBUSS_FORCE_INIT") == "true" {
			initArgs = append(initArgs, "--force")
		}
		initCmd := exec.Command("/usr/local/bin/membuss-cli", initArgs...)
		initCmd.Stdout = os.Stdout
		initCmd.Stderr = os.Stderr
		if err := initCmd.Run(); err != nil {
			return fmt.Errorf("membuss-cli init: %w", err)
		}
	}

	cfgPath, err := renderConfigToDataDir()
	if err != nil {
		return err
	}

	fmt.Fprintln(os.Stderr, "---- membuss rendered config ----")
	if data, rerr := os.ReadFile(cfgPath); rerr == nil {
		os.Stderr.Write(data)
	} else {
		fmt.Fprintf(os.Stderr, "(read %s: %v)\n", cfgPath, rerr)
	}
	fmt.Fprintln(os.Stderr, "---------------------------------")

	// Pass the data dir (not the config file) so the
	// daemon's --datadir resolution in config.Load
	// points at the file we just wrote. The daemon
	// does NOT need -config: it derives the path from
	// <datadir>/config.yaml.
	args := []string{"/usr/local/bin/membuss"}
	if dir := os.Getenv("MEMBUSS_DATA_DIR"); dir != "" {
		args = append(args, "-datadir", dir)
	}
	if os.Getenv("MEMBUSS_NO_ANCHOR") == "true" {
		args = append(args, "-no-anchor")
	}
	if extra := os.Getenv("MEMBUSS_EXTRA_FLAGS"); extra != "" {
		args = append(args, strings.Fields(extra)...)
	}
	return execSyscall(args)
}

func renderConfigToDataDir() (string, error) {
	dir := os.Getenv("MEMBUSS_DATA_DIR")
	if dir == "" {
		return "", fmt.Errorf("MEMBUSS_DATA_DIR must be set")
	}
	cfgPath := filepath.Join(dir, "config.yaml")

	data, err := os.ReadFile(baseConfigPath)
	if err != nil {
		return "", fmt.Errorf("read base config %s: %w", baseConfigPath, err)
	}

	scalars := map[string]string{
		"data_dir":           os.Getenv("MEMBUSS_DATA_DIR"),
		"gateway_addr":       os.Getenv("MEMBUSS_GATEWAY_ADDR"),
		"api_addr":           os.Getenv("MEMBUSS_API_ADDR"),
		"grpc_addr":          os.Getenv("MEMBUSS_GRPC_ADDR"),
		"log_level":          os.Getenv("MEMBUSS_LOG_LEVEL"),
		"anchor_mode":        os.Getenv("MEMBUSS_ANCHOR_MODE"),
		"auto_gc_interval":   os.Getenv("MEMBUSS_AUTO_GC_INTERVAL"),
		"gc_min_age":         os.Getenv("MEMBUSS_GC_MIN_AGE"),
		"bloom_capacity":     os.Getenv("MEMBUSS_BLOOM_CAPACITY"),
		"bloom_fp_rate":      os.Getenv("MEMBUSS_BLOOM_FP_RATE"),
		"dht_mode":           os.Getenv("MEMBUSS_DHT_MODE"),
		"enable_geolocation": os.Getenv("MEMBUSS_ENABLE_GEOLOCATION"),
	}

	out := string(data)
	for k, v := range scalars {
		if v == "" {
			continue
		}
		out = replaceScalar(out, k, v)
	}

	if v := os.Getenv("MEMBUSS_LISTEN_ADDRS"); v != "" {
		out = replaceList(out, "listen_addrs", splitCSV(v))
	}
	if v := os.Getenv("MEMBUSS_ANNOUNCE_ADDRS"); v != "" {
		out = replaceList(out, "announce_addrs", splitCSV(v))
	}
	if v := os.Getenv("MEMBUSS_BOOTSTRAP_PEERS"); v != "" {
		out = replaceList(out, "bootstrap_peers", splitCSV(v))
	}

	// Atomic write: write to a temp file in the same dir
	// and rename. This avoids a half-written config if
	// the container is killed mid-write.
	tmp, err := os.CreateTemp(dir, "config.yaml.tmp.*")
	if err != nil {
		return "", fmt.Errorf("create temp config: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write([]byte(out)); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return "", fmt.Errorf("write temp config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return "", fmt.Errorf("close temp config: %w", err)
	}
	if err := os.Rename(tmpName, cfgPath); err != nil {
		os.Remove(tmpName)
		return "", fmt.Errorf("rename config: %w", err)
	}
	// Restore nonroot ownership so the daemon can rewrite
	// the config on the next run.
	_ = os.Chown(cfgPath, nonrootUID, nonrootGID)
	return cfgPath, nil
}

func replaceScalar(s, key, val string) string {
	prefix := key + ":"
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		if strings.HasPrefix(line, prefix) {
			lines[i] = prefix + " " + val
			return strings.Join(lines, "\n")
		}
	}
	return s + "\n" + prefix + " " + val + "\n"
}

func replaceList(s, key string, vals []string) string {
	lines := strings.Split(s, "\n")
	prefix := key + ":"
	out := bytes.Buffer{}
	wrote := false
	skipUntilBlank := false
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if !wrote && strings.HasPrefix(line, prefix) {
			out.WriteString(prefix + "\n")
			for _, v := range vals {
				out.WriteString("  - " + v + "\n")
			}
			wrote = true
			skipUntilBlank = true
			continue
		}
		if skipUntilBlank {
			if strings.TrimSpace(line) == "" {
				skipUntilBlank = false
				out.WriteString(line + "\n")
			}
			continue
		}
		out.WriteString(line + "\n")
	}
	if !wrote {
		out.WriteString("\n" + prefix + "\n")
		for _, v := range vals {
			out.WriteString("  - " + v + "\n")
		}
	}
	return out.String()
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func execSyscall(argv []string) error {
	if len(argv) == 0 {
		return fmt.Errorf("empty argv")
	}
	// syscall.Exec replaces the current process so the daemon
	// becomes PID 1 and receives signals directly from the
	// kernel. It only returns on error.
	return syscall.Exec(argv[0], argv, os.Environ())
}
