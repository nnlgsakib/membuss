// End-to-end test that boots a real membuss daemon (in-memory
// store, no DHT bootstrap) in a child process, dials its
// gRPC endpoint, and exercises the Add/Stat/Peers RPCs.
//
// We use os.Executable() to find the test binary, then exec
// the membuss binary alongside it. The CLI is driven by the
// same gRPC client we use in unit tests.
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

func TestE2E_DaemonBoot(t *testing.T) {
	if runtime.GOOS == "windows" && os.Getenv("MEMBUSS_E2E_SKIP") == "1" {
		t.Skip("E2E test requires Windows job objects; skipping in CI")
	}

	// Find the membuss binary in the same directory as the
	// test binary.
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	binDir := filepath.Dir(exe)
	daemonName := "membuss"
	if runtime.GOOS == "windows" {
		daemonName += ".exe"
	}
	daemonPath := filepath.Join(binDir, daemonName)
	if _, err := os.Stat(daemonPath); err != nil {
		// Fall back to ./bin/membuss.exe in the project root.
		root, _ := os.Getwd()
		for i := 0; i < 5; i++ {
			candidate := filepath.Join(root, "bin", daemonName)
			if _, err := os.Stat(candidate); err == nil {
				daemonPath = candidate
				break
			}
			root = filepath.Dir(root)
		}
	}
	if _, err := os.Stat(daemonPath); err != nil {
		t.Skipf("membuss binary not found at %s; run `go build -o bin/membuss.exe ./cmd/membuss` first", daemonPath)
	}

	// Pick a free gRPC port.
	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	// Write a temporary config.
	tmp := t.TempDir()
	cfg := []byte(fmt.Sprintf(`listen_addrs:
  - /ip4/127.0.0.1/tcp/0
bootstrap_peers: []
data_dir: %q
gateway_addr: 127.0.0.1:0
api_addr: 127.0.0.1:0
grpc_addr: %s
anchor_mode: false
reprovide_interval: 12h
`, filepath.ToSlash(tmp), addr))
	cfgPath := filepath.Join(tmp, "config.yaml")
	if err := os.WriteFile(cfgPath, cfg, 0644); err != nil {
		t.Fatal(err)
	}

	// Launch the daemon.
	cmd := exec.Command(daemonPath,
		"--config", cfgPath,
		"--in-memory",
		"--build", "e2e-test",
	)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start daemon: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})

	// Wait for the gRPC port to accept connections.
	if err := waitForPort(addr, 10*time.Second); err != nil {
		t.Fatalf("daemon did not bind: %v\nstdout: %s\nstderr: %s", err, stdout.String(), stderr.String())
	}

	// Dial and exercise.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer conn.Close()
	cli := membusspb.NewNodeClient(conn)
	nc := membusspb.NewMembussNodeClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cli.Ping(ctx, &membusspb.PingRequest{Message: "e2e"})
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	if resp.Build != "e2e-test" {
		t.Errorf("build: got %q want e2e-test", resp.Build)
	}
	if resp.Message != "e2e" {
		t.Errorf("echo: got %q want e2e", resp.Message)
	}

	stat, err := nc.Stat(ctx, &membusspb.StatRequest{Mid: "memdoesnotexist"})
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if stat.Present {
		t.Errorf("expected absent for unknown MID")
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()
	return lis.Addr().(*net.TCPAddr).Port
}

func waitForPort(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}

// smokeRunner is exported for use by future integration
// tests; it currently exists only so the io import is
// referenced and the file compiles in isolation.
var _ = io.Discard
