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
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"

	midpkg "github.com/nnlgsakib/membuss/core/mid"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
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

	// Parse the bound gateway and api addrs from stdout.
	gwAddr := parseBannerAddr(t, stdout.String(), "gateway_addr")
	apiAddr := parseBannerAddr(t, stdout.String(), "api_addr")

	apiURL := "http://" + apiAddr

	// /api/v1/healthz
	apiHresp, err := http.Get(apiURL + "/api/v1/healthz")
	if err != nil {
		t.Fatalf("api healthz: %v", err)
	}
	apiHresp.Body.Close()
	if apiHresp.StatusCode != http.StatusOK {
		t.Fatalf("api healthz status: %d", apiHresp.StatusCode)
	}

	// /api/v1/node/info
	infoBody, err := httpGetJSON(apiURL + "/api/v1/node/info")
	if err != nil {
		t.Fatalf("node info: %v", err)
	}
	if infoBody["ok"] != true {
		t.Fatalf("node info: not ok: %v", infoBody)
	}

	// POST /api/v1/add with a small raw body.
	payload := []byte("hello membuss gateway!")
	addResp, err := http.Post(apiURL+"/api/v1/add", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("api add: %v", err)
	}
	addBody, _ := io.ReadAll(addResp.Body)
	addResp.Body.Close()
	if addResp.StatusCode != http.StatusCreated {
		t.Fatalf("api add status: %d body: %s", addResp.StatusCode, string(addBody))
	}
	var addEnv struct {
		OK    bool `json:"ok"`
		Data  struct {
			MID    string `json:"mid"`
			Size   uint64 `json:"size"`
			Blocks uint64 `json:"blocks"`
		} `json:"data"`
	}

	if err := json.Unmarshal(addBody, &addEnv); err != nil {
		t.Fatalf("api add json: %v body=%s", err, string(addBody))
	}
	if !addEnv.OK || addEnv.Data.MID == "" {
		t.Fatalf("api add envelope: %+v", addEnv)
	}
	if addEnv.Data.Size != uint64(len(payload)) {
		t.Errorf("api add size: got %d want %d", addEnv.Data.Size, len(payload))
	}
	addMid := addEnv.Data.MID

	// /api/v1/stat/{mid}
	statBody, err := httpGetJSON(apiURL + "/api/v1/stat/" + addMid)
	if err != nil {
		t.Fatalf("api stat: %v", err)
	}
	if statBody["ok"] != true {
		t.Fatalf("api stat: not ok: %v", statBody)
	}

	// Mem-Gate E2E
	gwURL := "http://" + gwAddr

	// /healthz
	gwHresp, err := http.Get(gwURL + "/healthz")
	if err != nil {
		t.Fatalf("gw healthz: %v", err)
	}
	gwHresp.Body.Close()
	if gwHresp.StatusCode != http.StatusNoContent {
		t.Fatalf("gw healthz status: %d", gwHresp.StatusCode)
	}

	// GET /mem/{mid}
	getResp, err := http.Get(gwURL + "/mem/" + addMid)
	if err != nil {
		t.Fatalf("gw get: %v", err)
	}
	got, _ := io.ReadAll(getResp.Body)
	getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("gw get status: %d body=%s", getResp.StatusCode, string(got))
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("gw get bytes: got %q want %q", string(got), string(payload))
	}
	if getResp.Header.Get("X-Membuss-MID") != addMid {
		t.Errorf("X-Membuss-MID header: got %q want %q", getResp.Header.Get("X-Membuss-MID"), addMid)
	}
	if getResp.Header.Get("ETag") != "\""+addMid+"\"" {
		t.Errorf("ETag header: got %q want %q", getResp.Header.Get("ETag"), "\""+addMid+"\"")
	}
	if getResp.Header.Get("Accept-Ranges") != "bytes" {
		t.Errorf("Accept-Ranges header missing")
	}

	// Range request: bytes=2-5
	rangeReq, _ := http.NewRequest("GET", gwURL+"/mem/"+addMid, nil)
	rangeReq.Header.Set("Range", "bytes=2-5")
	rangeResp, err := http.DefaultClient.Do(rangeReq)
	if err != nil {
		t.Fatalf("gw range: %v", err)
	}
	rangeBody, _ := io.ReadAll(rangeResp.Body)
	rangeResp.Body.Close()
	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("gw range status: %d", rangeResp.StatusCode)
	}
	if !bytes.Equal(rangeBody, payload[2:6]) {
		t.Fatalf("gw range bytes: got %q want %q", string(rangeBody), string(payload[2:6]))
	}

	// ?format=dag-json
	djResp, err := http.Get(gwURL + "/mem/" + addMid + "?format=dag-json")
	if err != nil {
		t.Fatalf("gw dag-json: %v", err)
	}
	djBody, _ := io.ReadAll(djResp.Body)
	djResp.Body.Close()
	if djResp.StatusCode != http.StatusOK {
		t.Fatalf("gw dag-json status: %d body=%s", djResp.StatusCode, string(djBody))
	}
	var dagEnv map[string]any
	if err := json.Unmarshal(djBody, &dagEnv); err != nil {
		t.Fatalf("gw dag-json parse: %v body=%s", err, string(djBody))
	}
	if dagEnv["mid"] != addMid {
		t.Errorf("dag-json mid mismatch: got %v want %s", dagEnv["mid"], addMid)
	}

	// 404 on unknown MID (parseable, absent from store)
	notFoundMid := midpkg.FromBytes([]byte("definitely-not-uploaded-content"))
	notFound, err := http.Get(gwURL + "/mem/" + notFoundMid.String())
	if err != nil {
		t.Fatalf("gw 404: %v", err)
	}
	notFound.Body.Close()
	if notFound.StatusCode != http.StatusNotFound {
		t.Errorf("gw 404 status: got %d want 404", notFound.StatusCode)
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

// parseBannerAddr scans the daemon stdout for a line
// like '  gateway_addr:   127.0.0.1:12345' and returns
// the address portion.
func parseBannerAddr(t *testing.T, stdout, key string) string {
	t.Helper()
	for _, line := range strings.Split(stdout, "\n") {
		line = strings.TrimRight(line, "\r")
		idx := strings.Index(line, key+":")
		if idx < 0 {
			continue
		}
		rest := strings.TrimSpace(line[idx+len(key)+1:])
		if rest != "" {
			return rest
		}
	}
	t.Fatalf("could not find %q in daemon stdout:\n%s", key, stdout)
	return ""
}

// httpGetJSON GETs url and decodes the JSON envelope
// into a map. It returns a non-nil error on any non-200
// status or decode error.
func httpGetJSON(url string) (map[string]any, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}
	out := map[string]any{}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode: %w body=%s", err, string(body))
	}
	return out, nil
}
