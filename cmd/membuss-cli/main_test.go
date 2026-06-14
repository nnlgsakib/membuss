// Tests for the CLI's gRPC plumbing. We boot a tiny gRPC
// server in-process (using a custom in-memory Backend) and
// point the CLI at it via --addr. Each test captures stdout
// and asserts the human-readable output is well-formed.
package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc"

	serverpkg "github.com/nnlgsakib/membuss/rpc/server"
)

// fakeBackend mirrors the rpc/server test backend so we can
// drive the CLI against a known state without bringing up a
// full libp2p stack.
type fakeBackend struct {
	root        string
	rootSize    uint64
	leafBlocks  uint64
	sealedSet   map[string]bool
	dhtPeekProv []serverpkg.NodePeerInfo
	peers       []serverpkg.NodePeerInfo
	anchor      serverpkg.AnchorInfo
}

func (f *fakeBackend) Add(ctx context.Context, path, chunker string, chunkSize uint32, sealRoot bool) (serverpkg.AddResult, error) {
	f.root = "memfake"
	f.rootSize = 42
	f.leafBlocks = 3
	if sealRoot {
		f.sealedSet = map[string]bool{f.root: true}
	}
	return serverpkg.AddResult{MID: f.root, Size: f.rootSize, Blocks: f.leafBlocks, Sealed: sealRoot}, nil
}

func (f *fakeBackend) Get(ctx context.Context, midStr string, offset, limit uint64) (io.ReadCloser, error) {
	body := []byte("the quick brown fox jumps over the lazy dog\n")
	return io.NopCloser(bytes.NewReader(body)), nil
}

func (f *fakeBackend) Seal(ctx context.Context, midStr string, recursive bool) (serverpkg.SealResult, error) {
	if f.sealedSet == nil {
		f.sealedSet = map[string]bool{}
	}
	if f.sealedSet[midStr] {
		return serverpkg.SealResult{Pinned: 0, Already: true}, nil
	}
	f.sealedSet[midStr] = true
	return serverpkg.SealResult{Pinned: 1, Already: false}, nil
}

func (f *fakeBackend) Unseal(ctx context.Context, midStr string) (uint64, error) {
	if !f.sealedSet[midStr] {
		return 0, nil
	}
	delete(f.sealedSet, midStr)
	return 1, nil
}

func (f *fakeBackend) Stat(ctx context.Context, midStr string) (serverpkg.StatInfo, error) {
	if midStr != f.root {
		return serverpkg.StatInfo{}, nil
	}
	return serverpkg.StatInfo{Present: true, Size: f.rootSize, Blocks: f.leafBlocks, Sealed: f.sealedSet[midStr], Codec: 0x55}, nil
}

func (f *fakeBackend) Peers(limit uint32) ([]serverpkg.NodePeerInfo, uint32, error) {
	out := f.peers
	if limit > 0 && uint32(len(out)) > limit {
		out = out[:limit]
	}
	return out, uint32(len(f.peers)), nil
}

func (f *fakeBackend) DHTPeek(ctx context.Context, midStr string, limit uint32) ([]serverpkg.NodePeerInfo, error) {
	return f.dhtPeekProv, nil
}

func (f *fakeBackend) GC(ctx context.Context, all bool) (serverpkg.GCInfo, error) {
	return serverpkg.GCInfo{BytesFreed: 4096, BlocksKept: 12}, nil
}

func (f *fakeBackend) AnchorStatus() serverpkg.AnchorInfo {
	return f.anchor
}

// startTestServer boots a gRPC server on a free loopback port
// and returns its address. The caller is responsible for calling
// the returned cleanup function.
func startTestServer(t *testing.T, b serverpkg.Backend) (addr string, cleanup func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	serverpkg.NewServer(b).Register(srv)
	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String(), func() { srv.Stop() }
}

// withCLI invokes the CLI's Execute() with os.Args replaced
// with the supplied args. stdout is captured.
func withCLI(t *testing.T, args []string, stdin io.Reader) (stdout, stderr string, err error) {
	t.Helper()

	oldStdout, oldStderr, oldStdin := os.Stdout, os.Stderr, os.Stdin
	defer func() { os.Stdout, os.Stderr, os.Stdin = oldStdout, oldStderr, oldStdin }()

	rOut, wOut, _ := os.Pipe()
	rErr, wErr, _ := os.Pipe()
	os.Stdout = wOut
	os.Stderr = wErr
	if stdin != nil {
		os.Stdin = os.NewFile(uintptr(0), "/dev/stdin") // best-effort, ignored if nil
		_ = os.Stdin
	}

	done := make(chan struct{})
	var outBuf, errBuf bytes.Buffer
	go func() { _, _ = io.Copy(&outBuf, rOut); close(done) }()
	go func() { _, _ = io.Copy(&errBuf, rErr) }()

	// Replace os.Args for the duration of this test.
	oldArgs := os.Args
	os.Args = append([]string{"membuss-cli"}, args...)
	defer func() { os.Args = oldArgs }()

	err = newRootCmd().Execute()
	_ = wOut.Close()
	_ = wErr.Close()
	<-done
	return outBuf.String(), errBuf.String(), err
}

func TestCLI_Ping(t *testing.T) {
	addr, stop := startTestServer(t, &fakeBackend{})
	defer stop()

	out, _, err := withCLI(t, []string{"--addr", addr, "ping", "hello"}, nil)
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	if !strings.Contains(out, "build") {
		t.Errorf("expected build row; got %q", out)
	}
	if !strings.Contains(out, "echo") || !strings.Contains(out, "hello") {
		t.Errorf("expected echoed message; got %q", out)
	}
}

func TestCLI_Add(t *testing.T) {
	addr, stop := startTestServer(t, &fakeBackend{})
	defer stop()

	dir := t.TempDir()
	p := filepath.Join(dir, "x.txt")
	if err := os.WriteFile(p, []byte("hello"), 0644); err != nil {
		t.Fatal(err)
	}
	out, _, err := withCLI(t, []string{"--addr", addr, "add", p}, nil)
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if !strings.Contains(out, "memfake") {
		t.Errorf("expected MID in output; got %q", out)
	}
	if !strings.Contains(out, "blocks") {
		t.Errorf("expected blocks row; got %q", out)
	}
}

func TestCLI_Get_Stdout(t *testing.T) {
	addr, stop := startTestServer(t, &fakeBackend{})
	defer stop()
	out, _, err := withCLI(t, []string{"--addr", addr, "get", "memfake", "-o", "-"}, nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !strings.Contains(out, "the quick brown fox") {
		t.Errorf("expected content; got %q", out)
	}
}

func TestCLI_Get_ToFile(t *testing.T) {
	addr, stop := startTestServer(t, &fakeBackend{})
	defer stop()
	dir := t.TempDir()
	p := filepath.Join(dir, "out.bin")
	_, _, err := withCLI(t, []string{"--addr", addr, "get", "memfake", "-o", p}, nil)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	got, err := os.ReadFile(p)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(got), "quick brown fox") {
		t.Errorf("file content wrong: %q", got)
	}
}

func TestCLI_Seal_Idempotent(t *testing.T) {
	addr, stop := startTestServer(t, &fakeBackend{})
	defer stop()
	out1, _, err := withCLI(t, []string{"--addr", addr, "seal", "memfake"}, nil)
	if err != nil {
		t.Fatalf("seal1: %v", err)
	}
	if !strings.Contains(out1, "pinned") || !strings.Contains(out1, "1") {
		t.Errorf("first seal: expected pinned=1; got %q", out1)
	}
	out2, _, err := withCLI(t, []string{"--addr", addr, "seal", "memfake"}, nil)
	if err != nil {
		t.Fatalf("seal2: %v", err)
	}
	if !strings.Contains(out2, "already") || !strings.Contains(out2, "true") {
		t.Errorf("second seal: expected already=true; got %q", out2)
	}
}

func TestCLI_Peers(t *testing.T) {
	b := &fakeBackend{peers: []serverpkg.NodePeerInfo{
		{PeerID: "12D3KooA", Addrs: []string{"/ip4/1.2.3.4/tcp/4001"}},
	}}
	addr, stop := startTestServer(t, b)
	defer stop()
	out, _, err := withCLI(t, []string{"--addr", addr, "peers"}, nil)
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	if !strings.Contains(out, "12D3KooA") || !strings.Contains(out, "total") {
		t.Errorf("expected peer + total; got %q", out)
	}
}

func TestCLI_DHTPeek(t *testing.T) {
	b := &fakeBackend{dhtPeekProv: []serverpkg.NodePeerInfo{
		{PeerID: "12D3KooB", Addrs: []string{"/ip4/1.2.3.4/tcp/4001"}},
	}}
	addr, stop := startTestServer(t, b)
	defer stop()
	out, _, err := withCLI(t, []string{"--addr", addr, "dht", "peek", "memx"}, nil)
	if err != nil {
		t.Fatalf("dht peek: %v", err)
	}
	if !strings.Contains(out, "12D3KooB") {
		t.Errorf("expected provider; got %q", out)
	}
}

func TestCLI_GC(t *testing.T) {
	addr, stop := startTestServer(t, &fakeBackend{})
	defer stop()
	out, _, err := withCLI(t, []string{"--addr", addr, "gc"}, nil)
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if !strings.Contains(out, "bytes_freed") {
		t.Errorf("expected bytes_freed row; got %q", out)
	}
}

func TestCLI_AnchorStatus(t *testing.T) {
	b := &fakeBackend{anchor: serverpkg.AnchorInfo{
		PeerID:     "12D3KooC",
		UptimeSecs: 120,
		BlocksHeld: 99,
		Anchors:    4,
		Synced:     11,
	}}
	addr, stop := startTestServer(t, b)
	defer stop()
	out, _, err := withCLI(t, []string{"--addr", addr, "anchor", "status"}, nil)
	if err != nil {
		t.Fatalf("anchor status: %v", err)
	}
	if !strings.Contains(out, "12D3KooC") || !strings.Contains(out, "synced") || !strings.Contains(out, "11") {
		t.Errorf("expected anchor status; got %q", out)
	}
}

func TestCLI_Stat_MissingMID(t *testing.T) {
	addr, stop := startTestServer(t, &fakeBackend{})
	defer stop()
	out, _, err := withCLI(t, []string{"--addr", addr, "stat", "memdoesnotexist"}, nil)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !strings.Contains(out, "present") || !strings.Contains(out, "false") {
		t.Errorf("expected present=false; got %q", out)
	}
}

func TestCLI_FormatBytes(t *testing.T) {
	cases := []struct {
		n    uint64
		want string
	}{
		{0, "0 B"},
		{1024, "1.00 KiB"},
		{1024 * 1024, "1.00 MiB"},
	}
	for _, c := range cases {
		got := formatBytes(c.n)
		if got != c.want {
			t.Errorf("formatBytes(%d)=%q want %q", c.n, got, c.want)
		}
	}
}

func TestCLI_RejectsMissingArg(t *testing.T) {
	addr, stop := startTestServer(t, &fakeBackend{})
	defer stop()
	_, _, err := withCLI(t, []string{"--addr", addr, "add"}, nil)
	if err == nil {
		t.Fatal("expected error for missing file arg")
	}
}

func TestCLI_DaemonStatus_AliasesPing(t *testing.T) {
	addr, stop := startTestServer(t, &fakeBackend{})
	defer stop()
	out, _, err := withCLI(t, []string{"--addr", addr, "daemon", "status"}, nil)
	if err != nil {
		t.Fatalf("daemon status: %v", err)
	}
	if !strings.Contains(out, "ok\tbuild=") {
		t.Errorf("expected ok/build= line; got %q", out)
	}
}

// Sanity check that cobra's command tree has the documented
// subcommands. This guards against accidental removals during
// refactors.
func TestCLI_CommandTree(t *testing.T) {
	want := []string{"add", "get", "seal", "unseal", "stat", "peers", "dht", "gc", "anchor", "ping", "daemon"}
	root := newRootCmd()
	got := make(map[string]bool, len(root.Commands()))
	for _, c := range root.Commands() {
		got[c.Name()] = true
	}
	for _, w := range want {
		if !got[w] {
			t.Errorf("missing command: %s", w)
		}
	}
}
