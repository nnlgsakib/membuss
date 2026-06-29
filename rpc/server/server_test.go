// Tests for the gRPC MembussNode server. These use the
// MemBackend, an in-memory implementation of server.Backend
// that powers every RPC. The gRPC server is bound to a real
// loopback TCP port for the duration of each test.
package server

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nnlgsakib/membuss/net/memex"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

// memBackend is a test-only Backend. It implements every
// Backend method against fixed in-memory data; Peers, DHTPeek
// and AnchorStatus return whatever the test configures.
type memBackend struct {
	root        string
	rootSize    uint64
	leafBlocks  uint64
	sealed      bool
	anchor      AnchorInfo
	dhtPeekProv []NodePeerInfo
	peers       []NodePeerInfo
	sealedSet   map[string]bool
}

func (m *memBackend) Add(ctx context.Context, path, chunker string, chunkSize uint32, sealRoot bool, name, mimeType string) (AddResult, error) {
	m.root = "memtest"
	m.rootSize = 12
	m.leafBlocks = 1
	if sealRoot {
		m.sealed = true
	}
	return AddResult{MID: m.root, Size: m.rootSize, Blocks: m.leafBlocks, Sealed: m.sealed}, nil
}

func (m *memBackend) Get(ctx context.Context, midStr string, offset, limit uint64) (io.ReadCloser, error) {
	return m.GetWithProgress(ctx, midStr, offset, limit, nil)
}

func (m *memBackend) GetWithProgress(ctx context.Context, midStr string, offset, limit uint64, progressFn func(update memex.ProgressUpdate)) (io.ReadCloser, error) {
	data := []byte("hello world!")
	if progressFn != nil {
		progressFn(memex.ProgressUpdate{BlocksResolved: uint64(len(data)), BlocksTotal: uint64(len(data))})
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memBackend) Seal(ctx context.Context, midStr string, recursive bool) (SealResult, error) {
	if m.sealedSet == nil {
		m.sealedSet = map[string]bool{}
	}
	if m.sealedSet[midStr] {
		return SealResult{Pinned: 0, Already: true}, nil
	}
	m.sealedSet[midStr] = true
	m.sealed = true
	return SealResult{Pinned: 1, Already: false}, nil
}

func (m *memBackend) Unseal(ctx context.Context, midStr string) (uint64, error) {
	if !m.sealedSet[midStr] {
		return 0, nil
	}
	delete(m.sealedSet, midStr)
	m.sealed = len(m.sealedSet) > 0
	return 1, nil
}

func (m *memBackend) Stat(ctx context.Context, midStr string) (StatInfo, error) {
	if midStr != m.root {
		return StatInfo{Present: false}, nil
	}
	return StatInfo{Present: true, Size: m.rootSize, Blocks: m.leafBlocks, Sealed: m.sealedSet[midStr] || m.sealed, Codec: 0x55}, nil
}

func (m *memBackend) Peers(limit uint32) ([]NodePeerInfo, uint32, error) {
	out := m.peers
	if limit > 0 && uint32(len(out)) > limit {
		out = out[:limit]
	}
	return out, uint32(len(m.peers)), nil
}

func (m *memBackend) DHTPeek(ctx context.Context, midStr string, limit uint32) ([]NodePeerInfo, error) {
	return m.dhtPeekProv, nil
}

func (m *memBackend) GC(ctx context.Context, all bool) (GCInfo, error) {
	return GCInfo{BytesFreed: 1024, BlocksKept: 5}, nil
}

func (m *memBackend) Delete(ctx context.Context, midStr string) (DeleteResult, error) {
	return DeleteResult{BlocksDeleted: 1, BytesFreed: 1024}, nil
}


func (m *memBackend) AnchorStatus() AnchorInfo {
	return m.anchor
}

// dial boots a gRPC server backed by b on a loopback port and
// returns clients for both services.
func dial(t *testing.T, b Backend) (membusspb.MembussNodeClient, membusspb.NodeClient) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	NewServer(b).Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return membusspb.NewMembussNodeClient(conn), membusspb.NewNodeClient(conn)
}

func TestPing_RoundTrip(t *testing.T) {
	_, cli := dial(t, &memBackend{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Ping(ctx, &membusspb.PingRequest{Message: "hello"})
	if err != nil {
		t.Fatalf("ping: %v", err)
	}
	if resp.Message != "hello" {
		t.Errorf("echo: got %q want %q", resp.Message, "hello")
	}
	if resp.Build == "" {
		t.Errorf("build should be set")
	}
}

func TestAdd_ReturnsSealedMID(t *testing.T) {
	cli, _ := dial(t, &memBackend{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Add(ctx, &membusspb.AddRequest{Path: "/tmp/x"})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	if resp.Mid == "" {
		t.Errorf("mid empty")
	}
	if !resp.Sealed {
		t.Errorf("expected sealed=true")
	}
	if resp.Size == 0 || resp.Blocks == 0 {
		t.Errorf("size/blocks not set")
	}
}

func TestAdd_RejectsEmptyPath(t *testing.T) {
	cli, _ := dial(t, &memBackend{})
	_, err := cli.Add(context.Background(), &membusspb.AddRequest{})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGet_StreamsContent(t *testing.T) {
	cli, _ := dial(t, &memBackend{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := cli.Get(ctx, &membusspb.GetRequest{Mid: "memtest"})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	var got []byte
	var idx uint64
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		got = append(got, frame.Data...)
		if frame.Index != idx {
			t.Errorf("index: got %d want %d", frame.Index, idx)
		}
		idx++
	}
	if string(got) != "hello world!" {
		t.Errorf("content: got %q", got)
	}
}

func TestSeal_Idempotent(t *testing.T) {
	b := &memBackend{}
	cli, _ := dial(t, b)
	ctx := context.Background()
	r1, err := cli.Seal(ctx, &membusspb.SealRequest{Mid: "memtest"})
	if err != nil {
		t.Fatalf("seal1: %v", err)
	}
	if r1.Already {
		t.Errorf("first seal should not be Already")
	}
	r2, err := cli.Seal(ctx, &membusspb.SealRequest{Mid: "memtest"})
	if err != nil {
		t.Fatalf("seal2: %v", err)
	}
	if !r2.Already {
		t.Errorf("second seal should be Already")
	}
}

func TestStat_ReflectsSeal(t *testing.T) {
	b := &memBackend{root: "memtest", sealed: true}
	cli, _ := dial(t, b)
	resp, err := cli.Stat(context.Background(), &membusspb.StatRequest{Mid: "memtest"})
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !resp.Present || !resp.Sealed {
		t.Errorf("expected present+sealed; got %+v", resp)
	}
}

func TestStat_MissingMID(t *testing.T) {
	cli, _ := dial(t, &memBackend{})
	resp, err := cli.Stat(context.Background(), &membusspb.StatRequest{Mid: "memdoesnotexist"})
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if resp.Present {
		t.Errorf("expected absent")
	}
}

func TestPeers_Empty(t *testing.T) {
	cli, _ := dial(t, &memBackend{})
	resp, err := cli.Peers(context.Background(), &membusspb.PeersRequest{})
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	if len(resp.Peers) != 0 || resp.Total != 0 {
		t.Errorf("expected empty; got %+v", resp)
	}
}

func TestPeers_WithEntries(t *testing.T) {
	b := &memBackend{peers: []NodePeerInfo{
		{PeerID: "12D3KooA", Addrs: []string{"/ip4/1.2.3.4/tcp/4001"}},
		{PeerID: "12D3KooB", Addrs: []string{"/ip4/5.6.7.8/tcp/4001"}},
	}}
	cli, _ := dial(t, b)
	resp, err := cli.Peers(context.Background(), &membusspb.PeersRequest{Limit: 1})
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	if resp.Total != 2 {
		t.Errorf("total: got %d want 2", resp.Total)
	}
	if len(resp.Peers) != 1 {
		t.Errorf("limit: got %d want 1", len(resp.Peers))
	}
}

func TestDHTPeek_ReturnsProviders(t *testing.T) {
	b := &memBackend{dhtPeekProv: []NodePeerInfo{
		{PeerID: "12D3KooA", Addrs: []string{"/ip4/1.2.3.4/tcp/4001"}},
	}}
	cli, _ := dial(t, b)
	resp, err := cli.DHTPeek(context.Background(), &membusspb.DHTPeekRequest{Mid: "memtest"})
	if err != nil {
		t.Fatalf("dht peek: %v", err)
	}
	if len(resp.Providers) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(resp.Providers))
	}
	if resp.Providers[0].PeerId != "12D3KooA" {
		t.Errorf("peer id: got %q", resp.Providers[0].PeerId)
	}
}

func TestGC_ReportsBytes(t *testing.T) {
	cli, _ := dial(t, &memBackend{})
	resp, err := cli.GC(context.Background(), &membusspb.GCRequest{})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if resp.BytesFreed != 1024 || resp.BlocksKept != 5 {
		t.Errorf("unexpected gc: %+v", resp)
	}
}

func TestAnchorStatus_PassThrough(t *testing.T) {
	b := &memBackend{anchor: AnchorInfo{
		PeerID:     "12D3KooA",
		UptimeSecs: 60,
		BlocksHeld: 42,
		Anchors:    3,
		Backlog:    0,
		Synced:     7,
	}}
	cli, _ := dial(t, b)
	resp, err := cli.AnchorStatus(context.Background(), &membusspb.AnchorStatusRequest{})
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	if resp.PeerId != "12D3KooA" || resp.Synced != 7 {
		t.Errorf("unexpected: %+v", resp)
	}
}

func TestUnseal_ReportsRemoved(t *testing.T) {
	b := &memBackend{}
	b.sealedSet = map[string]bool{"memtest": true}
	b.sealed = true
	cli, _ := dial(t, b)
	resp, err := cli.Unseal(context.Background(), &membusspb.UnsealRequest{Mid: "memtest"})
	if err != nil {
		t.Fatalf("unseal: %v", err)
	}
	if resp.Removed != 1 {
		t.Errorf("removed: got %d want 1", resp.Removed)
	}
}

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		n    uint64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.00 KiB"},
		{1024 * 1024, "1.00 MiB"},
		{2 * 1024 * 1024 * 1024, "2.00 GiB"},
	}
	for _, c := range cases {
		got := FormatBytes(c.n)
		if got != c.want {
			t.Errorf("FormatBytes(%d)=%q want %q", c.n, got, c.want)
		}
	}
}

func TestBackend_AllMethodsCallable(t *testing.T) {
	// Smoke test that every Backend method is reached at least
	// once over a full client roundtrip. Catches future
	// "method removed" regressions.
	b := &memBackend{
		peers:       []NodePeerInfo{{PeerID: "P"}},
		anchor:      AnchorInfo{PeerID: "P", Synced: 1},
		dhtPeekProv: []NodePeerInfo{{PeerID: "P"}},
	}
	cli, _ := dial(t, b)
	ctx := context.Background()
	if _, err := cli.Add(ctx, &membusspb.AddRequest{Path: "/x"}); err != nil {
		t.Fatal(err)
	}
	s, err := cli.Get(ctx, &membusspb.GetRequest{Mid: "memtest"})
	if err != nil {
		t.Fatal(err)
	}
	for {
		_, err := s.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cli.Seal(ctx, &membusspb.SealRequest{Mid: "memtest"}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Unseal(ctx, &membusspb.UnsealRequest{Mid: "memtest"}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Stat(ctx, &membusspb.StatRequest{Mid: "memtest"}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Peers(ctx, &membusspb.PeersRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.DHTPeek(ctx, &membusspb.DHTPeekRequest{Mid: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.GC(ctx, &membusspb.GCRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.AnchorStatus(ctx, &membusspb.AnchorStatusRequest{}); err != nil {
		t.Fatal(err)
	}
}

func TestMain(m *testing.M) {
	// Build identifier used in PingResponse.
	Build = "test"
	m.Run()
}
