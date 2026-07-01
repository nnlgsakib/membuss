package memex

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/libp2p/go-libp2p/p2p/security/noise"
	libp2pquic "github.com/libp2p/go-libp2p/p2p/transport/quic"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"
	"github.com/libp2p/go-libp2p/core/network"
	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/chunk"
	"github.com/nnlgsakib/membuss/core/dag"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

func newTestHost(t *testing.T) host.Host {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tcpAddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	quicAddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/udp/0/quic-v1")
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrs(tcpAddr, quicAddr),
		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(libp2pquic.NewTransport),
		libp2p.Security(noise.ID, noise.New),
	)
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	return h
}

func newTestEngine(t *testing.T, h host.Host) (*Engine, *store.Memstore) {
	t.Helper()
	bs := store.NewMemstore()
	eng, err := New(Config{Host: h, Blockstore: bs})
	if err != nil {
		t.Fatalf("memex.New: %v", err)
	}
	eng.Start()
	t.Cleanup(eng.Stop)
	return eng, bs
}

func makeContent(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i >> 16)
	}
	return b
}

func buildDAG(t *testing.T, content []byte, bs store.Blockstore) string {
	t.Helper()
	factory := chunk.NewFixed(256 * 1024)
	ch, err := factory(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("chunker: %v", err)
	}
	root, err := dag.NewBuilder(bs).Build(ch)
	if err != nil {
		t.Fatalf("dag build: %v", err)
	}
	return root.String()
}

// TestMemex_5MBRoundTrip is the headline integration test:
// node A seals a 5MB file, node B requests the root MID, the
// fetched bytes must match the original.
func TestMemex_5MBRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	content := makeContent(t, 5*1024*1024)
	sum := sha256.Sum256(content)

	hA := newTestHost(t)
	t.Cleanup(func() { _ = hA.Close() })
	_, bsA := newTestEngine(t, hA)
	rootStr := buildDAG(t, content, bsA)
	if bsA.Len() < 20 {
		t.Fatalf("provider blockstore unexpectedly small: %d", bsA.Len())
	}

	hB := newTestHost(t)
	t.Cleanup(func() { _ = hB.Close() })
	engB, _ := newTestEngine(t, hB)

	if err := hA.Connect(ctx, peer.AddrInfo{ID: hB.ID(), Addrs: hB.Addrs()}); err != nil {
		t.Fatalf("hA connect hB: %v", err)
	}
	if err := hB.Connect(ctx, peer.AddrInfo{ID: hA.ID(), Addrs: hA.Addrs()}); err != nil {
		t.Fatalf("hB connect hA: %v", err)
	}

	root, err := mid.Parse(rootStr)
	if err != nil {
		t.Fatalf("parse root: %v", err)
	}

	sess, err := NewSession(SessionConfig{
		Engine:    engB,
		Root:      root,
		Providers: []peer.AddrInfo{{ID: hA.ID(), Addrs: hA.Addrs()}},
		Timeout:   45 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	rc, err := sess.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != len(content) {
		t.Fatalf("len mismatch: got %d want %d", len(got), len(content))
	}
	if sha256.Sum256(got) != sum {
		t.Fatalf("content mismatch")
	}
}

// TestMemex_ServerHandlesEmptyStream ensures a peer opening a
// stream and immediately closing it does not panic the
// engine.
func TestMemex_ServerHandlesEmptyStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	hA := newTestHost(t)
	hB := newTestHost(t)
	t.Cleanup(func() { _ = hA.Close(); _ = hB.Close() })
	newTestEngine(t, hA)
	newTestEngine(t, hB)

	if err := hB.Connect(ctx, peer.AddrInfo{ID: hA.ID(), Addrs: hA.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}

	stream, err := hB.NewStream(ctx, hA.ID(), ProtocolID)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(5 * time.Second))

	// Server should close without sending anything.
	buf := make([]byte, 1)
	_, err = stream.Read(buf)
	if err == nil {
		t.Fatal("expected EOF, got nil")
	}
}


// TestMemex_ObjectInfoTransmit is the regression test for
// the "metadata does not travel across nodes" bug. Node A
// stores a DAG and writes the Phase 19 ObjectInfo for the
// root (filename + MIME). Node B fetches the root and the
// ObjectInfo must show up in its local meta namespace, so
// downstream readers (gateway, explorer) can see the
// uploader-supplied name + mime without any extra round
// trips.
func TestMemex_ObjectInfoTransmit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	content := makeContent(t, 1*1024*1024)

	hA := newTestHost(t)
	t.Cleanup(func() { _ = hA.Close() })
	_, bsA := newTestEngine(t, hA)
	rootStr := buildDAG(t, content, bsA)

	root, err := mid.Parse(rootStr)
	if err != nil {
		t.Fatalf("parse root: %v", err)
	}

	// Phase 19: the uploader on node A wrote the filename
	// + MIME alongside the DAG.
	if err := store.SetObjectInfo(bsA, root, store.ObjectInfo{
		Name:     "hello.txt",
		MimeType: "text/plain; charset=utf-8",
		Size:     uint64(len(content)),
	}); err != nil {
		t.Fatalf("SetObjectInfo: %v", err)
	}

	hB := newTestHost(t)
	t.Cleanup(func() { _ = hB.Close() })
	engB, bsB := newTestEngine(t, hB)

	if err := hA.Connect(ctx, peer.AddrInfo{ID: hB.ID(), Addrs: hB.Addrs()}); err != nil {
		t.Fatalf("hA connect hB: %v", err)
	}
	if err := hB.Connect(ctx, peer.AddrInfo{ID: hA.ID(), Addrs: hA.Addrs()}); err != nil {
		t.Fatalf("hB connect hA: %v", err)
	}

	sess, err := NewSession(SessionConfig{
		Engine:    engB,
		Root:      root,
		Providers: []peer.AddrInfo{{ID: hA.ID(), Addrs: hA.Addrs()}},
		Timeout:   DefaultSessionTimeout,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	rc, err := sess.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	// Drain the reader so the read loop finishes persisting
	// ObjectInfo to the local meta store.
	if _, err := io.Copy(io.Discard, rc); err != nil {
		t.Fatalf("drain: %v", err)
	}

	// Give the read loop a brief moment to land any final
	// ObjectInfo writes.
	time.Sleep(100 * time.Millisecond)

	oi, err := store.GetObjectInfo(bsB, root)
	if err != nil {
		t.Fatalf("GetObjectInfo on requester: %v", err)
	}
	if oi.Name == "" && oi.MimeType == "" {
		t.Fatal("ObjectInfo did not travel across nodes (empty descriptor in local meta)")
	}
	if oi.Name != "hello.txt" {
		t.Errorf("name: got %q want %q", oi.Name, "hello.txt")
	}
	if oi.MimeType != "text/plain; charset=utf-8" {
		t.Errorf("mime: got %q want %q", oi.MimeType, "text/plain; charset=utf-8")
	}
	if oi.Size != uint64(len(content)) {
		t.Errorf("size: got %d want %d", oi.Size, uint64(len(content)))
	}
}

// TestMemex_MultipleStreamsPerProvider verifies that opening
// multiple streams per provider produces the same correct
// result as a single stream. The content must be identical.
func TestMemex_MultipleStreamsPerProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	content := makeContent(t, 2*1024*1024)
	sum := sha256.Sum256(content)

	hA := newTestHost(t)
	t.Cleanup(func() { _ = hA.Close() })
	_, bsA := newTestEngine(t, hA)
	rootStr := buildDAG(t, content, bsA)

	hB := newTestHost(t)
	t.Cleanup(func() { _ = hB.Close() })
	engB, _ := newTestEngine(t, hB)

	if err := hA.Connect(ctx, peer.AddrInfo{ID: hB.ID(), Addrs: hB.Addrs()}); err != nil {
		t.Fatalf("hA connect hB: %v", err)
	}
	if err := hB.Connect(ctx, peer.AddrInfo{ID: hA.ID(), Addrs: hA.Addrs()}); err != nil {
		t.Fatalf("hB connect hA: %v", err)
	}

	root, err := mid.Parse(rootStr)
	if err != nil {
		t.Fatalf("parse root: %v", err)
	}

	sess, err := NewSession(SessionConfig{
		Engine:            engB,
		Root:              root,
		Providers:         []peer.AddrInfo{{ID: hA.ID(), Addrs: hA.Addrs()}},
		Timeout:           45 * time.Second,
		StreamsPerProvider: 3,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	rc, err := sess.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != len(content) {
		t.Fatalf("len mismatch: got %d want %d", len(got), len(content))
	}
	if sha256.Sum256(got) != sum {
		t.Fatalf("content mismatch")
	}
}

// TestMemex_MultiStreamDefaults verifies that the default
// StreamsPerProvider value is applied when not explicitly set.
func TestMemex_MultiStreamDefaults(t *testing.T) {
	if DefaultStreamsPerProvider != 2 {
		t.Fatalf("DefaultStreamsPerProvider = %d, want 2", DefaultStreamsPerProvider)
	}
	// Verify the constant is bounded.
	if MaxStreamsPerProvider < DefaultStreamsPerProvider {
		t.Fatalf("MaxStreamsPerProvider (%d) < DefaultStreamsPerProvider (%d)",
			MaxStreamsPerProvider, DefaultStreamsPerProvider)
	}
}

// TestMemex_CircuitRelayFallback verifies that if a direct connection to a provider fails,
// the stream falls back to dialing via an active circuit relay.
func TestMemex_CircuitRelayFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Create Host A (provider)
	hA := newTestHost(t)
	t.Cleanup(func() { _ = hA.Close() })
	_, bsA := newTestEngine(t, hA)
	content := []byte("hello through the relay fallback!")
	rootStr := buildDAG(t, content, bsA)
	root, err := mid.Parse(rootStr)
	if err != nil {
		t.Fatalf("failed to parse root MID: %v", err)
	}

	// 2. Create Host B (relay node)
	hB, err := libp2p.New(
		libp2p.ListenAddrs(multiaddr.StringCast("/ip4/127.0.0.1/tcp/0")),
	)
	if err != nil {
		t.Fatalf("failed to create relay host: %v", err)
	}
	_, err = relay.New(hB,
		relay.WithResources(relay.DefaultResources()),
		relay.WithReservationAddressFilter(func(addr multiaddr.Multiaddr) bool {
			return true
		}),
	)
	if err != nil {
		t.Fatalf("failed to create relay service: %v", err)
	}
	t.Cleanup(func() { _ = hB.Close() })

	// 3. Create Host C (requester node)
	hC := newTestHost(t)
	t.Cleanup(func() { _ = hC.Close() })
	engC, _ := newTestEngine(t, hC)

	// Connect A to B and make a reservation
	if err := hA.Connect(ctx, peer.AddrInfo{ID: hB.ID(), Addrs: hB.Addrs()}); err != nil {
		t.Fatalf("hA connect to relay hB: %v", err)
	}
	_, err = client.Reserve(ctx, hA, peer.AddrInfo{ID: hB.ID(), Addrs: hB.Addrs()})
	if err != nil {
		t.Fatalf("hA reservation on relay hB: %v", err)
	}

	// Connect C to B
	if err := hC.Connect(ctx, peer.AddrInfo{ID: hB.ID(), Addrs: hB.Addrs()}); err != nil {
		t.Fatalf("hC connect to relay hB: %v", err)
	}

	// Clear direct addresses of A from C's peerstore to force fallback
	hC.Peerstore().ClearAddrs(hA.ID())

	// Run Fetch with session
	sess, err := NewSession(SessionConfig{
		Engine:    engC,
		Root:      root,
		Providers: []peer.AddrInfo{{ID: hA.ID()}},
		Timeout:   15 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	rc, err := sess.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if string(got) != string(content) {
		t.Fatalf("fetched content mismatch: got %q, want %q", string(got), string(content))
	}
}

// TestMemex_ProviderScoringAndLatencyScheduling verifies that peer latency
// and success metrics are correctly recorded and used by the scheduler to prioritize
// faster / higher-scoring peers over slower ones.
func TestMemex_ProviderScoringAndLatencyScheduling(t *testing.T) {
	hC := newTestHost(t)
	t.Cleanup(func() { _ = hC.Close() })
	engC, _ := newTestEngine(t, hC)

	// Create two fake peer IDs
	privA, _, _ := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	peerA, _ := peer.IDFromPrivateKey(privA)
	privB, _, _ := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	peerB, _ := peer.IDFromPrivateKey(privB)

	// Peer A: Fast node (10ms latency, high successes)
	for i := 0; i < 5; i++ {
		engC.RecordPeerSuccess(peerA, 10*time.Millisecond)
	}

	// Peer B: Slow/unreliable node (500ms latency, some failures)
	for i := 0; i < 3; i++ {
		engC.RecordPeerSuccess(peerB, 500*time.Millisecond)
	}
	engC.RecordPeerFailure(peerB)

	scoreA := engC.PeerScore(peerA)
	scoreB := engC.PeerScore(peerB)

	if scoreA <= scoreB {
		t.Fatalf("expected fast peer score (%f) to be higher than slow peer score (%f)", scoreA, scoreB)
	}

	// Now check scheduler preference. We'll set up a dummy Session with A and B as active streams.
	sess, err := NewSession(SessionConfig{
		Engine:    engC,
		Root:      mid.FromBytes([]byte("dummy root")),
		Providers: []peer.AddrInfo{{ID: peerA}, {ID: peerB}},
	})
	if err != nil {
		t.Fatalf("failed to create session: %v", err)
	}

	// Add both to active streams
	sess.streams = []streamInfo{
		{peerID: peerA, ch: make(chan sessionEvent, 10)},
		{peerID: peerB, ch: make(chan sessionEvent, 10)},
	}

	// Enqueue a want
	id1 := mid.FromBytes([]byte("test content 1"))
	sess.wantStates[id1.String()] = &wantState{
		mid:            id1,
		triedProviders: make(map[peer.ID]struct{}),
	}

	// Run scheduleWants
	sess.scheduleWants()

	// Verify that the scheduler preferred the higher-scoring peerA
	ws1 := sess.wantStates[id1.String()]
	if ws1.currentProvider != peerA {
		t.Fatalf("expected scheduler to select faster peer %s, but got %s", peerA, ws1.currentProvider)
	}
}

// TestMemex_BlockIntegrityVerification verifies that a received block with
// mismatched hash/MID is discarded and the provider peer is recorded as failed.
func TestMemex_BlockIntegrityVerification(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hC := newTestHost(t)
	t.Cleanup(func() { _ = hC.Close() })
	engC, bsC := newTestEngine(t, hC)

	// Create valid MID for content A
	contentA := []byte("correct block data")
	idA := mid.FromBytes(contentA)

	// Build a MemexMessage containing a block with idA, but corrupt data
	corruptData := []byte("completely different corrupt data")
	msg := membusspb.MemexMessage{
		Blocks: []*membusspb.Block{
			{
				Mid:  idA.String(),
				Data: corruptData,
			},
		},
	}

	buf, err := proto.Marshal(&msg)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}

	// Write framed message to a buffer manually (big-endian length prefix)
	frameBuf := new(bytes.Buffer)
	lenBuf := make([]byte, 4)
	l := uint32(len(buf))
	lenBuf[0] = byte(l >> 24)
	lenBuf[1] = byte(l >> 16)
	lenBuf[2] = byte(l >> 8)
	lenBuf[3] = byte(l)
	frameBuf.Write(lenBuf)
	frameBuf.Write(buf)

	// Setup mock stream
	mockStr := &mockReadStream{
		r: frameBuf,
	}

	sess, err := NewSession(SessionConfig{
		Engine:    engC,
		Root:      idA,
		Providers: []peer.AddrInfo{{ID: peer.ID("mock-peer")}},
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	sess.wantStates[idA.String()] = &wantState{
		mid:            idA,
		triedProviders: make(map[peer.ID]struct{}),
	}

	ps := &pipelineState{
		capCh: make(chan struct{}, 10),
	}

	// Run readLoop, which will process the corrupt block, record failure, and exit on EOF
	err = sess.readLoop(ctx, mockStr, ps)
	if err != nil && err != io.EOF {
		t.Fatalf("readLoop failed: %v", err)
	}

	// Verify that the corrupt block was NOT saved in the blockstore
	has, err := bsC.Has(idA)
	if err != nil {
		t.Fatalf("bs.Has failed: %v", err)
	}
	if has {
		t.Fatalf("expected corrupt block data to be discarded and NOT saved in the blockstore")
	}

	// Verify that the peer was penalized with a failure in metrics
	score := engC.PeerScore(peer.ID("mock-peer"))
	if score > 1.0 {
		t.Fatalf("expected mock-peer to have a penalized score, but got %f", score)
	}
}

type mockReadStream struct {
	network.Stream
	r io.Reader
}

func (m *mockReadStream) Read(p []byte) (int, error) {
	return m.r.Read(p)
}

func (m *mockReadStream) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockReadStream) Conn() network.Conn {
	return &mockReadConn{}
}

type mockReadConn struct {
	network.Conn
}

func (c *mockReadConn) RemotePeer() peer.ID {
	return peer.ID("mock-peer")
}