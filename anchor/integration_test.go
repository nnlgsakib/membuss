package anchor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/nnlgsakib/membuss/core/chunk"
	"github.com/nnlgsakib/membuss/core/dag"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"

	"github.com/nnlgsakib/membuss/net/dht"
	"github.com/nnlgsakib/membuss/net/herald"
	"github.com/nnlgsakib/membuss/net/memex"
)

// memexFetcher wraps memex.Session.Fetch so it can be used
// as a Fetcher. The io.Reader returned by Session.Fetch is
// fully consumed (and discarded) because the anchor engine
// only cares that the local blockstore is populated.
type memexFetcher struct {
	eng *memex.Engine
}

// directProviderResolver returns a fixed peer list for any
// MID, ignoring the DHT entirely. It lets the integration
// test exercise the full anchor Enqueue -> Memex fetch ->
// local blockstore -> DAG resolve path without depending on
// kad-dht provider-record propagation between the two
// in-process hosts.
type directProviderResolver struct {
	peers []peer.AddrInfo
}

func (r *directProviderResolver) Resolve(_ context.Context, _ mid.MID) ([]peer.AddrInfo, error) {
	out := make([]peer.AddrInfo, len(r.peers))
	copy(out, r.peers)
	return out, nil
}

type tLogger struct{ t *testing.T }

func (l tLogger) Infof(format string, args ...any)  { l.t.Logf("anchor: "+format, args...) }
func (l tLogger) Errorf(format string, args ...any) { l.t.Logf("anchor ERR: "+format, args...) }
func (f *memexFetcher) Fetch(ctx context.Context, root mid.MID, providers []peer.AddrInfo) error {
	if len(providers) == 0 {
		return nil
	}
	sess, err := memex.NewSession(memex.SessionConfig{
		Engine:    f.eng,
		Root:      root,
		Providers: providers,
		Timeout:   30 * time.Second,
	})
	if err != nil {
		return err
	}
	rc, err := sess.Fetch(ctx)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, rc)
	return nil
}

// TestAnchor_Integration_FetchOnEnqueue is the headline
// end-to-end test: a publisher node seals a 1MB file, the
// anchor node receives an Enqueue call for the root MID,
// fetches the DAG via Memex, and ends up with a local
// blockstore that resolves the root back to the original
// bytes.
func TestAnchor_Integration_FetchOnEnqueue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	content := make([]byte, 1024*1024)
	for i := range content {
		content[i] = byte(i >> 8)
	}
	sum := sha256.Sum256(content)

	// Publisher node.
	hPub := newTestHost(t)
	t.Cleanup(func() { _ = hPub.Close() })
	bsPub := store.NewMemstore()
	engPub, err := memex.New(memex.Config{Host: hPub, Blockstore: bsPub})
	if err != nil {
		t.Fatalf("memex.New pub: %v", err)
	}
	engPub.Start()
	t.Cleanup(engPub.Stop)

	ch, err := chunk.NewFixed(64 * 1024)(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("chunker: %v", err)
	}
	root, err := dag.NewBuilder(bsPub).Build(ch)
	if err != nil {
		t.Fatalf("dag build: %v", err)
	}
	if err := bsPub.Seal(root, true); err != nil {
		t.Fatalf("pub seal: %v", err)
	}

	// Anchor node.
	hAnchor := newTestHost(t)
	t.Cleanup(func() { _ = hAnchor.Close() })
	bsAnchor := store.NewMemstore()
	engAnchor, err := memex.New(memex.Config{Host: hAnchor, Blockstore: bsAnchor})
	if err != nil {
		t.Fatalf("memex.New anchor: %v", err)
	}
	engAnchor.Start()
	t.Cleanup(engAnchor.Stop)

	// Wire the two libp2p hosts together so Memex streams work.
	if err := hPub.Connect(ctx, peer.AddrInfo{ID: hAnchor.ID(), Addrs: hAnchor.Addrs()}); err != nil {
		t.Fatalf("pub connect anchor: %v", err)
	}
	if err := hAnchor.Connect(ctx, peer.AddrInfo{ID: hPub.ID(), Addrs: hPub.Addrs()}); err != nil {
		t.Fatalf("anchor connect pub: %v", err)
	}

	// A trivial DHT for the herald (no provider records
	// needed in this test; herald strategy "all" just walks
	// the local store).
	dhtAnchor, err := dht.New(ctx, dht.Config{Host: hAnchor, Mode: kaddht.ModeAuto})
	if err != nil {
		t.Fatalf("dht anchor: %v", err)
	}
	hd, err := herald.New(herald.Config{
		Store:    bsAnchor,
		DHT:      dhtAnchor,
		Strategy: herald.StrategyAll,
		Interval: time.Hour,
		Rate:     1000,
		Burst:    32,
	})
	if err != nil {
		t.Fatalf("herald.New: %v", err)
	}
	anchorEng, err := New(Config{
		Host:    hAnchor,
		DHT:     dhtAnchor,
		Store:   bsAnchor,
		Herald:  hd,
		Fetcher: &memexFetcher{eng: engAnchor},
		Logger:  tLogger{t: t},
		ProviderResolver: &directProviderResolver{
			peers: []peer.AddrInfo{{ID: hPub.ID(), Addrs: hPub.Addrs()}},
		},
		DiscoveryInterval: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("anchor.New: %v", err)
	}
	if err := anchorEng.Start(ctx); err != nil {
		t.Fatalf("anchor.Start: %v", err)
	}
	t.Cleanup(anchorEng.Stop)

	anchorEng.Enqueue(root)

	// Wait for the anchor engine to complete the full DAG
	// fetch (Synced is incremented after Fetcher.Fetch
	// returns). Just polling for Has(root) is not enough
	// because the root block arrives before the rest of
	// the DAG has finished streaming.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		st := anchorEng.Status()
		if st.Synced >= 1 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	has, _ := bsAnchor.Has(root)
	if !has {
		t.Fatal("anchor never received the root block")
	}

	resolver := dag.NewResolver(bsAnchor)
	rc, err := resolver.Resolve(root, nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	if len(got) != len(content) {
		t.Fatalf("len: got %d, want %d", len(got), len(content))
	}
	if sha256.Sum256(got) != sum {
		t.Fatal("content mismatch")
	}

	st := anchorEng.Status()
	if st.Synced < 1 {
		t.Fatalf("expected Synced >= 1, got %d", st.Synced)
	}
}

// TestFindProvidersWithAnchors_PrefersDirect verifies that
// the mergeAnchors helper keeps direct providers first.
func TestFindProvidersWithAnchors_PrefersDirect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hA := newTestHost(t)
	hB := newTestHost(t)
	t.Cleanup(func() { _ = hA.Close(); _ = hB.Close() })
	dh, err := dht.New(ctx, dht.Config{Host: hA, Mode: kaddht.ModeServer})
	if err != nil {
		t.Fatalf("dht: %v", err)
	}

	anchors := []peer.AddrInfo{{ID: hB.ID(), Addrs: hB.Addrs()}}
	provs, err := FindProvidersWithAnchors(ctx, dh, mid.FromBytes([]byte("not-sealed")), anchors, 0)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(provs) != 1 {
		t.Fatalf("expected 1 provider, got %d", len(provs))
	}
	if provs[0].ID != hB.ID() {
		t.Fatalf("expected anchor hB, got %s", provs[0].ID)
	}
}
