package anchor


import (
	"bytes"
	"context"
	"testing"
	"time"

	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/nnlgsakib/membuss/core/chunk"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/dag"
	"github.com/nnlgsakib/membuss/core/store"
	"github.com/nnlgsakib/membuss/net/dht"
	"github.com/nnlgsakib/membuss/net/herald"
	"github.com/nnlgsakib/membuss/net/memex"
)

// memNode is a small in-process Membuss node for the 5-node
// integration test. It owns a libp2p host, a DHT, a Memex
// engine, a blockstore, and an optional anchor engine.
type memNode struct {
	host   interface{ Close() error }
	dht    *dht.MemDHT
	memex  *memex.Engine
	store  *store.Memstore
	anchor *AnchorEngine
	herald *herald.MemHerald
}

func (m *memNode) CloseAll() {
	if m.anchor != nil {
		m.anchor.Stop()
	}
	if m.herald != nil {
		m.herald.Stop()
	}
	if m.memex != nil {
		m.memex.Stop()
	}
	if m.dht != nil {
		_ = m.dht.Close()
	}
}

// peerInfo is a tiny alias for peer.AddrInfo so the test file
// is shorter to read.
type peerInfo = peer.AddrInfo

// buildNode constructs a fresh in-process Membuss node. When
// anchorMode is true the node is wired with a full Anchor
// engine.
func buildNode(t *testing.T, anchorMode bool) *memNode {
	t.Helper()
	h := newTestHost(t)
	t.Cleanup(func() { _ = h.Close() })

	dhtCtx, dcancel := context.WithCancel(context.Background())
	t.Cleanup(dcancel)
	mdht, err := dht.New(dhtCtx, dht.Config{
		Host: h,
		Mode: kaddht.ModeServer,
	})
	if err != nil {
		t.Fatalf("dht.New: %v", err)
	}

	bs := store.NewMemstore()
	eng, err := memex.New(memex.Config{Host: h, Blockstore: bs})
	if err != nil {
		t.Fatalf("memex.New: %v", err)
	}
	eng.Start()
	t.Cleanup(eng.Stop)

	n := &memNode{host: h, dht: mdht, memex: eng, store: bs}

	if anchorMode {
		hd, herr := herald.New(herald.Config{
			Store:    bs,
			DHT:      mdht,
			Strategy: herald.StrategyRoots,
			Interval: 30 * time.Second,
			Rate:     100,
			Burst:    8,
		})
		if herr != nil {
			t.Fatalf("herald.New: %v", herr)
		}
		hd.Start(dhtCtx)
		t.Cleanup(hd.Stop)
		n.herald = hd

		fetcher := &memexFetcher{eng: eng}
		ae, aerr := New(Config{
			Host:              h,
			DHT:               mdht,
			Store:             bs,
			Herald:            hd,
			Fetcher:           fetcher,
			DiscoveryInterval: 1 * time.Second,
		})
		if aerr != nil {
			t.Fatalf("anchor.New: %v", aerr)
		}
		if serr := ae.Start(dhtCtx); serr != nil {
			t.Fatalf("anchor.Start: %v", serr)
		}
		t.Cleanup(ae.Stop)
		n.anchor = ae
	}
	return n
}

// hostAddrInfo returns the AddrInfo of a host for direct
// dialing.
func hostAddrInfo(t *testing.T, h interface{}) peerInfo {
	t.Helper()
	real, ok := h.(host.Host)
	if !ok {
		t.Fatalf("host is not a libp2p host")
	}
	return peer.AddrInfo{ID: real.ID(), Addrs: real.Addrs()}
}

// addAndSeal chunks the content, builds a DAG, stores every
// block on n.store, and seals the root.
func addAndSeal(n *memNode, content []byte) (string, error) {
	factory := chunk.NewFixed(256 * 1024)
	ch, err := factory(bytes.NewReader(content))
	if err != nil {
		return "", err
	}
	root, err := dag.NewBuilder(n.store).Build(ch)
	if err != nil {
		return "", err
	}
	if err := n.store.Seal(root, true); err != nil {
		return "", err
	}
	return root.String(), nil
}

// directProviderResolver is a ProviderResolver that always
// returns the same peer list regardless of MID. We use it
// to make the anchor engine deterministic in tests.
type phase10DirectProviderResolver struct {
	peers []peerInfo
}

func (r *phase10DirectProviderResolver) Resolve(_ context.Context, _ mid.MID) ([]peerInfo, error) {
	out := make([]peerInfo, len(r.peers))
	copy(out, r.peers)
	return out, nil
}

// buildNodeWithDirectProvider is like buildNode but, when
// anchorMode is true, plugs a directProviderResolver into
// the anchor engine so its discovery loop is deterministic.
func buildNodeWithDirectProvider(t *testing.T, anchorMode bool, directPeers ...peerInfo) *memNode {
	t.Helper()
	h := newTestHost(t)
	t.Cleanup(func() { _ = h.Close() })

	dhtCtx, dcancel := context.WithCancel(context.Background())
	t.Cleanup(dcancel)
	mdht, err := dht.New(dhtCtx, dht.Config{
		Host: h,
		Mode: kaddht.ModeServer,
	})
	if err != nil {
		t.Fatalf("dht.New: %v", err)
	}

	bs := store.NewMemstore()
	eng, err := memex.New(memex.Config{Host: h, Blockstore: bs})
	if err != nil {
		t.Fatalf("memex.New: %v", err)
	}
	eng.Start()
	t.Cleanup(eng.Stop)

	n := &memNode{host: h, dht: mdht, memex: eng, store: bs}

	if anchorMode {
		hd, herr := herald.New(herald.Config{
			Store:    bs,
			DHT:      mdht,
			Strategy: herald.StrategyRoots,
			Interval: 30 * time.Second,
			Rate:     100,
			Burst:    8,
		})
		if herr != nil {
			t.Fatalf("herald.New: %v", herr)
		}
		hd.Start(dhtCtx)
		t.Cleanup(hd.Stop)
		n.herald = hd

		fetcher := &memexFetcher{eng: eng}
		cfg := Config{
			Host:              h,
			DHT:               mdht,
			Store:             bs,
			Herald:            hd,
			Fetcher:           fetcher,
			DiscoveryInterval: 1 * time.Second,
		}
		if len(directPeers) > 0 {
			cfg.ProviderResolver = &phase10DirectProviderResolver{peers: directPeers}
		}
		ae, aerr := New(cfg)
		if aerr != nil {
			t.Fatalf("anchor.New: %v", aerr)
		}
		if serr := ae.Start(dhtCtx); serr != nil {
			t.Fatalf("anchor.Start: %v", serr)
		}
		t.Cleanup(ae.Stop)
		n.anchor = ae
	}
	return n
}

// peerInfo is re-exported for the test scenario.
