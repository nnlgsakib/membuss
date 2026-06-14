package anchor

import (
	"context"
	"crypto/sha256"
	"io"
	"testing"
	"time"

	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/net/memex"
)

// TestAnchor_5Node_FetchAfterProviderShutdown is the Phase 10
// headline integration test: 5 in-process nodes, the publisher
// adds and seals a 10 MB file, an anchor node syncs it via a
// direct provider resolver (in-process DHT propagation between
// hosts is unreliable), the publisher is shut down, and a third
// node retrieves the file from the anchor.
func TestAnchor_5Node_FetchAfterProviderShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Step 1: build the publisher (node 0) so we know its
	// libp2p address. The other 4 nodes will use this address
	// as a direct provider when fetching.
	pub := buildNode(t, false)
	t.Cleanup(pub.CloseAll)

	pubInfo := hostAddrInfo(t, pub.host)
	// Build nodes 1..4 (the anchor is index 3, i.e. the 4th).
	const total = 5
	others := make([]*memNode, 0, total-1)
	for i := 1; i < total; i++ {
		anchorMode := i == total-1
		n := buildNodeWithDirectProvider(t, anchorMode, pubInfo)
		others = append(others, n)
	}
	t.Cleanup(func() {
		for _, n := range others {
			n.CloseAll()
		}
	})

	// Wire up connectivity: each non-publisher node connects
	// to the publisher. We use the DHT/host Connect path.
	for _, n := range others {
		connectCtx, ccancel := context.WithTimeout(ctx, 5*time.Second)
		_ = n.dht.Host().Connect(connectCtx, pubInfo)
		ccancel()
	}

	// Step 2: publisher adds and seals the 10 MB file.
	const size = 10 * 1024 * 1024
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i >> 8)
	}
	sum := sha256.Sum256(content)

	root, err := addAndSeal(pub, content)
	if err != nil {
		t.Fatalf("add+seal: %v", err)
	}
	t.Logf("publisher added and sealed root=%s", root)
	parsedRoot := mid.MustParse(root)

	// Step 3: wait for the anchor (last in `others`) to sync
	// the root. The anchor uses a direct provider resolver
	// pointing at the publisher, so the discovery loop is
	// deterministic and fast.
	anchor := others[len(others)-1]
	// Hint the anchor that the publisher has new content. The
	// anchor's discovery loop will resolve the publisher via
	// its direct provider resolver and pull the DAG.
	anchor.anchor.Enqueue(parsedRoot)
	deadline := time.Now().Add(60 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatalf("anchor did not sync within deadline")
		}
		has, _ := anchor.store.Has(parsedRoot)
		if has {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Log("anchor has the root MID")

	// Step 4: shut down the publisher so the only remaining
	// copy of the content is on the anchor.
	pub.CloseAll()
	t.Log("publisher shut down")

	// Step 5: a third node (others[1]) retrieves the file
	// from the anchor. The anchor is the only possible
	// provider; if the bytes match, the anchor fulfilled its
	// role.
	fetcher := others[1]
	anchorInfo := hostAddrInfo(t, anchor.host)
	ses, err := memex.NewSession(memex.SessionConfig{
		Engine:    fetcher.memex,
		Root:      parsedRoot,
		Providers: []peerInfo{anchorInfo},
		Timeout:   60 * time.Second,
	})
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	rc, err := ses.FetchWithBackoff(ctx, memex.DefaultRetryConfig())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read fetched: %v", err)
	}
	if len(got) != size {
		t.Fatalf("fetched length: got %d want %d", len(got), size)
	}
	gotSum := sha256.Sum256(got)
	if gotSum != sum {
		t.Fatalf("fetched bytes mismatch")
	}
}
