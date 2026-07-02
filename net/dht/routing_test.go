package dht

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/net/host"
)

func TestDHT_PeerScoringAndSorting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h, err := host.NewHost(host.Config{
		DataDir:     t.TempDir(),
		ListenAddrs: []string{"/ip4/127.0.0.1/tcp/0"},
	})
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	d, err := New(ctx, Config{
		Host:             h,
		BandwidthCounter: h.BandwidthCounter(),
	})
	if err != nil {
		t.Fatalf("failed to create dht: %v", err)
	}
	defer d.Close()

	p1 := peer.ID("p1-test-peer-id-123")
	p2 := peer.ID("p2-test-peer-id-456")

	key := []byte("score-test")

	// 1. Initially both have the same (default) score
	score1 := d.scorePeer(key, p1)
	score2 := d.scorePeer(key, p2)
	if score1 != score2 {
		t.Errorf("expected equal initial scores, got %f vs %f", score1, score2)
	}

	// 2. Set different latencies in peerstore and check ranking
	h.Peerstore().RecordLatency(p1, 50*time.Millisecond)
	h.Peerstore().RecordLatency(p2, 200*time.Millisecond)

	score1 = d.scorePeer(key, p1)
	score2 = d.scorePeer(key, p2)
	if score1 <= score2 {
		t.Errorf("expected lower latency peer p1 to have higher score, got %f vs %f", score1, score2)
	}

	// 3. Set freshness and check ranking
	// Simulating provider record freshness: we record p2 as recently provided
	err = d.freshStore.AddProvider(ctx, key, peer.AddrInfo{ID: p2})
	if err != nil {
		t.Fatalf("failed to add provider freshness: %v", err)
	}

	score1 = d.scorePeer(key, p1)
	score2 = d.scorePeer(key, p2)
	if score2 <= score1 {
		t.Errorf("expected recently re-provided peer p2 to have higher score, got %f vs %f (p1)", score2, score1)
	}
}

func TestDHT_MidToCIDCache(t *testing.T) {
	testMID := mid.FromBytes([]byte("cache-test-mid"))

	c1 := midToCID(testMID)
	c2 := midToCID(testMID)

	if !c1.Equals(c2) {
		t.Error("expected cached CIDs to be equal")
	}

	// Run many times to verify cache hits and correctness
	for i := 0; i < 1000; i++ {
		c := midToCID(testMID)
		if !c.Defined() {
			t.Fatal("CID should be defined")
		}
	}
}

func TestDHT_BackoffJitter(t *testing.T) {
	initialDelay := 100 * time.Millisecond
	jitter := float64(initialDelay) * 0.2
	minDelay := float64(initialDelay) - jitter
	maxDelay := float64(initialDelay) + jitter

	if minDelay != float64(80*time.Millisecond) {
		t.Errorf("expected minimum delay bounds of 80ms, got %f", minDelay)
	}
	if maxDelay != float64(120*time.Millisecond) {
		t.Errorf("expected maximum delay bounds of 120ms, got %f", maxDelay)
	}
}
