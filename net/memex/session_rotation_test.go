package memex

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/nnlgsakib/membuss/core/mid"
)

func TestSession_ProviderRotationAndRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Generate a block/content
	content := []byte("block-rotation-test-content-12345")
	
	// Create hosts
	hA := newTestHost(t) // Provider A (has the block)
	t.Cleanup(func() { _ = hA.Close() })
	
	hB := newTestHost(t) // Provider B (does not have the block)
	t.Cleanup(func() { _ = hB.Close() })

	hClient := newTestHost(t) // Client
	t.Cleanup(func() { _ = hClient.Close() })

	// Start engines
	engA, bsA := newTestEngine(t, hA)
	_ = engA
	_, _ = newTestEngine(t, hB)
	engClient, _ := newTestEngine(t, hClient)

	// Build DAG/block and put in Provider A only
	rootStr := buildDAG(t, content, bsA)
	root, err := mid.Parse(rootStr)
	if err != nil {
		t.Fatalf("parse root: %v", err)
	}

	// Connect Client to both Provider A and Provider B
	if err := hClient.Connect(ctx, peer.AddrInfo{ID: hA.ID(), Addrs: hA.Addrs()}); err != nil {
		t.Fatalf("client connect A: %v", err)
	}
	if err := hClient.Connect(ctx, peer.AddrInfo{ID: hB.ID(), Addrs: hB.Addrs()}); err != nil {
		t.Fatalf("client connect B: %v", err)
	}

	// Create session on Client
	sess, err := NewSession(SessionConfig{
		Engine:    engClient,
		Root:      root,
		Providers: []peer.AddrInfo{
			{ID: hB.ID(), Addrs: hB.Addrs()}, // Provider B is first in the list
			{ID: hA.ID(), Addrs: hA.Addrs()}, // Provider A is second
		},
		ParallelPeers: 2, // Active pool size
		Timeout:       5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	// Fetch should succeed by querying B, getting DONT_HAVE, rotating to A, and successfully fetching from A
	rc, err := sess.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch: got %q, want %q", string(got), string(content))
	}
}

func TestSession_MidSessionDiscoveryAndReplacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	content := []byte("block-discovery-replacement-test-content")

	// Provider C (has the block)
	hC := newTestHost(t)
	t.Cleanup(func() { _ = hC.Close() })

	// Provider B (does not have the block, will fail)
	hB := newTestHost(t)
	t.Cleanup(func() { _ = hB.Close() })

	// Client
	hClient := newTestHost(t)
	t.Cleanup(func() { _ = hClient.Close() })

	// Start engines
	engC, bsC := newTestEngine(t, hC)
	_ = engC
	_, _ = newTestEngine(t, hB)
	engClient, _ := newTestEngine(t, hClient)

	// Build DAG/block and put in Provider C only
	rootStr := buildDAG(t, content, bsC)
	root, err := mid.Parse(rootStr)
	if err != nil {
		t.Fatalf("parse root: %v", err)
	}

	// Connect Client to B and C
	if err := hClient.Connect(ctx, peer.AddrInfo{ID: hB.ID(), Addrs: hB.Addrs()}); err != nil {
		t.Fatalf("client connect B: %v", err)
	}
	if err := hClient.Connect(ctx, peer.AddrInfo{ID: hC.ID(), Addrs: hC.Addrs()}); err != nil {
		t.Fatalf("client connect C: %v", err)
	}

	// Define ProviderFinder that returns Provider C
	finderCalled := make(chan struct{}, 1)
	providerFinder := func(ctx context.Context, m mid.MID) ([]peer.AddrInfo, error) {
		select {
		case finderCalled <- struct{}{}:
		default:
		}
		return []peer.AddrInfo{{ID: hC.ID(), Addrs: hC.Addrs()}}, nil
	}

	// Create session on Client starting only with Provider B
	sess, err := NewSession(SessionConfig{
		Engine:         engClient,
		Root:           root,
		Providers:      []peer.AddrInfo{{ID: hB.ID(), Addrs: hB.Addrs()}},
		ParallelPeers:  1,
		Timeout:        5 * time.Second,
		ProviderFinder: providerFinder,
	})
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}

	// Fetch should call ProviderFinder after B returns DONT_HAVE or fails, add C, and fetch from C
	rc, err := sess.Fetch(ctx)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if !bytes.Equal(got, content) {
		t.Errorf("content mismatch: got %q, want %q", string(got), string(content))
	}

	select {
	case <-finderCalled:
		// success: finder was called to replace/find provider!
	default:
		t.Error("expected ProviderFinder to be called mid-session")
	}
}
