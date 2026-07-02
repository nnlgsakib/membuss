package dht

import (
	"context"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestMemDHT_Bootstrap_UnreachablePeers(t *testing.T) {
	h := newTestHost(t)
	t.Cleanup(func() { h.Close() })

	mdht, err := New(context.Background(), Config{Host: h})
	if err != nil {
		t.Fatalf("new dht: %v", err)
	}
	t.Cleanup(func() { _ = mdht.Close() })

	badPeer := peer.AddrInfo{ID: peer.ID("QmInvalidAddress123")}

	err = mdht.Bootstrap(context.Background(), []peer.AddrInfo{badPeer})
	if err == nil {
		t.Fatal("expected Bootstrap to return error for unreachable peer list, got nil")
	}
	if !strings.Contains(err.Error(), "all bootstrap peers unreachable") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestMemDHT_Bootstrap_NoPeers(t *testing.T) {
	h := newTestHost(t)
	t.Cleanup(func() { h.Close() })

	mdht, err := New(context.Background(), Config{Host: h})
	if err != nil {
		t.Fatalf("new dht: %v", err)
	}
	t.Cleanup(func() { _ = mdht.Close() })

	err = mdht.Bootstrap(context.Background(), nil)
	if err != nil {
		t.Errorf("expected Bootstrap with no peers to succeed, got: %v", err)
	}
}
