// Tests for small helpers in cmd/membuss that don't require
// the full libp2p stack.
package main

import "testing"

func TestParsePeer_FullMultiaddr(t *testing.T) {
	ai, err := parsePeer("/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X")
	if err != nil {
		t.Fatalf("parsePeer: %v", err)
	}
	if ai.ID.String() != "12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X" {
		t.Errorf("peer id: %s", ai.ID)
	}
	if len(ai.Addrs) == 0 {
		t.Errorf("expected at least one addr")
	}
}

func TestParsePeer_MultiaddrWithoutP2P(t *testing.T) {
	ai, err := parsePeer("/ip4/1.2.3.4/tcp/4001")
	if err != nil {
		t.Fatalf("parsePeer: %v", err)
	}
	if len(ai.Addrs) != 1 {
		t.Errorf("expected 1 addr, got %d", len(ai.Addrs))
	}
}

func TestParsePeer_PlainPeerID(t *testing.T) {
	ai, err := parsePeer("12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X")
	if err != nil {
		t.Fatalf("parsePeer: %v", err)
	}
	if ai.ID.String() != "12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X" {
		t.Errorf("peer id: %s", ai.ID)
	}
}

func TestParsePeer_RejectsGarbage(t *testing.T) {
	if _, err := parsePeer("not-a-valid-thing"); err == nil {
		t.Fatal("expected error for garbage input")
	}
}

func TestParsePeer_RejectsEmpty(t *testing.T) {
	if _, err := parsePeer(""); err == nil {
		t.Fatal("expected error for empty input")
	}
}

func TestParsePeers_Empty(t *testing.T) {
	out, err := parsePeers(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty, got %d", len(out))
	}
}

func TestParsePeers_Mixed(t *testing.T) {
	out, err := parsePeers([]string{
		"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X",
		"12D3KooWA51FHYMd93Wpna8ypLo4D7YUgfipJVsiyfgUi4zAjaAQ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2, got %d", len(out))
	}
	if out[0].ID.String() != "12D3KooWPjceQrSwdWXPyLLeABRXmuqt69Rg3sBYbU1Nft9HyQ6X" {
		t.Errorf("peer 0 id: %s", out[0].ID)
	}
}
