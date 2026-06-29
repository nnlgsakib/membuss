// Tests for small helpers in cmd/membuss that don't require
// the full libp2p stack.
package main

import (
	"os"
	"path/filepath"
	"testing"
)

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

func TestMigrateBadgerFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Write mock legacy badger files in root
	files := []string{
		"000001.sst",
		"000002.vlog",
		"MANIFEST",
		"KEYREGISTRY",
		"DISCARD",
		"LOCK",
	}

	for _, name := range files {
		err := os.WriteFile(filepath.Join(tempDir, name), []byte("testdata"), 0600)
		if err != nil {
			t.Fatalf("failed to write test file: %v", err)
		}
	}

	// Write a non-badger file that should NOT be migrated
	nonBadger := "config.yaml"
	err := os.WriteFile(filepath.Join(tempDir, nonBadger), []byte("config"), 0600)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	// Run migration
	err = migrateBadgerFiles(tempDir)
	if err != nil {
		t.Fatalf("migrateBadgerFiles returned error: %v", err)
	}

	// Check that legacy badger files were moved
	datastoreDir := filepath.Join(tempDir, "datastore")
	for _, name := range files {
		_, err := os.Stat(filepath.Join(datastoreDir, name))
		if err != nil {
			t.Errorf("file %s was not moved to datastore: %v", name, err)
		}
		_, err = os.Stat(filepath.Join(tempDir, name))
		if err == nil {
			t.Errorf("file %s still exists in root", name)
		}
	}

	// Check that non-badger file stayed in place
	_, err = os.Stat(filepath.Join(tempDir, nonBadger))
	if err != nil {
		t.Errorf("config.yaml was incorrectly moved or deleted: %v", err)
	}
	_, err = os.Stat(filepath.Join(datastoreDir, nonBadger))
	if err == nil {
		t.Errorf("config.yaml was incorrectly moved to datastore")
	}
}
