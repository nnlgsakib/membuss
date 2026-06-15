package host

import (
	"os"
	"github.com/libp2p/go-libp2p/core/crypto"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestGenerateIdentity(t *testing.T) {
	priv, err := GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	if priv == nil {
		t.Fatal("nil key")
	}
	pid, err := PeerIDFromKey(priv)
	if err != nil {
		t.Fatalf("PeerIDFromKey: %v", err)
	}
	// libp2p PeerIDs for Ed25519 keys start with "12D3Koo".
	if !strings.HasPrefix(pid.String(), "12D3Koo") {
		t.Fatalf("unexpected PeerID prefix: %s", pid)
	}
}

func TestSaveLoadIdentity(t *testing.T) {
	dir := t.TempDir()
	priv, err := GenerateIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if err := SaveIdentity(dir, priv); err != nil {
		t.Fatalf("SaveIdentity: %v", err)
	}
	// On POSIX the 0600 mode is preserved; on Windows the bit
	// is ignored, so we skip the mode assertion there.
	if runtime.GOOS != "windows" {
		st, err := os.Stat(filepath.Join(dir, IdentityFilename))
		if err != nil {
			t.Fatal(err)
		}
		if got := st.Mode().Perm(); got != IdentityFileMode {
			t.Fatalf("identity.key mode: got %o want %o", got, IdentityFileMode)
		}
	}
	// Load round-trip.
	loaded, err := LoadIdentity(dir)
	if err != nil {
		t.Fatalf("LoadIdentity: %v", err)
	}
	origBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	loadBytes, err := crypto.MarshalPrivateKey(loaded)
	if err != nil {
		t.Fatal(err)
	}
	if string(origBytes) != string(loadBytes) {
		t.Fatal("loaded key differs from generated key")
	}
}

func TestSaveIdentity_Atomic(t *testing.T) {
	dir := t.TempDir()
	priv, _ := GenerateIdentity()
	if err := SaveIdentity(dir, priv); err != nil {
		t.Fatal(err)
	}
	// No .tmp file should linger.
	if _, err := os.Stat(filepath.Join(dir, IdentityFilename+".tmp")); err == nil {
		t.Fatal("temp file should not linger")
	}
}

func TestLoadIdentity_Missing(t *testing.T) {
	_, err := LoadIdentity(t.TempDir())
	if err == nil {
		t.Fatal("expected error on missing identity.key")
	}
	if !os.IsNotExist(err) {
		t.Fatalf("expected os.IsNotExist, got %v", err)
	}
}

func TestIdentityExists(t *testing.T) {
	dir := t.TempDir()
	if IdentityExists(dir) {
		t.Fatal("IdentityExists on empty dir should be false")
	}
	priv, _ := GenerateIdentity()
	_ = SaveIdentity(dir, priv)
	if !IdentityExists(dir) {
		t.Fatal("IdentityExists after save should be true")
	}
}
