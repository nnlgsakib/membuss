package host

import (
	"os"
	"path/filepath"
	"testing"
)

// TestNewHost_PersistentIdentity verifies that the on-disk
// Ed25519 key is reused across calls so the PeerID is stable
// for a given data dir.
func TestNewHost_PersistentIdentity(t *testing.T) {
	dir := t.TempDir()

	h1, err := NewHost(Config{DataDir: dir})
	if err != nil {
		t.Fatalf("first host: %v", err)
	}
	pid1 := h1.ID()
	_ = h1.Close()

	h2, err := NewHost(Config{DataDir: dir})
	if err != nil {
		t.Fatalf("second host: %v", err)
	}
	defer h2.Close()
	if h2.ID() != pid1 {
		t.Fatalf("identity changed across restarts: %s != %s", h2.ID(), pid1)
	}

	// The identity file should exist. POSIX perms are 0600
	// when the host filesystem honours them; on Windows the
	// umask is ignored, so we only assert existence.
	info, err := os.Stat(filepath.Join(dir, IdentityFilename))
	if err != nil {
		t.Fatalf("stat identity: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("identity file is empty")
	}
}

// TestNewHost_InProcess is a minimal smoke test of the
// in-process host factory used by tests in other packages.
func TestNewHost_InProcess(t *testing.T) {
	h, err := NewHost(Config{InProcess: true})
	if err != nil {
		t.Fatalf("in-process: %v", err)
	}
	defer h.Close()
	if h.ID().String() == "" {
		t.Fatal("empty peer id")
	}
}