// Phase 16: identity persistence helpers used by
// `membuss-cli init` and (as a fallback) by NewHost.
//
// The host package has always been able to load or create the
// Ed25519 private key via loadOrCreateIdentity, but the
// function was package-private. Splitting it out into a small
// public surface here lets the init command run before any
// host is constructed: init must be able to write the key
// from a clean working directory, then read it back to derive
// the PeerID for the summary table, all without booting a
// libp2p stack.
package host

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/libp2p/go-libp2p/core/crypto"
)

// IdentityFileMode is the on-disk mode for the Ed25519 private
// key. 0600 means the owner can read/write; group and world are
// denied, so the key is not exfiltratable by another local user.
const IdentityFileMode os.FileMode = 0o600

// IdentityDirMode is the mode used when creating the data
// directory itself. 0700 is the conventional safe choice for
// a directory that may contain secrets (identity.key) or
// databases (datastore/).
const IdentityDirMode os.FileMode = 0o700

// GenerateIdentity creates a fresh Ed25519 private key using
// crypto/rand. The caller is expected to SaveIdentity it.
func GenerateIdentity() (crypto.PrivKey, error) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("host: generate identity: %w", err)
	}
	return priv, nil
}

// SaveIdentity writes priv to <dir>/identity.key with 0600
// permissions, creating <dir> with 0700 if it does not already
// exist. An existing file is replaced atomically (write to a
// temp path, then rename) so a partial write never leaves a
// zero-length key on disk.
func SaveIdentity(dir string, priv crypto.PrivKey) error {
	if dir == "" {
		return errors.New("host: empty identity dir")
	}
	if priv == nil {
		return errors.New("host: nil private key")
	}
	if err := os.MkdirAll(dir, IdentityDirMode); err != nil {
		return fmt.Errorf("host: mkdir %s: %w", dir, err)
	}
	raw, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return fmt.Errorf("host: marshal identity: %w", err)
	}
	target := filepath.Join(dir, IdentityFilename)
	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, raw, IdentityFileMode); err != nil {
		return fmt.Errorf("host: write identity tmp: %w", err)
	}
	if err := os.Rename(tmp, target); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("host: rename identity: %w", err)
	}
	_ = os.Chmod(target, IdentityFileMode)
	return nil
}

// LoadIdentity reads the Ed25519 private key from
// <dir>/identity.key. A missing file returns
// os.ErrNotExist; the caller is expected to detect that and
// fall back to GenerateIdentity + SaveIdentity.
func LoadIdentity(dir string) (crypto.PrivKey, error) {
	if dir == "" {
		return nil, errors.New("host: empty identity dir")
	}
	path := filepath.Join(dir, IdentityFilename)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	priv, err := crypto.UnmarshalPrivateKey(data)
	if err != nil {
		return nil, fmt.Errorf("host: unmarshal identity: %w", err)
	}
	return priv, nil
}

// IdentityExists reports whether a saved key is present.
func IdentityExists(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, IdentityFilename))
	return err == nil
}
