// Phase 16: data-directory resolution and the YAML-with-comments
// serializer used by `membuss-cli init`.
//
// The data directory is the single place a node keeps its
// long-lived state: the YAML config, the libp2p Ed25519
// identity, the persisted bloom-filter snapshot, the BadgerDB
// datastore, and (optionally) a log file. Its location is
// resolved by ResolveDataDir at every entry point so the
// --datadir flag, the MEMBUSS_DATADIR environment variable, and
// the per-OS default ($HOME/.memdata on Linux/macOS,
// %USERPROFILE%\.memdata on Windows) all behave consistently.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// DataDirEnv is the environment variable the operator can set
// to override the default data-directory location.
const DataDirEnv = "MEMBUSS_DATADIR"

// DefaultDataDirName is the conventional per-OS subdirectory
// Membuss keeps its state in. The full default path is
// <UserHomeDir>/<DefaultDataDirName>.
const DefaultDataDirName = ".memdata"

// ConfigFileName is the YAML config filename written by
// `membuss-cli init` and read by LoadConfig.
const ConfigFileName = "config.yaml"

// IdentityFileName is the Ed25519 private-key filename. The
// constant lives in net/host; it is repeated here so config
// callers do not have to import net/host to locate the file.
const IdentityFileName = "identity.key"

// DefaultConfigPath is the absolute path of the per-data-dir
// config.yaml. It is a convenience for the common case.
func DefaultConfigPath(datadir string) string {
	return filepath.Join(datadir, ConfigFileName)
}

// ResolveDataDir returns the data directory the daemon / CLI
// should use. The resolution order is:
//
//  1. flagValue - the value of the --datadir flag, if non-empty.
//  2. MEMBUSS_DATADIR environment variable, if non-empty.
//  3. <UserHomeDir>/.memdata.
//
// The returned path is cleaned but is not resolved against the
// current working directory; the caller is expected to use it as
// an absolute path or a path relative to the cwd as appropriate.
// If UserHomeDir fails (e.g. the HOME / USERPROFILE env var is
// unset in a stripped-down container), an empty string is
// returned and the caller is expected to detect this and
// surface a clear error.
func ResolveDataDir(flagValue string) string {
	if flagValue != "" {
		return filepath.Clean(flagValue)
	}
	if v := os.Getenv(DataDirEnv); v != "" {
		return filepath.Clean(v)
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, DefaultDataDirName)
}

// IsInitialized reports whether datadir has been initialised by
// `membuss-cli init` (i.e. contains a config.yaml). It does NOT
// validate the contents of that file; LoadConfig is responsible
// for parse errors.
func IsInitialized(datadir string) bool {
	if datadir == "" {
		return false
	}
	_, err := os.Stat(DefaultConfigPath(datadir))
	return err == nil
}

// LoadConfig reads the config.yaml in datadir, applies the
// defaults from DefaultConfig, and returns the result. If the
// file is missing it returns an error telling the operator to
// run `membuss-cli init` so the failure mode is self-explanatory
// instead of a bare "open ... : no such file or directory".
func LoadConfig(datadir string) (*Config, error) {
	if datadir == "" {
		return nil, errors.New("config: empty data directory")
	}
	path := DefaultConfigPath(datadir)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf(
				"config: node not initialized at %s: "+
					"run `membuss-cli init --datadir %s` first",
				datadir, datadir)
		}
		return nil, fmt.Errorf("config: read %q: %w", path, err)
	}
	cfg := DefaultConfig(datadir)
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("config: parse %q: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: invalid %q: %w", path, err)
	}
	return cfg, nil
}

// DefaultConfig returns a Config populated with the safe defaults
// from Default(), but with DataDir set to the given path (which
// is also validated by Validate). This is what `membuss-cli
// init` writes to disk.
func DefaultConfig(datadir string) *Config {
	cfg := Default()
	cfg.DataDir = datadir
	return cfg
}
