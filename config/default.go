package config

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// WriteDefault emits a starter YAML config to path. The file is created
// (with 0o600 permissions) along with any missing parent directories.
// Existing files are NOT overwritten; an error is returned instead so
// callers can decide whether to merge, diff, or refuse.
func WriteDefault(path string) error {
	if _, err := os.Stat(path); err == nil {
		return os.ErrExist
	} else if !os.IsNotExist(err) {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	data, err := yaml.Marshal(Default())
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
