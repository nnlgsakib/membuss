package config

import (
	"os"
	"path/filepath"
	"testing"
)

// withEnv sets env to val for the duration of the test, and
// restores the previous value (or unsets it) on cleanup.
func withEnv(t *testing.T, key, val string) {
	t.Helper()
	prev, had := os.LookupEnv(key)
	if err := os.Setenv(key, val); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, prev)
		} else {
			_ = os.Unsetenv(key)
		}
	})
}

func TestResolveDataDir_Priority(t *testing.T) {
	// Unset the env var so the default is the fallback.
	withEnv(t, DataDirEnv, "")

	// 1) Flag wins.
	flagPath := filepath.Join(t.TempDir(), "from-flag")
	if got := ResolveDataDir(flagPath); got != filepath.Clean(flagPath) {
		t.Fatalf("flag path: got %q want %q", got, filepath.Clean(flagPath))
	}

	// 2) Env wins when no flag.
	withEnv(t, DataDirEnv, filepath.Join(t.TempDir(), "from-env"))
	envPath := os.Getenv(DataDirEnv)
	if got := ResolveDataDir(""); got != filepath.Clean(envPath) {
		t.Fatalf("env path: got %q want %q", got, filepath.Clean(envPath))
	}

	// 3) Default when neither flag nor env is set.
	withEnv(t, DataDirEnv, "")
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		t.Skip("no UserHomeDir in this environment")
	}
	wantDefault := filepath.Join(home, DefaultDataDirName)
	if got := ResolveDataDir(""); got != wantDefault {
		t.Fatalf("default: got %q want %q", got, wantDefault)
	}
}

func TestIsInitialized(t *testing.T) {
	dir := t.TempDir()
	if IsInitialized(dir) {
		t.Fatal("empty dir should not report as initialized")
	}
	if err := os.WriteFile(DefaultConfigPath(dir), []byte("data_dir: "+dir+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if !IsInitialized(dir) {
		t.Fatal("dir with config.yaml should report as initialized")
	}
}

func TestLoadConfig_MissingFile(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadConfig(dir)
	if err == nil {
		t.Fatal("LoadConfig on empty dir must return an error")
	}
	// The error must mention the init hint so the operator can
	// self-help instead of digging through log files.
	if !contains(err.Error(), "membuss-cli init") {
		t.Fatalf("error must mention init hint, got: %v", err)
	}
}

func TestLoadConfig_ReadsConfig(t *testing.T) {
	dir := t.TempDir()
	body := "data_dir: " + dir + "\nlog_level: debug\n"
	if err := os.WriteFile(DefaultConfigPath(dir), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(dir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DataDir != dir {
		t.Fatalf("DataDir: got %q want %q", cfg.DataDir, dir)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("LogLevel: got %q want %q", cfg.LogLevel, "debug")
	}
}

func TestDefaultConfig_OverrideDataDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "custom")
	cfg := DefaultConfig(dir)
	if cfg.DataDir != dir {
		t.Fatalf("DataDir: got %q want %q", cfg.DataDir, dir)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default-with-datadir must validate: %v", err)
	}
}

func TestWriteConfig_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)

	path := DefaultConfigPath(dir)
	if err := WriteConfig(cfg, path); err != nil {
		t.Fatal(err)
	}

	// File exists with restricted mode.
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() == 0 {
		t.Fatal("config file is empty")
	}

	// Round-trip: LoadConfig should accept what WriteConfig wrote.
	cfg2, err := LoadConfig(dir)
	if err != nil {
		t.Fatalf("round-trip load failed: %v", err)
	}
	if cfg2.DataDir != dir {
		t.Fatalf("round-trip DataDir: got %q want %q", cfg2.DataDir, dir)
	}
}

// contains is a tiny dependency-free substring check.
func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
