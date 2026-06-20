package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"

	wailsRuntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

// App struct
type App struct {
	ctx           context.Context
	daemonManager *DaemonManager
	config        *DesktopConfig
}

// NewApp creates a new App application struct
func NewApp() *App {
	return &App{}
}

// startup is called when the app starts. The context is saved
// so we can call the runtime methods
func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	cfg, err := LoadConfig()
	if err != nil {
		wailsRuntime.LogErrorf(ctx, "failed to load config: %v", err)
		cfg = &DesktopConfig{}
	}
	a.config = cfg
	a.daemonManager = NewDaemonManager(cfg)

	// If setup is complete and auto_start is true, start the daemon
	if cfg.SetupComplete && cfg.AutoStart {
		wailsRuntime.LogInfo(ctx, "automatically starting daemon process...")
		go func() {
			err := a.daemonManager.Start(cfg.DataDir)
			if err != nil {
				wailsRuntime.LogErrorf(ctx, "failed to auto-start daemon: %v", err)
			}
		}()
	}
}

// GetConfig returns the current desktop config settings.
func (a *App) GetConfig() *DesktopConfig {
	return a.config
}

// SaveConfig updates and saves the desktop config.
func (a *App) SaveConfig(cfg *DesktopConfig) error {
	a.config.DataDir = cfg.DataDir
	a.config.SetupComplete = cfg.SetupComplete
	a.config.GRPCAddr = cfg.GRPCAddr
	a.config.APIAddr = cfg.APIAddr
	a.config.GatewayAddr = cfg.GatewayAddr
	a.config.KeepAlive = cfg.KeepAlive
	a.config.AutoStart = cfg.AutoStart
	return a.config.Save()
}

// SelectDirectory opens the OS-native directory picker.
func (a *App) SelectDirectory() (string, error) {
	dir, err := wailsRuntime.OpenDirectoryDialog(a.ctx, wailsRuntime.OpenDialogOptions{
		Title: "Choose Membuss Data Directory",
	})
	if err != nil {
		return "", err
	}
	if dir == "" {
		return "", errors.New("directory selection cancelled")
	}
	return dir, nil
}

// InstallBinaries downloads and extracts the daemon binaries, emitting progress events.
func (a *App) InstallBinaries(targetDir string) error {
	if targetDir == "" {
		return errors.New("target directory cannot be empty")
	}

	progressCallback := func(percent int, msg string) {
		wailsRuntime.EventsEmit(a.ctx, "install_progress", map[string]any{
			"percent": percent,
			"message": msg,
		})
	}

	// Run installation in background
	go func() {
		err := a.daemonManager.DownloadLatestRelease(targetDir, progressCallback)
		if err != nil {
			wailsRuntime.EventsEmit(a.ctx, "install_progress", map[string]any{
				"percent": -1, // Indicates error
				"message": err.Error(),
			})
			return
		}

		// Save the folder and set setup as complete
		a.config.DataDir = targetDir
		a.config.SetupComplete = true
		_ = a.config.Save()
	}()

	return nil
}

// GetNodeConfig returns the config.yaml values in a key-value format.
func (a *App) GetNodeConfig() (map[string]any, error) {
	if a.config.DataDir == "" {
		return nil, errors.New("no data directory configured")
	}
	return LoadYamlConfig(a.config.DataDir)
}

// SaveNodeConfig serializes config.yaml updates.
func (a *App) SaveNodeConfig(cfg map[string]any) error {
	if a.config.DataDir == "" {
		return errors.New("no data directory configured")
	}
	return SaveYamlConfig(a.config.DataDir, cfg)
}

// GetNodeConfigRaw returns the raw YAML content of config.yaml.
func (a *App) GetNodeConfigRaw() (string, error) {
	if a.config.DataDir == "" {
		return "", errors.New("no data directory configured")
	}
	path := filepath.Join(a.config.DataDir, "config.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return string(data), nil
}

// SaveNodeConfigRaw writes raw YAML content to config.yaml.
func (a *App) SaveNodeConfigRaw(content string) error {
	if a.config.DataDir == "" {
		return errors.New("no data directory configured")
	}
	path := filepath.Join(a.config.DataDir, "config.yaml")
	return os.WriteFile(path, []byte(content), 0644)
}

// WriteDefaultConfig generates a complete config.yaml with all default fields.
func (a *App) WriteDefaultConfig() error {
	if a.config.DataDir == "" {
		return errors.New("no data directory configured")
	}
	return WriteDefaultConfig(a.config.DataDir)
}

// StartNode launches the daemon.
func (a *App) StartNode() error {
	if a.config.DataDir == "" {
		return errors.New("no data directory configured")
	}
	return a.daemonManager.Start(a.config.DataDir)
}

// StopNode terminates the daemon.
func (a *App) StopNode() error {
	return a.daemonManager.Stop()
}

// CheckNodeStatus checks if the daemon is online and queries Node Info.
func (a *App) CheckNodeStatus() (map[string]any, error) {
	isRunning := a.daemonManager.IsRunning()
	
	// Probe the HTTP API
	info, err := a.daemonManager.CheckStatus(a.config.APIAddr)
	statusMap := map[string]any{
		"process_running": isRunning,
		"api_online":      err == nil,
	}

	if err == nil {
		statusMap["info"] = info
	} else {
		statusMap["error"] = err.Error()
	}

	return statusMap, nil
}

// CheckExplorer checks if the gateway's explorer is online.
func (a *App) CheckExplorer() bool {
	return a.daemonManager.CheckExplorer(a.config.GatewayAddr)
}

// VerifyInstallation checks if the installed binaries and configurations are intact.
func (a *App) VerifyInstallation() map[string]any {
	cfg := a.config
	result := map[string]any{
		"valid":          true,
		"data_dir_ok":    true,
		"daemon_bin_ok":  true,
		"cli_bin_ok":     true,
		"node_config_ok": true,
	}

	if !cfg.SetupComplete {
		result["valid"] = false
		return result
	}

	// 1. Check DataDir exists
	if fi, err := os.Stat(cfg.DataDir); err != nil || !fi.IsDir() {
		result["valid"] = false
		result["data_dir_ok"] = false
	}

	// 2. Check binaries
	exeExt := ""
	if runtime.GOOS == "windows" {
		exeExt = ".exe"
	}
	daemonPath := filepath.Join(cfg.DataDir, "bin", "membuss"+exeExt)
	cliPath := filepath.Join(cfg.DataDir, "bin", "membuss-cli"+exeExt)

	if _, err := os.Stat(daemonPath); err != nil {
		result["valid"] = false
		result["daemon_bin_ok"] = false
	}
	if _, err := os.Stat(cliPath); err != nil {
		result["valid"] = false
		result["cli_bin_ok"] = false
	}

	// 3. Check config.yaml
	configPath := filepath.Join(cfg.DataDir, "config.yaml")
	if _, err := os.Stat(configPath); err != nil {
		result["valid"] = false
		result["node_config_ok"] = false
	}

	return result
}

// ResetSetup clears the config and stops the node.
func (a *App) ResetSetup() error {
	// 1. Stop node if running
	_ = a.daemonManager.Stop()

	// 2. Reset config fields
	a.config.SetupComplete = false
	a.config.DataDir = ""

	// 3. Save reset state
	return a.config.Save()
}

// GetDaemonLogs reads the last few lines from the daemon log file.
func (a *App) GetDaemonLogs() (string, error) {
	if a.config.DataDir == "" {
		return "", errors.New("no data directory configured")
	}
	logPath := filepath.Join(a.config.DataDir, "logs", "daemon.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "No daemon logs found yet.", nil
		}
		return "", err
	}

	// Limit to last 64KB of log data to keep frontend performance optimal
	const maxBytes = 64 * 1024
	if len(data) > maxBytes {
		return string(data[len(data)-maxBytes:]), nil
	}
	return string(data), nil
}

// domReady is called when the renderer has loaded.
func (a *App) domReady(ctx context.Context) {
	// Custom hooks if needed on DOM load
}

// beforeClose is triggered when the window is closed.
// If the daemon is running, emit an event to the frontend to confirm close.
func (a *App) beforeClose(ctx context.Context) bool {
	if a.config.KeepAlive && a.config.SetupComplete && a.daemonManager.IsRunning() {
		// Emit event to frontend to handle close confirmation
		wailsRuntime.EventsEmit(ctx, "request-close")
		return true // Block close, let frontend handle it
	}

	// If not keeping alive or node not running, stop daemon and allow close
	_ = a.daemonManager.Stop()
	return false // Allow app termination
}
// Trigger rebuild
