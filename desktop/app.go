package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/nnlgsakib/membuss/core/version"
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
	a.config.InstalledVersion = cfg.InstalledVersion
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
		versionTag, err := a.daemonManager.DownloadLatestRelease(targetDir, progressCallback)
		if err != nil {
			wailsRuntime.EventsEmit(a.ctx, "install_progress", map[string]any{
				"percent": -1, // Indicates error
				"message": err.Error(),
			})
			return
		}

		// Save the folder and set setup as complete
		a.config.DataDir = targetDir
		a.config.InstalledVersion = versionTag
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

	// Normalize Windows backslashes to forward slashes in path fields to prevent escape sequence parse errors in YAML
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "geolocation_db:") ||
			strings.HasPrefix(trimmed, "data_dir:") ||
			strings.HasPrefix(trimmed, "cert_file:") ||
			strings.HasPrefix(trimmed, "key_file:") {
			lines[i] = strings.ReplaceAll(line, "\\", "/")
		}
	}
	content = strings.Join(lines, "\n")

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

// UpdateCheckResult holds the version check status.
type UpdateCheckResult struct {
	HasUpdate      bool   `json:"has_update"`
	CurrentVersion string `json:"current_version"`
	LatestVersion  string `json:"latest_version"`
}

// CheckForUpdate queries GitHub for the latest release and compares with the installed version.
func (a *App) CheckForUpdate() (*UpdateCheckResult, error) {
	// 1. Determine current version
	currentVer := a.config.InstalledVersion
	if currentVer == "" {
		exeExt := ""
		if runtime.GOOS == "windows" {
			exeExt = ".exe"
		}
		cliPath := filepath.Join(a.config.DataDir, "bin", "membuss-cli"+exeExt)
		currentVer = getInstalledBinaryVersion(cliPath)
		if currentVer == "" {
			currentVer = version.Version
		}
	}
	currentVer = strings.TrimPrefix(currentVer, "v")

	// 2. Fetch latest release info from GitHub API
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("GET", "https://api.github.com/repos/nnlgsakib/membuss/releases/latest", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Membuss-Desktop-App")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch latest release: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("github api returned status %s", resp.Status)
	}

	var release map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return nil, fmt.Errorf("failed to decode release JSON: %w", err)
	}

	latestVer, _ := release["tag_name"].(string)
	latestVerClean := strings.TrimPrefix(latestVer, "v")

	hasUpdate := isVersionNewer(currentVer, latestVerClean)

	return &UpdateCheckResult{
		HasUpdate:      hasUpdate,
		CurrentVersion: "v" + currentVer,
		LatestVersion:  latestVer,
	}, nil
}

// getInstalledBinaryVersion runs the installed CLI binary and parses its version string.
func getInstalledBinaryVersion(cliPath string) string {
	if _, err := os.Stat(cliPath); err != nil {
		return ""
	}
	cmd := exec.Command(cliPath, "version")
	hideConsoleWindow(cmd)
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "membuss version") {
			parts := strings.Fields(line)
			for i, part := range parts {
				if part == "version" && i+1 < len(parts) {
					return parts[i+1]
				}
			}
		}
	}
	return ""
}

// UpgradeBinaries stops the node, removes the old binaries, downloads the latest release, and updates config.
func (a *App) UpgradeBinaries() error {
	if a.config.DataDir == "" {
		return errors.New("no data directory configured")
	}

	// 1. Stop the node and force-kill system-wide to ensure files are not locked
	wailsRuntime.LogInfo(a.ctx, "stopping node and force killing processes before upgrading...")
	_ = a.daemonManager.Stop()
	_ = killProcess("membuss*")
	_ = killProcess("membuss-cli*")
	time.Sleep(1 * time.Second)

	// 2. Remove old binaries
	exeExt := ""
	if runtime.GOOS == "windows" {
		exeExt = ".exe"
	}
	daemonPath := filepath.Join(a.config.DataDir, "bin", "membuss"+exeExt)
	cliPath := filepath.Join(a.config.DataDir, "bin", "membuss-cli"+exeExt)

	wailsRuntime.LogInfo(a.ctx, "removing old binaries...")
	if err := os.Remove(daemonPath); err != nil && !os.IsNotExist(err) {
		oldPath := daemonPath + ".old"
		_ = os.Remove(oldPath) // remove previous old file if any
		if renameErr := os.Rename(daemonPath, oldPath); renameErr != nil {
			return fmt.Errorf("failed to remove or rename old daemon binary: %w", err)
		}
	}
	if err := os.Remove(cliPath); err != nil && !os.IsNotExist(err) {
		oldPath := cliPath + ".old"
		_ = os.Remove(oldPath) // remove previous old file if any
		if renameErr := os.Rename(cliPath, oldPath); renameErr != nil {
			return fmt.Errorf("failed to remove or rename old CLI binary: %w", err)
		}
	}

	// 3. Download and extract the latest release in a background goroutine
	progressCallback := func(percent int, msg string) {
		wailsRuntime.EventsEmit(a.ctx, "upgrade_progress", map[string]any{
			"percent": percent,
			"message": msg,
		})
	}

	go func() {
		versionTag, err := a.daemonManager.DownloadLatestRelease(a.config.DataDir, progressCallback)
		if err != nil {
			wailsRuntime.EventsEmit(a.ctx, "upgrade_progress", map[string]any{
				"percent": -1,
				"message": err.Error(),
			})
			return
		}

		// Update config with installed version
		a.config.InstalledVersion = versionTag
		_ = a.config.Save()

		// Send success event
		wailsRuntime.EventsEmit(a.ctx, "upgrade_progress", map[string]any{
			"percent": 100,
			"message": "Upgrade complete!",
		})
	}()

	return nil
}

// IsNodeRunningSystemWide checks if any membuss daemon process is running on the system.
func (a *App) IsNodeRunningSystemWide() bool {
	return isProcessRunning("membuss")
}

// ForceKillNode attempts to force-kill any running membuss daemon processes on the system.
func (a *App) ForceKillNode() error {
	wailsRuntime.LogInfo(a.ctx, "force killing node daemon processes...")
	_ = a.daemonManager.Stop()
	_ = killProcess("membuss*")
	_ = killProcess("membuss-cli*")
	time.Sleep(500 * time.Millisecond)
	return nil
}

// isVersionNewer helper function to compare two semantic versions.
func isVersionNewer(current, latest string) bool {
	current = strings.TrimPrefix(strings.TrimSpace(current), "v")
	latest = strings.TrimPrefix(strings.TrimSpace(latest), "v")
	if current == "" {
		return latest != ""
	}
	if latest == "" {
		return false
	}
	cParts := strings.Split(current, ".")
	lParts := strings.Split(latest, ".")
	for i := 0; i < len(cParts) && i < len(lParts); i++ {
		var cVal, lVal int
		fmt.Sscanf(cParts[i], "%d", &cVal)
		fmt.Sscanf(lParts[i], "%d", &lVal)
		if lVal > cVal {
			return true
		}
		if lVal < cVal {
			return false
		}
	}
	return len(lParts) > len(cParts)
}

// isProcessRunning checks if a process with the given name is active on the host system.
func isProcessRunning(name string) bool {
	if runtime.GOOS == "windows" {
		if !strings.HasSuffix(name, ".exe") {
			name += ".exe"
		}
		cmd := exec.Command("tasklist", "/FI", fmt.Sprintf("IMAGENAME eq %s", name), "/NH")
		hideConsoleWindow(cmd)
		out, err := cmd.Output()
		if err != nil {
			return false
		}
		return strings.Contains(strings.ToLower(string(out)), strings.ToLower(name))
	} else {
		cmd := exec.Command("pgrep", "-x", name)
		hideConsoleWindow(cmd)
		err := cmd.Run()
		return err == nil
	}
}

// killProcess kills all processes matching the given name.
func killProcess(name string) error {
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		if !strings.HasSuffix(name, ".exe") && !strings.Contains(name, "*") {
			name += ".exe"
		}
		cmd = exec.Command("taskkill", "/F", "/IM", name)
	} else {
		cmd = exec.Command("pkill", "-9", "-f", name)
	}
	hideConsoleWindow(cmd)
	return cmd.Run()
}

