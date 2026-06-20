package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/nnlgsakib/membuss/config"
	"github.com/nnlgsakib/membuss/core/version"
	"gopkg.in/yaml.v3"
)

// DesktopConfig stores the GUI application configurations.
type DesktopConfig struct {
	DataDir          string `json:"data_dir"`
	SetupComplete    bool   `json:"setup_complete"`
	GRPCAddr         string `json:"grpc_addr"`
	APIAddr          string `json:"api_addr"`
	GatewayAddr      string `json:"gateway_addr"`
	KeepAlive        bool   `json:"keep_alive"` // Keep daemon running when GUI closes
	AutoStart        bool   `json:"auto_start"` // Start daemon on GUI start
	InstalledVersion string `json:"installed_version"`
}

// GetConfigPath returns the persistent configuration path:
// Windows: %APPDATA%/Membuss/desktop-config.json
// macOS/Linux: ~/.config/membuss/desktop-config.json
func GetConfigPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	appDir := filepath.Join(dir, "Membuss")
	if err := os.MkdirAll(appDir, 0755); err != nil {
		return "", err
	}
	return filepath.Join(appDir, "desktop-config.json"), nil
}

// LoadConfig loads the GUI settings.
func LoadConfig() (*DesktopConfig, error) {
	path, err := GetConfigPath()
	if err != nil {
		return nil, err
	}

	cfg := &DesktopConfig{
		GRPCAddr:    "127.0.0.1:50051",
		APIAddr:     "127.0.0.1:5001",
		GatewayAddr: "127.0.0.1:8080",
		KeepAlive:   true,
		AutoStart:   true,
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil // Return defaults
		}
		return nil, err
	}

	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// Save saves the GUI settings.
func (c *DesktopConfig) Save() error {
	path, err := GetConfigPath()
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// DaemonManager manages the background daemon process.
type DaemonManager struct {
	mu     sync.Mutex
	cmd    *exec.Cmd
	config *DesktopConfig
	ctx    context.Context
}

func NewDaemonManager(cfg *DesktopConfig) *DaemonManager {
	return &DaemonManager{
		config: cfg,
	}
}

// IsRunning checks if the daemon command is active locally.
func (dm *DaemonManager) IsRunning() bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return dm.cmd != nil && dm.cmd.Process != nil && dm.cmd.ProcessState == nil
}

// Start spawns the daemon in the background.
func (dm *DaemonManager) Start(dataDir string) error {
	if dm.IsRunning() {
		return errors.New("daemon is already running")
	}

	dm.mu.Lock()
	defer dm.mu.Unlock()

	exeName := "membuss"
	if runtime.GOOS == "windows" {
		exeName = "membuss.exe"
	}
	binaryPath := filepath.Join(dataDir, "bin", exeName)

	if _, err := os.Stat(binaryPath); err != nil {
		return fmt.Errorf("daemon binary not found at %s. Please run setup first", binaryPath)
	}

	configPath := filepath.Join(dataDir, "config.yaml")

	// Spawn the daemon
	cmd := exec.Command(binaryPath, "-datadir", dataDir, "-config", configPath)
	hideConsoleWindow(cmd)
	
	// Create a log file inside the data directory
	logDir := filepath.Join(dataDir, "logs")
	_ = os.MkdirAll(logDir, 0755)
	logFile, err := os.OpenFile(filepath.Join(logDir, "daemon.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err == nil {
		cmd.Stdout = logFile
		cmd.Stderr = logFile
	}

	if err := cmd.Start(); err != nil {
		if logFile != nil {
			logFile.Close()
		}
		return fmt.Errorf("failed to start daemon: %w", err)
	}

	dm.cmd = cmd

	// Monitor process termination in a separate goroutine
	go func() {
		_ = cmd.Wait()
		if logFile != nil {
			logFile.Close()
		}
		dm.mu.Lock()
		if dm.cmd == cmd {
			dm.cmd = nil
		}
		dm.mu.Unlock()
	}()

	return nil
}

// Stop sends a termination/interrupt signal to the daemon.
func (dm *DaemonManager) Stop() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.cmd == nil || dm.cmd.Process == nil {
		return nil
	}

	// On Windows, TaskKill or interrupting isn't as clean as SIGINT on unix,
	// but we can kill the process. Let's try to terminate gracefully.
	var err error
	if runtime.GOOS == "windows" {
		err = dm.cmd.Process.Kill()
	} else {
		err = dm.cmd.Process.Signal(os.Interrupt)
	}

	if err != nil {
		return fmt.Errorf("failed to stop daemon: %w", err)
	}

	dm.cmd = nil
	return nil
}

// DownloadLatestRelease queries GitHub releases and downloads the appropriate zip file.
// If the latest release has no compatible asset compiled yet (common in local dev),
// it will fall back to checking if the binaries are compiled in the local bin/ folder
// and copying them to targetDir to simulate a download.
func (dm *DaemonManager) DownloadLatestRelease(targetDir string, progressCb func(percent int, msg string)) (string, error) {
	progressCb(5, "Checking GitHub for latest releases...")

	// Make HTTP Client with timeout
	client := &http.Client{Timeout: 15 * time.Second}
	req, err := http.NewRequest("GET", "https://api.github.com/repos/nnlgsakib/membuss/releases/latest", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Membuss-Desktop-App")

	resp, err := client.Do(req)
	var latestRelease map[string]any
	if err == nil && resp.StatusCode == 200 {
		defer resp.Body.Close()
		_ = json.NewDecoder(resp.Body).Decode(&latestRelease)
	}

	var downloadUrl string
	var versionTag string
	if latestRelease != nil {
		versionTag, _ = latestRelease["tag_name"].(string)
		assets, _ := latestRelease["assets"].([]any)

		// Search for platform-specific asset
		expectedSuffix := fmt.Sprintf("-%s-%s.zip", runtime.GOOS, runtime.GOARCH)
		for _, a := range assets {
			assetMap, ok := a.(map[string]any)
			if !ok {
				continue
			}
			name, _ := assetMap["name"].(string)
			if strings.HasSuffix(name, expectedSuffix) {
				downloadUrl, _ = assetMap["browser_download_url"].(string)
				break
			}
		}
	}

	binDir := filepath.Join(targetDir, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		return "", err
	}

	// Fallback implementation: If download URL is empty, we look for locally built binaries.
	if downloadUrl == "" {
		progressCb(30, "No compatible asset found in GitHub release. Falling back to local binaries...")
		time.Sleep(500 * time.Millisecond)

		// Look for locally built binaries in workspace root bin directory.
		// Go workspace root is 2 directories up from /desktop.
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		
		// If running in development, we can find binary at root bin/
		rootBin := filepath.Clean(filepath.Join(wd, "..", "bin"))
		
		exeExt := ""
		if runtime.GOOS == "windows" {
			exeExt = ".exe"
		}
		daemonSrc := filepath.Join(rootBin, "membuss"+exeExt)
		cliSrc := filepath.Join(rootBin, "membuss-cli"+exeExt)

		if _, err := os.Stat(daemonSrc); err != nil {
			return "", fmt.Errorf("local development binaries not found at %s. Please run 'make build' at the root repository first", rootBin)
		}

		progressCb(60, "Copying local development binaries...")
		
		err = copyFile(daemonSrc, filepath.Join(binDir, "membuss"+exeExt))
		if err != nil {
			return "", fmt.Errorf("failed to copy local daemon binary: %w", err)
		}
		
		err = copyFile(cliSrc, filepath.Join(binDir, "membuss-cli"+exeExt))
		if err != nil {
			return "", fmt.Errorf("failed to copy local CLI binary: %w", err)
		}

		progressCb(100, "Local installation complete!")
		return version.Version, nil
	}

	// If downloadUrl is found, perform the actual download
	progressCb(20, fmt.Sprintf("Downloading %s release...", versionTag))
	zipPath := filepath.Join(targetDir, "membuss-latest.zip")
	
	out, err := os.Create(zipPath)
	if err != nil {
		return "", err
	}
	defer out.Close()

	dresp, err := client.Get(downloadUrl)
	if err != nil {
		return "", err
	}
	defer dresp.Body.Close()

	if dresp.StatusCode != 200 {
		return "", fmt.Errorf("download failed: status %s", dresp.Status)
	}

	// Copy and report progress
	totalSize := dresp.ContentLength
	var downloaded int64
	buf := make([]byte, 32*1024)
	
	for {
		n, rerr := dresp.Body.Read(buf)
		if n > 0 {
			_, werr := out.Write(buf[:n])
			if werr != nil {
				return "", werr
			}
			downloaded += int64(n)
			if totalSize > 0 {
				percent := 20 + int(float64(downloaded)/float64(totalSize)*60.0) // Scale to 20-80% progress
				progressCb(percent, fmt.Sprintf("Downloading... (%d%%)", percent))
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return "", rerr
		}
	}
	out.Close()

	progressCb(85, "Extracting binaries...")
	err = extractZip(zipPath, binDir)
	if err != nil {
		return "", fmt.Errorf("failed to extract zip: %w", err)
	}

	progressCb(95, "Cleaning up downloaded archive...")
	_ = os.Remove(zipPath)

	progressCb(100, "Installation complete!")
	return versionTag, nil
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	
	// Ensure executable permissions
	return os.Chmod(dst, 0755)
}

// extractZip extracts all zip files to dest directory.
func extractZip(src, dest string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		fpath := filepath.Join(dest, f.Name)

		// Check for Zip Slip vulnerability
		if !strings.HasPrefix(fpath, filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("illegal file path in zip: %s", fpath)
		}

		if f.FileInfo().IsDir() {
			_ = os.MkdirAll(fpath, os.ModePerm)
			continue
		}

		if err = os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
			return err
		}

		outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			outFile.Close()
			return err
		}

		_, err = io.Copy(outFile, rc)
		outFile.Close()
		rc.Close()

		if err != nil {
			return err
		}
	}
	return nil
}

// CheckStatus pings the HTTP API endpoint to verify node health.
func (dm *DaemonManager) CheckStatus(apiAddr string) (map[string]any, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s/api/v1/node/info", apiAddr))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status code %s", resp.Status)
	}

	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}

	return payload, nil
}

// CheckExplorer checks if the gateway's explorer is online.
func (dm *DaemonManager) CheckExplorer(gatewayAddr string) bool {
	conn, err := net.DialTimeout("tcp", gatewayAddr, 1*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// LoadYamlConfig reads config.yaml from target dir.
func LoadYamlConfig(dataDir string) (map[string]any, error) {
	path := filepath.Join(dataDir, "config.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]any), nil
		}
		return nil, err
	}
	var out map[string]any
	if err := yaml.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// SaveYamlConfig serializes and writes config.yaml.
func SaveYamlConfig(dataDir string, cfg map[string]any) error {
	path := filepath.Join(dataDir, "config.yaml")
	_ = os.MkdirAll(dataDir, 0755)
	
	// Set default data_dir in config.yaml to target directory
	cfg["data_dir"] = filepath.ToSlash(dataDir)
	if geo, ok := cfg["geolocation_db"].(string); ok {
		cfg["geolocation_db"] = filepath.ToSlash(geo)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// WriteDefaultConfig generates a complete config.yaml with all fields
// from config.Default(), overriding data_dir with the target directory.
func WriteDefaultConfig(dataDir string) error {
	cfg := config.Default()
	cfg.DataDir = filepath.ToSlash(dataDir)

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}

	path := filepath.Join(dataDir, "config.yaml")
	_ = os.MkdirAll(dataDir, 0755)
	return os.WriteFile(path, data, 0644)
}
