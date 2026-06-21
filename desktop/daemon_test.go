package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestExtractTarGz(t *testing.T) {
	// Create a temporary directory for test outputs
	tmpDir, err := os.MkdirTemp("", "membuss-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a mock tar.gz archive in memory
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gzw)

	// Add a directory entry
	dirHeader := &tar.Header{
		Name:     "subdir/",
		Typeflag: tar.TypeDir,
		Mode:     0755,
	}
	if err := tw.WriteHeader(dirHeader); err != nil {
		t.Fatalf("failed to write dir header: %v", err)
	}

	// Add a file entry inside directory
	file1Content := []byte("hello daemon")
	file1Header := &tar.Header{
		Name:     "subdir/daemon",
		Typeflag: tar.TypeReg,
		Size:     int64(len(file1Content)),
		Mode:     0755,
	}
	if err := tw.WriteHeader(file1Header); err != nil {
		t.Fatalf("failed to write file1 header: %v", err)
	}
	if _, err := tw.Write(file1Content); err != nil {
		t.Fatalf("failed to write file1 content: %v", err)
	}

	// Add a file entry at root
	file2Content := []byte("hello cli")
	file2Header := &tar.Header{
		Name:     "cli",
		Typeflag: tar.TypeReg,
		Size:     int64(len(file2Content)),
		Mode:     0644,
	}
	if err := tw.WriteHeader(file2Header); err != nil {
		t.Fatalf("failed to write file2 header: %v", err)
	}
	if _, err := tw.Write(file2Content); err != nil {
		t.Fatalf("failed to write file2 content: %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("failed to close tar writer: %v", err)
	}
	if err := gzw.Close(); err != nil {
		t.Fatalf("failed to close gzip writer: %v", err)
	}

	// Write mock archive to disk
	archivePath := filepath.Join(tmpDir, "mock.tar.gz")
	if err := os.WriteFile(archivePath, buf.Bytes(), 0644); err != nil {
		t.Fatalf("failed to write mock archive: %v", err)
	}

	// Extract mock archive using extractTarGz
	destDir := filepath.Join(tmpDir, "extracted")
	if err := extractTarGz(archivePath, destDir); err != nil {
		t.Fatalf("extractTarGz failed: %v", err)
	}

	// Validate subdirectory existence
	subDirPath := filepath.Join(destDir, "subdir")
	fi, err := os.Stat(subDirPath)
	if err != nil {
		t.Fatalf("expected subdirectory to exist: %v", err)
	}
	if !fi.IsDir() {
		t.Fatalf("expected subdir to be a directory")
	}

	// Validate subdirectory file
	f1Path := filepath.Join(subDirPath, "daemon")
	f1Data, err := os.ReadFile(f1Path)
	if err != nil {
		t.Fatalf("failed to read extracted file1: %v", err)
	}
	if string(f1Data) != string(file1Content) {
		t.Errorf("expected content %q, got %q", file1Content, f1Data)
	}

	// Validate root level file
	f2Path := filepath.Join(destDir, "cli")
	f2Data, err := os.ReadFile(f2Path)
	if err != nil {
		t.Fatalf("failed to read extracted file2: %v", err)
	}
	if string(f2Data) != string(file2Content) {
		t.Errorf("expected content %q, got %q", file2Content, f2Data)
	}

	// On non-Windows platforms, check that file permissions are set to 0755
	if runtime.GOOS != "windows" {
		fi1, _ := os.Stat(f1Path)
		if fi1.Mode().Perm() != 0755 {
			t.Errorf("expected permission 0755 for f1, got %o", fi1.Mode().Perm())
		}
		fi2, _ := os.Stat(f2Path)
		if fi2.Mode().Perm() != 0755 {
			t.Errorf("expected permission 0755 for f2, got %o", fi2.Mode().Perm())
		}
	}
}

func TestIsPortFreeAndFindNextFreePort(t *testing.T) {
	// Bind to a port temporarily
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on dynamic port: %v", err)
	}
	defer ln.Close()

	addr := ln.Addr().String()

	// Check if the port is free (should be false since we are listening)
	if isPortFree(addr) {
		t.Errorf("expected port %s to be busy, but it was reported free", addr)
	}

	// Try to find the next free port starting from this busy one
	nextFree, err := findNextFreePort(addr)
	if err != nil {
		t.Fatalf("findNextFreePort failed: %v", err)
	}

	if nextFree == addr {
		t.Errorf("expected next free port to be different from %s, got same", addr)
	}

	// The next free port should actually be free
	if !isPortFree(nextFree) {
		t.Errorf("expected found port %s to be free, but it was reported busy", nextFree)
	}
}
