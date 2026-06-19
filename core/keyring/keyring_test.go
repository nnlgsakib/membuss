package keyring

import (
	"testing"
)

func TestKeyRingRoundTrip(t *testing.T) {
	tempDir := t.TempDir()
	kr := NewKeyRing(tempDir)

	// Test Generate Ed25519
	key1, err := kr.Generate("testkey", "ed25519")
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	if key1.Name != "testkey" {
		t.Errorf("expected name 'testkey', got %s", key1.Name)
	}
	if key1.MemNSName == "" {
		t.Error("expected non-empty MemNSName")
	}

	// Test Get
	key1Get, err := kr.Get("testkey")
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}
	if key1Get.MemNSName != key1.MemNSName {
		t.Errorf("mismatched MemNSName: got %s, want %s", key1Get.MemNSName, key1.MemNSName)
	}

	// Test Export
	pemBytes, err := kr.Export("testkey")
	if err != nil {
		t.Fatalf("failed to export key: %v", err)
	}
	if len(pemBytes) == 0 {
		t.Error("exported PEM bytes are empty")
	}

	// Test Import under a new name
	err = kr.Import("importedkey", pemBytes)
	if err != nil {
		t.Fatalf("failed to import key: %v", err)
	}

	importedKey, err := kr.Get("importedkey")
	if err != nil {
		t.Fatalf("failed to get imported key: %v", err)
	}
	if importedKey.MemNSName != key1.MemNSName {
		t.Errorf("imported key MemNSName %s, want %s", importedKey.MemNSName, key1.MemNSName)
	}

	// Test List
	list, err := kr.List()
	if err != nil {
		t.Fatalf("failed to list keys: %v", err)
	}
	foundTest := false
	foundImported := false
	for _, k := range list {
		if k.Name == "testkey" {
			foundTest = true
		}
		if k.Name == "importedkey" {
			foundImported = true
		}
	}
	if !foundTest || !foundImported {
		t.Errorf("expected testkey and importedkey in list, got: %+v", list)
	}

	// Test Delete
	err = kr.Delete("importedkey")
	if err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}
	_, err = kr.Get("importedkey")
	if err == nil {
		t.Error("expected error getting deleted key, got nil")
	}
}
