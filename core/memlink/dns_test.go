package memlink

import (
	"context"
	"errors"
	"testing"
)

func TestParseTXTRecord(t *testing.T) {
	// Test standard mutable pointer
	txt1 := "memns=/memns/k51qzi5uqu5dkkciu33khkzbcmxtyhn376i1e83tya8kuy7z9euedzyr42d8 memns-ttl=300"
	rec1, err := ParseTXTRecord(txt1)
	if err != nil {
		t.Fatalf("failed to parse txt1: %v", err)
	}
	if rec1.MemNSName != "/memns/k51qzi5uqu5dkkciu33khkzbcmxtyhn376i1e83tya8kuy7z9euedzyr42d8" {
		t.Errorf("expected memns name '/memns/k51...', got %q", rec1.MemNSName)
	}
	if rec1.TTL != 300 {
		t.Errorf("expected TTL 300, got %d", rec1.TTL)
	}

	// Test static pointer + delegate fallback + routes
	txt2 := "mem=/mem/mem1abc memns-routes=production:70,staging:30 memns-delegate=/memns/k51other"
	rec2, err := ParseTXTRecord(txt2)
	if err != nil {
		t.Fatalf("failed to parse txt2: %v", err)
	}
	if rec2.MID != "/mem/mem1abc" {
		t.Errorf("expected MID '/mem/mem1abc', got %q", rec2.MID)
	}
	if rec2.Routes["production"] != 70 || rec2.Routes["staging"] != 30 {
		t.Errorf("expected routes production:70, staging:30, got %+v", rec2.Routes)
	}
	if rec2.Delegate != "/memns/k51other" {
		t.Errorf("expected delegate '/memns/k51other', got %q", rec2.Delegate)
	}

	// Test invalid record
	txt3 := "some=other key=value"
	_, err = ParseTXTRecord(txt3)
	if err == nil {
		t.Error("expected error for invalid TXT record, got nil")
	}
}

func TestDNSResolver(t *testing.T) {
	mockResolveMemNS := func(ctx context.Context, name string) (string, error) {
		if name == "/memns/k51owner" {
			return "/mem/mem1owner", nil
		}
		if name == "/memns/k51delegate" {
			return "/mem/mem1delegate", nil
		}
		return "", errors.New("memns resolution failed")
	}

	resolver := NewDNSResolver(mockResolveMemNS)

	// Mock LookupTXT
	lookupCount := 0
	mockLookup := func(host string) ([]string, error) {
		lookupCount++
		if host == "_memlink.example.com" {
			return []string{"memns=/memns/k51owner memns-ttl=10"}, nil
		}
		if host == "_memlink.delegate.com" {
			return []string{"memns=/memns/k51failed memns-delegate=/memns/k51delegate"}, nil
		}
		return nil, errors.New("host not found")
	}
	resolver.SetLookupTXT(mockLookup)

	ctx := context.Background()

	// 1. Test standard resolution
	val, err := resolver.Resolve(ctx, "example.com")
	if err != nil {
		t.Fatalf("resolve error: %v", err)
	}
	if val != "/mem/mem1owner" {
		t.Errorf("expected '/mem/mem1owner', got %q", val)
	}
	if lookupCount != 1 {
		t.Errorf("expected lookup count 1, got %d", lookupCount)
	}

	// 2. Test cache hits (subsequent resolution within TTL shouldn't count towards lookupCount)
	val, err = resolver.Resolve(ctx, "example.com")
	if err != nil {
		t.Fatalf("resolve error: %v", err)
	}
	if val != "/mem/mem1owner" {
		t.Errorf("expected cached '/mem/mem1owner', got %q", val)
	}
	if lookupCount != 1 {
		t.Errorf("expected lookup count to remain 1, got %d", lookupCount)
	}

	// 3. Test delegate fallback resolve path
	val, err = resolver.Resolve(ctx, "delegate.com")
	if err != nil {
		t.Fatalf("resolve error: %v", err)
	}
	if val != "/mem/mem1delegate" {
		t.Errorf("expected fallback '/mem/mem1delegate', got %q", val)
	}
}
