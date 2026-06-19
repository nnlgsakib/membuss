package memns

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	kaddht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/multiformats/go-multiaddr"

	"github.com/nnlgsakib/membuss/core/keyring"
	"github.com/nnlgsakib/membuss/net/dht"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

// Helper to generate a keyring and a key.
func makeTestKey(t *testing.T, dir, name string) (*keyring.KeyRing, *keyring.Key) {
	t.Helper()
	kr := keyring.NewKeyRing(dir)
	key, err := kr.Generate(name, "ed25519")
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	return kr, key
}

// Helper to build a host listening on localhost
func newTestHost(t *testing.T) host.Host {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}
	tcpAddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/0")
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrs(tcpAddr),
	)
	if err != nil {
		t.Fatalf("libp2p.New: %v", err)
	}
	return h
}

func TestRecordSigningAndVerification(t *testing.T) {
	tempDir := t.TempDir()
	_, key := makeTestKey(t, tempDir, "owner")

	// 1. Build and verify a standard record
	val := "/mem/mem1abc"
	ttl := 10 * time.Second
	rec, err := BuildRecord(key, val, 1, ttl, nil, "initial publish")
	if err != nil {
		t.Fatalf("failed to build record: %v", err)
	}

	if err := VerifyRecord(rec); err != nil {
		t.Fatalf("failed to verify record: %v", err)
	}

	// Verify changelog
	if rec.Changelog == nil || len(rec.Changelog.Entries) != 1 {
		t.Fatalf("changelog entries missing or invalid")
	}
	entry := rec.Changelog.Entries[0]
	if string(entry.Value) != val || entry.Sequence != 1 || entry.Message != "initial publish" {
		t.Errorf("changelog entry details mismatch: %+v", entry)
	}

	// 2. Tampered record value
	rec.Value = []byte("/mem/mem1tampered")
	if err := VerifyRecord(rec); err == nil {
		t.Error("expected verification error for tampered record, got nil")
	}

	// 3. Expired record
	rec2, err := BuildRecord(key, val, 2, -1*time.Second, nil, "expired")
	if err != nil {
		t.Fatalf("failed to build expired record: %v", err)
	}
	if err := VerifyRecord(rec2); err == nil {
		t.Error("expected verification error for expired record, got nil")
	}
}

func TestDelegateSigning(t *testing.T) {
	tempDir := t.TempDir()
	kr := keyring.NewKeyRing(tempDir)
	ownerKey, err := kr.Generate("owner", "ed25519")
	if err != nil {
		t.Fatalf("gen owner: %v", err)
	}
	delegateKey, err := kr.Generate("delegate", "ed25519")
	if err != nil {
		t.Fatalf("gen delegate: %v", err)
	}

	// Build a record signed by delegate key, but owner key set in meta
	val := "/mem/mem1delegated"
	pubBytes, _ := crypto.MarshalPublicKey(delegateKey.PubKey)
	ownerBytes, _ := crypto.MarshalPublicKey(ownerKey.PubKey)

	rec, err := BuildRecord(delegateKey, val, 1, 10*time.Second, nil, "delegated publish")
	if err != nil {
		t.Fatalf("failed to build record: %v", err)
	}

	// Set delegates list in record and owner key in meta
	rec.Delegates = [][]byte{pubBytes}
	rec.Meta["owner_key"] = base64.StdEncoding.EncodeToString(ownerBytes)

	canonical := CanonicalBytes(rec)
	sig, err := delegateKey.PrivKey.Sign(canonical)
	if err != nil {
		t.Fatalf("failed to resign: %v", err)
	}
	rec.Signature = sig

	// 1. Verification should succeed because delegate is in delegates list
	if err := VerifyRecord(rec); err != nil {
		t.Fatalf("delegate verification failed: %v", err)
	}

	// 2. Remove delegate from list -> verify fails
	rec.Delegates = nil
	canonical = CanonicalBytes(rec)
	sig, _ = delegateKey.PrivKey.Sign(canonical)
	rec.Signature = sig
	if err := VerifyRecord(rec); err == nil {
		t.Error("expected verification failure when delegate is not in delegates list")
	}
}

func TestMemRouteWeightedPick(t *testing.T) {
	routes := []*membusspb.MemRoute{
		{Target: []byte("targetA"), Weight: 80, Label: "A"},
		{Target: []byte("targetB"), Weight: 20, Label: "B"},
	}

	counts := make(map[string]int)
	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		picked := SelectRoute(ctx, routes, "fallback")
		counts[picked]++
	}

	aCount := counts["targetA"]
	bCount := counts["targetB"]

	if aCount < 750 || aCount > 850 {
		t.Errorf("A/B distribution skewed: targetA picked %d times (expected ~800), targetB picked %d times", aCount, bCount)
	}
}

// Mock DNS Resolver to test recursion and loops
type mockDNSResolver struct {
	records map[string]string
}

func (m *mockDNSResolver) Resolve(ctx context.Context, domain string) (string, error) {
	if val, ok := m.records[domain]; ok {
		return val, nil
	}
	return "", errors.New("domain not found")
}

func TestResolverRecursionAndLoops(t *testing.T) {
	cache := NewRecordCache(10)
	resolver := NewResolver(nil, nil, cache)

	mockDNS := &mockDNSResolver{
		records: make(map[string]string),
	}
	resolver.SetDNSResolver(mockDNS)

	privB, _, _ := crypto.GenerateEd25519Key(rand.Reader)
	keyB := &keyring.Key{Name: "keyB", PrivKey: privB, PubKey: privB.GetPublic(), MemNSName: "/memns/Bhash"}

	privC, _, _ := crypto.GenerateEd25519Key(rand.Reader)
	keyC := &keyring.Key{Name: "keyC", PrivKey: privC, PubKey: privC.GetPublic(), MemNSName: "/memns/Chash"}

	recC, _ := BuildRecord(keyC, "/mem/mid1", 1, 10*time.Second, nil, "")
	recB, _ := BuildRecord(keyB, "/memns/Chash", 1, 10*time.Second, nil, "")

	cache.Add("Chash", recC)
	cache.Add("Bhash", recB)

	mockDNS.records["example.com"] = "/memns/Bhash"

	ctx := context.Background()

	// 1. Resolve domain A recursively
	val, err := resolver.Resolve(ctx, "example.com")
	if err != nil {
		t.Fatalf("failed to resolve: %v", err)
	}
	if val != "/mem/mid1" {
		t.Errorf("expected final resolved value '/mem/mid1', got %q", val)
	}

	// 2. Resolve loop: A -> B -> A
	privA, _, _ := crypto.GenerateEd25519Key(rand.Reader)
	keyA := &keyring.Key{Name: "keyA", PrivKey: privA, PubKey: privA.GetPublic(), MemNSName: "/memns/Ahash"}

	recA, _ := BuildRecord(keyA, "/memns/Bhash", 1, 10*time.Second, nil, "")
	recB2, _ := BuildRecord(keyB, "/memns/Ahash", 2, 10*time.Second, nil, "")

	cache.Add("Ahash", recA)
	cache.Add("Bhash", recB2)

	_, err = resolver.Resolve(ctx, "/memns/Ahash")
	if err == nil {
		t.Fatal("expected loop detection error, got nil")
	}
	if err.Error() != "memns: loop detected, max resolution depth reached" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDHTPublishResolve(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	h1 := newTestHost(t)
	h2 := newTestHost(t)
	defer h1.Close()
	defer h2.Close()

	d1, err := dht.New(ctx, dht.Config{Host: h1, Mode: kaddht.ModeServer})
	if err != nil {
		t.Fatalf("dht1: %v", err)
	}
	d2, err := dht.New(ctx, dht.Config{Host: h2, Mode: kaddht.ModeServer})
	if err != nil {
		t.Fatalf("dht2: %v", err)
	}
	defer d1.Close()
	defer d2.Close()

	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), time.Hour)
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), time.Hour)
	if err := h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if err := d1.Bootstrap(ctx, []peer.AddrInfo{{ID: h2.ID(), Addrs: h2.Addrs()}}); err != nil {
		t.Fatalf("d1 bootstrap: %v", err)
	}
	if err := d2.Bootstrap(ctx, []peer.AddrInfo{{ID: h1.ID(), Addrs: h1.Addrs()}}); err != nil {
		t.Fatalf("d2 bootstrap: %v", err)
	}

	for i := 0; i < 50; i++ {
		if d1.RoutingTableSize() >= 1 && d2.RoutingTableSize() >= 1 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if d1.RoutingTableSize() == 0 || d2.RoutingTableSize() == 0 {
		t.Fatal("routing tables failed to learn about peers")
	}

	kr := keyring.NewKeyRing(t.TempDir())
	key, err := kr.Generate("testkey", "ed25519")
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	val := "/mem/mem1abcdef"
	rec, err := BuildRecord(key, val, 1, 10*time.Second, nil, "initial publish")
	if err != nil {
		t.Fatalf("failed to build record: %v", err)
	}

	err = PublishDHT(ctx, d1, key, rec)
	if err != nil {
		t.Fatalf("dht publish failed: %v", err)
	}

	rec2, err := ResolveDHT(ctx, d2, key.MemNSName)
	if err != nil {
		t.Fatalf("dht resolve failed: %v", err)
	}

	if string(rec2.Value) != val {
		t.Errorf("resolved value mismatched: got %q, want %q", rec2.Value, val)
	}
}

func TestPubSubPublishSubscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h1 := newTestHost(t)
	h2 := newTestHost(t)
	defer h1.Close()
	defer h2.Close()

	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), time.Hour)
	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), time.Hour)
	if err := h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}); err != nil {
		t.Fatalf("connect: %v", err)
	}

	pm1, err := NewPubSubManager(h1)
	if err != nil {
		t.Fatalf("pm1: %v", err)
	}
	pm2, err := NewPubSubManager(h2)
	if err != nil {
		t.Fatalf("pm2: %v", err)
	}

	kr := keyring.NewKeyRing(t.TempDir())
	key, err := kr.Generate("testkey", "ed25519")
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	ch := make(chan *membusspb.MemNSRecord, 10)
	err = pm2.SubscribePub(ctx, key.MemNSName, ch)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	_, err = pm1.GetTopic(key.MemNSName)
	if err != nil {
		t.Fatalf("join topic: %v", err)
	}

	time.Sleep(1 * time.Second)

	val := "/mem/mem1gossip"
	rec, err := BuildRecord(key, val, 1, 10*time.Second, nil, "gossip publish")
	if err != nil {
		t.Fatalf("build record: %v", err)
	}

	err = pm1.PublishPub(ctx, key, rec)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case received := <-ch:
		if string(received.Value) != val {
			t.Errorf("received value mismatched: got %q, want %q", received.Value, val)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for GossipSub message propagation")
	}
}

func TestMemLogHistory(t *testing.T) {
	tempDir := t.TempDir()
	kr := keyring.NewKeyRing(tempDir)
	key, err := kr.Generate("owner", "ed25519")
	if err != nil {
		t.Fatalf("gen key: %v", err)
	}

	var lastRecord *membusspb.MemNSRecord
	for i := uint64(1); i <= 5; i++ {
		val := fmt.Sprintf("/mem/mem1version%d", i)
		routes := []*membusspb.MemRoute{}
		msg := fmt.Sprintf("deploy v%d", i)

		rec, err := BuildRecord(key, val, i, 10*time.Second, routes, msg)
		if err != nil {
			t.Fatalf("failed to build record at seq %d: %v", i, err)
		}

		if lastRecord != nil {
			rec.Changelog.Entries = append(lastRecord.Changelog.Entries, rec.Changelog.Entries...)
			canonical := CanonicalBytes(rec)
			sig, err := key.PrivKey.Sign(canonical)
			if err != nil {
				t.Fatalf("failed to sign: %v", err)
			}
			rec.Signature = sig
		}

		lastRecord = rec
	}

	if len(lastRecord.Changelog.Entries) != 5 {
		t.Fatalf("expected 5 changelog entries, got %d", len(lastRecord.Changelog.Entries))
	}

	for i, entry := range lastRecord.Changelog.Entries {
		seq := uint64(i + 1)
		if entry.Sequence != seq {
			t.Errorf("expected sequence %d, got %d", seq, entry.Sequence)
		}
		expectedVal := fmt.Sprintf("/mem/mem1version%d", seq)
		if string(entry.Value) != expectedVal {
			t.Errorf("expected value %q, got %q", expectedVal, entry.Value)
		}
		expectedMsg := fmt.Sprintf("deploy v%d", seq)
		if entry.Message != expectedMsg {
			t.Errorf("expected message %q, got %q", expectedMsg, entry.Message)
		}

		logBytes := CanonicalLogBytes(entry.Sequence, entry.Value, entry.Timestamp)
		ok, err := key.PubKey.Verify(logBytes, entry.Signature)
		if err != nil || !ok {
			t.Errorf("failed to verify signature for entry at seq %d", seq)
		}
	}
}
