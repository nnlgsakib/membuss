package herald

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nnlgsakib/membuss/core/keyring"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/shard"
	"github.com/nnlgsakib/membuss/net/dht"
	"github.com/nnlgsakib/membuss/net/host"
)

// fakeStore is a tiny in-memory SealedLister.
type fakeStore struct {
	mu     sync.Mutex
	sealed []mid.MID
	blocks []mid.MID
}

func (f *fakeStore) AllSealed() ([]mid.MID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]mid.MID, len(f.sealed))
	copy(out, f.sealed)
	return out, nil
}

func (f *fakeStore) AllBlocks() ([]mid.MID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]mid.MID, len(f.blocks))
	copy(out, f.blocks)
	return out, nil
}

func (f *fakeStore) Get(m mid.MID) ([]byte, error) {
	return nil, errors.New("not found")
}

func (f *fakeStore) Seal(m mid.MID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sealed = append(f.sealed, m)
}

func (f *fakeStore) AddBlock(m mid.MID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocks = append(f.blocks, m)
}

// fakeProvider records every Provide call.
type fakeProvider struct {
	mu       sync.Mutex
	provided []mid.MID
}

func (p *fakeProvider) Provide(ctx context.Context, m mid.MID) error {
	p.mu.Lock()
	p.provided = append(p.provided, m)
	p.mu.Unlock()
	return nil
}

func (p *fakeProvider) Count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.provided)
}

func TestHerald_ReprovideSealed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store := &fakeStore{}
	store.Seal(mid.FromBytes([]byte("hello")))
	store.Seal(mid.FromBytes([]byte("world")))

	prov := &fakeProvider{}
	h, err := New(Config{
		Store:    store,
		DHT:      prov,
		Strategy: StrategyRoots,
		Interval: time.Hour,
		Rate:     1000, // no throttling in test
		Burst:    32,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	n := h.RunOnce(ctx)
	if n != 2 {
		t.Fatalf("RunOnce: got %d, want 2", n)
	}
	if prov.Count() != 2 {
		t.Fatalf("Provide count: got %d, want 2", prov.Count())
	}
	if h.LastCount() != 2 {
		t.Fatalf("LastCount: got %d, want 2", h.LastCount())
	}
	if h.LastRun().IsZero() {
		t.Fatal("LastRun should be set after RunOnce")
	}
}

func TestHerald_StrategyAll(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := &fakeStore{}
	store.AddBlock(mid.FromBytes([]byte("root")))
	store.AddBlock(mid.FromBytes([]byte("block-1")))
	store.AddBlock(mid.FromBytes([]byte("block-2")))

	prov := &fakeProvider{}
	h, err := New(Config{
		Store:    store,
		DHT:      prov,
		Strategy: StrategyAll,
		Interval: time.Hour,
		Rate:     1000,
		Burst:    32,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	n := h.RunOnce(ctx)
	if n != 3 {
		t.Fatalf("RunOnce: got %d, want 3", n)
	}
}

func TestHerald_StartStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	store := &fakeStore{}
	store.Seal(mid.FromBytes([]byte("only")))
	prov := &fakeProvider{}
	h, err := New(Config{
		Store:    store,
		DHT:      prov,
		Strategy: StrategyRoots,
		Interval: 50 * time.Millisecond,
		Rate:     1000,
		Burst:    32,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.Start(ctx)
	h.Stop()
	// Start's immediate pass should have provided at least
	// once; the loop tick may have provided again.
	if prov.Count() < 1 {
		t.Fatalf("expected at least 1 provide, got %d", prov.Count())
	}
}

func TestTokenBucket_BurstThenLimit(t *testing.T) {
	now := time.Unix(0, 0)
	tb := newTokenBucket(1.0, 3, func() time.Time { return now })

	// First 3 should succeed (burst).
	for i := 0; i < 3; i++ {
		if err := tb.Wait(context.Background()); err != nil {
			t.Fatalf("burst wait %d: %v", i, err)
		}
	}
	// 4th should not be ready immediately. Use a
	// short-deadline context to avoid hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := tb.Wait(ctx); err == nil {
		t.Fatal("expected bucket to be empty after burst")
	}
}

func TestHerald_RepublishMemNS_NoRecord(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	tempDir := t.TempDir()

	// Generate and save identity.key
	priv, err := host.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	if err := host.SaveIdentity(tempDir, priv); err != nil {
		t.Fatalf("SaveIdentity: %v", err)
	}

	kr := keyring.NewKeyRing(tempDir)

	// List keys should show "self"
	keys, err := kr.List()
	if err != nil {
		t.Fatalf("List keys: %v", err)
	}
	if len(keys) != 1 || keys[0].Name != "self" {
		t.Fatalf("expected only 'self' key, got %v", keys)
	}

	// We have "self" key but no "self.record" file.
	// Run herald with this keyring. It should skip the missing record silently.
	store := &fakeStore{}
	prov := &fakeProvider{}

	h, err := New(Config{
		Store:    store,
		DHT:      prov,
		Strategy: StrategyRoots,
		Interval: time.Hour,
		Rate:     1000,
		Burst:    32,
		KeyRing:  kr,
		MemDHT:   &dht.MemDHT{},
		Now:      time.Now,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// RunOnce should execute cleanly without panics/errors
	n := h.RunOnce(ctx)
	if n != 0 {
		t.Fatalf("expected 0 MIDs announced, got %d", n)
	}
}

func TestHerald_StrategyShardsUsesRing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := &fakeStore{}
	m1 := mid.FromBytes([]byte("shard-a"))
	m2 := mid.FromBytes([]byte("shard-b"))
	store.Seal(m1)
	store.Seal(m2)

	ring := shard.NewHashRing()
	ring.AddPeer("peer-A")
	ring.AddPeer("peer-B")

	prov := &fakeProvider{}
	h, err := New(Config{
		Store:     store,
		DHT:       prov,
		Strategy:  StrategyShards,
		Interval:  time.Hour,
		Rate:      1000,
		Burst:     32,
		ShardRing: ring,
		PeerID:    "peer-A",
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	n := h.RunOnce(ctx)
	if n == 0 {
		t.Fatal("StrategyShards: RunOnce provided 0 MIDs; expected at least 1")
	}
	if n > 2 {
		t.Fatalf("StrategyShards: RunOnce provided %d MIDs; max is 2", n)
	}

	provided := prov.provided
	for _, m := range provided {
		peers, err := ring.Assign(m, 1)
		if err != nil {
			t.Fatalf("Assign: %v", err)
		}
		if peers[0] != "peer-A" {
			t.Fatalf("StrategyShards provided %s which belongs to %s, not peer-A", m, peers[0])
		}
	}
}

func TestHerald_StrategyShardsFallsBackWithoutRing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := &fakeStore{}
	store.Seal(mid.FromBytes([]byte("root-1")))
	store.Seal(mid.FromBytes([]byte("root-2")))

	prov := &fakeProvider{}
	h, err := New(Config{
		Store:    store,
		DHT:      prov,
		Strategy: StrategyShards,
		Interval: time.Hour,
		Rate:     1000,
		Burst:    32,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	n := h.RunOnce(ctx)
	if n != 2 {
		t.Fatalf("StrategyShards without ring: RunOnce got %d, want 2 (fallback to roots)", n)
	}
}

func TestHerald_StrategyShardsOtherPeerNotProvided(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := &fakeStore{}
	m1 := mid.FromBytes([]byte("belongs-to-B"))
	store.Seal(m1)

	ring := shard.NewHashRing()
	ring.AddPeer("peer-A")
	ring.AddPeer("peer-B")

	prov := &fakeProvider{}
	h, err := New(Config{
		Store:     store,
		DHT:       prov,
		Strategy:  StrategyShards,
		Interval:  time.Hour,
		Rate:      1000,
		Burst:     32,
		ShardRing: ring,
		PeerID:    "peer-A",
		Replicas:  1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	n := h.RunOnce(ctx)
	peers, _ := ring.Assign(m1, 1)
	if peers[0] == "peer-A" {
		if n != 1 {
			t.Fatalf("expected 1 provide (MID belongs to peer-A), got %d", n)
		}
	} else {
		if n != 0 {
			t.Fatalf("expected 0 provides (MID belongs to %s, not peer-A), got %d", peers[0], n)
		}
	}
}
