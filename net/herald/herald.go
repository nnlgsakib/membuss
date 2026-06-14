// Package herald implements Mem-Herald, the Membuss
// reprovisioner.
//
// Mem-Herald keeps content discoverable. Every ReprovideInterval
// (default 12h) it walks the local store and re-announces a
// subset of its MIDs to the DHT as provider records, so the
// network can find the data even if no peer has asked for it
// recently.
//
// Three strategies are supported:
//
//   - roots  (default): only the sealed root MIDs. Cheapest
//     and sufficient for most nodes.
//   - all: every block MID in the store. Used by Anchor
//     nodes that back up the whole network.
//   - shards: only erasure shard MIDs this node is responsible
//     for. The most selective; requires a shard ring.
//
// Provides are rate-limited to 100/minute (≈ 1.67/second) so
// the DHT is not flooded at startup. A leaky-bucket style
// limiter is used so bursts are tolerated up to a small bucket
// size.
package herald

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nnlgsakib/membuss/core/mid"
)

// Strategy selects which MIDs the herald re-announces.
type Strategy string

const (
	// StrategyRoots announces only the directly sealed root
	// MIDs. Default.
	StrategyRoots Strategy = "roots"
	// StrategyAll announces every block MID in the store.
	// Used by Anchor nodes.
	StrategyAll Strategy = "all"
	// StrategyShards announces only the erasure shard MIDs
	// this node is responsible for. The node is expected to
	// have a configured shard ring; see core/shard.
	StrategyShards Strategy = "shards"

	// DefaultRate is the long-run rate of provider
	// announcements: 100 per minute.
	DefaultRate = 100.0 / 60.0
	// DefaultBurst is the maximum burst the rate limiter
	// allows before throttling kicks in.
	DefaultBurst = 32
)

// SealedLister is the subset of the store that the herald
// needs. Production code passes *store.MemStore; tests can
// supply an in-memory fake.
type SealedLister interface {
	// AllSealed returns every directly sealed root MID.
	AllSealed() ([]mid.MID, error)
	// AllBlocks returns every block MID the store holds.
	// Required by the "all" strategy. For stores that
	// cannot enumerate blocks cheaply, return AllSealed
	// instead and the "all" strategy will degrade to
	// "roots".
	AllBlocks() ([]mid.MID, error)
}

// Provider announces that this node is a provider of m. The
// DHT facade in net/dht satisfies this interface.
type Provider interface {
	Provide(ctx context.Context, m mid.MID) error
}

// Config configures a MemHerald.
type Config struct {
	// Store is the local store to enumerate MIDs from.
	// Required.
	Store SealedLister
	// DHT is the local DHT facade. Required.
	DHT Provider
	// Strategy selects which MIDs to re-announce. The
	// default is StrategyRoots.
	Strategy Strategy
	// Interval is the time between reprovide rounds. The
	// default is 12 hours.
	Interval time.Duration
	// Rate is the long-run rate of provider announcements
	// in messages/second. Default is DefaultRate
	// (100/minute).
	Rate float64
	// Burst is the maximum burst the limiter allows.
	// Default is DefaultBurst.
	Burst int
	// Now overrides the wall clock for tests. Default
	// is time.Now.
	Now func() time.Time
}

// MemHerald is the long-lived reprovisioner.
type MemHerald struct {
	cfg Config
	lim *tokenBucket

	mu        sync.Mutex
	lastRun   time.Time
	lastCount int
}

// New returns a MemHerald ready to be started. Call Start to
// begin the background loop.
func New(cfg Config) (*MemHerald, error) {
	if cfg.Store == nil {
		return nil, errors.New("herald: nil store")
	}
	if cfg.DHT == nil {
		return nil, errors.New("herald: nil dht")
	}
	if cfg.Strategy == "" {
		cfg.Strategy = StrategyRoots
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 12 * time.Hour
	}
	if cfg.Rate <= 0 {
		cfg.Rate = DefaultRate
	}
	if cfg.Burst <= 0 {
		cfg.Burst = DefaultBurst
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &MemHerald{
		cfg: cfg,
		lim: newTokenBucket(cfg.Rate, cfg.Burst, cfg.Now),
	}, nil
}

// Start launches the background reprovide loop. It returns
// immediately; the loop runs until ctx is cancelled. A first
// pass is also fired immediately so the node announces its
// content right at startup.
func (h *MemHerald) Start(ctx context.Context) {
	go h.loop(ctx)
	h.RunOnce(ctx)
}

// Stop is a no-op kept for symmetry with other long-lived
// engines; the loop terminates when ctx is cancelled.
func (h *MemHerald) Stop() {}

// RunOnce performs a single reprovide pass synchronously and
// returns the number of MIDs announced.
func (h *MemHerald) RunOnce(ctx context.Context) int {
	mids := h.collect(ctx)
	announced := 0
	for _, m := range mids {
		if err := h.lim.Wait(ctx); err != nil {
			break
		}
		if err := h.cfg.DHT.Provide(ctx, m); err != nil {
			continue
		}
		announced++
	}
	h.mu.Lock()
	h.lastRun = h.cfg.Now()
	h.lastCount = announced
	h.mu.Unlock()
	return announced
}

// LastRun returns the time of the most recent completed
// reprovide pass. The zero value means RunOnce has not yet
// completed.
func (h *MemHerald) LastRun() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastRun
}

// LastCount returns the number of MIDs announced in the most
// recent reprovide pass.
func (h *MemHerald) LastCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastCount
}

// Strategy returns the configured strategy.
func (h *MemHerald) Strategy() Strategy { return h.cfg.Strategy }

func (h *MemHerald) loop(ctx context.Context) {
	t := time.NewTicker(h.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = h.RunOnce(ctx)
		}
	}
}

func (h *MemHerald) collect(ctx context.Context) []mid.MID {
	if ctx.Err() != nil {
		return nil
	}
	switch h.cfg.Strategy {
	case StrategyAll:
		mids, err := h.cfg.Store.AllBlocks()
		if err != nil {
			return nil
		}
		return mids
	case StrategyShards:
		// Shard enumeration requires a configured shard
		// ring. Until that is wired in, fall back to
		// roots so the herald still does useful work.
		mids, err := h.cfg.Store.AllSealed()
		if err != nil {
			return nil
		}
		return mids
	case StrategyRoots, "":
		mids, err := h.cfg.Store.AllSealed()
		if err != nil {
			return nil
		}
		return mids
	default:
		return nil
	}
}

// tokenBucket is a simple rate limiter with a fixed capacity
// and refill rate. It is safe for concurrent use.
type tokenBucket struct {
	mu       sync.Mutex
	rate     float64 // tokens/second
	burst    float64
	tokens   float64
	lastFill time.Time
	now      func() time.Time
}

func newTokenBucket(rate float64, burst int, now func() time.Time) *tokenBucket {
	return &tokenBucket{
		rate:     rate,
		burst:    float64(burst),
		tokens:   float64(burst),
		lastFill: now(),
		now:      now,
	}
}

// Wait blocks until one token is available or ctx is done.
// It returns ctx.Err() if the context fires first.
func (b *tokenBucket) Wait(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		b.mu.Lock()
		b.refillLocked()
		if b.tokens >= 1.0 {
			b.tokens -= 1.0
			b.mu.Unlock()
			return nil
		}
		// Compute the time until the next token.
		need := 1.0 - b.tokens
		wait := time.Duration(float64(time.Second) * need / b.rate)
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (b *tokenBucket) refillLocked() {
	now := b.now()
	elapsed := now.Sub(b.lastFill).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.lastFill = now
}
