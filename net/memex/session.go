package memex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/dag"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// SessionConfig configures a MemexSession.
type SessionConfig struct {
	Engine        *Engine
	Root          mid.MID
	Providers     []peer.AddrInfo
	ParallelPeers int
	Timeout       time.Duration
}

// Session is a single in-flight retrieval. A Session drives
// one Fetch call; reuse by creating a new Session.
type Session struct {
	cfg SessionConfig
}

// NewSession returns a Session ready to fetch cfg.Root.
func NewSession(cfg SessionConfig) (*Session, error) {
	if cfg.Engine == nil {
		return nil, errors.New("memex session: nil engine")
	}
	if cfg.Root.IsZero() {
		return nil, errors.New("memex session: zero root")
	}
	if len(cfg.Providers) == 0 {
		return nil, errors.New("memex session: no providers")
	}
	return &Session{cfg: cfg}, nil
}

// Fetch drives the session to completion. It returns an
// io.Reader that yields the reassembled content of the DAG
// rooted at Root when every block in the DAG has been
// retrieved and verified.
func (s *Session) Fetch(ctx context.Context) (io.Reader, error) {
	timeout := s.cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultSessionTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	fanout := s.cfg.ParallelPeers
	if fanout <= 0 {
		fanout = MaxParallelPeers
	}
	if fanout > MaxParallelPeers {
		fanout = MaxParallelPeers
	}
	if fanout > len(s.cfg.Providers) {
		fanout = len(s.cfg.Providers)
	}

	// pending is a shared queue of MIDs we still need.
	pending := make(chan mid.MID, 1024)

	// We track two sets:
	//   enqueued  - MIDs we have ever asked for (or are about to).
	//   resolved  - MIDs we know to be locally present.
	// The session ends when enqueued == resolved. A walker
	// discovers children of resolved-but-not-yet-walked
	// internal nodes, enqueuing them.
	var mu sync.Mutex
	enqueued := make(map[string]struct{})
	resolved := make(map[string]struct{})

	addEnqueued := func(m mid.MID) bool {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := enqueued[m.String()]; ok {
			return false
		}
		enqueued[m.String()] = struct{}{}
		return true
	}
	markResolved := func(m mid.MID) {
		mu.Lock()
		resolved[m.String()] = struct{}{}
		mu.Unlock()
	}
	allResolved := func() bool {
		mu.Lock()
		defer mu.Unlock()
		if len(enqueued) != len(resolved) {
			return false
		}
		for k := range enqueued {
			if _, ok := resolved[k]; !ok {
				return false
			}
		}
		return true
	}

	// Phase 13: filter the provider list through the
	// bloom manager. A provider whose stored filter
	// reports "definitely absent" for the root is
	// excluded; unknown peers are kept.
	filtered := s.selectPeersForMID(s.cfg.Root)
	if len(filtered) == 0 {
		return nil, errors.New("memex session: no provider after bloom filter")
	}
	// Replace the fan-out's provider list for the rest
	// of this session.
	liveProviders := filtered
	// Honor the user-requested fanout bound.
	if fanout > len(liveProviders) {
		fanout = len(liveProviders)
	}

	// Seed with the root.
	addEnqueued(s.cfg.Root)
	pending <- s.cfg.Root

	// runProvider opens one stream per peer slot. Each
	// stream runs a reader and a writer until the session
	// ends.
	var wg sync.WaitGroup
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
	provider := liveProviders[i]
		go func() {
			defer wg.Done()
			_ = s.runProvider(ctx, provider, pending, markResolved)
		}()
	}

	// Closer: walks the DAG, enqueueing children as parents
	// become resolved.
	seenWalked := make(map[string]struct{})

	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(5 * time.Millisecond)
		defer t.Stop()
		for {
			if ctx.Err() != nil {
				return
			}
			// Drain any newly-resolved MIDs and enqueue
			// their children.
			walked := 0
			mu.Lock()
			for k := range resolved {
				if _, seen := seenWalked[k]; seen {
					continue
				}
				seenWalked[k] = struct{}{}
				midStr := k
				mu.Unlock()
				if err := s.enqueueChildren(ctx, midStr, addEnqueued, pending); err != nil {
					return
				}
				mu.Lock()
				walked++
			}
			mu.Unlock()
			if allResolved() {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	}()
	// When the closer signals done, we close the pending channel
	// so the provider goroutines exit promptly.
	<-done
	close(pending)
	wg.Wait()

	// Final assembly.
	if !allResolved() {
		return nil, fmt.Errorf("memex session: not all blocks resolved")
	}
	resolver := dag.NewResolver(asBlockstore(s.cfg.Engine.bs))
	rc, err := resolver.Resolve(s.cfg.Root, nil)
	if err != nil {
		return nil, fmt.Errorf("memex session: resolve: %w", err)
	}
	return rc, nil
}

// enqueueChildren parses the block at midStr (which must be
// local) and enqueues any child MIDs not yet seen. It
// returns ctx.Err() if the context fires while pushing.
func (s *Session) enqueueChildren(ctx context.Context, midStr string, addEnqueued func(mid.MID) bool, pending chan<- mid.MID) error {
	id, err := mid.Parse(midStr)
	if err != nil {
		return nil // not a valid MID, nothing to walk
	}
	data, err := s.cfg.Engine.bs.Get(id)
	if err != nil {
		// Not local yet. The closer will come back to it
		// once the block arrives.
		return nil
	}
	var node membusspb.DAGNode
	if uerr := proto.Unmarshal(data, &node); uerr != nil || len(node.Links) == 0 {
		return nil
	}
	for _, ls := range node.Links {
		child, err := mid.Parse(ls)
		if err != nil {
			continue
		}
		if !addEnqueued(child) {
			continue
		}
		select {
		case pending <- child:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// runProvider opens a single Memex stream to provider, then
// runs a read loop and a write loop concurrently.
func (s *Session) runProvider(ctx context.Context, provider peer.AddrInfo, pending <-chan mid.MID, markResolved func(mid.MID)) error {
	stream, err := s.cfg.Engine.openStream(ctx, provider.ID)
	if err != nil {
		return fmt.Errorf("memex session: open %s: %w", provider.ID, err)
	}
	defer stream.Close()
	_ = stream.SetDeadline(time.Now().Add(DefaultPeerTimeout))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = s.readLoop(ctx, stream, markResolved)
	}()
	go func() {
		defer wg.Done()
		_ = s.writeLoop(ctx, stream, pending)
	}()
	wg.Wait()
	return nil
}

func (s *Session) readLoop(ctx context.Context, stream network.Stream, markResolved func(mid.MID)) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		buf := readFrame(stream)
		if buf == nil {
			return nil
		}
		var msg membusspb.MemexMessage
		if err := proto.Unmarshal(buf, &msg); err != nil {
			return fmt.Errorf("memex session: unmarshal: %w", err)
		}
		for _, b := range msg.Blocks {
			if b == nil || b.Mid == "" {
				continue
			}
			id, err := mid.Parse(b.Mid)
			if err != nil {
				continue
			}
			if err := s.cfg.Engine.bs.Put(id, b.Data); err != nil {
				// Hash mismatch: malicious or buggy
				// peer. Skip.
				continue
			}
			markResolved(id)
		}
		// Phase 19: persist any ObjectInfo metadata the
		// provider sent alongside the blocks. This is how
		// filename + MIME type travel across the network.
		if len(msg.ObjectInfos) > 0 {
			s.storeObjectInfos(msg.ObjectInfos)
		}
	}
}

// storeObjectInfos persists received ObjectInfo descriptors
// into the local store's meta namespace. It is best-effort;
// errors are silently ignored so a corrupt descriptor from
// a remote peer cannot break the session.
func (s *Session) storeObjectInfos(infos map[string]*membusspb.ObjectInfo) {
	type metaPutter interface {
		PutMeta(key string, value []byte) error
	}
	mp, ok := s.cfg.Engine.bs.(metaPutter)
	if !ok {
		return
	}
	for midStr, oi := range infos {
		if midStr == "" || oi == nil {
			continue
		}
		raw, err := json.Marshal(struct {
			Name     string `json:"name,omitempty"`
			MimeType string `json:"mime_type,omitempty"`
			Size     uint64 `json:"size,omitempty"`
		}{
			Name:     oi.Name,
			MimeType: oi.MimeType,
			Size:     oi.Size,
		})
		if err != nil {
			continue
		}
		_ = mp.PutMeta("obj/"+midStr, raw)
	}
}

func (s *Session) writeLoop(ctx context.Context, stream network.Stream, pending <-chan mid.MID) error {
	for {
		select {
		case m, ok := <-pending:
			if !ok {
				return nil
			}
			if err := writeFrame(stream, &membusspb.MemexMessage{Wants: []*membusspb.WantEntry{{
				Mid:           m.String(),
				SendDontHave: true,
			}}}); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Idle tick.
		}
	}
}

// asBlockstore adapts the engine's Blockstore into the
// dag.NewResolver interface.
func asBlockstore(b Blockstore) store.Blockstore {
	if s, ok := b.(store.Blockstore); ok {
		return s
	}
	return &memexBlockstoreAdapter{b}
}

type memexBlockstoreAdapter struct{ b Blockstore }

func (a *memexBlockstoreAdapter) Put(m mid.MID, data []byte) error { return a.b.Put(m, data) }
func (a *memexBlockstoreAdapter) Get(m mid.MID) ([]byte, error)    { return a.b.Get(m) }
func (a *memexBlockstoreAdapter) Has(m mid.MID) (bool, error)      { return a.b.Has(m) }
func (a *memexBlockstoreAdapter) Delete(m mid.MID) error           { return nil }
// PutMeta / GetMeta are not part of the narrower
// memex.Blockstore contract. They are added here so
// the adapter satisfies the (now larger) store.Blockstore
// interface that the dag.Resolver depends on. Reads
// return ErrNotFound (no metadata access from this
// adapter); writes are no-ops. In practice the
// engine always passes a real store.Blockstore
// (see asBlockstore's fast path), so these methods
// are only exercised in tests.
func (a *memexBlockstoreAdapter) PutMeta(key string, value []byte) error { return nil }
func (a *memexBlockstoreAdapter) GetMeta(key string) ([]byte, error) {
	return nil, store.ErrNotFound
}

// RetryConfig configures FetchWithBackoff's exponential retry
// schedule. Zero values fall back to sane defaults.
type RetryConfig struct {
	// Initial is the first retry delay. Default 100ms.
	Initial time.Duration
	// Max caps a single backoff sleep. Default 30s.
	Max time.Duration
	// Factor multiplies the previous delay after each failure.
	// Default 2.0.
	Factor float64
	// MaxAttempts bounds the retries. Default 4.
	MaxAttempts int
}

// DefaultRetryConfig returns the package-default retry schedule.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		Initial:     100 * time.Millisecond,
		Max:         30 * time.Second,
		Factor:      2.0,
		MaxAttempts: 4,
	}
}

// FetchWithBackoff invokes Fetch, retrying with exponential
// backoff when a transient failure is returned. A "transient
// failure" is any error other than context.Canceled,
// context.DeadlineExceeded, or a "not found" /
// ErrNotFound-style terminal error. The retry loop terminates
// when Fetch returns nil, a non-retryable error, or after
// cfg.MaxAttempts total attempts.
//
// The returned reader is the content of the most recent
// successful Fetch. Callers MUST Close it.
func (s *Session) FetchWithBackoff(ctx context.Context, cfg RetryConfig) (io.Reader, error) {
	if cfg.Initial <= 0 {
		cfg.Initial = 100 * time.Millisecond
	}
	if cfg.Max <= 0 {
		cfg.Max = 30 * time.Second
	}
	if cfg.Factor < 1 {
		cfg.Factor = 2.0
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 4
	}
	delay := cfg.Initial
	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		r, err := s.Fetch(ctx)
		if err == nil {
			return r, nil
		}
		lastErr = err
		// Terminal errors do not retry.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		if !isRetryableMemexErr(err) {
			return nil, err
		}
		if attempt == cfg.MaxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		delay = time.Duration(float64(delay) * cfg.Factor)
		if delay > cfg.Max {
			delay = cfg.Max
		}
	}
	return nil, fmt.Errorf("memex session: gave up after %d attempts: %w", cfg.MaxAttempts, lastErr)
}

// isRetryableMemexErr reports whether err is a transient
// failure (network error, partial resolution) that is worth
// retrying. We treat the "not all blocks resolved" error and
// any "open stream" / "dial" / "deadline" error as retryable.
func isRetryableMemexErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not all blocks resolved"):
		return true
	case strings.Contains(msg, "open "):
		return true
	case strings.Contains(msg, "context deadline"):
		return true
	case strings.Contains(msg, "no provider"):
		return false
	}
	return true
}

// selectPeersForMID applies the Phase 13 bloom filter
// optimization to a provider list. A provider whose
// stored filter says "definitely absent" for want is
// excluded. Providers for which the manager has no
// information are kept (the safe default).
//
// The returned slice is a fresh copy: callers may
// freely mutate it.
func (s *Session) selectPeersForMID(want mid.MID) []peer.AddrInfo {
	mgr := s.cfg.Engine.BloomManager()
	if mgr == nil {
		return s.cfg.Providers
	}
	return mgr.FilteredProviders(want, s.cfg.Providers)
}
