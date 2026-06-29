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
	ProgressFn    func(blocksResolved, blocksTotal uint64)

	// PipelineDepth controls the maximum number of in-flight
	// want requests per provider stream. When the pipeline is
	// full, writeLoop waits for readLoop to resolve or cancel
	// requests before sending more. Zero uses DefaultPipelineDepth.
	PipelineDepth int

	// StreamsPerProvider controls how many concurrent libp2p
	// streams are opened to each provider peer. Multiple
	// streams allow true parallel block transfers — while one
	// stream is receiving a large block, other streams can
	// transfer different blocks concurrently. Higher values
	// increase throughput at the cost of more open streams.
	// Zero uses DefaultStreamsPerProvider.
	StreamsPerProvider int
}

// pipelineState tracks in-flight request count for one provider
// stream and provides a channel for writeLoop to wait when the
// pipeline is full.
type pipelineState struct {
	inFlight int
	maxDepth int
	// capCh is a buffered channel used as a semaphore. readLoop
	// sends on it when blocks are resolved (freeing capacity).
	// writeLoop receives from it to know when to send more.
	capCh chan struct{}
}

type sessionEvent struct {
	isCancel bool
	mid      mid.MID
}

// Session is a single in-flight retrieval. A Session drives
// one Fetch call; reuse by creating a new Session.
type Session struct {
	cfg SessionConfig

	mu          sync.Mutex
	enqueued    map[string]struct{}
	resolved    map[string]struct{}
	wantlist    map[string]mid.MID
	streamChans []chan sessionEvent
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
	return &Session{
		cfg:      cfg,
		enqueued: make(map[string]struct{}),
		resolved: make(map[string]struct{}),
		wantlist: make(map[string]mid.MID),
	}, nil
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

	// Re-initialize state for this Fetch attempt
	s.mu.Lock()
	s.enqueued = make(map[string]struct{})
	s.resolved = make(map[string]struct{})
	s.wantlist = make(map[string]mid.MID)
	s.streamChans = nil
	s.mu.Unlock()

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

	fctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Seed with the root.
	s.checkAndEnqueue(fctx, s.cfg.Root)

	// runProvider opens one stream per peer slot. Each
	// stream runs a reader and a writer until the session
	// ends.
	var wg sync.WaitGroup
	wg.Add(fanout)
	for i := 0; i < fanout; i++ {
		provider := liveProviders[i]
		go func() {
			defer wg.Done()
			_ = s.runProvider(fctx, provider)
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
			var toWalk []string
			s.mu.Lock()
			for k := range s.resolved {
				if _, seen := seenWalked[k]; seen {
					continue
				}
				seenWalked[k] = struct{}{}
				toWalk = append(toWalk, k)
			}
			s.mu.Unlock()

			for _, midStr := range toWalk {
				if err := s.enqueueChildren(ctx, midStr); err != nil {
					return
				}
			}

			s.mu.Lock()
			hasUnwalked := false
			for k := range s.resolved {
				if _, seen := seenWalked[k]; !seen {
					hasUnwalked = true
					break
				}
			}
			allRes := len(s.enqueued) == len(s.resolved)
			s.mu.Unlock()

			if allRes && !hasUnwalked {
				return
			}
			select {
			case <-fctx.Done():
				return
			case <-t.C:
			}
		}
	}()

	<-done
	cancel()
	wg.Wait()

	// Final assembly.
	if !s.allResolved() {
		return nil, errors.New("memex session: not all blocks resolved")
	}
	resolver := dag.NewResolver(asBlockstore(s.cfg.Engine.bs))
	rc, err := resolver.Resolve(s.cfg.Root, nil)
	if err != nil {
		return nil, fmt.Errorf("memex session: resolve: %w", err)
	}
	return rc, nil
}

// checkAndEnqueue checks if the given block is already locally present,
// and if not, puts it in the wantlist to be fetched by the active stream loops.
func (s *Session) checkAndEnqueue(ctx context.Context, id mid.MID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	midStr := id.String()
	if _, ok := s.enqueued[midStr]; ok {
		return
	}
	s.enqueued[midStr] = struct{}{}

	has, err := s.cfg.Engine.bs.Has(id)
	if err == nil && has {
		s.resolved[midStr] = struct{}{}
		if s.cfg.ProgressFn != nil {
			s.cfg.ProgressFn(uint64(len(s.resolved)), uint64(len(s.enqueued)))
		}
	} else {
		s.wantlist[midStr] = id
		// Notify active slots about the new want
		for _, ch := range s.streamChans {
			select {
			case ch <- sessionEvent{isCancel: false, mid: id}:
			default:
			}
		}
	}
}

func (s *Session) markResolved(id mid.MID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	midStr := id.String()
	s.resolved[midStr] = struct{}{}
	delete(s.wantlist, midStr)

	if s.cfg.ProgressFn != nil {
		s.cfg.ProgressFn(uint64(len(s.resolved)), uint64(len(s.enqueued)))
	}

	// Notify active slots to cancel the want
	for _, ch := range s.streamChans {
		select {
		case ch <- sessionEvent{isCancel: true, mid: id}:
		default:
		}
	}
}

func (s *Session) allResolved() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.enqueued) != len(s.resolved) {
		return false
	}
	for k := range s.enqueued {
		if _, ok := s.resolved[k]; !ok {
			return false
		}
	}
	return true
}

// enqueueChildren parses the block at midStr (which must be
// local) and enqueues any child MIDs not yet seen. It
// returns ctx.Err() if the context fires while pushing.
func (s *Session) enqueueChildren(ctx context.Context, midStr string) error {
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

	var childMIDs []mid.MID

	if id.Codec() == mid.CodecMemFS {
		var node membusspb.MemFSNode
		if uerr := proto.Unmarshal(data, &node); uerr == nil {
			switch node.Type {
			case membusspb.MemFSType_FILE:
				for _, b := range node.Blocks {
					if b == nil || len(b.Mid) == 0 {
						continue
					}
					var codec uint64 = mid.CodecMemFS
					if b.Size > 0 {
						codec = mid.CodecRaw
					}
					child, err := mid.FromMultihash(codec, b.Mid)
					if err == nil {
						childMIDs = append(childMIDs, child)
					}
				}
			case membusspb.MemFSType_DIR:
				for _, e := range node.Entries {
					if e == nil || len(e.Mid) == 0 {
						continue
					}
					var codec uint64 = mid.CodecMemFS
					if e.Type == membusspb.MemFSType_RAW {
						codec = mid.CodecRaw
					}
					child, err := mid.FromMultihash(codec, e.Mid)
					if err == nil {
						childMIDs = append(childMIDs, child)
					}
				}
			}
		}
	} else {
		var node membusspb.DAGNode
		if uerr := proto.Unmarshal(data, &node); uerr == nil && len(node.Links) > 0 {
			for _, ls := range node.Links {
				child, err := mid.Parse(ls)
				if err == nil {
					childMIDs = append(childMIDs, child)
				}
			}
		}
	}

	for _, child := range childMIDs {
		s.checkAndEnqueue(ctx, child)
	}
	return nil
}

// runProvider opens one or more Memex streams to provider
// (controlled by StreamsPerProvider) and runs a read/write
// loop pair on each stream concurrently. Multiple streams
// allow true parallel block transfers: while one stream is
// receiving a large block, other streams can transfer
// different blocks concurrently.
func (s *Session) runProvider(ctx context.Context, provider peer.AddrInfo) error {
	streamsPerPeer := s.cfg.StreamsPerProvider
	if streamsPerPeer <= 0 {
		streamsPerPeer = DefaultStreamsPerProvider
	}
	if streamsPerPeer > MaxStreamsPerProvider {
		streamsPerPeer = MaxStreamsPerProvider
	}

	var wg sync.WaitGroup
	wg.Add(streamsPerPeer)
	for i := 0; i < streamsPerPeer; i++ {
		go func() {
			defer wg.Done()
			_ = s.runStream(ctx, provider, i)
		}()
	}
	wg.Wait()
	return nil
}

// runStream opens a single Memex stream to provider and runs
// a read loop and a write loop concurrently. streamIdx is
// used for logging/diagnostics only.
func (s *Session) runStream(ctx context.Context, provider peer.AddrInfo, streamIdx int) error {
	stream, err := s.cfg.Engine.openStream(ctx, provider.ID)
	type dialNotifier interface {
		NotifyDialResult(peer.ID, error)
	}
	if dn, ok := s.cfg.Engine.host.(dialNotifier); ok {
		dn.NotifyDialResult(provider.ID, err)
	}
	if err != nil {
		return fmt.Errorf("memex session: open %s stream %d: %w", provider.ID, streamIdx, err)
	}
	defer stream.Close()

	// Register channel for this provider stream
	eventChan := make(chan sessionEvent, 1024)
	s.mu.Lock()
	s.streamChans = append(s.streamChans, eventChan)
	// Seed the worker with all current active wants
	for _, m := range s.wantlist {
		eventChan <- sessionEvent{isCancel: false, mid: m}
	}
	s.mu.Unlock()

	// Create pipeline state for this stream.
	depth := s.cfg.PipelineDepth
	if depth <= 0 {
		depth = DefaultPipelineDepth
	}
	ps := &pipelineState{
		maxDepth: depth,
		capCh:    make(chan struct{}, depth),
	}

	defer func() {
		s.mu.Lock()
		for i, ch := range s.streamChans {
			if ch == eventChan {
				s.streamChans = append(s.streamChans[:i], s.streamChans[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
		close(eventChan)
	}()

	pctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var swg sync.WaitGroup
	swg.Add(2)
	go func() {
		defer swg.Done()
		defer cancel()
		_ = s.readLoop(pctx, stream, ps)
	}()
	go func() {
		defer swg.Done()
		defer cancel()
		_ = s.writeLoop(pctx, stream, eventChan, ps)
	}()
	swg.Wait()
	return nil
}

func (s *Session) readLoop(ctx context.Context, stream network.Stream, ps *pipelineState) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		_ = stream.SetReadDeadline(time.Now().Add(DefaultPeerTimeout))
		buf := readFrame(stream)
		if buf == nil {
			return nil
		}
		var msg membusspb.MemexMessage
		if err := proto.Unmarshal(buf, &msg); err != nil {
			return fmt.Errorf("memex session: unmarshal: %w", err)
		}
		resolvedCount := 0
		for _, b := range msg.Blocks {
			if b == nil || b.Mid == "" {
				continue
			}
			id, err := mid.Parse(b.Mid)
			if err != nil {
				continue
			}
			if err := s.cfg.Engine.bs.Put(id, b.Data); err != nil {
				continue
			}
			s.markResolved(id)
			resolvedCount++
		}
		// Signal writeLoop that capacity opened up.
		for i := 0; i < resolvedCount; i++ {
			select {
			case ps.capCh <- struct{}{}:
			default:
			}
		}
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

func (s *Session) writeLoop(ctx context.Context, stream network.Stream, eventChan <-chan sessionEvent, ps *pipelineState) error {
	const (
		maxBatchSize = 32
		flushTimeout = 5 * time.Millisecond
	)

	var pending []sessionEvent

	for {
		// Drain pending events first.
		var firstEv sessionEvent
		var gotFirst bool
		if len(pending) > 0 {
			firstEv = pending[0]
			pending = pending[1:]
			gotFirst = true
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case ev, ok := <-eventChan:
				if !ok {
					return nil
				}
				firstEv = ev
				gotFirst = true
			}
		}

		if !gotFirst {
			continue
		}

		// Wait for pipeline capacity before sending wants.
		if !firstEv.isCancel {
			for ps.inFlight >= ps.maxDepth {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-ps.capCh:
					ps.inFlight--
				case ev, ok := <-eventChan:
					if !ok {
						return nil
					}
					pending = append(pending, ev)
				}
			}
		}

		// Build batch.
		var msg membusspb.MemexMessage
		newWantCount := 0

		addEvent := func(ev sessionEvent) {
			if ev.isCancel {
				msg.Cancel = append(msg.Cancel, ev.mid.String())
			} else {
				msg.Wants = append(msg.Wants, &membusspb.WantEntry{
					Mid:          ev.mid.String(),
					SendDontHave: true,
				})
				newWantCount++
			}
		}

		addEvent(firstEv)

		batchCount := 1
		timer := time.NewTimer(flushTimeout)
		closed := false

	batchLoop:
		for batchCount < maxBatchSize && !closed {
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				break batchLoop
			case nextEv, nextOk := <-eventChan:
				if !nextOk {
					closed = true
					break batchLoop
				}

				// If it's a want, check pipeline capacity.
				if !nextEv.isCancel {
					if ps.inFlight+newWantCount >= ps.maxDepth {
						pending = append(pending, nextEv)
						break batchLoop
					}
				}

				addEvent(nextEv)
				batchCount++
			}
		}
		timer.Stop()

		if len(msg.Wants) == 0 && len(msg.Cancel) == 0 {
			if closed {
				return nil
			}
			continue
		}

		_ = stream.SetWriteDeadline(time.Now().Add(DefaultPeerTimeout))
		if err := writeFrame(stream, &msg); err != nil {
			return err
		}

		// Record in-flight wants.
		ps.inFlight += newWantCount

		if closed {
			return nil
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
// retrying. We treat the "not all blocks resolved" error,
// libp2p stream/connection errors, and context deadline
// errors as retryable.
func isRetryableMemexErr(err error) bool {
	if err == nil {
		return false
	}
	// Check for specific libp2p error types.
	if errors.Is(err, network.ErrReset) {
		return true
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not all blocks resolved"):
		return true
	case strings.Contains(msg, "open stream"):
		return true
	case strings.Contains(msg, "context deadline"):
		return true
	case strings.Contains(msg, "connection refused"):
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
