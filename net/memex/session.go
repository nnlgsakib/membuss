package memex

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/dag"
	"github.com/nnlgsakib/membuss/core/mid"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

// ProgressUpdate reports both block-level and byte-level progress
// during a Fetch call. BlocksResolved/BlocksTotal track DAG
// resolution. BytesDelivered/BytesTotal track content bytes
// written to the reader. Throughput is bytes/sec. ETA is
// estimated seconds remaining (0 if unknown).
type ProgressUpdate struct {
	BlocksResolved uint64
	BlocksTotal    uint64
	BytesDelivered uint64
	BytesTotal     uint64
	Throughput     float64
	ETA            float64
}

// SessionConfig configures a MemexSession.
type SessionConfig struct {
	Engine         *Engine
	Root           mid.MID
	Providers      []peer.AddrInfo
	ParallelPeers  int
	Timeout        time.Duration
	ProgressFn     func(update ProgressUpdate)
	ProviderFinder func(ctx context.Context, m mid.MID) ([]peer.AddrInfo, error)

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

type wantState struct {
	mid             mid.MID
	attempts        int
	triedProviders  map[peer.ID]struct{}
	currentProvider peer.ID
	lastSent        time.Time
}

type streamInfo struct {
	peerID peer.ID
	queue  *eventQueue
	stream network.Stream
}

// Session is a single in-flight retrieval. A Session drives
// one Fetch call; reuse by creating a new Session.
type Session struct {
	cfg SessionConfig

	mu              sync.Mutex
	enqueued        map[string]struct{}
	resolved        map[string]struct{}
	wantlist        map[string]mid.MID
	streams         []streamInfo
	wantStates      map[string]*wantState
	schedulerWakeCh chan struct{}

	provMu          sync.Mutex
	liveProviders   []peer.AddrInfo
	activeProviders map[peer.ID]struct{}
	failedProviders map[peer.ID]struct{}
	managerWakeCh   chan struct{}
	activeWG        *sync.WaitGroup

	// resolvedCh is a buffered channel used to wake the closer
	// goroutine immediately when a block is resolved, instead
	// of polling on a 5ms ticker.
	resolvedCh chan struct{}

	// walkerDone signals that the DAG walker goroutine has
	// finished writing content to the pipe. The closer
	// goroutine waits on this before exiting to ensure all
	// descendant blocks are enqueued while the walker is
	// still traversing the DAG.
	walkerDone chan struct{}
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
		cfg:             cfg,
		enqueued:        make(map[string]struct{}),
		resolved:        make(map[string]struct{}),
		wantlist:        make(map[string]mid.MID),
		wantStates:      make(map[string]*wantState),
		schedulerWakeCh: make(chan struct{}, 1),
		managerWakeCh:   make(chan struct{}, 1),
		resolvedCh:      make(chan struct{}, 1),
		walkerDone:      make(chan struct{}),
	}, nil
}

// countingWriter wraps an io.Writer and tracks total bytes
// written. Used to measure byte-level progress during streaming
// assembly.
type countingWriter struct {
	w     io.Writer
	n     uint64
	start time.Time
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += uint64(n)
	return n, err
}

// Progress returns bytes written and elapsed time.
func (cw *countingWriter) Progress() (bytes uint64, elapsed time.Duration) {
	return cw.n, time.Since(cw.start)
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

	s.mu.Lock()
	s.enqueued = make(map[string]struct{})
	s.resolved = make(map[string]struct{})
	s.wantlist = make(map[string]mid.MID)
	s.wantStates = make(map[string]*wantState)
	select {
	case <-s.schedulerWakeCh:
	default:
	}
	s.streams = nil
	select {
	case <-s.resolvedCh:
	default:
	}
	s.mu.Unlock()

	filtered := s.selectPeersForMID(s.cfg.Root)
	if len(filtered) == 0 {
		return nil, errors.New("memex session: no provider after bloom filter")
	}

	s.provMu.Lock()
	s.liveProviders = filtered
	s.activeProviders = make(map[peer.ID]struct{})
	s.failedProviders = make(map[peer.ID]struct{})
	s.activeWG = &sync.WaitGroup{}
	select {
	case <-s.managerWakeCh:
	default:
	}
	s.provMu.Unlock()

	fctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.checkAndEnqueue(fctx, s.cfg.Root)

	go s.schedulerLoop(fctx)

	// Start the provider manager loop
	var managerWG sync.WaitGroup
	managerWG.Add(1)
	go func() {
		defer managerWG.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-fctx.Done():
				return
			case <-ticker.C:
				s.manageProviders(fctx, fanout)
			case <-s.managerWakeCh:
				s.manageProviders(fctx, fanout)
			}
		}
	}()

	// Wake up provider manager to start the initial providers
	s.wakeProviderManager()

	seenWalked := make(map[string]struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
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
			case <-s.resolvedCh:
			}
		}
	}()

	<-done
	cancel()
	managerWG.Wait()
	s.provMu.Lock()
	activeWG := s.activeWG
	s.provMu.Unlock()
	if activeWG != nil {
		activeWG.Wait()
	}

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

// FetchStream is like Fetch but streams content as blocks
// arrive. The caller can start reading before all blocks are
// fetched. Blocks that haven't arrived yet cause the walker
// to block until they do — providers fetch them concurrently.
// Progress is reported via ProgressFn with byte-level metrics.
func (s *Session) FetchStream(ctx context.Context) (io.Reader, error) {
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

	s.mu.Lock()
	s.enqueued = make(map[string]struct{})
	s.resolved = make(map[string]struct{})
	s.wantlist = make(map[string]mid.MID)
	s.wantStates = make(map[string]*wantState)
	select {
	case <-s.schedulerWakeCh:
	default:
	}
	s.streams = nil
	select {
	case <-s.resolvedCh:
	default:
	}
	select {
	case <-s.walkerDone:
	default:
	}
	s.mu.Unlock()

	filtered := s.selectPeersForMID(s.cfg.Root)
	if len(filtered) == 0 {
		return nil, errors.New("memex session: no provider after bloom filter")
	}

	s.provMu.Lock()
	s.liveProviders = filtered
	s.activeProviders = make(map[peer.ID]struct{})
	s.failedProviders = make(map[peer.ID]struct{})
	s.activeWG = &sync.WaitGroup{}
	select {
	case <-s.managerWakeCh:
	default:
	}
	s.provMu.Unlock()

	fctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.checkAndEnqueue(fctx, s.cfg.Root)

	go s.schedulerLoop(fctx)

	// Start the provider manager loop
	var managerWG sync.WaitGroup
	managerWG.Add(1)
	go func() {
		defer managerWG.Done()
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-fctx.Done():
				return
			case <-ticker.C:
				s.manageProviders(fctx, fanout)
			case <-s.managerWakeCh:
				s.manageProviders(fctx, fanout)
			}
		}
	}()

	// Wake up provider manager to start the initial providers
	s.wakeProviderManager()

	seenWalked := make(map[string]struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
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
				select {
				case <-fctx.Done():
					return
				case <-s.walkerDone:
					return
				}
			}
			select {
			case <-fctx.Done():
				return
			case <-s.walkerDone:
				continue
			case <-s.resolvedCh:
			}
		}
	}()

	pipeReader, pipeWriter := io.Pipe()
	cw := &countingWriter{w: pipeWriter, start: time.Now()}

	var progressDone chan struct{}
	if s.cfg.ProgressFn != nil {
		progressDone = make(chan struct{})
		go func() {
			defer close(progressDone)
			t := time.NewTicker(100 * time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-fctx.Done():
					return
				case <-s.walkerDone:
					return
				case <-t.C:
					bytes, elapsed := cw.Progress()
					s.mu.Lock()
					resolved := uint64(len(s.resolved))
					total := uint64(len(s.enqueued))
					s.mu.Unlock()
					var throughput float64
					if elapsed.Seconds() > 0 {
						throughput = float64(bytes) / elapsed.Seconds()
					}
					s.cfg.ProgressFn(ProgressUpdate{
						BlocksResolved: resolved,
						BlocksTotal:    total,
						BytesDelivered: bytes,
						Throughput:     throughput,
					})
				}
			}
		}()
	}

	go func() {
		resolver := dag.NewResolver(asBlockstore(s.cfg.Engine.bs))
		_, err := resolver.Resolve(s.cfg.Root, nil)
		if err != nil {
			_ = pipeWriter.CloseWithError(err)
		} else {
			_ = pipeWriter.Close()
		}
		close(s.walkerDone)
	}()

	<-s.walkerDone
	cancel()
	managerWG.Wait()
	s.provMu.Lock()
	activeWG := s.activeWG
	s.provMu.Unlock()
	if activeWG != nil {
		activeWG.Wait()
	}

	if s.cfg.ProgressFn != nil {
		bytes, elapsed := cw.Progress()
		s.mu.Lock()
		resolved := uint64(len(s.resolved))
		total := uint64(len(s.enqueued))
		s.mu.Unlock()
		var throughput float64
		if elapsed.Seconds() > 0 {
			throughput = float64(bytes) / elapsed.Seconds()
		}
		s.cfg.ProgressFn(ProgressUpdate{
			BlocksResolved: resolved,
			BlocksTotal:    total,
			BytesDelivered: bytes,
			BytesTotal:     bytes,
			Throughput:     throughput,
		})
	}
	if progressDone != nil {
		<-progressDone
	}

	return pipeReader, nil
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
			s.cfg.ProgressFn(ProgressUpdate{
				BlocksResolved: uint64(len(s.resolved)),
				BlocksTotal:    uint64(len(s.enqueued)),
			})
		}
	} else {
		s.wantlist[midStr] = id
		s.wantStates[midStr] = &wantState{
			mid:            id,
			triedProviders: make(map[peer.ID]struct{}),
		}
		s.wakeScheduler()
	}
}

func (s *Session) markResolved(id mid.MID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	midStr := id.String()
	ws, ok := s.wantStates[midStr]
	if ok && ws.currentProvider != "" {
		s.cfg.Engine.RecordPeerSuccess(ws.currentProvider, time.Since(ws.lastSent))
	}

	s.resolved[midStr] = struct{}{}
	delete(s.wantlist, midStr)
	delete(s.wantStates, midStr)

	if s.cfg.ProgressFn != nil {
		s.cfg.ProgressFn(ProgressUpdate{
			BlocksResolved: uint64(len(s.resolved)),
			BlocksTotal:    uint64(len(s.enqueued)),
		})
	}

	// Notify active slots to cancel the want
	for _, st := range s.streams {
		st.queue.Push(sessionEvent{isCancel: true, mid: id})
	}

	// Wake the closer goroutine immediately so it can
	// enqueue children of the newly resolved block.
	select {
	case s.resolvedCh <- struct{}{}:
	default:
	}
}

func (s *Session) markFailed(id mid.MID, peerID peer.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	midStr := id.String()
	ws, ok := s.wantStates[midStr]
	if ok && ws.currentProvider == peerID {
		s.cfg.Engine.RecordPeerFailure(peerID)
		ws.triedProviders[peerID] = struct{}{}
		ws.currentProvider = ""

		// Send cancel to this provider stream's queue so writeLoop can cancel it and free capacity
		for _, st := range s.streams {
			if st.peerID == peerID {
				st.queue.Push(sessionEvent{isCancel: true, mid: id})
			}
		}
		s.wakeScheduler()
	}
}

func (s *Session) wakeScheduler() {
	select {
	case s.schedulerWakeCh <- struct{}{}:
	default:
	}
}

func (s *Session) handleProviderDisconnect(peerID peer.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ws := range s.wantStates {
		if ws.currentProvider == peerID {
			ws.triedProviders[peerID] = struct{}{}
			ws.currentProvider = ""
		}
	}
	s.wakeScheduler()
}

// allActiveProvidersUseless reports true when every active stream's peer
// has already been tried (returned DONT_HAVE or timed out) for ALL
// pending wants. This means no active provider can make further progress.
// Caller must hold s.mu.
func (s *Session) allActiveProvidersUseless() bool {
	if len(s.wantStates) == 0 {
		return false
	}
	if len(s.streams) == 0 {
		return false
	}
	for _, st := range s.streams {
		useless := true
		for _, ws := range s.wantStates {
			if _, tried := ws.triedProviders[st.peerID]; !tried {
				useless = false
				break
			}
		}
		if !useless {
			return false
		}
	}
	return true
}

func (s *Session) wakeProviderManager() {
	select {
	case s.managerWakeCh <- struct{}{}:
	default:
	}
}

// closeUselessProviders disconnects any active provider that has been
// tried (returned DONT_HAVE or timed out) for ALL pending wants.
// This frees up a slot so the provider manager can discover and start
// a replacement provider via ProviderFinder or the live provider list.
func (s *Session) closeUselessProviders() {
	s.mu.Lock()
	if len(s.wantStates) == 0 {
		s.mu.Unlock()
		return
	}
	var toReset []network.Stream
	for _, st := range s.streams {
		allTried := true
		for _, ws := range s.wantStates {
			if _, tried := ws.triedProviders[st.peerID]; !tried {
				allTried = false
				break
			}
		}
		if allTried {
			toReset = append(toReset, st.stream)
		}
	}
	s.mu.Unlock()

	for _, stream := range toReset {
		if stream != nil {
			_ = stream.Reset()
		}
	}
}

func (s *Session) manageProviders(ctx context.Context, fanout int) {
	// Phase 1: Check if we need discovery. This runs regardless of active
	// provider count. If all active providers are useless (tried for all
	// pending wants), we need new providers even if we're at fanout.
	needDiscovery := false
	s.mu.Lock()
	hasPending := len(s.wantStates) > 0
	allUseless := hasPending && s.allActiveProvidersUseless()
	s.mu.Unlock()

	if allUseless && s.cfg.ProviderFinder != nil {
		needDiscovery = true
	}

	s.provMu.Lock()
	activeCount := len(s.activeProviders)

	// Phase 2: Close useless providers to free slots for replacements.
	if allUseless {
		s.provMu.Unlock()
		s.closeUselessProviders()
		s.provMu.Lock()
	}

	// Phase 3: Start new providers from candidates not yet active/failed.
	needed := fanout - len(s.activeProviders)
	if needed > 0 {
		var toStart []peer.AddrInfo
		for _, p := range s.liveProviders {
			if _, active := s.activeProviders[p.ID]; active {
				continue
			}
			if _, failed := s.failedProviders[p.ID]; failed {
				continue
			}
			toStart = append(toStart, p)
			if len(toStart) >= needed {
				break
			}
		}

		for _, p := range toStart {
			s.activeProviders[p.ID] = struct{}{}
			if s.activeWG != nil {
				s.activeWG.Add(1)
			}
			go func(prov peer.AddrInfo) {
				defer func() {
					if s.activeWG != nil {
						s.activeWG.Done()
					}
					s.provMu.Lock()
					delete(s.activeProviders, prov.ID)
					s.failedProviders[prov.ID] = struct{}{}
					s.provMu.Unlock()
					s.wakeProviderManager()
				}()
				_ = s.runProvider(ctx, prov)
			}(p)
		}
	}

	// Also trigger discovery when at capacity but we have pending wants
	// and no candidates can serve them.
	if !needDiscovery && activeCount >= fanout && hasPending && s.cfg.ProviderFinder != nil {
		s.mu.Lock()
		needDiscovery = s.allActiveProvidersUseless()
		s.mu.Unlock()
	}

	// Also trigger discovery when below fanout.
	if !needDiscovery && len(s.activeProviders) < fanout && hasPending && s.cfg.ProviderFinder != nil {
		needDiscovery = true
	}

	s.provMu.Unlock()

	// Phase 4: Trigger async DHT/peer exchange discovery.
	if needDiscovery {
		searchMID := s.cfg.Root
		s.mu.Lock()
		for _, ws := range s.wantStates {
			searchMID = ws.mid
			break
		}
		s.mu.Unlock()

		go func(m mid.MID) {
			discCtx, discCancel := context.WithTimeout(ctx, 5*time.Second)
			defer discCancel()
			newProvs, err := s.cfg.ProviderFinder(discCtx, m)
			if err != nil || len(newProvs) == 0 {
				return
			}

			s.provMu.Lock()
			defer s.provMu.Unlock()

			for _, np := range newProvs {
				exists := false
				for _, lp := range s.liveProviders {
					if lp.ID == np.ID {
						exists = true
						break
					}
				}
				if !exists {
					s.liveProviders = append(s.liveProviders, np)
				}
			}
			s.wakeProviderManager()
		}(searchMID)
	}
}

func (s *Session) schedulerLoop(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.scheduleWants()
			s.closeUselessProviders()
		case <-s.schedulerWakeCh:
			s.scheduleWants()
		}
	}
}

func (s *Session) scheduleWants() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	const maxBlockAttempts = 10
	const blockTimeout = 3 * time.Second

	for midStr, ws := range s.wantStates {
		if _, ok := s.resolved[midStr]; ok {
			delete(s.wantStates, midStr)
			continue
		}

		needsScheduling := false
		if ws.currentProvider == "" {
			needsScheduling = true
		} else if now.Sub(ws.lastSent) > blockTimeout {
			// Timeout: mark current provider as tried (failed)
			ws.triedProviders[ws.currentProvider] = struct{}{}
			s.cfg.Engine.RecordPeerFailure(ws.currentProvider)
			ws.currentProvider = ""
			needsScheduling = true
		}

		if !needsScheduling {
			continue
		}

		candidates := s.selectPeersForMID(ws.mid)
		if len(candidates) == 0 {
			candidates = s.cfg.Providers
		}

		// Also include providers discovered via ProviderFinder (in liveProviders).
		s.provMu.Lock()
		seenCands := make(map[peer.ID]struct{})
		for _, c := range candidates {
			seenCands[c.ID] = struct{}{}
		}
		for _, lp := range s.liveProviders {
			if _, already := seenCands[lp.ID]; !already {
				candidates = append(candidates, lp)
			}
		}
		s.provMu.Unlock()

		type activeCandidate struct {
			peerID peer.ID
			queue  *eventQueue
		}
		var activeList []activeCandidate
		for _, st := range s.streams {
			isCand := false
			for _, c := range candidates {
				if c.ID == st.peerID {
					isCand = true
					break
				}
			}
			if isCand {
				if _, tried := ws.triedProviders[st.peerID]; !tried {
					activeList = append(activeList, activeCandidate{peerID: st.peerID, queue: st.queue})
				}
			}
		}

		if len(activeList) == 0 {
			// No untried active stream can serve this want.
			// If we've hit the attempt limit, reset triedProviders
			// so that when new providers arrive, they can be tried.
			if ws.attempts >= maxBlockAttempts {
				ws.triedProviders = make(map[peer.ID]struct{})
				ws.attempts = 0
			}
			// Wake the provider manager so it can discover replacements.
			s.mu.Unlock()
			s.wakeProviderManager()
			s.mu.Lock()
			continue
		}

		if ws.attempts >= maxBlockAttempts {
			continue
		}

		var selected activeCandidate
		maxEffectiveScore := -1.0
		for _, ac := range activeList {
			load := 0
			for _, otherWs := range s.wantStates {
				if otherWs.currentProvider == ac.peerID {
					load++
				}
			}
			score := s.cfg.Engine.PeerScore(ac.peerID)
			effectiveScore := score / float64(load+1)
			if maxEffectiveScore == -1.0 || effectiveScore > maxEffectiveScore {
				maxEffectiveScore = effectiveScore
				selected = ac
			}
		}

		ws.currentProvider = selected.peerID
		ws.lastSent = now
		ws.attempts++

		selected.queue.Push(sessionEvent{isCancel: false, mid: ws.mid})
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
	// Register queue for this provider stream
	queue := newEventQueue()
	s.mu.Lock()
	s.streams = append(s.streams, streamInfo{peerID: provider.ID, queue: queue, stream: stream})
	s.wakeScheduler()
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
		for i, st := range s.streams {
			if st.queue == queue {
				s.streams = append(s.streams[:i], s.streams[i+1:]...)
				break
			}
		}
		s.mu.Unlock()
		queue.Close()
		s.handleProviderDisconnect(provider.ID)
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
		_ = s.writeLoop(pctx, stream, queue, ps)
	}()
	swg.Wait()
	return nil
}
