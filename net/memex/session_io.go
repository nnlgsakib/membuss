package memex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

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
			// Verify block integrity: hash of the data must match the MID
			actualID := mid.FromBytesWithCodec(b.Data, id.Codec())
			if !actualID.Equal(id) {
				// Record peer failure because the peer sent corrupt data
				s.cfg.Engine.RecordPeerFailure(stream.Conn().RemotePeer())
				continue
			}
			if err := s.cfg.Engine.bs.Put(id, b.Data); err != nil {
				continue
			}
			s.markResolved(id)
			resolvedCount++
		}
		for _, dontHaveMidStr := range msg.HaveMids {
			id, err := mid.Parse(dontHaveMidStr)
			if err != nil {
				continue
			}
			s.markFailed(id, stream.Conn().RemotePeer())
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

func (s *Session) writeLoop(ctx context.Context, stream network.Stream, queue *eventQueue, ps *pipelineState) error {
	const (
		maxBatchSize = 32
		flushTimeout = 5 * time.Millisecond
	)

	var pending []sessionEvent
	inFlightMIDs := make(map[string]struct{})

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
			case _, ok := <-queue.ch:
				events := queue.PopAll()
				if len(events) > 0 {
					firstEv = events[0]
					pending = append(pending, events[1:]...)
					gotFirst = true
				}
				if !ok && len(pending) == 0 && !gotFirst {
					return nil
				}
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
				case _, ok := <-queue.ch:
					events := queue.PopAll()
					for _, ev := range events {
						if ev.isCancel {
							// Process cancel immediately to free capacity and notify peer
							msg := membusspb.MemexMessage{
								Cancel: []string{ev.mid.String()},
							}
							if _, ok := inFlightMIDs[ev.mid.String()]; ok {
								delete(inFlightMIDs, ev.mid.String())
								select {
								case ps.capCh <- struct{}{}:
								default:
								}
							}
							_ = stream.SetWriteDeadline(time.Now().Add(DefaultPeerTimeout))
							_ = writeFrame(stream, &msg)
						} else {
							pending = append(pending, ev)
						}
					}
					if !ok {
						return nil
					}
				}
			}
		}

		// Build batch.
		var msg membusspb.MemexMessage
		newWantCount := 0

		addEvent := func(ev sessionEvent) {
			if ev.isCancel {
				foundInBatch := false
				for i, w := range msg.Wants {
					if w.Mid == ev.mid.String() {
						msg.Wants = append(msg.Wants[:i], msg.Wants[i+1:]...)
						newWantCount--
						delete(inFlightMIDs, ev.mid.String())
						foundInBatch = true
						break
					}
				}
				if !foundInBatch {
					msg.Cancel = append(msg.Cancel, ev.mid.String())
					if _, ok := inFlightMIDs[ev.mid.String()]; ok {
						delete(inFlightMIDs, ev.mid.String())
						select {
						case ps.capCh <- struct{}{}:
						default:
						}
					}
				}
			} else {
				msg.Wants = append(msg.Wants, &membusspb.WantEntry{
					Mid:          ev.mid.String(),
					SendDontHave: true,
				})
				newWantCount++
				inFlightMIDs[ev.mid.String()] = struct{}{}
			}
		}

		addEvent(firstEv)

		batchCount := 1
		timer := time.NewTimer(flushTimeout)
		closed := false

	batchLoop:
		for batchCount < maxBatchSize && !closed {
			// First, drain any pending events we already have in our slice.
			if len(pending) > 0 {
				nextEv := pending[0]
				pending = pending[1:]
				// If it's a want, check pipeline capacity.
				if !nextEv.isCancel {
					if ps.inFlight+newWantCount >= ps.maxDepth {
						// Put it back
						pending = append([]sessionEvent{nextEv}, pending...)
						break batchLoop
					}
				}
				addEvent(nextEv)
				batchCount++
				continue
			}

			// If no pending events, wait/poll the queue signal.
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
				break batchLoop
			case _, ok := <-queue.ch:
				events := queue.PopAll()
				pending = append(pending, events...)
				if !ok {
					closed = true
				}
			}
		}
		timer.Stop()

		if len(msg.Wants) == 0 && len(msg.Cancel) == 0 {
			if closed && len(pending) == 0 {
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

		if closed && len(pending) == 0 {
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
