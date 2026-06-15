// Phase 11: Mem-Herald relay announcer.
//
// Anchor nodes and any node with RelayService=true re-publish
// their presence under the DHT's /membuss/relays/v1 key on
// every ReprovideInterval. This keeps the relay list fresh in
// the face of DHT churn and gives freshly bootstrapping nodes
// a non-empty candidate set for AutoRelay.
//
// The announcer is intentionally tiny: a single ticker
// goroutine. Republish errors are logged and swallowed; the
// next tick will try again. The announcer does not advertise
// other peers' addresses; it only advertises the local node.
package herald

import (
	"context"
	"errors"
	"sync"
	"time"

)

// RelayAnnouncer periodically publishes the local node to
// the DHT's relay list. The lifecycle is Start/Stop; Start
// also fires one immediate publish so the node appears in
// the relay list right at startup.
type RelayAnnouncer struct {
	// DHT is the local DHT facade. Required.
	DHT RelayPublisher
	// Interval is the time between republishes. Zero
	// defaults to 12 hours (same as the regular herald).
	Interval time.Duration
	// Now overrides the wall clock for tests. Default is
	// time.Now.
	Now func() time.Time
	// Logger is an optional structured logger; nil means
	// silent. The daemon wires its slog logger here.
	Logger AnnouncerLogger
}

// RelayPublisher is the slice of *dht.MemDHT that the relay
// announcer needs. Defining the interface here (rather than
// importing the concrete type) keeps the announcer decoupled
// from the DHT package and trivial to test with a fake.
type RelayPublisher interface {
	PublishAsRelay(ctx context.Context) error
}

// AnnouncerLogger is the minimal logging surface the
// announcer needs. *slog.Logger satisfies this.
type AnnouncerLogger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type relayAnnouncerState struct {
	mu       sync.Mutex
	lastRun  time.Time
	stopOnce sync.Once
	stopCh   chan struct{}
}

func newRelayAnnouncerState() *relayAnnouncerState {
	return &relayAnnouncerState{stopCh: make(chan struct{})}
}

// NewRelayAnnouncer validates the config and returns a ready-
// to-use announcer. Callers should call Start in a goroutine
// (or synchronously: Start fires one immediate publish and
// returns).
func NewRelayAnnouncer(cfg RelayAnnouncer) (*RelayAnnouncer, error) {
	if cfg.DHT == nil {
		return nil, errors.New("herald: relay announcer: nil DHT")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 12 * time.Hour
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &RelayAnnouncer{
		DHT:     cfg.DHT,
		Interval: cfg.Interval,
		Now:     cfg.Now,
		Logger:  cfg.Logger,
	}, nil
}

// Start launches the background republish loop. It returns
// immediately. A first publish is also fired immediately so
// the node appears in the relay list at startup.
func (r *RelayAnnouncer) Start(ctx context.Context) {
	r.RunOnce(ctx)
	go r.loop(ctx)
}

// loop is the long-lived ticker.
func (r *RelayAnnouncer) loop(ctx context.Context) {
	t := time.NewTicker(r.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.RunOnce(ctx)
		}
	}
}

// RunOnce performs a single publish synchronously and
// returns the error (or nil) from the DHT call. The daemon
// ignores the return value; tests assert on it.
func (r *RelayAnnouncer) RunOnce(ctx context.Context) error {
	err := r.DHT.PublishAsRelay(ctx)
	if r.Logger != nil {
		if err != nil {
			r.Logger.Warn("relay announce failed", "err", err.Error())
		} else {
			r.Logger.Info("relay announce ok")
		}
	}
	return err
}
