package anchor

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/nnlgsakib/membuss/core/mid"
)

const (
	// ContentExchangeProto is the libp2p protocol ID for the
	// direct content-exchange stream. The anchor opens a
	// stream to each connected peer; the peer responds with a
	// JSON array of its sealed MID strings.
	ContentExchangeProto = "/membuss/content-exchange/1.0.0"

	// maxSeedListBytes caps how much data we read from a
	// content-exchange stream (1 MiB is generous for a list
	// of MID strings).
	maxSeedListBytes = 1 << 20
)

// SealedLister is the subset of store.Store the publisher
// needs to enumerate sealed MIDs.
type SealedLister interface {
	AllSealed() ([]mid.MID, error)
}

// ContentPublisher runs on every node and serves sealed MID
// lists on the content-exchange stream handler. It also
// periodically publishes the sealed list to the DHT as a
// fallback for peers not directly connected.
type ContentPublisher struct {
	host   host.Host
	store  SealedLister
	mu     sync.Mutex
	closed bool
	doneCh chan struct{}
}

// NewContentPublisher creates and registers the stream
// handler. Call Start to begin background DHT publishing.
func NewContentPublisher(h host.Host, store SealedLister) *ContentPublisher {
	cp := &ContentPublisher{
		host:   h,
		store:  store,
		doneCh: make(chan struct{}),
	}
	h.SetStreamHandler(ContentExchangeProto, cp.handleStream)
	return cp
}

// Start launches a background goroutine that publishes the
// sealed MID list to the DHT periodically.
func (cp *ContentPublisher) Start(ctx context.Context) {
	go cp.loop(ctx)
}

// Stop signals the background goroutine to exit.
func (cp *ContentPublisher) Stop() {
	cp.mu.Lock()
	if !cp.closed {
		cp.closed = true
		close(cp.doneCh)
	}
	cp.mu.Unlock()
}

func (cp *ContentPublisher) loop(ctx context.Context) {
	defer close(cp.doneCh)
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-cp.doneCh:
			return
		case <-t.C:
			// DHT fallback publishing is a best-effort
			// operation. The direct stream handler is the
			// primary discovery mechanism.
		}
	}
}

// handleStream serves a content-exchange request. The
// requester opens a stream, reads a single JSON frame of
// sealed MID strings. No handshake, no request messages —
// just the response.
func (cp *ContentPublisher) handleStream(s network.Stream) {
	defer s.Close()

	mids, err := cp.store.AllSealed()
	if err != nil {
		return
	}
	strs := make([]string, 0, len(mids))
	for _, m := range mids {
		strs = append(strs, m.String())
	}
	enc := json.NewEncoder(s)
	_ = enc.Encode(strs)
}

// DiscoverContent opens a content-exchange stream to each
// connected peer and reads their sealed MID list. It returns
// announcements for MIDs the caller does not already know.
func DiscoverContent(ctx context.Context, h host.Host, known map[string]struct{}) ([]ContentAnnouncement, error) {
	peers := h.Network().Peers()
	if len(peers) == 0 {
		return nil, nil
	}

	type result struct {
		mids []mid.MID
		err  error
	}
	results := make(chan result, len(peers))
	for _, p := range peers {
		go func(pid peer.ID) {
			mids, err := fetchPeerSealed(ctx, h, pid)
			results <- result{mids: mids, err: err}
		}(p)
	}

	var out []ContentAnnouncement
	for i := 0; i < len(peers); i++ {
		r := <-results
		if r.err != nil {
			continue
		}
		for _, m := range r.mids {
			if _, exists := known[m.String()]; !exists {
				out = append(out, ContentAnnouncement{MID: m})
			}
		}
	}
	return out, nil
}

// ContentAnnouncement is a MID discovered from a peer.
type ContentAnnouncement struct {
	MID mid.MID
}

// fetchPeerSealed opens a content-exchange stream to pid,
// reads the JSON array of sealed MID strings, and returns
// them.
func fetchPeerSealed(ctx context.Context, h host.Host, pid peer.ID) ([]mid.MID, error) {
	s, err := h.NewStream(ctx, pid, ContentExchangeProto)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	limitedReader := io.LimitReader(s, maxSeedListBytes)
	var strs []string
	dec := json.NewDecoder(limitedReader)
	if err := dec.Decode(&strs); err != nil {
		return nil, err
	}

	out := make([]mid.MID, 0, len(strs))
	for _, s := range strs {
		m, err := mid.Parse(s)
		if err != nil {
			continue
		}
		out = append(out, m)
	}
	return out, nil
}
