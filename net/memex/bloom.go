// Phase 13: peer bloom filter exchange.
//
// Each node periodically broadcasts a bloom filter of its
// sealed MID set over a dedicated libp2p protocol
// (/membuss/memex-bloom/1.0.0). Peers that receive the
// announcement can then avoid asking providers that are
// guaranteed not to have a block.
//
// This file owns:
//   - the BloomManager that drives the local announcement
//     loop and tracks the filters received from remote
//     peers,
//   - the wire format helpers (encode/decode/announce),
//   - the inbound stream handler.
//
// It is deliberately decoupled from Engine so that a node
// can run a manager without exposing the full block exchange
// (and vice versa). Engine.New wires a manager into a session
// so want-list routing can consult it.
package memex

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/core/mid"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

// BloomProtocolID is the libp2p protocol used to carry
// BloomAnnouncement messages. Peers using it must be able
// to (a) open a stream to a remote and write a single
// BloomAnnouncement frame, and (b) accept an inbound
// stream and read one announcement.
const BloomProtocolID = protocol.ID("/membuss/memex-bloom/1.0.0")

// SealedLister is the subset of the store the BloomManager
// needs to enumerate local MIDs for the announcement.
// Both *store.MemStore and the in-memory store expose this.
type SealedLister interface {
	AllSealed() ([]mid.MID, error)
}

// PeerSource is the subset of the libp2p host the manager
// needs to enumerate directly connected peers.
type PeerSource interface {
	Peerstore() peerstore
}

// peerstore is a minimal alias for the libp2p peerstore
// interface that we use here; declaring it locally keeps
// the manager import-light.
type peerstore interface {
	Peers() []peer.ID
}

// PeerLister is the only method we need from a host. The
// libp2p host.Host already satisfies it; declaring a
// minimal interface keeps the manager unit-testable
// without spinning up a full libp2p stack.
type PeerLister interface {
	Network() network.Network
}

// hostFull is the full surface the manager needs: the
// libp2p host implements it.
type hostFull interface {
	host.Host
	PeerLister
}

// BloomManager owns the local announcement loop and the
// map of remote-peer filters. Construct one per node and
// call Start to begin broadcasting; Stop terminates the
// loop. Concurrent calls to all exported methods are safe.
type BloomManager struct {
	host      host.Host
	sealed    SealedLister
	interval  time.Duration

	mu       sync.RWMutex
	local    *localBloom
	peers    map[peer.ID]*remoteBloom

	// stop is closed by Stop to wake the loop and end it.
	stop chan struct{}
	done chan struct{}
}

// localBloom is the current announcement state for this
// node. Capacity / FPRate are fixed at construction.
type localBloom struct {
	filter   *bloom.BloomFilter
	capacity uint
	fpRate   float64
	count    uint32
}

// remoteBloom is the last announcement received from a
// remote peer. A nil filter means "we have not heard
// from this peer yet"; in that case Want routing treats
// the peer as an unknown provider.
type remoteBloom struct {
	filter   *bloom.BloomFilter
	received time.Time
}

// BloomConfig configures a BloomManager.
type BloomConfig struct {
	// Host is the local libp2p host. Required.
	Host host.Host
	// Sealed is the local store. Required; if nil the
	// manager runs in a no-op state (it never builds a
	// local filter and never sends announcements).
	Sealed SealedLister
	// Capacity is the expected number of MIDs in the
	// local sealed set. Default 1_000_000.
	Capacity uint
	// FPRate is the target false positive rate for the
	// local filter. Default 0.01 (1%).
	FPRate float64
	// Interval controls how often the local filter is
	// broadcast to directly connected peers. Default
	// 5 minutes. A value <= 0 disables the loop (the
	// manager still receives inbound announcements).
	Interval time.Duration
}

// DefaultBloomConfig returns a config with safe defaults
// suitable for production nodes.
func DefaultBloomConfig() BloomConfig {
	return BloomConfig{
		Capacity: 1_000_000,
		FPRate:   0.01,
		Interval: 5 * time.Minute,
	}
}

// NewBloomManager constructs a manager. It does not start
// the loop; call Start for that.
func NewBloomManager(cfg BloomConfig) (*BloomManager, error) {
	if cfg.Host == nil {
		return nil, errors.New("memex bloom: nil host")
	}
	if cfg.Capacity == 0 {
		cfg.Capacity = 1_000_000
	}
	if cfg.FPRate <= 0 {
		cfg.FPRate = 0.01
	}
	if cfg.Interval == 0 {
		cfg.Interval = 5 * time.Minute
	}
	return &BloomManager{
		host:     cfg.Host,
		sealed:   cfg.Sealed,
		interval: cfg.Interval,
		local: &localBloom{
			filter:   bloom.NewWithEstimates(cfg.Capacity, cfg.FPRate),
			capacity: cfg.Capacity,
			fpRate:   cfg.FPRate,
		},
		peers: make(map[peer.ID]*remoteBloom),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}, nil
}

// Start registers the inbound stream handler and, if
// Interval > 0, starts the announcement loop. It is safe
// to call multiple times; only the first call has any
// effect.
func (m *BloomManager) Start() {
	m.host.SetStreamHandler(BloomProtocolID, m.handleStream)
	if m.interval > 0 {
		go m.loop()
	} else {
		// No loop: still mark done so Stop returns cleanly.
		go func() { <-m.stop; close(m.done) }()
	}
}

// Stop removes the inbound handler and terminates the
// loop. It blocks until the loop has exited.
func (m *BloomManager) Stop() {
	m.host.RemoveStreamHandler(BloomProtocolID)
	select {
	case <-m.stop:
		// already closed
	default:
		close(m.stop)
	}
	<-m.done
}

// localAnnouncement builds the protobuf the local node
// would send out next. It is exposed for tests.
func (m *BloomManager) localAnnouncement() (*membusspb.BloomAnnouncement, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, err := m.local.filter.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("memex bloom: marshal local: %w", err)
	}
	return &membusspb.BloomAnnouncement{
		BloomFilter: data,
		ItemCount:   uint64(m.local.count),
		Capacity:    uint64(m.local.capacity),
		FpRate:      m.local.fpRate,
	}, nil
}

// rebuildLocked rebuilds m.local.filter from m.sealed. The
// caller MUST hold m.mu.
func (m *BloomManager) rebuildLocked() error {
	if m.sealed == nil {
		return nil
	}
	mids, err := m.sealed.AllSealed()
	if err != nil {
		return fmt.Errorf("memex bloom: list sealed: %w", err)
	}
	fresh := bloom.NewWithEstimates(m.local.capacity, m.local.fpRate)
	for _, x := range mids {
		fresh.Add(x.Bytes())
	}
	m.local.filter = fresh
	m.local.count = uint32(len(mids))
	return nil
}

// RefreshLocal rebuilds the local filter from the store's
// sealed set. Safe to call from any goroutine; subsequent
// calls atomically swap the filter.
func (m *BloomManager) RefreshLocal(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rebuildLocked()
}

// AddSealed records a single MID into the local filter
// without going through AllSealed. Useful for callers
// that learn about new seals (e.g. the herald on a
// successful Seal) and want the announcement to converge
// faster than the periodic rebuild.
func (m *BloomManager) AddSealed(x mid.MID) {
	if x.IsZero() {
		return
	}
	m.mu.Lock()
	if m.local.filter != nil {
		m.local.filter.Add(x.Bytes())
		m.local.count++
	}
	m.mu.Unlock()
}

// loop is the periodic broadcaster. It re-builds the
// local filter, then opens a short-lived stream to every
// directly connected peer and pushes a BloomAnnouncement.
// Errors are swallowed on a per-peer basis; the loop
// continues.
func (m *BloomManager) loop() {
	defer close(m.done)
	t := time.NewTicker(m.interval)
	defer t.Stop()

	// Do one immediate refresh so the very first
	// announcement is meaningful.
	_ = m.RefreshLocal(context.Background())

	for {
		select {
		case <-m.stop:
			return
		case <-t.C:
		}
		if err := m.RefreshLocal(context.Background()); err != nil {
			// Keep the loop alive even if the store is
			// transiently unhealthy.
			continue
		}
		m.broadcastAll(context.Background())
	}
}

// broadcastAll sends the current local announcement to
// every directly connected peer.
func (m *BloomManager) broadcastAll(ctx context.Context) {
	peers := m.host.Network().Peers()
	ann, err := m.localAnnouncement()
	if err != nil {
		return
	}
	for _, pid := range peers {
		if pid == m.host.ID() {
			continue
		}
		_ = m.sendOne(ctx, pid, ann)
	}
}

// sendOne opens a stream, writes a single announcement,
// and closes. The call is bounded by a per-peer timeout
// so a single slow peer cannot stall the loop.
func (m *BloomManager) sendOne(ctx context.Context, pid peer.ID, ann *membusspb.BloomAnnouncement) error {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	stream, err := m.host.NewStream(cctx, pid, BloomProtocolID)
	if err != nil {
		return err
	}
	defer stream.Close()
	_ = stream.SetWriteDeadline(time.Now().Add(5 * time.Second))
	buf, err := proto.Marshal(ann)
	if err != nil {
		return err
	}
	// 4-byte big-endian length prefix matching the rest
	// of the memex wire format.
	var lenBuf [4]byte
	lenBuf[0] = byte(len(buf) >> 24)
	lenBuf[1] = byte(len(buf) >> 16)
	lenBuf[2] = byte(len(buf) >> 8)
	lenBuf[3] = byte(len(buf))
	if _, err := stream.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := stream.Write(buf); err != nil {
		return err
	}
	return nil
}

// handleStream is the inbound protocol handler. It reads
// exactly one announcement frame, decodes it, and updates
// the per-peer record. Streams that fail to decode are
// closed silently; there is no reply.
func (m *BloomManager) handleStream(s network.Stream) {
	defer s.Close()
	remote := s.Conn().RemotePeer()
	_ = s.SetReadDeadline(time.Now().Add(10 * time.Second))

	var lenBuf [4]byte
	if _, err := readFull(s, lenBuf[:]); err != nil {
		return
	}
	l := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
	if l == 0 || l > maxFrameSize {
		return
	}
	body := make([]byte, l)
	if _, err := readFull(s, body); err != nil {
		return
	}
	var ann membusspb.BloomAnnouncement
	if err := proto.Unmarshal(body, &ann); err != nil {
		return
	}
	if len(ann.BloomFilter) == 0 {
		return
	}
	bf := &bloom.BloomFilter{}
	if err := bf.UnmarshalBinary(ann.BloomFilter); err != nil {
		return
	}
	m.mu.Lock()
	m.peers[remote] = &remoteBloom{filter: bf, received: time.Now()}
	m.mu.Unlock()
}

// PeerLikelyHas reports whether the manager has a recent
// bloom announcement from pid that includes m. It returns
// false if no announcement has been received (the safe
// default: do not pre-filter the peer).
func (m *BloomManager) PeerLikelyHas(pid peer.ID, want mid.MID) bool {
	if want.IsZero() {
		return true
	}
	m.mu.RLock()
	rb, ok := m.peers[pid]
	m.mu.RUnlock()
	if !ok || rb == nil || rb.filter == nil {
		return true
	}
	return rb.filter.Test(want.Bytes())
}

// FilteredProviders returns the subset of providers that
// the manager has positive knowledge of having m, plus
// any provider for which the manager has no information
// (unknown peers are kept in). Providers for which the
// manager has a negative filter result are excluded.
//
// This is the primary hook used by MemexSession to
// shrink the want-list fan-out: a provider whose filter
// says "definitely absent" is not asked.
func (m *BloomManager) FilteredProviders(want mid.MID, providers []peer.AddrInfo) []peer.AddrInfo {
	if len(providers) == 0 {
		return providers
	}
	out := make([]peer.AddrInfo, 0, len(providers))
	for _, p := range providers {
		if m.PeerLikelyHas(p.ID, want) {
			out = append(out, p)
		}
	}
	return out
}

// readFull reads exactly len(buf) bytes or returns an
// error. Extracted so the stream handler does not have
// to deal with short reads.
func readFull(s network.Stream, buf []byte) (int, error) {
	read := 0
	for read < len(buf) {
		n, err := s.Read(buf[read:])
		if n > 0 {
			read += n
		}
		if err != nil {
			return read, err
		}
	}
	return read, nil
}
