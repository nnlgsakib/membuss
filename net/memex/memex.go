// Package memex implements Memex, the Membuss block exchange
// protocol over libp2p streams.
//
// Memex is a simple want/have/block protocol inspired by
// bitswap but stripped to the essentials. A requester opens
// a /membuss/memex/1.0.0 stream to a provider, sends a want
// list, and reads blocks back. The wire format is the
// MemexMessage protobuf, framed by a 4-byte big-endian
// length prefix (raw stream is accepted as a fallback).
//
// The package exposes two layers:
//
//   - Engine runs the libp2p protocol on a host. It serves
//     blocks from a local Blockstore and accepts blocks
//     pushed by remote peers, depositing them into the same
//     Blockstore after integrity verification.
//   - Session drives a single retrieval. Given a root MID
//     and a set of provider peers, it fans out parallel
//     streams, walks the DAG as child MIDs are discovered,
//     and yields an io.Reader over the reassembled content.
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

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	corepeerstore "github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	ma "github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	membusspb "github.com/nnlgsakib/membuss/proto"
	"github.com/nnlgsakib/membuss/core/mid"
)

// ProtocolID is the full libp2p protocol identifier for Memex.
const ProtocolID = protocol.ID("/membuss/memex/1.0.0")

const (
	// DefaultSessionTimeout bounds a single MemexSession
	// (root fetch plus all DAG descendants).
	DefaultSessionTimeout = 30 * time.Second
	// DefaultPeerTimeout bounds a single want sent on a
	// single stream. Slow peers are abandoned and the
	// remaining want is retried on another peer.
	DefaultPeerTimeout = 10 * time.Second
	// MaxParallelPeers is the upper bound on the number of
	// provider streams a single session opens at once.
	MaxParallelPeers = 8
	// DefaultPipelineDepth is the default number of concurrent
	// in-flight wants per provider stream. A depth of 8 keeps
	// the pipe full without overwhelming a single peer.
	DefaultPipelineDepth = 8
	// DefaultStreamsPerProvider is the default number of
	// concurrent streams opened to each provider peer.
	DefaultStreamsPerProvider = 2
	// MaxStreamsPerProvider caps the number of streams per peer.
	MaxStreamsPerProvider = 8
	// maxFrameSize caps a single MemexMessage frame.
	maxFrameSize = 16 << 20

	// defaultChunkSize is the default block/chunk size used for
	// timeout estimation. Matches the standard 256KB chunk.
	defaultChunkSize = 256 * 1024
	// perBlockTimeout is the estimated time allowance per block
	// on a slow connection, used by EstimateTimeout.
	perBlockTimeout = 500 * time.Millisecond
	// minSessionTimeout is the floor for EstimateTimeout.
	minSessionTimeout = 30 * time.Second
	// maxSessionTimeout is the ceiling for EstimateTimeout.
	maxSessionTimeout = 5 * time.Minute
)

// EstimateTimeout returns a session timeout proportional to the
// estimated number of blocks. contentBytes is the total size of
// the content in bytes. The formula accounts for network round-trips
// at per-blockTimeout per block across MaxParallelPeers parallel
// streams, clamped to [minSessionTimeout, maxSessionTimeout].
func EstimateTimeout(contentBytes uint64) time.Duration {
	if contentBytes == 0 {
		return DefaultSessionTimeout
	}
	blocks := (contentBytes + defaultChunkSize - 1) / uint64(defaultChunkSize)
	parallel := uint64(MaxParallelPeers)
	if parallel == 0 {
		parallel = 1
	}
	batches := (blocks + parallel - 1) / parallel
	d := time.Duration(batches) * perBlockTimeout
	if d < minSessionTimeout {
		d = minSessionTimeout
	}
	if d > maxSessionTimeout {
		d = maxSessionTimeout
	}
	return d
}

// Blockstore is the local block storage that the engine and
// the session both read from and write to. The
// core/store.Blockstore interface is a perfect match.
type Blockstore interface {
	Put(m mid.MID, data []byte) error
	Get(m mid.MID) ([]byte, error)
	Has(m mid.MID) (bool, error)
}

// wantWaiter is a local in-process subscription to a block
// arriving over an inbound Memex stream. WantManager fans out
// the result so that multiple sessions waiting on the same MID
// are all notified.
type wantWaiter struct {
	ch chan mid.MID
}

type wantManager struct {
	mu       sync.Mutex
	waiters  map[string][]*wantWaiter
}

func newWantManager() *wantManager {
	return &wantManager{waiters: make(map[string][]*wantWaiter)}
}

// subscribe registers interest in m. The returned channel
// receives the MID exactly once when the block becomes
// available locally (via either a local Put or a remote
// push). Callers MUST call unsubscribe when they give up.
func (w *wantManager) subscribe(m mid.MID) *wantWaiter {
	wt := &wantWaiter{ch: make(chan mid.MID, 1)}
	w.mu.Lock()
	w.waiters[m.String()] = append(w.waiters[m.String()], wt)
	w.mu.Unlock()
	return wt
}

func (w *wantManager) unsubscribe(m mid.MID, wt *wantWaiter) {
	w.mu.Lock()
	defer w.mu.Unlock()
	list := w.waiters[m.String()]
	for i, x := range list {
		if x == wt {
			list = append(list[:i], list[i+1:]...)
			break
		}
	}
	if len(list) == 0 {
		delete(w.waiters, m.String())
	}
}

// deliver notifies all waiters for m and clears the list.
func (w *wantManager) deliver(m mid.MID) {
	w.mu.Lock()
	list := w.waiters[m.String()]
	delete(w.waiters, m.String())
	w.mu.Unlock()
	for _, wt := range list {
		select {
		case wt.ch <- m:
		default:
		}
	}
}

// Engine is the long-lived Memex node on a libp2p host. There
// is normally one Engine per node.
type Engine struct {
	host host.Host
	bs   Blockstore
	wm   *wantManager
	// bloom is the optional peer filter exchange. nil
	// disables the Phase 13 want-list optimization. Set
	// by New(Bloom: mgr).
	bloom *BloomManager

	metricsMu   sync.RWMutex
	peerMetrics map[peer.ID]*peerMetrics
}

type peerMetrics struct {
	mu         sync.RWMutex
	successes  int
	failures   int
	avgLatency time.Duration
}

// Config configures an Engine.
type Config struct {
	Host      host.Host
	Blockstore Blockstore
	// Bloom is the optional peer filter exchange. When
	// non-nil the engine uses it to skip providers that
	// are guaranteed not to have a given block.
	Bloom *BloomManager
}

// New constructs an Engine. Call Start to register the
// protocol handler.
func New(cfg Config) (*Engine, error) {
	if cfg.Host == nil {
		return nil, errors.New("memex: nil host")
	}
	if cfg.Blockstore == nil {
		return nil, errors.New("memex: nil blockstore")
	}
	return &Engine{
		host:        cfg.Host,
		bs:          cfg.Blockstore,
		wm:          newWantManager(),
		bloom:       cfg.Bloom,
		peerMetrics: make(map[peer.ID]*peerMetrics),
	}, nil
}

// Start registers the protocol handler. It is safe to call
// multiple times; only the first call has any effect.
func (e *Engine) Start() {
	e.host.SetStreamHandler(ProtocolID, e.handleStream)
}

// Stop removes the protocol handler. It is the original
// fire-and-forget Stop. Use StopWait when you have a context
// to bound the wait.
func (e *Engine) Stop() {
	e.host.RemoveStreamHandler(ProtocolID)
}

// StopWait removes the protocol handler and waits for in-flight
// stream handlers to drain, bounded by ctx. It returns ctx.Err()
// if the context fires before drain completes.
func (e *Engine) StopWait(ctx context.Context) error {
	e.host.RemoveStreamHandler(ProtocolID)
	// The Engine itself has no per-stream goroutine registry; the
	// session layer is responsible for its own draining. We still
	// honor the context: if it fires before Stop returns, we surface
	// the error. libp2p's stream handler set is synchronous on the
	// SetStreamHandler path so the call below returns promptly.
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Best-effort: yield to the runtime so any in-flight
		// SetStreamHandler call has a chance to return.
		time.Sleep(0)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Blockstore returns the local block store backing this
// engine. Sessions and the integration tests use it to add
// blocks to be served to remote peers.
func (e *Engine) Blockstore() Blockstore { return e.bs }

// WantManager returns the in-process want manager. Tests use
// it to wait for an inbound block delivery.
func (e *Engine) WantManager() *wantManager { return e.wm }

// BloomManager returns the peer-filter exchange, or nil if
// none was configured. Sessions consult it to skip
// providers that are guaranteed not to have a given block.
func (e *Engine) BloomManager() *BloomManager { return e.bloom }

// handleStream is the inbound /membuss/memex/1.0.0 handler.
// Each frame read from the stream is dispatched: incoming
// blocks are verified and stored, incoming wants are
// answered with local blocks (or with explicit dont-haves
// when send_dont_have is set).
func (e *Engine) handleStream(s network.Stream) {
	defer s.Close()
	remote := s.Conn().RemotePeer()

	// Apply a read deadline so a stalled peer cannot pin a
	// goroutine forever.
	_ = s.SetReadDeadline(time.Now().Add(DefaultPeerTimeout))
	_ = s.SetWriteDeadline(time.Now().Add(DefaultPeerTimeout))

	for {
		_ = s.SetReadDeadline(time.Now().Add(DefaultPeerTimeout))
		buf := readFrame(s)
		if buf == nil {
			return
		}
		var msg membusspb.MemexMessage
		if err := proto.Unmarshal(buf, &msg); err != nil {
			return
		}

		// Incoming blocks: verify + store + notify waiters.
		for _, b := range msg.Blocks {
			if b == nil {
				continue
			}
			if err := e.acceptBlock(b); err != nil {
				_ = err
			}
		}

		// Incoming wants: serve from the local store.
		if len(msg.Wants) > 0 {
			resp := e.serveWants(msg.Wants)
			if len(resp.Wants)+len(resp.Blocks)+len(resp.HaveMids)+len(resp.Cancel) > 0 {
				// Reset deadlines to give the response a
				// chance to flush.
				_ = s.SetWriteDeadline(time.Now().Add(DefaultPeerTimeout))
				if err := writeFrame(s, resp); err != nil {
					return
				}
			}
		}

		// Cancel list: drop any pending want for these MIDs.
		// We don't track outbound want tables on the engine
		// itself (sessions do), so this is a no-op for the
		// simple engine path. It is honoured by sessions.
		_ = msg.Cancel
		_ = remote
	}
}

// acceptBlock validates a remote-delivered block and stores
// it. It also wakes up any local waiters on the same MID.
func (e *Engine) acceptBlock(b *membusspb.Block) error {
	if b.Mid == "" {
		return errors.New("memex: block missing mid")
	}
	id, err := mid.Parse(b.Mid)
	if err != nil {
		return fmt.Errorf("memex: parse mid: %w", err)
	}
	if err := e.bs.Put(id, b.Data); err != nil {
		return fmt.Errorf("memex: store put: %w", err)
	}
	e.wm.deliver(id)
	return nil
}

// serveWants answers a list of wants by looking them up in
// the local store.
func (e *Engine) serveWants(wants []*membusspb.WantEntry) *membusspb.MemexMessage {
	resp := &membusspb.MemexMessage{}
	for _, w := range wants {
		if w == nil || w.Mid == "" {
			continue
		}
		id, err := mid.Parse(w.Mid)
		if err != nil {
			continue
		}
		has, err := e.bs.Has(id)
		if err != nil {
			continue
		}
		if has {
			data, err := e.bs.Get(id)
			if err != nil {
				continue
			}
			resp.Blocks = append(resp.Blocks, &membusspb.Block{
				Mid:  w.Mid,
				Data: data,
				Size: uint64(len(data)),
			})
			// Phase 19: if the blockstore supports
			// GetMeta, look up ObjectInfo for this MID
			// and include it in the response so the
			// requester can persist the filename + MIME
			// type locally.
			if oi, ok := e.objectInfoFor(id); ok {
				if resp.ObjectInfos == nil {
					resp.ObjectInfos = make(map[string]*membusspb.ObjectInfo)
				}
				resp.ObjectInfos[w.Mid] = oi
			}
			continue
		}
		if w.SendDontHave {
			resp.HaveMids = append(resp.HaveMids, w.Mid)
		}
	}
	return resp
}

// objectInfoFor looks up the per-MID ObjectInfo stored in
// the meta namespace. Returns nil, false when the store
// does not support GetMeta or when no descriptor exists.
func (e *Engine) objectInfoFor(m mid.MID) (*membusspb.ObjectInfo, bool) {
	type metaGetter interface {
		GetMeta(key string) ([]byte, error)
	}
	mg, ok := e.bs.(metaGetter)
	if !ok {
		return nil, false
	}
	raw, err := mg.GetMeta("obj/" + m.String())
	if err != nil || len(raw) == 0 {
		return nil, false
	}
	var info struct {
		Name     string `json:"name,omitempty"`
		MimeType string `json:"mime_type,omitempty"`
		Size     uint64 `json:"size,omitempty"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return nil, false
	}
	return &membusspb.ObjectInfo{
		Name:     info.Name,
		MimeType: info.MimeType,
		Size:     info.Size,
	}, true
}

// ReadResult is the success outcome of a Session.Fetch call.
type ReadResult struct {
	Reader io.Reader
	// Root is the MID that was fetched.
	Root mid.MID
}

// readFrame reads a single length-prefixed protobuf frame
// from s. If the length prefix is missing, a bounded raw
// read is used as a fallback. A nil result means the stream
// ended cleanly or hit a recoverable read boundary.
func readFrame(s network.Stream) []byte {
	var lenBuf [4]byte
	n, err := s.Read(lenBuf[:])
	if err != nil || n < 4 {
		// Try a bounded read for unframed streams.
		const max = 1 << 20
		buf := make([]byte, 0, 4096)
		tmp := make([]byte, 4096)
		for {
			k, e := s.Read(tmp[:])
			if k > 0 {
				buf = append(buf, tmp[:k]...)
			}
			if e != nil || len(buf) >= max {
				if len(buf) == 0 {
					return nil
				}
				return buf
			}
		}
	}
	l := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
	if l == 0 || l > maxFrameSize {
		return nil
	}
	buf := make([]byte, l)
	read := 0
	for read < int(l) {
		k, e := s.Read(buf[read:])
		if k > 0 {
			read += k
		}
		if e != nil {
			return buf[:read]
		}
	}
	return buf
}

// writeFrame marshals m and writes it with a 4-byte
// big-endian length prefix.
func writeFrame(s network.Stream, m *membusspb.MemexMessage) error {
	buf, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	if len(buf) > maxFrameSize {
		return errors.New("memex: frame too large")
	}
	var lenBuf [4]byte
	lenBuf[0] = byte(len(buf) >> 24)
	lenBuf[1] = byte(len(buf) >> 16)
	lenBuf[2] = byte(len(buf) >> 8)
	lenBuf[3] = byte(len(buf))
	if _, err := s.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err = s.Write(buf)
	return err
}

// openStream opens a Memex stream to pid with a timeout. Falls back to circuit relay if direct connection fails.
func (e *Engine) openStream(ctx context.Context, pid peer.ID) (network.Stream, error) {
	cctx, cancel := context.WithTimeout(ctx, DefaultPeerTimeout)
	defer cancel()

	// 1. Try direct connection first (explicitly allowing limited connections if already established)
	cctx = network.WithAllowLimitedConn(cctx, "membuss-memex-direct")
	cctx = network.WithUseTransient(cctx, "membuss-memex-direct")
	stream, err := e.host.NewStream(cctx, pid, ProtocolID)
	if err == nil {
		return stream, nil
	}

	// 2. Direct dial failed. Try circuit relay fallback.
	// Find all peers in our peerstore that support the Circuit Relay v2 hop protocol.
	var relays []peer.ID
	for _, p := range e.host.Peerstore().Peers() {
		protocols, err := e.host.Peerstore().SupportsProtocols(p, "/libp2p/circuit/relay/v2/hop")
		if err == nil && len(protocols) > 0 {
			relays = append(relays, p)
		}
	}

	// Fall back to currently connected peers if no protocols registered in peerstore
	if len(relays) == 0 {
		relays = e.host.Network().Peers()
	}

	if len(relays) == 0 {
		return nil, fmt.Errorf("direct stream open failed: %w (no relay candidates available)", err)
	}

	// Construct relayed multiaddresses for the target peer `pid` via each relay candidate
	var relayAddrs []ma.Multiaddr
	for _, relayID := range relays {
		if relayID == pid || relayID == e.host.ID() {
			continue
		}
		// Standard circuit relay v2 address: /p2p/<relayID>/p2p-circuit/p2p/<pid>
		maddrStr := fmt.Sprintf("/p2p/%s/p2p-circuit/p2p/%s", relayID.String(), pid.String())
		maddr, merr := ma.NewMultiaddr(maddrStr)
		if merr == nil {
			relayAddrs = append(relayAddrs, maddr)
		}

		// Also construct specific physical relay addresses if available
		addrs := e.host.Peerstore().Addrs(relayID)
		for _, addr := range addrs {
			var fullRelayAddr ma.Multiaddr
			if !strings.Contains(addr.String(), "/p2p/") {
				p2pPart, perr := ma.NewMultiaddr(fmt.Sprintf("/p2p/%s", relayID.String()))
				if perr == nil {
					fullRelayAddr = addr.Encapsulate(p2pPart)
				}
			} else {
				fullRelayAddr = addr
			}

			if fullRelayAddr != nil {
				circuitPart, cerr := ma.NewMultiaddr(fmt.Sprintf("/p2p-circuit/p2p/%s", pid.String()))
				if cerr == nil {
					relayAddrs = append(relayAddrs, fullRelayAddr.Encapsulate(circuitPart))
				}
			}
		}
	}

	if len(relayAddrs) == 0 {
		return nil, fmt.Errorf("direct stream open failed: %w (could not construct relay addresses)", err)
	}

	// Add the relayed multiaddresses to the target peer's peerstore with a temporary TTL
	e.host.Peerstore().AddAddrs(pid, relayAddrs, corepeerstore.TempAddrTTL)

	// Clear swarm backoff for the peer to make sure it doesn't block the fallback dial
	if sw, ok := e.host.Network().(*swarm.Swarm); ok {
		sw.Backoff().Clear(pid)
	}

	// Retry opening the stream via the relay addresses
	rctx, rcancel := context.WithTimeout(ctx, DefaultPeerTimeout)
	defer rcancel()

	rctx = network.WithAllowLimitedConn(rctx, "membuss-memex-fallback")
	rctx = network.WithUseTransient(rctx, "membuss-memex-fallback")
	rstream, rerr := e.host.NewStream(rctx, pid, ProtocolID)
	if rerr != nil {
		return nil, fmt.Errorf("direct stream open failed: %w; relay fallback failed: %v", err, rerr)
	}

	return rstream, nil
}

// RecordPeerSuccess records a successful block transfer from pid with measured latency.
func (e *Engine) RecordPeerSuccess(pid peer.ID, latency time.Duration) {
	if pid == "" {
		return
	}
	e.metricsMu.Lock()
	m, exists := e.peerMetrics[pid]
	if !exists {
		m = &peerMetrics{}
		e.peerMetrics[pid] = m
	}
	e.metricsMu.Unlock()

	m.mu.Lock()
	m.successes++
	if m.avgLatency == 0 {
		m.avgLatency = latency
	} else {
		// EMA with alpha = 0.2
		m.avgLatency = time.Duration(float64(m.avgLatency)*0.8 + float64(latency)*0.2)
	}
	m.mu.Unlock()
}

// RecordPeerFailure records a failed block transfer or timeout from pid.
func (e *Engine) RecordPeerFailure(pid peer.ID) {
	if pid == "" {
		return
	}
	e.metricsMu.Lock()
	m, exists := e.peerMetrics[pid]
	if !exists {
		m = &peerMetrics{}
		e.peerMetrics[pid] = m
	}
	e.metricsMu.Unlock()

	m.mu.Lock()
	m.failures++
	m.mu.Unlock()
}

// PeerScore calculates a performance score for a peer. Higher is better.
func (e *Engine) PeerScore(pid peer.ID) float64 {
	if pid == "" {
		return 0
	}

	// 1. Get connection latency from the libp2p peerstore (via peerstore.Metrics interface)
	var pstoreLatency time.Duration
	if m, ok := e.host.Peerstore().(corepeerstore.Metrics); ok {
		pstoreLatency = m.LatencyEWMA(pid)
	}

	// 2. Get dynamic latency & success rate from memex engine metrics
	var dynamicLatency time.Duration
	successRate := 1.0

	e.metricsMu.RLock()
	m, exists := e.peerMetrics[pid]
	e.metricsMu.RUnlock()

	if exists {
		m.mu.RLock()
		dynamicLatency = m.avgLatency
		total := m.successes + m.failures
		if total > 0 {
			successRate = float64(m.successes) / float64(total)
		}
		m.mu.RUnlock()
	}

	// Determine representative latency
	var latency time.Duration
	if dynamicLatency > 0 {
		latency = dynamicLatency
	} else if pstoreLatency > 0 {
		latency = pstoreLatency
	} else {
		latency = 200 * time.Millisecond // assume a default latency of 200ms
	}

	// Success rate multiplier: heavily penalize nodes with high failure rates.
	effectiveLatency := float64(latency)
	if successRate > 0 {
		effectiveLatency = effectiveLatency / successRate
	} else {
		effectiveLatency = effectiveLatency * 100.0 // heavily penalize 0% success rate
	}

	// Score is inversely proportional to effective latency (in seconds)
	// Higher score = better / faster peer
	return 1.0 / (effectiveLatency/float64(time.Second) + 0.001)
}
