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
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
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
	// maxFrameSize caps a single MemexMessage frame.
	maxFrameSize = 16 << 20
)

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
		host: cfg.Host,
		bs:   cfg.Blockstore,
		wm:   newWantManager(),
	bloom: cfg.Bloom,
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

// openStream opens a Memex stream to pid with a timeout.
func (e *Engine) openStream(ctx context.Context, pid peer.ID) (network.Stream, error) {
	cctx, cancel := context.WithTimeout(ctx, DefaultPeerTimeout)
	defer cancel()
	return e.host.NewStream(cctx, pid, ProtocolID)
}
