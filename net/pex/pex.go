// Package pex implements Mem-PEX, the Membuss peer exchange
// gossip protocol.
//
// Each node maintains a peer table capped at maxPeers. Every
// gossipInterval, the node picks a small random subset of its
// currently connected peers, opens a /membuss/pex/1.0.0 stream
// to each, and exchanges a PEXMessage. The union of the local
// table and the remote table is then merged in, with newer
// last_seen timestamps winning and dead entries being evicted.
//
// Newly discovered peers with advertised multiaddrs are
// asynchronously dialed so that gossip actively grows the
// node's connectivity.
package pex

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	libp2pcore "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/nnlgsakib/membuss/proto"
)

// ProtocolID is the full libp2p protocol identifier for
// Mem-PEX.
const ProtocolID = protocol.ID("/membuss/pex/1.0.0")

const (
	// maxPeers caps the in-memory peer table.
	maxPeers = 256
	// fanout is the number of random peers we gossip with per
	// round.
	fanout = 5
	// gossipInterval is the time between gossip rounds.
	gossipInterval = 30 * time.Second
	// streamTimeout bounds a single PEX round trip.
	streamTimeout = 10 * time.Second
	// dialTimeout bounds the dial of a newly discovered peer.
	dialTimeout = 5 * time.Second
)

// PEX is the Membuss peer exchange engine. It is safe for
// concurrent use.
type PEX struct {
	host libp2pcore.Host

	mu    sync.Mutex
	peers map[peer.ID]*entry

	loopStop chan struct{}
	loopDone chan struct{}

	now   func() time.Time
	rng   *rand.Rand
	rngMu sync.Mutex
}

// entry is one row of the peer table.
type entry struct {
	info     *membusspb.PeerInfo
	addrInfo peer.AddrInfo
}

// Config configures a PEX instance.
type Config struct {
	Host libp2pcore.Host
	// Now overrides the wall clock; tests use this to control
	// time. Defaults to time.Now.
	Now func() time.Time
	// Rand overrides the random source; tests use this for
	// determinism. Defaults to a goroutine-safe source seeded
	// from the system time.
	Rand *rand.Rand
}

// New constructs a PEX. Call Start to begin gossiping.
func New(cfg Config) (*PEX, error) {
	if cfg.Host == nil {
		return nil, errors.New("pex: nil host")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	rng := cfg.Rand
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &PEX{
		host:     cfg.Host,
		peers:    make(map[peer.ID]*entry, maxPeers),
		loopStop: make(chan struct{}),
		loopDone: make(chan struct{}),
		now:      now,
		rng:      rng,
	}, nil
}

// Start registers the stream handler and starts the gossip
// loop. The loop exits when ctx is cancelled or Stop is
// called.
func (p *PEX) Start(ctx context.Context) {
	p.host.SetStreamHandler(ProtocolID, p.handleStream)
	go p.gossipLoop(ctx)
}

// Stop unregisters the stream handler and waits for the gossip
// loop to exit.
func (p *PEX) Stop() {
	p.host.RemoveStreamHandler(ProtocolID)
	select {
	case <-p.loopStop:
	default:
		close(p.loopStop)
	}
	<-p.loopDone
}

// AddPeer inserts or refreshes a peer in the table. It is safe
// to call from any goroutine.
func (p *PEX) AddPeer(ai peer.AddrInfo) {
	if ai.ID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.upsertLocked(ai, p.now().Unix())
}

// Peers returns a snapshot of the current peer table sorted by
// peer ID for determinism.
func (p *PEX) Peers() []*membusspb.PeerInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*membusspb.PeerInfo, 0, len(p.peers))
	for _, e := range p.peers {
		out = append(out, cloneInfo(e.info))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerId < out[j].PeerId })
	return out
}

// PeerCount returns the number of peers in the table.
func (p *PEX) PeerCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.peers)
}

// upsertLocked adds or refreshes ai in the table, keeping the
// entry with the newer last_seen timestamp. The caller must
// hold p.mu.
func (p *PEX) upsertLocked(ai peer.AddrInfo, seen int64) {
	if ai.ID == "" || ai.ID == p.host.ID() {
		return
	}
	cur, ok := p.peers[ai.ID]
	addrs := addrsToStrings(ai.Addrs)
	if ok {
		if cur.info.LastSeen >= seen {
			return
		}
		cur.addrInfo = ai
		cur.info = &membusspb.PeerInfo{
			PeerId:   ai.ID.String(),
			Addrs:    addrs,
			LastSeen: seen,
		}
		return
	}
	if len(p.peers) >= maxPeers {
		p.evictOldestLocked()
	}
	p.peers[ai.ID] = &entry{
		addrInfo: ai,
		info: &membusspb.PeerInfo{
			PeerId:   ai.ID.String(),
			Addrs:    addrs,
			LastSeen: seen,
		},
	}
}

// evictOldestLocked drops the entry with the smallest
// last_seen. The caller must hold p.mu.
func (p *PEX) evictOldestLocked() {
	var (
		oldestID peer.ID
		oldestTs int64 = 1<<62 - 1
		have     bool
	)
	for id, e := range p.peers {
		if !have || e.info.LastSeen < oldestTs {
			oldestID = id
			oldestTs = e.info.LastSeen
			have = true
		}
	}
	if have {
		delete(p.peers, oldestID)
	}
}

// snapshot returns a copy of the current entries sorted by
// PeerID. The caller must hold p.mu.
func (p *PEX) snapshot() []*membusspb.PeerInfo {
	out := make([]*membusspb.PeerInfo, 0, len(p.peers))
	for _, e := range p.peers {
		out = append(out, cloneInfo(e.info))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerId < out[j].PeerId })
	return out
}

// cloneInfo returns a deep copy of a PeerInfo. The protobuf
// generated type contains an internal mutex, so naive struct
// copies trip go vet.
func cloneInfo(in *membusspb.PeerInfo) *membusspb.PeerInfo {
	if in == nil {
		return nil
	}
	return proto.Clone(in).(*membusspb.PeerInfo)
}

// handleStream is the inbound stream handler. It reads the
// remote's PEXMessage, merges it into the table, and writes
// our own PEXMessage back.
func (p *PEX) handleStream(s network.Stream) {
	defer s.Close()
	remote := s.Conn().RemotePeer()
	p.AddPeer(peer.AddrInfo{ID: remote, Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()}})

	_ = s.SetReadDeadline(time.Now().Add(streamTimeout))
	_ = s.SetWriteDeadline(time.Now().Add(streamTimeout))

	incoming := readMsg(s)
	if incoming == nil {
		return
	}
	var inMsg membusspb.PEXMessage
	if err := proto.Unmarshal(incoming, &inMsg); err != nil {
		return
	}
	p.mergeFromMessage(inMsg.Peers, p.now().Unix())

	p.mu.Lock()
	reply := &membusspb.PEXMessage{Peers: p.snapshot()}
	p.mu.Unlock()
	out, err := proto.Marshal(reply)
	if err != nil {
		return
	}
	_, _ = s.Write(out)
}

// gossipLoop is the periodic driver.
func (p *PEX) gossipLoop(ctx context.Context) {
	defer close(p.loopDone)
	t := time.NewTicker(gossipInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.loopStop:
			return
		case <-t.C:
			p.gossipRound(ctx)
		}
	}
}

// gossipRound picks fanout random connected peers and runs a
// single PEX round with each.
func (p *PEX) gossipRound(ctx context.Context) {
	peers := p.host.Network().Peers()
	if len(peers) == 0 {
		return
	}
	p.rngMu.Lock()
	p.rng.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	p.rngMu.Unlock()
	if len(peers) > fanout {
		peers = peers[:fanout]
	}
	for _, pid := range peers {
		if pid == p.host.ID() {
			continue
		}
		if err := p.exchange(ctx, pid); err != nil {
			_ = err
		}
	}
}

// exchange runs a single PEX round with pid.
func (p *PEX) exchange(ctx context.Context, pid peer.ID) error {
	cctx, cancel := context.WithTimeout(ctx, streamTimeout)
	defer cancel()
	s, err := p.host.NewStream(cctx, pid, ProtocolID)
	if err != nil {
		return fmt.Errorf("pex: open stream to %s: %w", pid, err)
	}
	defer s.Close()
	_ = s.SetDeadline(time.Now().Add(streamTimeout))

	p.mu.Lock()
	out := &membusspb.PEXMessage{Peers: p.snapshot()}
	p.mu.Unlock()
	buf, err := proto.Marshal(out)
	if err != nil {
		return fmt.Errorf("pex: marshal: %w", err)
	}
	if _, err := s.Write(buf); err != nil {
		return fmt.Errorf("pex: write to %s: %w", pid, err)
	}
	resp := readMsg(s)
	if resp == nil {
		return errors.New("pex: empty reply")
	}
	var reply membusspb.PEXMessage
	if err := proto.Unmarshal(resp, &reply); err != nil {
		return fmt.Errorf("pex: unmarshal reply: %w", err)
	}
	p.mergeFromMessage(reply.Peers, p.now().Unix())
	return nil
}

// mergeFromMessage applies a list of PeerInfo records into the
// table. For each newly-discovered peer with addrs, an async
// dial is fired in the background.
func (p *PEX) mergeFromMessage(infos []*membusspb.PeerInfo, seen int64) {
	for _, info := range infos {
		if info == nil {
			continue
		}
		ai, ok := decodePeerInfo(info)
		if !ok {
			continue
		}
		p.mu.Lock()
		wasKnown := p.peers[ai.ID] != nil
		p.upsertLocked(ai, seen)
		p.mu.Unlock()
		if !wasKnown && len(ai.Addrs) > 0 {
			go p.dialAsync(ai)
		}
	}
}

// dialAsync attempts to connect to ai in the background.
func (p *PEX) dialAsync(ai peer.AddrInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	_ = p.host.Connect(ctx, ai)
}

// readMsg reads a single protobuf frame from s. The wire
// format is a 4-byte big-endian length prefix followed by
// that many payload bytes. If a length prefix is missing
// (eg. raw stream), the function falls back to a bounded
// buffered read so tests in any language work.
func readMsg(s network.Stream) []byte {
	const max = 1 << 20
	var lenBuf [4]byte
	n, err := s.Read(lenBuf[:])
	if err != nil || n < 4 {
		buf := make([]byte, 0, 4096)
		tmp := make([]byte, 4096)
		for {
			k, e := s.Read(tmp[:])
			if k > 0 {
				buf = append(buf, tmp[:k]...)
			}
			if e != nil || len(buf) >= max {
				return buf
			}
		}
	}
	l := uint32(lenBuf[0])<<24 | uint32(lenBuf[1])<<16 | uint32(lenBuf[2])<<8 | uint32(lenBuf[3])
	if l == 0 || l > max {
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

func addrsToStrings(in []multiaddr.Multiaddr) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	for i, a := range in {
		out[i] = a.String()
	}
	return out
}

func decodePeerInfo(info *membusspb.PeerInfo) (peer.AddrInfo, bool) {
	if info == nil || info.PeerId == "" {
		return peer.AddrInfo{}, false
	}
	pid, err := peer.Decode(info.PeerId)
	if err != nil {
		return peer.AddrInfo{}, false
	}
	addrs := make([]multiaddr.Multiaddr, 0, len(info.Addrs))
	for _, s := range info.Addrs {
		a, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			continue
		}
		addrs = append(addrs, a)
	}
	return peer.AddrInfo{ID: pid, Addrs: addrs}, true
}