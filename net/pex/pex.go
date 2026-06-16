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
//
// Phase 12: PEX is reachability-aware. Each entry in the
// table carries a Reachability verdict (PUBLIC, PRIVATE,
// RELAY_ONLY, UNKNOWN) and a relay_addrs list. Outgoing
// gossip filters entries to keep only those useful to the
// recipient:
//
//   - PUBLIC          -> include, full addrs
//   - RELAY_ONLY      -> include, only relay_addrs
//   - PRIVATE with
//     last_dial_success=false -> exclude entirely
//   - last_seen older than the freshness window -> exclude
//   - self -> exclude always
//
// On the receive side, PEX attempts a connect using the
// address shape that matches the entry's reachability:
// direct addrs for PUBLIC, relay_addrs for RELAY_ONLY,
// nothing for PRIVATE.
package pex

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	libp2pcore "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"


	membusspb "github.com/nnlgsakib/membuss/proto"
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
	// freshnessWindow is the maximum age of an entry for it
	// to be eligible to be sent in a PEX round (Phase 12).
	// Anything older is treated as dead and dropped.
	freshnessWindow = 2 * time.Hour
)

// PEX is the Membuss peer exchange engine. It is safe for
// concurrent use.
type PEX struct {
	host libp2pcore.Host

	mu    sync.Mutex
	peers map[peer.ID]*entry

	persistPath string

	loopStop chan struct{}
	loopDone chan struct{}

	now   func() time.Time
	rng   *rand.Rand
	rngMu sync.Mutex
}

// entry is one row of the peer table. It tracks both the
// libp2p AddrInfo (for dialing) and the protobuf PeerInfo
// (for gossip), plus the last dial outcome so the filter
// can decide whether a PRIVATE peer is still worth
// sharing.
type entry struct {
	info     *membusspb.PeerInfo
	addrInfo peer.AddrInfo
	// relayAddrs are circuit relay multiaddrs for the peer,
	// kept separately from direct addrs so the filter can
	// strip them on outgoing gossip for PUBLIC entries.
	relayAddrs []multiaddr.Multiaddr
}

// Config configures a PEX instance.
type Config struct {
	Host libp2pcore.Host
	// PersistPath is the path to the peer table file.
	// If empty, persistence is disabled.
	PersistPath string
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
	p := &PEX{
		host:        cfg.Host,
		peers:       make(map[peer.ID]*entry, maxPeers),
		persistPath: cfg.PersistPath,
		loopStop:    make(chan struct{}),
		loopDone:    make(chan struct{}),
		now:         now,
		rng:         rng,
	}
	if cfg.PersistPath != "" {
		p.load()
	}
	return p, nil
}

// Start registers the stream handler and starts the gossip
// loop. The loop exits when ctx is cancelled or Stop is
// called.
func (p *PEX) Start(ctx context.Context) {
	p.host.SetStreamHandler(ProtocolID, p.handleStream)
	go p.gossipLoop(ctx)
}

// Stop unregisters the stream handler, persists the peer table,
// and waits for the gossip loop to exit.
func (p *PEX) Stop() {
	p.host.RemoveStreamHandler(ProtocolID)
	p.save()
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
	p.upsertLocked(ai, nil, p.now().Unix(), false)
}

// AddPeerWithReachability inserts or refreshes a peer with an
// explicit Reachability verdict and relay multiaddrs. The
// daemon uses this to record a peer's verdict as soon as
// AutoNAT or the relay subsystem reports it. Pass
// membusspb.Reachability_UNKNOWN to leave the verdict as-is.
func (p *PEX) AddPeerWithReachability(ai peer.AddrInfo, reach membusspb.Reachability, relayAddrs []multiaddr.Multiaddr) {
	if ai.ID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.upsertLocked(ai, relayAddrs, p.now().Unix(), false)
	if cur, ok := p.peers[ai.ID]; ok {
		if reach != membusspb.Reachability_UNKNOWN {
			cur.info.Reachability = reach
		}
		cur.relayAddrs = append([]multiaddr.Multiaddr(nil), relayAddrs...)
		cur.info.RelayAddrs = addrsToStrings(cur.relayAddrs)
	}
}

// MarkDialResult records the outcome of a Connect attempt
// against pid. A failure flips last_dial_success to false
// so the filter stops sharing a private peer we cannot
// reach.
func (p *PEX) MarkDialResult(pid peer.ID, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, exists := p.peers[pid]
	if !exists {
		return
	}
	e.info.LastDialSuccess = ok
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

// SetReachability updates the reachability verdict for a peer
// that is already in the table. It is a no-op when pid is
// unknown. Used by the daemon to push AutoNAT verdicts in
// after the peer is already in the table.
func (p *PEX) SetReachability(pid peer.ID, reach membusspb.Reachability) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.peers[pid]
	if !ok {
		return
	}
	e.info.Reachability = reach
}

// upsertLocked adds or refreshes ai in the table, keeping the
// entry with the newer last_seen timestamp. The caller must
// hold p.mu.
func (p *PEX) upsertLocked(ai peer.AddrInfo, relayAddrs []multiaddr.Multiaddr, seen int64, keepVerdict bool) {
	if ai.ID == "" || ai.ID == p.host.ID() {
		return
	}
	addrs := addrsToStrings(ai.Addrs)
	cur, ok := p.peers[ai.ID]
	if ok {
		if cur.info.LastSeen >= seen {
			// Always refresh reachability and relay
			// addrs even when the seen-timestamp
			// does not advance, because the
			// caller may have just learned a new
			// verdict.
			if !keepVerdict {
				cur.addrInfo = ai
				cur.info.Addrs = addrs
			}
			return
		}
		prevReach := cur.info.Reachability
		prevDial := cur.info.LastDialSuccess
		prevRelay := append([]multiaddr.Multiaddr(nil), cur.relayAddrs...)
		cur.addrInfo = ai
		cur.info = &membusspb.PeerInfo{
			PeerId:           ai.ID.String(),
			Addrs:            addrs,
			LastSeen:         seen,
			Reachability:     prevReach,
			LastDialSuccess:  prevDial,
			RelayAddrs:       addrsToStrings(append(prevRelay, relayAddrs...)),
		}
		cur.relayAddrs = append(append([]multiaddr.Multiaddr(nil), prevRelay...), relayAddrs...)
		return
	}
	if len(p.peers) >= maxPeers {
		p.evictOldestLocked()
	}
	p.peers[ai.ID] = &entry{
		addrInfo:   ai,
		relayAddrs: append([]multiaddr.Multiaddr(nil), relayAddrs...),
		info: &membusspb.PeerInfo{
			PeerId:      ai.ID.String(),
			Addrs:       addrs,
			LastSeen:    seen,
			RelayAddrs:  addrsToStrings(relayAddrs),
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

// filterForGossip applies the Phase 12 outgoing filter to
// the snapshot. The rules:
//
//   - skip self
//   - skip stale entries (last_seen older than freshnessWindow)
//   - keep PUBLIC entries with full addrs
//   - keep RELAY_ONLY entries with only relay_addrs (addrs
//     field is cleared so the recipient does not waste a
//     direct-connect attempt)
//   - drop PRIVATE entries whose last_dial_success is false
//   - drop PRIVATE entries whose last_dial_success is true
//     but whose addrs look useless (empty) AND we have no
//     relay_addrs to share; the caller's whole point is to
//     find reachable peers, and a private peer we cannot
//     reach is not reachable.
//
// The caller must hold p.mu.
func (p *PEX) filterForGossip(selfID peer.ID) []*membusspb.PeerInfo {
	now := p.now().Unix()
	cutoff := now - int64(freshnessWindow.Seconds())
	out := make([]*membusspb.PeerInfo, 0, len(p.peers))
	for _, e := range p.peers {
		if e.info.PeerId == selfID.String() {
			continue
		}
		if e.info.LastSeen < cutoff {
			continue
		}
		switch e.info.Reachability {
		case membusspb.Reachability_PUBLIC:
			// Include as-is.
			out = append(out, cloneInfo(e.info))
		case membusspb.Reachability_RELAY_ONLY:
			// Strip direct addrs; share only the
			// relay circuit so the recipient can
			// connect via relay.
			c := cloneInfo(e.info)
			c.Addrs = nil
			out = append(out, c)
		case membusspb.Reachability_PRIVATE:
			if !e.info.LastDialSuccess {
				continue
			}
			// PRIVATE with a working dial: still
			// only useful if the recipient can
			// reach us the same way. We share the
			// direct addrs because the recipient
			// may be on the same LAN.
			out = append(out, cloneInfo(e.info))
		case membusspb.Reachability_UNKNOWN:
			// Unknown reachability: do not share
			// addrs (we do not know if they are
			// useful) but still share the peer ID
			// so the recipient can dedupe. Empty
			// addrs list means the recipient will
			// not attempt a connect, which is
			// exactly the safe default.
			c := cloneInfo(e.info)
			c.Addrs = nil
			c.RelayAddrs = nil
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PeerId < out[j].PeerId })
	return out
}

// FilterForGossip is a public wrapper around filterForGossip
// for tests. Production callers go through snapshot+send.
func (p *PEX) FilterForGossip() []*membusspb.PeerInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.filterForGossip(p.host.ID())
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
	reply := &membusspb.PEXMessage{Peers: p.filterForGossip(p.host.ID())}
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
	saveTick := time.NewTicker(5 * time.Minute)
	defer saveTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.loopStop:
			return
		case <-saveTick.C:
			p.save()
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
	out := &membusspb.PEXMessage{Peers: p.filterForGossip(p.host.ID())}
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
// table. For each newly-discovered peer with usable addrs, an
// async dial is fired in the background using the address
// shape that matches the entry's reachability.
func (p *PEX) mergeFromMessage(infos []*membusspb.PeerInfo, seen int64) {
	for _, info := range infos {
		if info == nil {
			continue
		}
		ai, ok := decodePeerInfo(info)
		if !ok {
			continue
		}
		// Pull relay_addrs out of the protobuf and
		// convert them back to multiaddrs.
		var relay []multiaddr.Multiaddr
		for _, s := range info.RelayAddrs {
			if a, err := multiaddr.NewMultiaddr(s); err == nil {
				relay = append(relay, a)
			}
		}
		p.mu.Lock()
		wasKnown := p.peers[ai.ID] != nil
		p.upsertLocked(ai, relay, seen, true)
		// After upsert, restore the reachability /
		// last_dial_success fields from the wire
		// payload (upsertLocked already merged
		// relay_addrs and addrs but not those).
		if cur, exists := p.peers[ai.ID]; exists {
			cur.info.Reachability = info.Reachability
			cur.info.LastDialSuccess = info.LastDialSuccess
			cur.relayAddrs = relay
		}
		p.mu.Unlock()

		// Connect decision tree (Phase 12).
		if !wasKnown {
			p.dialFor(info, ai, relay)
		}
	}
}

// dialFor picks the right address set to use for a connect
// attempt based on the entry's reachability verdict.
//
//   - PUBLIC          -> direct addrs
//   - RELAY_ONLY      -> relay addrs only
//   - PRIVATE         -> skip
//   - UNKNOWN         -> try direct addrs; if the connect
//     fails the entry's last_dial_success will be flipped
//     to false by MarkDialResult
func (p *PEX) dialFor(info *membusspb.PeerInfo, ai peer.AddrInfo, relay []multiaddr.Multiaddr) {
	switch info.Reachability {
	case membusspb.Reachability_RELAY_ONLY:
		if len(relay) == 0 {
			return
		}
		go p.dialAsync(peer.AddrInfo{ID: ai.ID, Addrs: relay}, ai.ID)
	case membusspb.Reachability_PRIVATE:
		// Private peers are useless to us directly;
		// the spec says "skip".
		return
	case membusspb.Reachability_PUBLIC, membusspb.Reachability_UNKNOWN:
		if len(ai.Addrs) == 0 {
			return
		}
		go p.dialAsync(ai, ai.ID)
	}
}

// dialAsync attempts to connect to ai in the background and
// records the outcome on the table entry.
func (p *PEX) dialAsync(ai peer.AddrInfo, pid peer.ID) {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	err := p.host.Connect(ctx, ai)
	p.MarkDialResult(pid, err == nil)
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

// silence unused import warning when membusspb-only code
// paths are inlined by the linker.

// save persists the peer table to disk. It is a no-op when
// PersistPath is empty.
func (p *PEX) save() {
	if p.persistPath == "" {
		return
	}
	p.mu.Lock()
	msg := &membusspb.PEXMessage{Peers: p.snapshot()}
	p.mu.Unlock()
	data, err := proto.Marshal(msg)
	if err != nil {
		return
	}
	_ = os.WriteFile(p.persistPath, data, 0o600)
}

// load restores the peer table from disk. It is a no-op when
// PersistPath is empty or the file does not exist.
func (p *PEX) load() {
	if p.persistPath == "" {
		return
	}
	data, err := os.ReadFile(p.persistPath)
	if err != nil {
		return
	}
	var msg membusspb.PEXMessage
	if err := proto.Unmarshal(data, &msg); err != nil {
		return
	}
	seen := p.now().Unix()
	for _, info := range msg.Peers {
		ai, ok := decodePeerInfo(info)
		if !ok {
			continue
		}
		var relay []multiaddr.Multiaddr
		for _, s := range info.RelayAddrs {
			if a, err := multiaddr.NewMultiaddr(s); err == nil {
				relay = append(relay, a)
			}
		}
		p.mu.Lock()
		p.upsertLocked(ai, relay, seen, true)
		if cur, exists := p.peers[ai.ID]; exists {
			cur.info.Reachability = info.Reachability
			cur.info.LastDialSuccess = info.LastDialSuccess
			cur.relayAddrs = relay
		}
		p.mu.Unlock()
	}
}
