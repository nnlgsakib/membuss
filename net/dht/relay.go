// Phase 11: relay discovery via the Membuss DHT.
//
// A node that runs Config.RelayService=true advertises itself
// under RelaysKey in the Membuss DHT. Other nodes can call
// FindRelays to get a deduplicated list of candidate relays
// to feed into the AutoRelay subsystem.
//
// The stored value is a JSON-encoded []peer.AddrInfo. The kad-dht
// "membuss" permissive validator accepts the bytes as-is.
package dht

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// RelaysKey is the DHT key under which the local node publishes
// itself as a relay when Config.RelayService=true. The /v1
// suffix lets us rev the encoding without breaking existing
// clients.
const RelaysKey = "/membuss/relays/v1"

// anchorsKey is the existing anchor key from Phase 6. Re-
// declared here for symmetry with the relay helpers so callers
// do not need to import the anchor package.
const anchorsKey = "/membuss/anchors/v1"

// relayRecord is the JSON layout stored under RelaysKey.
//
// It is a small envelope: a version byte plus a list of
// peer.AddrInfo. Wrapping the addresses in a struct (instead
// of a bare JSON array) lets us rev the format later without
// breaking the validator path.
type relayRecord struct {
	Version uint          `json:"v"`
	Peers   []peer.AddrInfo `json:"peers"`
}

// PublishAsRelay writes the local node's AddrInfo under
// RelaysKey in the DHT. It is a no-op for in-process test
// hosts that have no listen addrs. Callers should rate-limit
// the call; the herald's relay announcer does so at one
// publish per ReprovideInterval.
func (m *MemDHT) PublishAsRelay(ctx context.Context) error {
	if m == nil || m.dht == nil {
		return errors.New("dht: nil")
	}
	h := m.dht.Host()
	if h == nil {
		return errors.New("dht: nil host")
	}
	addrs := h.Addrs()
	if len(addrs) == 0 {
		// No addresses = nothing to publish. This is
		// normal for the in-process test host and not
		// an error.
		return nil
	}
	rec := relayRecord{
		Version: 1,
		Peers:   []peer.AddrInfo{{ID: h.ID(), Addrs: addrs}},
	}
	buf, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("dht: marshal relay record: %w", err)
	}
	return m.dht.PutValue(ctx, RelaysKey, buf)
}

// FindRelays queries the DHT for the relay list and returns a
// deduplicated, sorted set of AddrInfo values. The number of
// candidates is bounded by max; pass 0 to take the default
// (32). Returns an empty slice (never nil) when no relays
// have been published yet.
func (m *MemDHT) FindRelays(ctx context.Context, max int) ([]peer.AddrInfo, error) {
	if m == nil || m.dht == nil {
		return nil, errors.New("dht: nil")
	}
	if max <= 0 {
		max = 32
	}
	buf, err := m.dht.GetValue(ctx, RelaysKey)
	if err != nil {
		// "no peers" / "not found" is the common case on
		// a fresh testnet; return an empty slice rather
		// than an error so the caller can fall back to
		// the static config list.
		return nil, nil
	}
	var rec relayRecord
	if err := json.Unmarshal(buf, &rec); err != nil {
		return nil, fmt.Errorf("dht: decode relay record: %w", err)
	}
	if len(rec.Peers) == 0 {
		return nil, nil
	}
	// Dedupe by peer.ID and sort for deterministic output.
	seen := make(map[peer.ID]struct{}, len(rec.Peers))
	out := make([]peer.AddrInfo, 0, len(rec.Peers))
	for _, p := range rec.Peers {
		if p.ID == "" {
			continue
		}
		if _, ok := seen[p.ID]; ok {
			continue
		}
		seen[p.ID] = struct{}{}
		out = append(out, p)
		if len(out) >= max {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// AddrInfoFromHost is a small convenience used by callers that
// have a host but no AddrInfo (e.g. the daemon when wiring
// AutoRelay's static relay list). The returned AddrInfo
// reflects the host's current self-perceived addresses.
func AddrInfoFromHost(h host.Host) peer.AddrInfo {
	if h == nil {
		return peer.AddrInfo{}
	}
	return peer.AddrInfo{ID: h.ID(), Addrs: h.Addrs()}
}

// quiet time import (testing the periodic republish cadence
// uses time-based scheduling in the herald, not here, but
// keep the import visible for future expansion).
var _ = time.Second
