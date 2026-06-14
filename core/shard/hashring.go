// Package shard distributes MIDs to peer IDs using rendezvous
// (highest-random-weight / HRW) hashing.
//
// The mapping MID -> [peer1, peer2, ...] is deterministic and
// depends only on the set of peers and the MID. Adding or
// removing a peer only remaps a fraction of MIDs: with N peers,
// the expected number of MIDs reassigned when a single peer is
// removed is 1/N of the total.
//
// Rendezvous hashing is preferred over a hash ring here because
// it has O(N) cost per assignment (no virtual nodes), produces
// the optimal minimum-disruption property, and is trivial to
// implement correctly.
package shard

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"sync"

	"github.com/nnlgsakib/membuss/core/mid"
)

// MinReplicas is the smallest replica count accepted by Assign.
const MinReplicas = 1

// MaxReplicas is the largest replica count accepted by Assign.
// It is bounded because the assignment cost is O(N * replicas),
// and we want to keep it predictable.
const MaxReplicas = 64

// HashRing maps MIDs to peer IDs using rendezvous hashing.
// It is safe for concurrent use after construction.
type HashRing struct {
	mu    sync.RWMutex
	peers []string
}

// NewHashRing returns an empty HashRing. Peers are added with
// AddPeer; assigning before any peer is added returns an error.
func NewHashRing() *HashRing {
	return &HashRing{}
}

// AddPeer registers a peer ID. Duplicate peer IDs are silently
// de-duplicated so a peer that joins twice does not skew the
// assignment distribution.
func (r *HashRing) AddPeer(peerID string) {
	if peerID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, p := range r.peers {
		if p == peerID {
			return
		}
	}
	r.peers = append(r.peers, peerID)
}

// RemovePeer drops a peer ID. Missing peers are not an error.
func (r *HashRing) RemovePeer(peerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, p := range r.peers {
		if p == peerID {
			r.peers = append(r.peers[:i], r.peers[i+1:]...)
			return
		}
	}
}

// Peers returns a copy of the current peer set, in arbitrary
// order. Callers MUST NOT mutate the returned slice.
func (r *HashRing) Peers() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, len(r.peers))
	copy(out, r.peers)
	return out
}

// Len returns the number of registered peers.
func (r *HashRing) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.peers)
}

// Assign returns the top-replicas peer IDs responsible for
// storing the given MID, ordered by score (highest first).
//
// replicas must be in [MinReplicas, MaxReplicas]. It is clamped
// to the number of available peers: if the ring has fewer
// peers than replicas, every peer is returned.
func (r *HashRing) Assign(m mid.MID, replicas int) ([]string, error) {
	if m.IsZero() {
		return nil, errors.New("shard: zero MID")
	}
	if replicas < MinReplicas {
		return nil, errors.New("shard: replicas below minimum")
	}
	if replicas > MaxReplicas {
		return nil, errors.New("shard: replicas above maximum")
	}

	r.mu.RLock()
	peers := r.peers
	r.mu.RUnlock()
	if len(peers) == 0 {
		return nil, errors.New("shard: no peers in ring")
	}

	type scored struct {
		peer  string
		score uint64
	}
	scores := make([]scored, len(peers))
	midBytes := []byte(m.String())
	for i, p := range peers {
		scores[i] = scored{peer: p, score: hrwScore(p, midBytes)}
	}
	sort.Slice(scores, func(a, b int) bool {
		if scores[a].score != scores[b].score {
			return scores[a].score > scores[b].score
		}
		// Tie-break by peer ID so the result is fully
		// deterministic.
		return scores[a].peer < scores[b].peer
	})
	if replicas > len(scores) {
		replicas = len(scores)
	}
	out := make([]string, replicas)
	for i := 0; i < replicas; i++ {
		out[i] = scores[i].peer
	}
	return out, nil
}

// hrwScore computes a 64-bit rendezvous score for a (peer, mid)
// pair. SHA-256 of "peer || mid" is taken and the first 8
// bytes are interpreted as a big-endian uint64. This is the
// canonical "highest random weight" formulation.
func hrwScore(peer string, midBytes []byte) uint64 {
	h := sha256.New()
	h.Write([]byte(peer))
	h.Write([]byte{0x00}) // domain separator
	h.Write(midBytes)
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}
