package main

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
	"github.com/nnlgsakib/membuss/net/memex"
)

var _ store.Blockstore = (*fetchingBlockstore)(nil)

// fetchingBlockstore wraps a store.Blockstore to intercept Get calls.
// If a block is not found locally, it queries the DHT and retrieves it via Memex.
type fetchingBlockstore struct {
	store.Blockstore
	b   *daemonBackend
	ctx context.Context
}

// Get retrieves a block by MID. If it's missing locally, it fetches it from the network.
func (f *fetchingBlockstore) Get(m mid.MID) ([]byte, error) {
	data, err := f.Blockstore.Get(m)
	if err == nil {
		return data, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return nil, err
	}
	if f.b == nil || f.b.memex == nil || f.b.dht == nil {
		return nil, store.ErrNotFound
	}

	// Not found locally. Try fetching from network.
	provCtx, cancel := context.WithTimeout(f.ctx, 15*time.Second)
	provs, perr := f.b.dht.FindProviders(provCtx, m)
	cancel()
	if perr != nil || len(provs) == 0 {
		// Fallback: use currently connected swarm peers
		for _, pid := range f.b.host.Network().Peers() {
			provs = append(provs, f.b.host.Peerstore().PeerInfo(pid))
		}
	}
	if len(provs) == 0 {
		return nil, store.ErrNotFound
	}

	sess, serr := memex.NewSession(memex.SessionConfig{
		Engine:    f.b.memex,
		Root:      m,
		Providers: provs,
		Timeout:   30 * time.Second,
	})
	if serr != nil {
		return nil, store.ErrNotFound
	}

	rc, ferr := sess.FetchWithBackoff(f.ctx, memex.DefaultRetryConfig())
	if ferr != nil {
		return nil, store.ErrNotFound
	}

	if rc != nil {
		if c, ok := rc.(io.Closer); ok {
			_ = c.Close()
		} else {
			_, _ = io.Copy(io.Discard, rc)
		}
	}

	// Re-try local retrieval now that the session has successfully fetched it
	return f.Blockstore.Get(m)
}
