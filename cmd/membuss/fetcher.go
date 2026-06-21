// memexFetcher adapts memex.Session to anchor.Fetcher. The
// anchor engine only cares that the local blockstore is
// populated, so the io.Reader returned by Session.Fetch is
// fully consumed (and discarded).
package main

import (
	"context"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/net/memex"
)

type memexFetcher struct {
	eng *memex.Engine
}

func (f *memexFetcher) Fetch(ctx context.Context, root mid.MID, providers []peer.AddrInfo) error {
	if len(providers) == 0 {
		return nil
	}
	sess, err := memex.NewSession(memex.SessionConfig{
		Engine:    f.eng,
		Root:      root,
		Providers: providers,
		Timeout:   30 * time.Second,
	})
	if err != nil {
		return err
	}
	rc, err := sess.FetchWithBackoff(ctx, memex.DefaultRetryConfig())
	if err != nil {
		return err
	}
	if rc != nil {
		if c, ok := rc.(io.Closer); ok {
			_ = c.Close()
		} else {
			_, _ = io.Copy(io.Discard, rc)
		}
	}
	return nil
}
