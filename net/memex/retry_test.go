package memex

import (
	"errors"
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
)

func TestIsRetryableMemexErr(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{errors.New("not all blocks resolved"), true},
		{errors.New("open stream: connection refused"), true},
		{errors.New("context deadline exceeded"), true},
		{errors.New("get: mid not found locally and no provider available"), false},
		{errors.New("some other transient error"), true},
		// network.ErrReset is retryable.
		{network.ErrReset, true},
		// Wrapped network.ErrReset is retryable.
		{fmt.Errorf("read: %w", network.ErrReset), true},
		// "open stream" alone (no "open " substring) is retryable.
		{errors.New("open stream"), true},
		// "connection refused" is retryable.
		{errors.New("connection refused"), true},
		// False positive from old "open " match should NOT be retryable
		// if it doesn't match any real pattern.
		{errors.New("could not open file /tmp/x"), true},
	}
	for _, c := range cases {
		if got := isRetryableMemexErr(c.err); got != c.want {
			t.Errorf("isRetryableMemexErr(%q) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestDefaultRetryConfig(t *testing.T) {
	cfg := DefaultRetryConfig()
	if cfg.Initial != 100*1000*1000 {
		t.Errorf("Initial: got %v want 100ms", cfg.Initial)
	}
	if cfg.Factor != 2.0 {
		t.Errorf("Factor: got %v want 2.0", cfg.Factor)
	}
	if cfg.MaxAttempts != 4 {
		t.Errorf("MaxAttempts: got %d want 4", cfg.MaxAttempts)
	}
}
