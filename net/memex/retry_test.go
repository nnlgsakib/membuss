package memex

import (
	"errors"
	"testing"
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
