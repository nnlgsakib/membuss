package memgate_v2

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestIPLimiter_AllowsBelowLimit(t *testing.T) {
	il := newIPLimiter(60, time.Minute) // 60 per minute
	defer func() { il.mu.Lock(); il.visitors = nil; il.mu.Unlock() }()
	h := il.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	for i := 0; i < 5; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "1.2.3.4:5000"
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("req %d status %d", i, rec.Code)
		}
	}
}

func TestIPLimiter_RejectsAboveBurst(t *testing.T) {
	// 60 per minute, burst 60. Fire 70 requests in a tight loop.
	il := newIPLimiter(60, time.Minute)
	defer func() { il.mu.Lock(); il.visitors = nil; il.mu.Unlock() }()
	h := il.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "5.6.7.8:5000"
	var ok, denied int
	for i := 0; i < 80; i++ {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code == http.StatusOK {
			ok++
		} else if rec.Code == http.StatusTooManyRequests {
			denied++
		}
	}
	if denied == 0 {
		t.Errorf("expected some 429 responses, got ok=%d denied=%d", ok, denied)
	}
}

func TestIPLimiter_NilIsNoop(t *testing.T) {
	var il *ipLimiter
	called := int32(0)
	h := il.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&called, 1)
		w.WriteHeader(http.StatusOK)
	}))
	for i := 0; i < 100; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}
	if atomic.LoadInt32(&called) != 100 {
		t.Errorf("nil limiter blocked requests: got %d", called)
	}
}

func TestIPLimiter_ZeroRPSIsNoop(t *testing.T) {
	il := newIPLimiter(0, time.Minute)
	if il != nil {
		t.Errorf("expected nil limiter for rps=0, got %+v", il)
	}
}
