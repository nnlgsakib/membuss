package memgate

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// ipLimiter is a per-source-IP rate limiter pool. Each unique
// client IP gets its own token-bucket limiter; old entries are
// reaped after idleTimeout of inactivity.
type ipLimiter struct {
	mu          sync.Mutex
	limit       rate.Limit
	burst       int
	idleTimeout time.Duration
	visitors    map[string]*visitor
}

type visitor struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// newIPLimiter returns a per-IP limiter pool that allows rps
// requests per second per IP with the given burst, and forgets
// about IPs that have not connected in idleTimeout.
func newIPLimiter(rps int, idleTimeout time.Duration) *ipLimiter {
	if rps <= 0 {
		return nil
	}
	il := &ipLimiter{
		limit:       rate.Limit(float64(rps) / 60.0), // rps is per-minute
		burst:       rps,
		idleTimeout: idleTimeout,
		visitors:    make(map[string]*visitor),
	}
	go il.reaper()
	return il
}

// limitFor returns (and lazily creates) the limiter for ip.
func (i *ipLimiter) limitFor(ip string) *rate.Limiter {
	i.mu.Lock()
	defer i.mu.Unlock()
	v, ok := i.visitors[ip]
	if !ok {
		v = &visitor{limiter: rate.NewLimiter(i.limit, i.burst)}
		i.visitors[ip] = v
	}
	v.lastSeen = time.Now()
	return v.limiter
}

func (i *ipLimiter) reaper() {
	t := time.NewTicker(i.idleTimeout)
	defer t.Stop()
	for range t.C {
		cutoff := time.Now().Add(-i.idleTimeout)
		i.mu.Lock()
		for ip, v := range i.visitors {
			if v.lastSeen.Before(cutoff) {
				delete(i.visitors, ip)
			}
		}
		i.mu.Unlock()
	}
}

// Middleware returns an http middleware that enforces the per-IP
// rate limit. When rps is <= 0 the middleware is a no-op.
func (i *ipLimiter) Middleware(next http.Handler) http.Handler {
	if i == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		if !i.limitFor(ip).Allow() {
			w.Header().Set("Retry-After", "60")
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("rate limit exceeded\n"))
			return
		}
		next.ServeHTTP(w, r)
	})
}
