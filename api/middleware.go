package api

import (
	"crypto/subtle"
	"net/http"
)

// apiKeyAuth returns a chi middleware that rejects requests
// whose X-Membuss-Key header does not match want. When want is
// empty the middleware is a no-op (auth disabled). The check
// uses constant-time comparison to avoid timing oracles.
func apiKeyAuth(want string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if want == "" {
				next.ServeHTTP(w, r)
				return
			}
			got := r.Header.Get("X-Membuss-Key")
			if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"ok":false,"error":"unauthorized"}` + "\n"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
