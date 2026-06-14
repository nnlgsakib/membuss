package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAPIKeyAuth_Disabled(t *testing.T) {
	h := apiKeyAuth("")
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(h(next))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if !called {
		t.Error("inner handler not called")
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status: got %d want 200", resp.StatusCode)
	}
}

func TestAPIKeyAuth_RejectsMissing(t *testing.T) {
	h := apiKeyAuth("secret")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("inner handler should not run")
	})
	srv := httptest.NewServer(h(next))
	defer srv.Close()
	resp, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: got %d want 401", resp.StatusCode)
	}
	if !strings.Contains(string(body), "unauthorized") {
		t.Errorf("body: %q", string(body))
	}
}

func TestAPIKeyAuth_RejectsWrong(t *testing.T) {
	h := apiKeyAuth("secret")
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("inner handler should not run")
	})
	srv := httptest.NewServer(h(next))
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL, nil)
	req.Header.Set("X-Membuss-Key", "wrong")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status: got %d want 401", resp.StatusCode)
	}
}

func TestAPIKeyAuth_AcceptsCorrect(t *testing.T) {
	h := apiKeyAuth("secret")
	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusTeapot)
	})
	srv := httptest.NewServer(h(next))
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL, nil)
	req.Header.Set("X-Membuss-Key", "secret")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	resp.Body.Close()
	if !called {
		t.Error("inner handler not called")
	}
	if resp.StatusCode != http.StatusTeapot {
		t.Errorf("status: got %d want 418", resp.StatusCode)
	}
}
