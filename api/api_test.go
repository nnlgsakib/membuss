// Tests for the Node API. We use an in-memory memBackend and
// httptest to drive the chi router.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/nnlgsakib/membuss/core/mid"
)

// memBackend is an in-memory Backend. It stores (mid, bytes)
// pairs in a map and answers every RPC from that map.
type memBackend struct {
	mu      sync.Mutex
	content map[string][]byte
	sealed  map[string]bool
	peers   []PeerInfo
}

func newMemBackend() *memBackend {
	return &memBackend{
		content: map[string][]byte{},
		sealed:  map[string]bool{},
	}
}

func (b *memBackend) put(data []byte) mid.MID {
	m := mid.FromBytes(data)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.content[m.String()] = data
	b.sealed[m.String()] = true
	return m
}

func (b *memBackend) Add(ctx context.Context, name string, r io.Reader) (AddResult, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return AddResult{}, err
	}
	m := mid.FromBytes(data)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.content[m.String()] = data
	b.sealed[m.String()] = true
	return AddResult{MID: m.String(), Size: uint64(len(data)), Blocks: 1}, nil
}

func (b *memBackend) Resolve(ctx context.Context, m mid.MID) (io.ReadCloser, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.content[m.String()]
	if !ok {
		return nil, 0, fmt.Errorf("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), uint64(len(data)), nil
}

func (b *memBackend) Seal(ctx context.Context, m mid.MID) (SealResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed[m.String()] {
		return SealResult{Pinned: 0, Already: true}, nil
	}
	b.sealed[m.String()] = true
	return SealResult{Pinned: 1, Already: false}, nil
}

func (b *memBackend) Unseal(ctx context.Context, m mid.MID) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.sealed[m.String()] {
		return 0, nil
	}
	delete(b.sealed, m.String())
	return 1, nil
}

func (b *memBackend) Stat(ctx context.Context, m mid.MID) (StatInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.content[m.String()]
	if !ok {
		return StatInfo{}, nil
	}
	return StatInfo{
		Present: true,
		Size:    uint64(len(data)),
		Blocks:  1,
		Sealed:  b.sealed[m.String()],
	}, nil
}

func (b *memBackend) Peers(limit int) ([]PeerInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := append([]PeerInfo{}, b.peers...)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (b *memBackend) GC(ctx context.Context) (GCInfo, error) {
	return GCInfo{BytesFreed: 1024, BlocksKept: 5}, nil
}

func (b *memBackend) NodeInfo() NodeInfo {
	return NodeInfo{PeerID: "12D3KooA", Addrs: []string{"/ip4/1.2.3.4/tcp/4001"}, Version: "0.1.0", Build: "test"}
}

// --- Phase 17: MemFS stubs on memBackend ---

// memStore is a minimal in-memory content store that the
// test memBackend uses to satisfy the new MemFS methods.
// Tests that need a real MemFS tree (AddFile / AddDirectory
// / Ls / GetPath) populate this via the dedicated helpers.
type memStore struct {
	mu      sync.Mutex
	content map[string][]byte
}

func (s *memStore) put(mid string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.content == nil {
		s.content = map[string][]byte{}
	}
	s.content[mid] = data
}

func (s *memStore) get(mid string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.content == nil {
		return nil, false
	}
	d, ok := s.content[mid]
	return d, ok
}

// AddFile is implemented by the production adapter; in the
// test backend it just falls through to Add so existing
// tests continue to pass. wrapDir is ignored.
func (b *memBackend) AddFile(ctx context.Context, name string, r io.Reader, wrapDir bool) (AddResult, error) {
	return b.Add(ctx, name, r)
}
func (b *memBackend) AddDirectory(ctx context.Context, parts []DirectoryPart) (AddResult, error) {
	return AddResult{}, fmt.Errorf("memfs: test backend does not implement AddDirectory")
}
func (b *memBackend) Ls(ctx context.Context, m mid.MID) ([]LsEntry, error) {
	return nil, fmt.Errorf("memfs: test backend does not implement Ls")
}
func (b *memBackend) GetPath(ctx context.Context, m mid.MID, path string) (io.ReadSeekCloser, uint64, string, error) {
	return nil, 0, "", fmt.Errorf("memfs: test backend does not implement GetPath")
}

func newTestAPI(t *testing.T, b Backend) *NodeAPI {
	t.Helper()
	a, err := New(Config{Backend: b, MaxUploadBytes: 1 << 20})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

func TestHealthz(t *testing.T) {
	srv := httptest.NewServer(newTestAPI(t, newMemBackend()).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if !env.OK {
		t.Errorf("ok: %v", env.OK)
	}
}

func TestAdd_RawBody(t *testing.T) {
	b := newMemBackend()
	srv := httptest.NewServer(newTestAPI(t, b).Router())
	defer srv.Close()
	body := []byte("hello world")
	resp, err := http.Post(srv.URL+"/api/v1/add", "application/octet-stream", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var env envelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if !env.OK {
		t.Errorf("ok: false; error: %s", env.Error)
	}
	data := env.Data.(map[string]any)
	midStr := data["mid"].(string)
	if !strings.HasPrefix(midStr, "mem") {
		t.Errorf("mid: %q", midStr)
	}
}

func TestAdd_Multipart(t *testing.T) {
	b := newMemBackend()
	srv := httptest.NewServer(newTestAPI(t, b).Router())
	defer srv.Close()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("file", "hello.txt")
	fw.Write([]byte("multipart body"))
	mw.Close()
	resp, err := http.Post(srv.URL+"/api/v1/add", mw.FormDataContentType(), &buf)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var env envelope
	json.NewDecoder(resp.Body).Decode(&env)
	if !env.OK {
		t.Errorf("ok: false; error: %s", env.Error)
	}
}

func TestAdd_TooLarge(t *testing.T) {
	b := newMemBackend()
	a, _ := New(Config{Backend: b, MaxUploadBytes: 10})
	srv := httptest.NewServer(a.Router())
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/api/v1/add", "application/octet-stream", bytes.NewReader(make([]byte, 100)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge && resp.StatusCode != http.StatusInternalServerError {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestGet_OK(t *testing.T) {
	b := newMemBackend()
	m := b.put([]byte("payload"))
	srv := httptest.NewServer(newTestAPI(t, b).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/get/" + m.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "payload" {
		t.Errorf("body: %q", got)
	}
	if resp.Header.Get("X-Membuss-MID") != m.String() {
		t.Errorf("mid header: %q", resp.Header.Get("X-Membuss-MID"))
	}
}

func TestGet_BadMID(t *testing.T) {
	srv := httptest.NewServer(newTestAPI(t, newMemBackend()).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/get/not-a-mid")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestSeal_Already(t *testing.T) {
	b := newMemBackend()
	m := b.put([]byte("x"))
	srv := httptest.NewServer(newTestAPI(t, b).Router())
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/api/v1/seal/"+m.String(), "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var env envelope
	json.NewDecoder(resp.Body).Decode(&env)
	data := env.Data.(map[string]any)
	if data["already"] != true {
		t.Errorf("already: %v", data["already"])
	}
}

func TestUnseal_OK(t *testing.T) {
	b := newMemBackend()
	m := b.put([]byte("x"))
	srv := httptest.NewServer(newTestAPI(t, b).Router())
	defer srv.Close()
	req, _ := http.NewRequest("DELETE", srv.URL+"/api/v1/seal/"+m.String(), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var env envelope
	json.NewDecoder(resp.Body).Decode(&env)
	if !env.OK {
		t.Errorf("ok: false; error: %s", env.Error)
	}
}

func TestStat_OK(t *testing.T) {
	b := newMemBackend()
	m := b.put([]byte("hello"))
	srv := httptest.NewServer(newTestAPI(t, b).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/stat/" + m.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var env envelope
	json.NewDecoder(resp.Body).Decode(&env)
	if !env.OK {
		t.Errorf("ok: false; error: %s", env.Error)
	}
}

func TestStat_NotFound(t *testing.T) {
	srv := httptest.NewServer(newTestAPI(t, newMemBackend()).Router())
	defer srv.Close()
	// Build a syntactically valid but unknown MID so the
	// handler reaches the backend and gets a 404.
	midStr := mid.FromBytes([]byte("never added")).String()
	resp, err := http.Get(srv.URL + "/api/v1/stat/" + midStr)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestPeers(t *testing.T) {
	b := newMemBackend()
	b.peers = []PeerInfo{{PeerID: "12D3KooA", Addrs: []string{"/ip4/1.1.1.1/tcp/4001"}}}
	srv := httptest.NewServer(newTestAPI(t, b).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/peers?limit=10")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var env envelope
	json.NewDecoder(resp.Body).Decode(&env)
	data := env.Data.(map[string]any)
	peers := data["peers"].([]any)
	if len(peers) != 1 {
		t.Errorf("peers: %d", len(peers))
	}
}

func TestNodeInfo(t *testing.T) {
	srv := httptest.NewServer(newTestAPI(t, newMemBackend()).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/node/info")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var env envelope
	json.NewDecoder(resp.Body).Decode(&env)
	data := env.Data.(map[string]any)
	if data["peer_id"] != "12D3KooA" {
		t.Errorf("peer_id: %v", data["peer_id"])
	}
	if data["build"] != "test" {
		t.Errorf("build: %v", data["build"])
	}
}

func TestGC(t *testing.T) {
	srv := httptest.NewServer(newTestAPI(t, newMemBackend()).Router())
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/api/v1/gc", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var env envelope
	json.NewDecoder(resp.Body).Decode(&env)
	data := env.Data.(map[string]any)
	if data["bytes_freed"] != float64(1024) {
		t.Errorf("bytes_freed: %v", data["bytes_freed"])
	}
}

func TestEnvelope_FailPath(t *testing.T) {
	srv := httptest.NewServer(newTestAPI(t, newMemBackend()).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/api/v1/get/bad")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var env envelope
	json.NewDecoder(resp.Body).Decode(&env)
	if env.OK || env.Error == "" {
		t.Errorf("expected fail envelope; got %+v", env)
	}
}