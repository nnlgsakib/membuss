// Tests for the explorer package. The tests use a
// memBackend (an in-memory implementation of explorer.Backend)
// and httptest to drive the chi router.
package explorer

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nnlgsakib/membuss/core/mid"
)

// memBackend is an in-memory implementation of Backend.
type memBackend struct {
	mu         sync.Mutex
	content    map[string][]byte
	sealed     map[string]bool
	providers  map[string][]string
	anchorMode bool
	started    time.Time
}

func newMemBackend() *memBackend {
	return &memBackend{
		content:   map[string][]byte{},
		sealed:    map[string]bool{},
		providers: map[string][]string{},
		started:   time.Now().Add(-42 * time.Second),
	}
}

func (b *memBackend) put(m mid.MID, data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.content[m.String()] = data
	b.sealed[m.String()] = true
}

func (b *memBackend) addProvider(m mid.MID, peer string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.providers[m.String()] = append(b.providers[m.String()], peer)
}

func (b *memBackend) setAnchorMode(v bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.anchorMode = v
}

func (b *memBackend) Stat(ctx context.Context, m mid.MID) (bool, uint64, uint64, bool, uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.content[m.String()]
	if !ok {
		return false, 0, 0, false, 0, nil
	}
	return true, uint64(len(data)), 1, true, m.Codec(), nil
}

func (b *memBackend) Seal(ctx context.Context, m mid.MID) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sealed[m.String()] = true
	return nil
}

func (b *memBackend) Unseal(ctx context.Context, m mid.MID) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.sealed, m.String())
	return nil
}

func (b *memBackend) Providers(ctx context.Context, m mid.MID, limit int) ([]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := append([]string{}, b.providers[m.String()]...)
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (b *memBackend) Peers(ctx context.Context, limit int) ([]PeerInfo, error) {
	return []PeerInfo{
		{PeerID: "12D3KooFake", Addrs: []string{"/ip4/127.0.0.1/tcp/4001"}},
		{PeerID: "12D3KooOther", Addrs: []string{"/ip4/127.0.0.1/tcp/4002", "/ip4/127.0.0.1/udp/4002/quic-v1"}},
	}, nil
}

func (b *memBackend) SealedMIDs(ctx context.Context) ([]mid.MID, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]mid.MID, 0, len(b.sealed))
	for s := range b.sealed {
		m, err := mid.Parse(s)
		if err == nil {
			out = append(out, m)
		}
	}
	return out, nil
}

func (b *memBackend) SealedCount(ctx context.Context) (int, error) {
	mids, err := b.SealedMIDs(ctx)
	if err != nil {
		return 0, err
	}
	return len(mids), nil
}

func (b *memBackend) BlockCount(ctx context.Context) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return uint64(len(b.content)), nil
}

func (b *memBackend) StoreBytes(ctx context.Context) (uint64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var n uint64
	for _, d := range b.content {
		n += uint64(len(d))
	}
	return n, nil
}

func (b *memBackend) AnchorPeers(ctx context.Context) ([]AnchorRow, error) {
	return []AnchorRow{}, nil
}

func (b *memBackend) AnchorStatus(ctx context.Context) AnchorInfo {
	return AnchorInfo{
		PeerID:     "12D3KooSelf",
		UptimeSecs: 42,
		BlocksHeld: 0,
		Anchors:    0,
		Backlog:    0,
		Synced:     0,
	}
}

func (b *memBackend) LocalPeerID(ctx context.Context) string { return "12D3KooSelf" }
func (b *memBackend) LocalAddrs(ctx context.Context) []string {
	return []string{"/ip4/127.0.0.1/tcp/4001", "/ip4/127.0.0.1/udp/4001/quic-v1"}
}
func (b *memBackend) NodeVersion(ctx context.Context) (string, string) { return "0.1.0", "test" }
func (b *memBackend) Uptime(ctx context.Context) time.Duration         { return time.Since(b.started) }
func (b *memBackend) AnchorMode(ctx context.Context) bool              { return b.anchorMode }

// --- helpers ---

func newTestServer(t *testing.T) (*httptest.Server, *memBackend) {
	t.Helper()
	b := newMemBackend()
	e, err := New(Config{Backend: b})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv := httptest.NewServer(e.Handler())
	t.Cleanup(srv.Close)
	return srv, b
}

// noRedirectClient returns an http.Client that does not
// follow redirects.
func noRedirectClient() *http.Client {
	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func get(t *testing.T, srv *httptest.Server, path string) (*http.Response, []byte) {
	t.Helper()
	resp, err := noRedirectClient().Get(srv.URL + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return resp, body
}

func postForm(t *testing.T, srv *httptest.Server, path string, data url.Values) (*http.Response, []byte) {
	t.Helper()
	resp, err := noRedirectClient().PostForm(srv.URL+path, data)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return resp, body
}

// --- tests ---

func TestHome(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, body := get(t, srv, "/")
	if resp.StatusCode != 200 {
		t.Errorf("status: %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "Membuss Explorer") {
		t.Errorf("body missing title: %q", string(body))
	}
	if !strings.Contains(string(body), "big-search") {
		t.Errorf("body missing search form")
	}
}

func TestMIDPresent(t *testing.T) {
	srv, b := newTestServer(t)
	m := mid.FromBytes([]byte("hello world"))
	b.put(m, []byte("hello world"))
	b.addProvider(m, "12D3KooProvider1")

	resp, body := get(t, srv, "/mid/"+m.String())
	if resp.StatusCode != 200 {
		t.Errorf("status: %d body=%s", resp.StatusCode, string(body))
	}
	for _, want := range []string{m.String(), "11 B", "1", "Download", "DAG", "Seal", "10 data", "4 parity"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("body missing %q", want)
		}
	}
}

func TestMIDNotFound(t *testing.T) {
	srv, _ := newTestServer(t)
	m := mid.FromBytes([]byte("missing"))
	resp, body := get(t, srv, "/mid/"+m.String())
	if resp.StatusCode != 200 {
		t.Errorf("status: %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "not present in the local store") {
		t.Errorf("body missing 'not present' message: %s", string(body))
	}
}

func TestMIDBadID(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, _ := get(t, srv, "/mid/not-a-valid-mid")
	if resp.StatusCode != 400 {
		t.Errorf("status: %d want 400", resp.StatusCode)
	}
}

func TestDAG(t *testing.T) {
	srv, b := newTestServer(t)
	m := mid.FromBytes([]byte("root"))
	b.put(m, []byte("root"))
	resp, body := get(t, srv, "/mid/"+m.String()+"/dag")
	if resp.StatusCode != 200 {
		t.Errorf("status: %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "dag-tree") {
		t.Errorf("body missing dag-tree")
	}
	if !strings.Contains(string(body), "dag.js") {
		t.Errorf("body missing dag.js script")
	}
}

func TestSealUnseal(t *testing.T) {
	srv, b := newTestServer(t)
	m := mid.FromBytes([]byte("test"))
	b.put(m, []byte("test"))
	resp, _ := postForm(t, srv, "/mid/"+m.String()+"/seal", nil)
	if resp.StatusCode != 303 {
		t.Errorf("seal status: %d want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/explorer/mid/"+m.String() {
		t.Errorf("seal redirect: %q", loc)
	}
	resp, _ = postForm(t, srv, "/mid/"+m.String()+"/unseal", nil)
	if resp.StatusCode != 303 {
		t.Errorf("unseal status: %d want 303", resp.StatusCode)
	}
}

func TestPeers(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, body := get(t, srv, "/peers")
	if resp.StatusCode != 200 {
		t.Errorf("status: %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "12D3KooFake") {
		t.Errorf("body missing peer")
	}
	if !strings.Contains(string(body), "2 peer(s) known") {
		t.Errorf("body missing peer count")
	}
}

func TestAnchors(t *testing.T) {
	srv, _ := newTestServer(t)
	resp, body := get(t, srv, "/anchors")
	if resp.StatusCode != 200 {
		t.Errorf("status: %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "Local Anchor Engine") {
		t.Errorf("body missing anchor engine section")
	}
}

func TestNode(t *testing.T) {
	srv, b := newTestServer(t)
	b.setAnchorMode(true)
	resp, body := get(t, srv, "/node")
	if resp.StatusCode != 200 {
		t.Errorf("status: %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "12D3KooSelf") {
		t.Errorf("body missing PeerID")
	}
	if !strings.Contains(string(body), "Anchor mode") {
		t.Errorf("body missing Anchor mode label")
	}
}

func TestSearch(t *testing.T) {
	srv, b := newTestServer(t)
	m := mid.FromBytes([]byte("searchable"))
	b.put(m, []byte("searchable"))

	resp, _ := postForm(t, srv, "/search", url.Values{"mid": {m.String()}})
	if resp.StatusCode != 303 {
		t.Errorf("search status: %d want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/explorer/mid/"+m.String() {
		t.Errorf("redirect location: %q", loc)
	}

	resp, _ = postForm(t, srv, "/search", url.Values{"mid": {""}})
	if resp.StatusCode != 400 {
		t.Errorf("empty search status: %d want 400", resp.StatusCode)
	}

	resp, _ = postForm(t, srv, "/search", url.Values{"mid": {"not-a-valid-mid"}})
	if resp.StatusCode != 400 {
		t.Errorf("bad search status: %d want 400", resp.StatusCode)
	}
}

func TestStatic(t *testing.T) {
	srv, _ := newTestServer(t)
	for _, p := range []string{"/static/style.css", "/static/dag.js"} {
		resp, body := get(t, srv, p)
		if resp.StatusCode != 200 {
			t.Errorf("%s status: %d", p, resp.StatusCode)
		}
		if len(body) == 0 {
			t.Errorf("%s empty", p)
		}
		if resp.Header.Get("Content-Type") == "" {
			t.Errorf("%s missing content-type", p)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.00 KiB"},
		{1024 * 1024, "1.00 MiB"},
		{1024*1024*1024 + 512*1024*1024, "1.50 GiB"},
	}
	for _, c := range cases {
		got := humanBytes(c.in)
		if got != c.want {
			t.Errorf("humanBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}