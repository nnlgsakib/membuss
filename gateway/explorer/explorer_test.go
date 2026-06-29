// Tests for the explorer package. The tests use a
// memBackend (an in-memory implementation of explorer.Backend)
// and httptest to drive the chi router.
package explorer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
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
	// resolveOK is flipped by individual tests to
	// exercise the "DHT had providers and the
	// resolve succeeded" branch of the explorer.
	resolveOK bool
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

func (b *memBackend) Stat(ctx context.Context, m mid.MID) (ContentInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.content[m.String()]
	if !ok {
		return ContentInfo{}, nil
	}
	return ContentInfo{
		MID:    m.String(),
		Size:   uint64(len(data)),
		Blocks: 1,
		Sealed: true,
		Present: true,
	}, nil
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

// Resolve is the test-backend stub. By default it
// returns ErrNotFound (the explorer renders "not
// found" for an empty providers list). Tests that
// want to exercise the "DHT had providers but fetch
// failed" branch can flip b.resolveOK to true; tests
// that want the "found it" branch can pre-populate
// b.store with the right bytes via put().
func (b *memBackend) Resolve(ctx context.Context, m mid.MID) (io.ReadCloser, ContentInfo, error) {
	return b.ResolveWithProgress(ctx, m, nil)
}

func (b *memBackend) ResolveWithProgress(ctx context.Context, m mid.MID, progressFn func(blocksResolved, blocksTotal uint64)) (io.ReadCloser, ContentInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	has := len(b.providers[m.String()]) > 0
	resolveOK := b.resolveOK
	data, stored := b.content[m.String()]
	if !resolveOK || !has || !stored {
		return nil, ContentInfo{}, ErrNotFound
	}
	if progressFn != nil {
		progressFn(uint64(len(data)), uint64(len(data)))
	}
	return io.NopCloser(strings.NewReader(string(data))), ContentInfo{
		MID:    m.String(),
		Size:   uint64(len(data)),
		Blocks: 1,
		Sealed: false,
	}, nil
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

func (b *memBackend) AllStoredMIDs(ctx context.Context) ([]StoredMIDView, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]StoredMIDView, 0, len(b.content))
	for k := range b.content {
		m, err := mid.Parse(k)
		if err != nil {
			continue
		}
		out = append(out, StoredMIDView{
			MID:    m.String(),
			Name:   "",
			Sealed: b.sealed[k],
		})
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
func (b *memBackend) BandwidthStats(ctx context.Context) (totalIn, totalOut int64, rateIn, rateOut float64, err error) {
	return 0, 0, 0, 0, nil
}

func (b *memBackend) Add(ctx context.Context, name string, r io.Reader) (ContentInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return ContentInfo{}, err
	}
	m := mid.FromBytes(data)
	b.put(m, data)
	return ContentInfo{
		MID:      m.String(),
		Size:     uint64(len(data)),
		Blocks:   1,
		Sealed:   true,
		Present:  true,
		Name:     name,
		MimeType: "application/octet-stream",
	}, nil
}

func (b *memBackend) AddDirectory(ctx context.Context, name string, files []DirectoryFile) (ContentInfo, error) {
	if len(files) == 0 {
		return ContentInfo{}, fmt.Errorf("empty directory")
	}
	data, err := io.ReadAll(files[0].R)
	if err != nil {
		return ContentInfo{}, err
	}
	m := mid.FromBytes(data)
	b.put(m, data)
	dirName := name
	if dirName == "" {
		dirName = "upload"
	}
	return ContentInfo{
		MID:      m.String(),
		Size:     uint64(len(data)),
		Blocks:   1,
		Sealed:   true,
		Present:  true,
		Name:     dirName,
		MimeType: "inode/directory",
	}, nil
}

func (b *memBackend) Rename(ctx context.Context, m mid.MID, name string) error {
	return nil
}

func (b *memBackend) Delete(ctx context.Context, m mid.MID) (uint64, uint64, error) {
	return 1, 1024, nil
}


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


// TestMIDNotFoundNoProviders covers the branch where the
// MID is not local and the DHT has no provider records.
// The page must still render the "not present" message
// plus a "not found in the DHT" hint and the empty
// providers list.
func TestMIDNotFoundNoProviders(t *testing.T) {
	srv, _ := newTestServer(t)
	m := mid.FromBytes([]byte("missing-noprov"))
	resp, body := get(t, srv, "/mid/"+m.String())
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d body=%s", resp.StatusCode, string(body))
	}
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "not present in the local store") {
		t.Errorf("body missing 'not present' message")
	}
	if !strings.Contains(bodyStr, "Not found") {
		t.Errorf("body missing 'Not found' hint: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "(no providers found)") {
		t.Errorf("body missing empty providers hint: %s", bodyStr)
	}
}

// TestMIDNotFoundAttempted covers the branch where the
// DHT has providers but the content is not local. The page
// should render the resolving/streaming page that will
// auto-fetch from the network via SSE.
func TestMIDNotFoundAttempted(t *testing.T) {
	srv, b := newTestServer(t)
	m := mid.FromBytes([]byte("missing-attempted"))
	b.addProvider(m, "12D3KooProvider1")
	resp, body := get(t, srv, "/mid/"+m.String())
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "Resolving content from network") {
		t.Errorf("body missing resolving page: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "resolve-stream") {
		t.Errorf("body missing SSE endpoint: %s", bodyStr)
	}
}

// TestMIDNotFoundResolved covers the branch where the
// DHT has providers AND the Memex fetch succeeded. After
// the fetch, handleMID re-stats and the page renders as
// if the content had always been local.
func TestMIDNotFoundResolved(t *testing.T) {
	srv, b := newTestServer(t)
	m := mid.FromBytes([]byte("resolved-by-dht"))
	b.addProvider(m, "12D3KooProvider1")
	// put() seeds the in-memory content; resolveOK
	// controls whether Resolve returns success.
	b.put(m, []byte("resolved-by-dht"))
	b.resolveOK = true
	resp, body := get(t, srv, "/mid/"+m.String())
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	bodyStr := string(body)
	if strings.Contains(bodyStr, "not present in the local store") {
		t.Errorf("body should NOT show not-present after successful resolve: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "Download") {
		t.Errorf("body should show Download link after successful resolve: %s", bodyStr)
	}
	if !strings.Contains(bodyStr, m.String()) {
		t.Errorf("body missing the MID: %s", bodyStr)
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

func TestDelete(t *testing.T) {
	srv, b := newTestServer(t)
	m := mid.FromBytes([]byte("test"))
	b.put(m, []byte("test"))
	resp, _ := postForm(t, srv, "/mid/"+m.String()+"/delete", nil)
	if resp.StatusCode != 303 {
		t.Errorf("delete status: %d want 303", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/explorer/" {
		t.Errorf("delete redirect: %q want /explorer/", loc)
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
	srv, b := newTestServer(t)
	b.setAnchorMode(true)
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

// --- Phase 17: MemFS stubs on memBackend ---

func (b *memBackend) MemFSInfo(ctx context.Context, m mid.MID) (MemFSInfo, error) {
	return MemFSInfo{}, fmt.Errorf("memfs: test backend stub")
}

func (b *memBackend) MemFSList(ctx context.Context, m mid.MID) ([]MemFSEntry, error) {
	return nil, fmt.Errorf("memfs: test backend stub")
}

func (b *memBackend) MemFSPathGet(ctx context.Context, m mid.MID, path string) (io.ReadSeekCloser, uint64, string, error) {
	return nil, 0, "", fmt.Errorf("memfs: test backend stub")
}

// --- Phase 18: MemNS stubs on memBackend ---

func (b *memBackend) KeyringKeys(ctx context.Context) ([]KeyringKeyInfo, error) {
	return nil, nil
}

func (b *memBackend) KeyringGenerate(ctx context.Context, name, keyType string) (KeyringKeyInfo, error) {
	return KeyringKeyInfo{}, nil
}

func (b *memBackend) KeyringDelete(ctx context.Context, name string) error {
	return nil
}

func (b *memBackend) MemNSPublish(ctx context.Context, keyName, value string, ttl uint32, message string) (MemNSRecordInfo, error) {
	return MemNSRecordInfo{}, nil
}

func (b *memBackend) ResolveMemNSRecord(ctx context.Context, name string) (MemNSRecordInfo, error) {
	return MemNSRecordInfo{}, fmt.Errorf("memns: test backend stub")
}

func (b *memBackend) ResolveMemLink(ctx context.Context, domain string) (MemLinkInfo, error) {
	return MemLinkInfo{}, fmt.Errorf("memlink: test backend stub")
}

func (b *memBackend) ConnectPeer(ctx context.Context, multiaddr string) error {
	return fmt.Errorf("connect: test backend stub")
}

func (b *memBackend) DescriptorExport(ctx context.Context, midStr string) ([]byte, error) {
	return nil, fmt.Errorf("descriptor: test backend stub")
}

func (b *memBackend) DescriptorMeta(ctx context.Context, midStr string) (map[string]interface{}, error) {
	return nil, fmt.Errorf("descriptor: test backend stub")
}

func (b *memBackend) DescriptorImport(ctx context.Context, data []byte) (string, error) {
	return "", fmt.Errorf("descriptor: test backend stub")
}

func TestUpload(t *testing.T) {
	srv, _ := newTestServer(t)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("file", "test.txt")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := fw.Write([]byte("hello upload")); err != nil {
		t.Fatalf("Write file: %v", err)
	}
	mw.Close()

	req, err := http.NewRequest("POST", srv.URL+"/upload", &buf)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := noRedirectClient().Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusSeeOther {
		t.Errorf("expected redirect (303), got status: %d", resp.StatusCode)
	}
}

func TestUploadFolder(t *testing.T) {
	srv, _ := newTestServer(t)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	
	// Create multiple file parts representing a folder upload
	files := map[string]string{
		"myfolder/file1.txt": "content1",
		"myfolder/file2.txt": "content2",
	}
	
	for path, content := range files {
		fw, err := mw.CreateFormFile("files", path)
		if err != nil {
			t.Fatalf("CreateFormFile: %v", err)
		}
		if _, err := fw.Write([]byte(content)); err != nil {
			t.Fatalf("Write file: %v", err)
		}
	}
	mw.Close()

	req, err := http.NewRequest("POST", srv.URL+"/upload", &buf)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := noRedirectClient().Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusSeeOther {
		t.Errorf("expected redirect (303), got status: %d", resp.StatusCode)
	}
}

func TestUploadFolder_SanitizedPaths(t *testing.T) {
	srv, _ := newTestServer(t)

	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)

	// In browser sanitization, filenames are stripped of path component,
	// and parallel "paths" are sent to preserve hierarchy.
	files := []struct {
		filename string
		path     string
		content  string
	}{
		{filename: "file1.txt", path: "myfolder/sub/file1.txt", content: "content1"},
		{filename: "file2.txt", path: "myfolder/sub2/file2.txt", content: "content2"},
	}

	for _, f := range files {
		fw, err := mw.CreateFormFile("files", f.filename)
		if err != nil {
			t.Fatalf("CreateFormFile: %v", err)
		}
		if _, err := fw.Write([]byte(f.content)); err != nil {
			t.Fatalf("Write file: %v", err)
		}
	}
	for _, f := range files {
		if err := mw.WriteField("paths", f.path); err != nil {
			t.Fatalf("WriteField paths: %v", err)
		}
	}
	mw.Close()

	req, err := http.NewRequest("POST", srv.URL+"/upload", &buf)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())

	resp, err := noRedirectClient().Do(req)
	if err != nil {
		t.Fatalf("Do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusSeeOther {
		t.Errorf("expected redirect (303), got status: %d", resp.StatusCode)
	}
}

