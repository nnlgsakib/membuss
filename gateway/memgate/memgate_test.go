// Tests for the Mem-Gate HTTP gateway. The tests use a
// memBackend (an in-memory Backend) and httptest to drive
// the chi router.
package memgate

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/nnlgsakib/membuss/core/memfs"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
)

// memBackend is an in-memory Backend. It serves a single
// (mid, content) pair; Resolve/RawBlock/DAGNodeJSON/Stat all
// return that content.
type memBackend struct {
	mu      sync.Mutex
	content map[string][]byte
	sealed  map[string]bool
	stat    map[string]ContentInfo
	failPing bool
}

func newMemBackend() *memBackend {
	return &memBackend{
		content: map[string][]byte{},
		sealed:  map[string]bool{},
		stat:    map[string]ContentInfo{},
	}
}

func (b *memBackend) put(m mid.MID, data []byte, contentType string) {
	b.putWithMeta(m, data, contentType, "", "")
}

func (b *memBackend) putWithMeta(m mid.MID, data []byte, contentType, name, mimeType string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.content[m.String()] = data
	b.stat[m.String()] = ContentInfo{
		MID:         m.String(),
		Size:        uint64(len(data)),
		Blocks:      1,
		ContentType: contentType,
		Sealed:      true,
		Name:        name,
		MimeType:    mimeType,
	}
}

func (b *memBackend) Resolve(ctx context.Context, m mid.MID) (io.ReadCloser, ContentInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.content[m.String()]
	if !ok {
		return nil, ContentInfo{}, errNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), b.stat[m.String()], nil
}

func (b *memBackend) RawBlock(ctx context.Context, m mid.MID) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.content[m.String()]
	if !ok {
		return nil, errNotFound
	}
	return append([]byte(nil), data...), nil
}

func (b *memBackend) DAGNodeJSON(ctx context.Context, m mid.MID) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	data, ok := b.content[m.String()]
	if !ok {
		return nil, errNotFound
	}
	out := map[string]any{
		"mid":  m.String(),
		"size": len(data),
		"links": []string{},
	}
	return json.Marshal(out)
}

func (b *memBackend) Stat(ctx context.Context, m mid.MID) (ContentInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	info, ok := b.stat[m.String()]
	if !ok {
		return ContentInfo{}, errNotFound
	}
	return info, nil
}

func (b *memBackend) Ping(ctx context.Context) error {
	if b.failPing {
		return errNotFound
	}
	return nil
}

// --- Phase 17: MemFS stubs on memBackend ---

// MemFSInfo is unimplemented on the test backend: it always
// returns notFound so the existing handler tests do not
// accidentally exercise the MemFS path.
func (b *memBackend) MemFSInfo(ctx context.Context, m mid.MID) (MemFSInfo, error) {
	return MemFSInfo{}, errNotFound
}

func (b *memBackend) MemFSPathGet(ctx context.Context, m mid.MID, path string) (io.ReadSeekCloser, uint64, string, error) {
	return nil, 0, "", errNotFound
}

func (b *memBackend) MemFSList(ctx context.Context, m mid.MID) ([]MemFSEntry, error) {
	return nil, errNotFound
}

var errNotFound = &notFoundError{}

type notFoundError struct{}

func (e *notFoundError) Error() string { return "not found" }

func newTestGate(t *testing.T, b Backend) *MemGate {
	t.Helper()
	mg, err := New(Config{Backend: b, MaxCacheBytes: 1 << 20})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return mg
}

// putRandom ingests data via the memBackend and returns the
// resulting MID (computed in-test rather than going through
// the full MID pipeline).
func putRandom(b *memBackend, data []byte) mid.MID {
	m := mid.FromBytes(data)
	b.put(m, data, http.DetectContentType(data))
	return m
}

func TestGet_Raw_Default(t *testing.T) {
	b := newMemBackend()
	body := []byte("hello, world!")
	m := putRandom(b, body)

	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/mem/" + m.String())
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if resp.Header.Get("X-Membuss-MID") != m.String() {
		t.Errorf("mid header: %q", resp.Header.Get("X-Membuss-MID"))
	}
	if resp.Header.Get("ETag") != `"`+m.String()+`"` {
		t.Errorf("etag: %q", resp.Header.Get("ETag"))
	}
	if !strings.Contains(resp.Header.Get("Cache-Control"), "immutable") {
		t.Errorf("cache-control: %q", resp.Header.Get("Cache-Control"))
	}
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, body) {
		t.Errorf("body: got %q want %q", got, body)
	}
}

func TestGet_Format_Raw(t *testing.T) {
	b := newMemBackend()
	body := []byte("\x00\x01\x02binary data")
	m := putRandom(b, body)
	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/mem/" + m.String() + "?format=raw")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Type") != "application/octet-stream" {
		t.Errorf("content-type: %q", resp.Header.Get("Content-Type"))
	}
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, body) {
		t.Errorf("body mismatch")
	}
}

func TestGet_Format_DAGJSON(t *testing.T) {
	b := newMemBackend()
	m := putRandom(b, []byte("hello"))
	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/mem/" + m.String() + "?format=dag-json")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		t.Errorf("content-type: %q", resp.Header.Get("Content-Type"))
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out["mid"] != m.String() {
		t.Errorf("mid: %v", out["mid"])
	}
}

func TestHead_ReturnsHeaders(t *testing.T) {
	b := newMemBackend()
	body := []byte("payload")
	m := putRandom(b, body)
	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	resp, err := http.Head(srv.URL + "/mem/" + m.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Length") != "7" {
		t.Errorf("content-length: %q", resp.Header.Get("Content-Length"))
	}
	if resp.Header.Get("Accept-Ranges") != "bytes" {
		t.Errorf("accept-ranges: %q", resp.Header.Get("Accept-Ranges"))
	}
}

func TestRange_Bytes_Suffix(t *testing.T) {
	b := newMemBackend()
	body := []byte("0123456789")
	m := putRandom(b, body)
	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/mem/"+m.String(), nil)
	req.Header.Set("Range", "bytes=-3")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 206 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Range") != "bytes 7-9/10" {
		t.Errorf("content-range: %q", resp.Header.Get("Content-Range"))
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "789" {
		t.Errorf("body: %q", string(got))
	}
}

func TestRange_Bytes_Open(t *testing.T) {
	b := newMemBackend()
	body := []byte("0123456789")
	m := putRandom(b, body)
	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/mem/"+m.String(), nil)
	req.Header.Set("Range", "bytes=2-5")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 206 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Range") != "bytes 2-5/10" {
		t.Errorf("content-range: %q", resp.Header.Get("Content-Range"))
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "2345" {
		t.Errorf("body: %q", string(got))
	}
}

func TestRange_Bytes_OpenEnd(t *testing.T) {
	b := newMemBackend()
	body := []byte("0123456789")
	m := putRandom(b, body)
	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/mem/"+m.String(), nil)
	req.Header.Set("Range", "bytes=4-")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 206 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	if string(got) != "456789" {
		t.Errorf("body: %q", string(got))
	}
}

func TestRange_Invalid(t *testing.T) {
	b := newMemBackend()
	m := putRandom(b, []byte("abcdefgh"))
	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL+"/mem/"+m.String(), nil)
	req.Header.Set("Range", "bytes=100-200")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestGet_BadMID(t *testing.T) {
	b := newMemBackend()
	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/mem/not-a-valid-mid")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestGet_MissingMID(t *testing.T) {
	b := newMemBackend()
	m := mid.FromBytes([]byte("never added"))
	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/mem/" + m.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestHealthz_OK(t *testing.T) {
	b := newMemBackend()
	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 204 {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestHealthz_503(t *testing.T) {
	b := newMemBackend()
	b.failPing = true
	srv := httptest.NewServer(newTestGate(t, b).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Errorf("status: %d", resp.StatusCode)
	}
}

func TestLRU_BasicGetPut(t *testing.T) {
	l := newLRU(100)
	l.put("a", []byte("hello"))
	if data, ok := l.get("a"); !ok || string(data) != "hello" {
		t.Errorf("get a: %q ok=%v", data, ok)
	}
	// touch updates recency
	l.put("b", []byte("world"))
	if _, ok := l.get("a"); !ok {
		t.Errorf("a evicted unexpectedly")
	}
	// Force eviction by exceeding cap. We need total > 100.
	l.put("c", make([]byte, 200))
	if _, ok := l.get("a"); ok {
		t.Errorf("a should be evicted")
	}
	if l.bytes() > l.max() {
		t.Errorf("bytes over cap: %d > %d", l.bytes(), l.max())
	}
}

func TestLRU_MarshalJSON(t *testing.T) {
	l := newLRU(100)
	l.put("a", []byte("hello"))
	b, err := json.Marshal(l)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"entries":1`) {
		t.Errorf("marshal: %s", b)
	}
}

func TestParseRange_Cases(t *testing.T) {
	cases := []struct {
		s       string
		size    int64
		start   int64
		end     int64
		wantErr bool
	}{
		{"bytes=0-9", 10, 0, 10, false},
		{"bytes=5-", 10, 5, 10, false},
		{"bytes=-3", 10, 7, 10, false},
		{"bytes=0-0", 10, 0, 1, false},
		{"bytes=100-200", 10, 0, 0, true},
		{"bytes=0-9,20-29", 30, 0, 0, true},
		{"items=0-9", 10, 0, 0, true},
	}
	for _, c := range cases {
		s, e, err := parseRange(c.s, c.size)
		if c.wantErr {
			if err == nil {
				t.Errorf("%q: expected error", c.s)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %v", c.s, err)
			continue
		}
		if s != c.start || e != c.end {
			t.Errorf("%q: got [%d,%d) want [%d,%d)", c.s, s, e, c.start, c.end)
		}
	}
}

func TestDetectContentType_OverrideWins(t *testing.T) {
	got := detectContentType("mem1abc.html", []byte("<html/>"), "text/x-custom")
	if got != "text/x-custom" {
		t.Errorf("override: %q", got)
	}
}

func TestDetectContentType_EmptyData(t *testing.T) {
	got := detectContentType("mem1abc", nil, "")
	if got != "application/octet-stream" {
		t.Errorf("empty: %q", got)
	}
}

func TestDetectContentType_HTMLByExtension(t *testing.T) {
	// Sanity: filepath.Ext strips the leading dot, mime picks it.
	_ = mime.TypeByExtension
	ct := detectContentType("mem1.html", []byte("x"), "")
	if !strings.HasPrefix(ct, "text/html") {
		t.Errorf("html: %q", ct)
	}
}

// TestDownloadDisposition verifies the Phase 19
// Content-Disposition behavior:
//   - Default: inline + filename so the browser can
//     render text/html and still fall back to a sensible
//     filename when the user chooses "Save As".
//   - ?download=1: attachment + filename.
func TestDownloadDisposition(t *testing.T) {
	be := newMemBackend()
	m := mid.FromBytes([]byte("hello world"))
	// Phase 19: the uploader captured a filename + mime type.
	// The default Content-Disposition should surface them.
	be.putWithMeta(m, []byte("hello world"), "text/plain", "hello world.txt", "text/plain; charset=utf-8")
	mg, err := New(Config{Backend: be, MaxCacheBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(mg.Router())
	defer srv.Close()
	url := srv.URL + "/mem/" + m.String()

	// 1) Default: inline with filename set to the original
	// name. Browsers render the body and still surface the
	// name in "Save As" / downloads UI.
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	disp := resp.Header.Get("Content-Disposition")
	if !strings.HasPrefix(disp, "inline;") {
		t.Fatalf("default Content-Disposition: got %q, want inline prefix", disp)
	}
	if !strings.Contains(disp, "hello world.txt") {
		t.Fatalf("default Content-Disposition should include uploader filename: %q", disp)
	}

	// 2) ?download=1: header set to attachment, default filename.
	resp2, err := http.Get(url + "?download=1")
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	disp2 := resp2.Header.Get("Content-Disposition")
	if !strings.HasPrefix(disp2, "attachment;") {
		t.Fatalf("download=1 Content-Disposition: got %q, want attachment prefix", disp2)
	}
	// Phase 19: when the uploader supplied a name, it is the
	// preferred filename (over the MID-derived default).
	if !strings.Contains(disp2, "hello world.txt") {
		t.Fatalf("download=1 Content-Disposition should include uploader filename: %q", disp2)
	}

	// 3) ?download=1&filename=foo.txt: header honours the override.
	custom := "myreport.txt"
	resp3, err := http.Get(url + "?download=1&filename=" + custom)
	if err != nil {
		t.Fatal(err)
	}
	resp3.Body.Close()
	disp3 := resp3.Header.Get("Content-Disposition")
	if !strings.Contains(disp3, custom) {
		t.Fatalf("custom filename: got %q, want substring %q", disp3, custom)
	}
}

// --- Phase 17: MemFS integration tests ---

// memfsBackend is a memBackend extended with MemFS support
// backed by an in-memory core/memfs.Builder. It lets us
// exercise the full /mem/{mid}/... HTTP surface against a
// real (if synthetic) tree.
type memfsBackend struct {
	*memBackend
	resolver *memfs.Resolver
}

func (b *memfsBackend) MemFSInfo(ctx context.Context, m mid.MID) (MemFSInfo, error) {
	st, err := b.resolver.Stat(ctx, m)
	if err != nil {
		return MemFSInfo{}, errNotFound
	}
	return MemFSInfo{
		MID:  m.String(),
		Type: memFSTypeString(st.Type),
		Size: st.Size,
	}, nil
}

func (b *memfsBackend) MemFSPathGet(ctx context.Context, m mid.MID, path string) (io.ReadSeekCloser, uint64, string, error) {
	node, err := b.resolver.ResolvePath(ctx, m, path)
	if err != nil {
		return nil, 0, "", errNotFound
	}
	if !node.IsFile() {
		return nil, 0, "", errNotFound
	}
	rc, err := b.resolver.Open(ctx, node.MustMID())
	if err != nil {
		return nil, 0, "", err
	}
	return rc, node.TotalSize(), node.MimeType(), nil
}

func (b *memfsBackend) MemFSList(ctx context.Context, m mid.MID) ([]MemFSEntry, error) {
	st, err := b.resolver.Stat(ctx, m)
	if err != nil {
		return nil, errNotFound
	}
	if st.Type != memfs.TypeDir {
		return nil, errNotFound
	}
	out := make([]MemFSEntry, 0, len(st.Entries))
	for _, e := range st.Entries {
		out = append(out, MemFSEntry{
			Name: e.Name,
			MID:  e.Mid.String(),
			Type: memFSTypeString(e.Type),
			Size: e.Size,
		})
	}
	return out, nil
}

// memFSTypeString mirrors the production adapter's helper.
func memFSTypeString(t memfs.MemFSType) string {
	switch t {
	case memfs.TypeFile:
		return "file"
	case memfs.TypeDir:
		return "dir"
	case memfs.TypeSymlink:
		return "symlink"
	default:
		return "raw"
	}
}

func TestMemFS_DirList(t *testing.T) {
	bs, _ := store.NewMemStore(store.Options{InMemory: true})
	defer bs.Close()
	b := memfs.NewBuilder(bs)
	mem := fstest.MapFS{
		"x.txt": &fstest.MapFile{Data: []byte("X")},
		"y.txt": &fstest.MapFile{Data: []byte("YY")},
	}
	root, err := b.AddDirectoryFromFS(mem, ".")
	if err != nil {
		t.Fatalf("add dir: %v", err)
	}
	be := &memfsBackend{memBackend: newMemBackend(), resolver: memfs.NewResolver(bs)}
	srv := httptest.NewServer(newTestGate(t, be).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/mem/" + root.MID.String() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "x.txt") || !strings.Contains(string(body), "y.txt") {
		t.Errorf("listing missing entries: %s", string(body))
	}
}

func TestMemFS_DirList_JSON(t *testing.T) {
	bs, _ := store.NewMemStore(store.Options{InMemory: true})
	defer bs.Close()
	b := memfs.NewBuilder(bs)
	root, err := b.AddDirectoryFromFS(fstest.MapFS{
		"a.txt": &fstest.MapFile{Data: []byte("A")},
	}, ".")
	if err != nil {
		t.Fatalf("add dir: %v", err)
	}
	be := &memfsBackend{memBackend: newMemBackend(), resolver: memfs.NewResolver(bs)}
	srv := httptest.NewServer(newTestGate(t, be).Router())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/mem/" + root.MID.String() + "/?format=json")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	var out struct {
		MID  string `json:"mid"`
		Type string `json:"type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Type != "dir" {
		t.Errorf("type: want dir, got %q", out.Type)
	}
}

func TestMemFS_PathGet(t *testing.T) {
	bs, _ := store.NewMemStore(store.Options{InMemory: true})
	defer bs.Close()
	b := memfs.NewBuilder(bs)
	root, err := b.AddDirectoryFromFS(fstest.MapFS{
		"hello.txt":          &fstest.MapFile{Data: []byte("Hello, world!")},
		"assets/style.css":   &fstest.MapFile{Data: []byte("body { color: blue; }")},
		"assets/index.js":    &fstest.MapFile{Data: []byte("console.log(1)")},
	}, ".")
	if err != nil {
		t.Fatalf("add dir: %v", err)
	}
	be := &memfsBackend{memBackend: newMemBackend(), resolver: memfs.NewResolver(bs)}
	srv := httptest.NewServer(newTestGate(t, be).Router())
	defer srv.Close()

	cases := []struct {
		urlPath  string
		wantBody string
		wantCT   string
	}{
		{"/hello.txt", "Hello, world!", "text/plain; charset=utf-8"},
		{"/assets/style.css", "body { color: blue; }", "text/css; charset=utf-8"},
		{"/assets/index.js", "console.log(1)", "application/javascript; charset=utf-8"},
	}

	for _, tc := range cases {
		resp, err := http.Get(srv.URL + "/mem/" + root.MID.String() + tc.urlPath)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			t.Fatalf("status: %d for path %s, body: %q", resp.StatusCode, tc.urlPath, string(body))
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if string(body) != tc.wantBody {
			t.Errorf("body for %s: got %q, want %q", tc.urlPath, string(body), tc.wantBody)
		}
		ct := resp.Header.Get("Content-Type")
		if ct != tc.wantCT {
			t.Errorf("content-type for %s: got %q, want %q", tc.urlPath, ct, tc.wantCT)
		}
	}
}

func TestHttpCachingAndMemoryCache(t *testing.T) {
	bs, _ := store.NewMemStore(store.Options{InMemory: true})
	defer bs.Close()
	builder := memfs.NewBuilder(bs)
	root, err := builder.AddDirectoryFromFS(fstest.MapFS{
		"hello.txt": &fstest.MapFile{Data: []byte("Hello, world!")},
	}, ".")
	if err != nil {
		t.Fatalf("add dir: %v", err)
	}

	be := &memfsBackend{
		memBackend: newMemBackend(),
		resolver:   memfs.NewResolver(bs),
	}
	// Ingest root directory bytes and metadata into memBackend so Resolve works
	rawDir, _ := bs.Get(root.MID)
	be.memBackend.put(root.MID, rawDir, "application/octet-stream")

	mg := newTestGate(t, be)
	srv := httptest.NewServer(mg.Router())
	defer srv.Close()

	client := &http.Client{}

	// Test 1: Resolve MID Cache & ETag Validation
	req, _ := http.NewRequest("GET", srv.URL+"/mem/"+root.MID.String(), nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("expected ETag header")
	}

	// Conditional request should return 304 Not Modified
	reqCond, _ := http.NewRequest("GET", srv.URL+"/mem/"+root.MID.String(), nil)
	reqCond.Header.Set("If-None-Match", etag)
	respCond, err := client.Do(reqCond)
	if err != nil {
		t.Fatal(err)
	}
	respCond.Body.Close()
	if respCond.StatusCode != http.StatusNotModified {
		t.Fatalf("expected 304, got %d", respCond.StatusCode)
	}

	// Test 2: DAG Node JSON Cache & ETag Validation
	reqJSON, _ := http.NewRequest("GET", srv.URL+"/mem/"+root.MID.String()+"?format=dag-json", nil)
	respJSON, err := client.Do(reqJSON)
	if err != nil {
		t.Fatal(err)
	}
	respJSON.Body.Close()
	etagJSON := respJSON.Header.Get("ETag")

	reqCondJSON, _ := http.NewRequest("GET", srv.URL+"/mem/"+root.MID.String()+"?format=dag-json", nil)
	reqCondJSON.Header.Set("If-None-Match", etagJSON)
	respCondJSON, err := client.Do(reqCondJSON)
	if err != nil {
		t.Fatal(err)
	}
	respCondJSON.Body.Close()
	if respCondJSON.StatusCode != http.StatusNotModified {
		t.Fatalf("expected 304 for conditional DAG-JSON, got %d", respCondJSON.StatusCode)
	}

	// Test 3: Raw Block Cache & ETag Validation
	reqRaw, _ := http.NewRequest("GET", srv.URL+"/mem/"+root.MID.String()+"?format=raw", nil)
	respRaw, err := client.Do(reqRaw)
	if err != nil {
		t.Fatal(err)
	}
	respRaw.Body.Close()
	etagRaw := respRaw.Header.Get("ETag")

	reqCondRaw, _ := http.NewRequest("GET", srv.URL+"/mem/"+root.MID.String()+"?format=raw", nil)
	reqCondRaw.Header.Set("If-None-Match", etagRaw)
	respCondRaw, err := client.Do(reqCondRaw)
	if err != nil {
		t.Fatal(err)
	}
	respCondRaw.Body.Close()
	if respCondRaw.StatusCode != http.StatusNotModified {
		t.Fatalf("expected 304 for conditional raw block, got %d", respCondRaw.StatusCode)
	}

	// Test 4: MemFS Path Cache & ETag Validation
	reqPath, _ := http.NewRequest("GET", srv.URL+"/mem/"+root.MID.String()+"/hello.txt", nil)
	respPath, err := client.Do(reqPath)
	if err != nil {
		t.Fatal(err)
	}
	respPath.Body.Close()
	etagPath := respPath.Header.Get("ETag")

	reqCondPath, _ := http.NewRequest("GET", srv.URL+"/mem/"+root.MID.String()+"/hello.txt", nil)
	reqCondPath.Header.Set("If-None-Match", etagPath)
	respCondPath, err := client.Do(reqCondPath)
	if err != nil {
		t.Fatal(err)
	}
	respCondPath.Body.Close()
	if respCondPath.StatusCode != http.StatusNotModified {
		t.Fatalf("expected 304 for conditional MemFS subpath, got %d", respCondPath.StatusCode)
	}
}
