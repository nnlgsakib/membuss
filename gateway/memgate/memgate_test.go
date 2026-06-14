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

	"github.com/nnlgsakib/membuss/core/mid"
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
	b.mu.Lock()
	defer b.mu.Unlock()
	b.content[m.String()] = data
	b.stat[m.String()] = ContentInfo{
		MID:         m.String(),
		Size:        uint64(len(data)),
		Blocks:      1,
		ContentType: contentType,
		Sealed:      true,
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