package memfs

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	"github.com/nnlgsakib/membuss/core/store"
)

// newTestStore returns an in-memory MemStore suitable for
// tests. It is closed automatically when the test ends.
func newTestStore(t *testing.T) *store.MemStore {
	t.Helper()
	s, err := store.NewMemStore(store.Options{InMemory: true})
	if err != nil {
		t.Fatalf("memfs: open in-memory store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// randomBytes returns n bytes of pseudo-random data seeded
// by the current time. crypto/rand is used so the output
// is not deterministic across runs.
func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("memfs: rand: %v", err)
	}
	return b
}

// TestAddFile_Small adds a file that fits in a single chunk.
// The resulting MemFSNode should carry the bytes inline
// (Data field populated) and produce the same bytes back
// through the Resolver.
func TestAddFile_Small(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	payload := randomBytes(t, 1024) // 1 KiB
	res, err := b.AddFile("hello.bin", bytes.NewReader(payload), 0o644, time.Unix(1700000000, 0), "application/octet-stream")
	if err != nil {
		t.Fatalf("AddFile: %v", err)
	}
	if res.Block != 1 {
		t.Errorf("AddFile: want 1 block, got %d", res.Block)
	}
	if res.Size != uint64(len(payload)) {
		t.Errorf("AddFile: want size %d, got %d", len(payload), res.Size)
	}

	// Resolve and read back.
	r := NewResolver(bs)
	node, err := r.Resolve(context.TODO(), res.MID)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !node.IsFile() {
		t.Fatalf("expected file node, got type %v", node.GetType())
	}
	if got := node.TotalSize(); got != res.Size {
		t.Errorf("TotalSize: want %d, got %d", res.Size, got)
	}
	if string(node.Bytes()) != string(payload) {
		t.Errorf("inline data mismatch")
	}
}

// TestAddFile_Large adds a 10 MiB file with the default
// 256 KiB chunker. The result should have ~40 raw blocks
// and a single MemFS FILE node referencing them all.
func TestAddFile_Large(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	payload := randomBytes(t, 10*1024*1024)
	res, err := b.AddFile("big.bin", bytes.NewReader(payload), 0o644, time.Time{}, "")
	if err != nil {
		t.Fatalf("AddFile: %v", err)
	}
	if res.Block < 38 || res.Block > 42 {
		t.Errorf("AddFile: want ~40 blocks, got %d", res.Block)
	}

	r := NewResolver(bs)
	node, err := r.Resolve(context.TODO(), res.MID)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !node.IsFile() {
		t.Fatalf("expected file node, got type %v", node.GetType())
	}
	// Read it back via the streaming reader and compare.
	rc, err := r.Open(context.TODO(), res.MID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("round-trip mismatch: got %d bytes, want %d", len(got), len(payload))
	}
}

// TestAddFile_ReaderSeek tests the Seek implementation by
// reading a 1 MiB file from a random offset and checking
// the bytes match the original.
func TestAddFile_ReaderSeek(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	payload := randomBytes(t, 1024*1024)
	res, err := b.AddFile("seek.bin", bytes.NewReader(payload), 0o644, time.Time{}, "")
	if err != nil {
		t.Fatalf("AddFile: %v", err)
	}
	r := NewResolver(bs)
	rc, err := r.Open(context.TODO(), res.MID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	// Seek to 700 KiB and read 100 KiB.
	const off = 700 * 1024
	const n = 100 * 1024
	if _, err := rc.Seek(off, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(rc, buf); err != nil {
		t.Fatalf("ReadFull: %v", err)
	}
	if !bytes.Equal(buf, payload[off:off+n]) {
		t.Errorf("seeked content mismatch")
	}
}

// TestAddDirectoryFromFS builds a real temp directory tree
// on disk, walks it with AddDirectoryFromFS, and asserts
// every file's content round-trips correctly via path
// resolution.
func TestAddDirectoryFromFS(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	root := t.TempDir()
	mustWrite := func(p string, data []byte) {
		full := filepath.Join(root, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(full, data, 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	mustWrite("a.txt", []byte("alpha"))
	mustWrite("b/b.txt", []byte("bravo-bravo"))
	mustWrite("b/c/c.txt", []byte("charlie-charlie-charlie"))

	res, err := b.AddDirectoryFromFS(os.DirFS(root), ".")
	if err != nil {
		t.Fatalf("AddDirectoryFromFS: %v", err)
	}

	r := NewResolver(bs)

	// Resolve a.txt, b/b.txt, b/c/c.txt.
	cases := []struct {
		path string
		want string
	}{
		{"a.txt", "alpha"},
		{"b/b.txt", "bravo-bravo"},
		{"b/c/c.txt", "charlie-charlie-charlie"},
	}
	for _, tc := range cases {
		node, err := r.ResolvePath(context.TODO(), res.MID, tc.path)
		if err != nil {
			t.Errorf("ResolvePath(%q): %v", tc.path, err)
			continue
		}
		if !node.IsFile() {
			t.Errorf("ResolvePath(%q): not a file", tc.path)
			continue
		}
		if string(node.Bytes()) != tc.want {
			t.Errorf("ResolvePath(%q): got %q, want %q", tc.path, node.Bytes(), tc.want)
		}
	}
}

// TestAddDirectoryFromFS_FSTestMapFS exercises the same
// builder using an in-memory fstest.MapFS so the test does
// not need a real temp directory. This is the recommended
// pattern for short, hermetic tests.
func TestAddDirectoryFromFS_FSTestMapFS(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	mem := fstest.MapFS{
		"hello.txt":     &fstest.MapFile{Data: []byte("hi")},
		"src/main.go":   &fstest.MapFile{Data: []byte("package main\n")},
		"src/util/util.go": &fstest.MapFile{Data: []byte("package util\n")},
	}
	res, err := b.AddDirectoryFromFS(mem, ".")
	if err != nil {
		t.Fatalf("AddDirectoryFromFS: %v", err)
	}

	r := NewResolver(bs)
	for path, want := range map[string]string{
		"hello.txt":              "hi",
		"src/main.go":            "package main\n",
		"src/util/util.go":       "package util\n",
	} {
		node, err := r.ResolvePath(context.TODO(), res.MID, path)
		if err != nil {
			t.Errorf("ResolvePath(%q): %v", path, err)
			continue
		}
		if string(node.Bytes()) != want {
			t.Errorf("ResolvePath(%q): got %q, want %q", path, node.Bytes(), want)
		}
	}
}

// TestDedup asserts that adding the same file content twice
// produces the same MID and the store holds only one copy of
// each raw block. MemFS dedup is automatic because
// Blockstore.Put is idempotent on the same MID.
func TestDedup(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	payload := randomBytes(t, 4096)
	res1, err := b.AddFile("a", bytes.NewReader(payload), 0o644, time.Time{}, "")
	if err != nil {
		t.Fatalf("AddFile 1: %v", err)
	}
	res2, err := b.AddFile("b", bytes.NewReader(payload), 0o644, time.Time{}, "")
	if err != nil {
		t.Fatalf("AddFile 2: %v", err)
	}
	if !res1.MID.Equal(res2.MID) {
		t.Errorf("dedup: identical content produced different MIDs: %s vs %s", res1.MID, res2.MID)
	}
	all, err := bs.AllBlocks()
	if err != nil {
		t.Fatalf("AllBlocks: %v", err)
	}
	// 1 raw block + 1 FILE envelope.
	if len(all) != 2 {
		t.Errorf("dedup: want 2 stored blocks (1 raw + 1 file), got %d", len(all))
	}
}

// TestPathResolution walks a 3-level deep directory and
// resolves a deep file via the MID + path, asserting the
// content matches.
func TestPathResolution(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	mem := fstest.MapFS{
		"a/b/c/d/leaf.txt": &fstest.MapFile{Data: []byte("the leaf")},
	}
	_ = mem // placeholder; will be replaced below
	mem = fstest.MapFS{
		"a/b/c/d/leaf.txt": &fstest.MapFile{Data: []byte("the leaf")},
	}
	res, err := b.AddDirectoryFromFS(mem, ".")
	if err != nil {
		t.Fatalf("AddDirectoryFromFS: %v", err)
	}
	r := NewResolver(bs)
	node, err := r.ResolvePath(context.TODO(), res.MID, "a/b/c/d/leaf.txt")
	if err != nil {
		t.Fatalf("ResolvePath: %v", err)
	}
	if !node.IsFile() {
		t.Fatalf("expected file, got type %v", node.GetType())
	}
	if string(node.Bytes()) != "the leaf" {
		t.Errorf("content mismatch: got %q", node.Bytes())
	}
}

// TestPathResolution_TrailingSlash ensures paths with
// trailing slashes resolve the same as without.
func TestPathResolution_TrailingSlash(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	mem := fstest.MapFS{
		"x.txt": &fstest.MapFile{Data: []byte("X")},
	}
	res, err := b.AddDirectoryFromFS(mem, ".")
	if err != nil {
		t.Fatalf("AddDirectoryFromFS: %v", err)
	}
	r := NewResolver(bs)
	node, err := r.ResolvePath(context.TODO(), res.MID, "x.txt/")
	if err != nil {
		t.Fatalf("ResolvePath: %v", err)
	}
	if !node.IsFile() {
		t.Fatalf("expected file, got type %v", node.GetType())
	}
}

// TestStat_Dir walks a directory and asserts Stat returns
// the expected size, type and entry list.
func TestStat_Dir(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	mem := fstest.MapFS{
		"a.txt": &fstest.MapFile{Data: []byte("AAAA")},
		"b.txt": &fstest.MapFile{Data: []byte("BBBBBBBBB")},
	}
	res, err := b.AddDirectoryFromFS(mem, ".")
	if err != nil {
		t.Fatalf("AddDirectoryFromFS: %v", err)
	}
	r := NewResolver(bs)
	st, err := r.Stat(context.TODO(), res.MID)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Type != TypeDir {
		t.Errorf("Stat.Type: want DIR, got %v", st.Type)
	}
	if st.Size != 13 {
		t.Errorf("Stat.Size: want 13, got %d", st.Size)
	}
	if len(st.Entries) != 2 {
		t.Errorf("Stat.Entries: want 2, got %d", len(st.Entries))
	}
	// Entries should be sorted.
	if st.Entries[0].Name != "a.txt" || st.Entries[1].Name != "b.txt" {
		t.Errorf("Stat.Entries: not sorted, got %s %s", st.Entries[0].Name, st.Entries[1].Name)
	}
}

// TestSymlink creates a MemFS SYMLINK and resolves it.
func TestSymlink(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	res, err := b.AddSymlink("link", "/some/target", 0o777, time.Time{})
	if err != nil {
		t.Fatalf("AddSymlink: %v", err)
	}
	r := NewResolver(bs)
	node, err := r.Resolve(context.TODO(), res.MID)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !node.IsSymlink() {
		t.Fatalf("expected symlink, got type %v", node.GetType())
	}
	if node.SymlinkTarget() != "/some/target" {
		t.Errorf("symlink target: got %q, want %q", node.SymlinkTarget(), "/some/target")
	}
}

// TestResolvePath_NotFound asserts that ResolvePath returns
// an error when a segment does not exist in the DIR node.
func TestResolvePath_NotFound(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	mem := fstest.MapFS{
		"x.txt": &fstest.MapFile{Data: []byte("X")},
	}
	res, err := b.AddDirectoryFromFS(mem, ".")
	if err != nil {
		t.Fatalf("AddDirectoryFromFS: %v", err)
	}
	r := NewResolver(bs)
	if _, err := r.ResolvePath(context.TODO(), res.MID, "missing.txt"); err == nil {
		t.Errorf("ResolvePath: expected error for missing entry")
	}
}

// TestDirFS_Walk validates that AddDirectoryFromFS
// preserves the directory structure under a manual walk.
// (fstest.TestFS cannot be used directly because MemFS is
// not a real fs.FS — we just verify the structure looks
// right.)
func TestDirFS_Walk(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	mem := fstest.MapFS{
		"file.txt":  &fstest.MapFile{Data: []byte("data")},
		"dir/x.txt": &fstest.MapFile{Data: []byte("xx")},
	}
	res, err := b.AddDirectoryFromFS(mem, ".")
	if err != nil {
		t.Fatalf("AddDirectoryFromFS: %v", err)
	}
	r := NewResolver(bs)

	// Walk and count.
	count := 0
	err = r.Walk(context.TODO(), res.MID, func(path string, n *Node, err error) error {
		if err != nil {
			return err
		}
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Walk: %v", err)
	}
	// 1 root DIR + 1 file.txt + 1 dir/x.txt (via the dir
	// subnode traversal). The walk yields root first, then
	// children; the dir subnode is visited before its
	// child file.
	if count < 3 {
		t.Errorf("walk visited %d nodes, want >= 3", count)
	}
}

func TestAddFile_Empty(t *testing.T) {
	bs := newTestStore(t)
	b := NewBuilder(bs)

	res, err := b.AddFile("empty.txt", bytes.NewReader(nil), 0644, time.Now(), "text/plain")
	if err != nil {
		t.Fatalf("AddFile empty: %v", err)
	}
	if res.Size != 0 {
		t.Errorf("Size: got %d, want 0", res.Size)
	}

	r := NewResolver(bs)
	n, err := r.Resolve(context.TODO(), res.MID)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !n.IsFile() {
		t.Errorf("expected file node, got type %v", n.GetType())
	}
	if n.TotalSize() != 0 {
		t.Errorf("TotalSize: got %d, want 0", n.TotalSize())
	}
}
