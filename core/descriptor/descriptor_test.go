package descriptor

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/nnlgsakib/membuss/core/chunk"
	"github.com/nnlgsakib/membuss/core/dag"
	"github.com/nnlgsakib/membuss/core/mid"
	"github.com/nnlgsakib/membuss/core/store"
)

// buildTestDAG creates a small DAG in a Memstore for testing.
func buildTestDAG(t *testing.T) (store.Store, mid.MID) {
	t.Helper()
	s := store.NewMemstore()

	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}
	factory := chunk.NewFixed(1024)
	c, err := factory(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	root, err := dag.NewBuilder(s).Build(c)
	if err != nil {
		t.Fatal(err)
	}

	if err := store.SetObjectInfo(s, root, store.ObjectInfo{
		Name:     "test.txt",
		MimeType: "text/plain",
		Size:     uint64(len(data)),
	}); err != nil {
		t.Fatal(err)
	}

	return s, root
}

func TestBuildAndParse(t *testing.T) {
	s, root := buildTestDAG(t)

	d, err := Build(s, root,
		WithChunker("fixed"),
		WithChunkSize(1024),
		WithBootstrapPeers([]string{"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWTest"}),
	)
	if err != nil {
		t.Fatal(err)
	}

	if !d.RootMID.Equal(root) {
		t.Errorf("root MID mismatch: got %s, want %s", d.RootMID, root)
	}
	if d.BlockCount == 0 {
		t.Error("expected non-zero block count")
	}
	if d.TotalSize == 0 {
		t.Error("expected non-zero total size")
	}
	if d.Name != "test.txt" {
		t.Errorf("name: got %q, want %q", d.Name, "test.txt")
	}
	if d.Chunker != "fixed" {
		t.Errorf("chunker: got %q, want %q", d.Chunker, "fixed")
	}
	if len(d.BootstrapPeers) != 1 {
		t.Errorf("bootstrap peers: got %d, want 1", len(d.BootstrapPeers))
	}

	// Serialize and parse back.
	data, err := d.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	d2, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}

	if !d2.RootMID.Equal(d.RootMID) {
		t.Errorf("parsed root MID mismatch")
	}
	if d2.BlockCount != d.BlockCount {
		t.Errorf("parsed block count: got %d, want %d", d2.BlockCount, d.BlockCount)
	}
	if d2.TotalSize != d.TotalSize {
		t.Errorf("parsed total size: got %d, want %d", d2.TotalSize, d.TotalSize)
	}
	if d2.Name != d.Name {
		t.Errorf("parsed name: got %q, want %q", d2.Name, d.Name)
	}
	if d2.Chunker != d.Chunker {
		t.Errorf("parsed chunker: got %q, want %q", d2.Chunker, d.Chunker)
	}
	if len(d2.Blocks) != len(d.Blocks) {
		t.Errorf("parsed blocks count: got %d, want %d", len(d2.Blocks), len(d.Blocks))
	}
}

func TestSerializeFormat(t *testing.T) {
	s, root := buildTestDAG(t)

	d, err := Build(s, root)
	if err != nil {
		t.Fatal(err)
	}

	data, err := d.Serialize()
	if err != nil {
		t.Fatal(err)
	}

	// Check magic.
	if !bytes.Equal(data[:4], []byte("MEMB")) {
		t.Errorf("magic: got %x, want MEMB", data[:4])
	}
	// Check version.
	if data[4] != 1 {
		t.Errorf("version: got %d, want 1", data[4])
	}
	// Check total length: 4 + 1 + payload_len + 32.
	payload := data[5 : len(data)-32]
	checksum := data[len(data)-32:]
	h := sha256.Sum256(payload)
	if !bytes.Equal(h[:], checksum) {
		t.Error("checksum mismatch")
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr string
	}{
		{"too short", []byte("abc"), "too short"},
		{"bad magic", append([]byte("NOPE"), make([]byte, 35)...), "invalid magic"},
		{"bad version", append(append([]byte("MEMB"), 99), make([]byte, 32)...), "unsupported version"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.data)
			if err == nil {
				t.Fatal("expected error")
			}
			if !bytes.Contains([]byte(err.Error()), []byte(tt.wantErr)) {
				t.Errorf("got %q, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestVerify(t *testing.T) {
	s, root := buildTestDAG(t)

	d, err := Build(s, root)
	if err != nil {
		t.Fatal(err)
	}

	missing, err := d.Verify(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 0 {
		t.Errorf("expected 0 missing blocks, got %d", len(missing))
	}

	// Verify with empty store should list all blocks as missing.
	empty := store.NewMemstore()
	missing, err = d.Verify(empty)
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != len(d.Blocks) {
		t.Errorf("expected %d missing blocks, got %d", len(d.Blocks), len(missing))
	}
}

func TestExportImport(t *testing.T) {
	s, root := buildTestDAG(t)

	d, err := Build(s, root, WithBootstrapPeers([]string{"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWTest"}))
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.mbuss")

	if err := d.Export(path); err != nil {
		t.Fatal(err)
	}

	// Verify file exists.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Error("exported file is empty")
	}

	// Import back.
	d2, err := Import(path)
	if err != nil {
		t.Fatal(err)
	}
	if !d2.RootMID.Equal(d.RootMID) {
		t.Error("imported root MID mismatch")
	}
	if len(d2.BootstrapPeers) != 1 {
		t.Errorf("imported bootstrap peers: got %d, want 1", len(d2.BootstrapPeers))
	}
}

func TestMagnetLink(t *testing.T) {
	s, root := buildTestDAG(t)

	d, err := Build(s, root,
		WithBootstrapPeers([]string{"/ip4/1.2.3.4/tcp/4001/p2p/12D3KooWTest"}),
		WithMemNSName("mysite"),
	)
	if err != nil {
		t.Fatal(err)
	}
	d.Erasure = &ErasureInfo{DataShards: 10, ParityShards: 4}

	uri := d.ToMagnetLink()
	if !bytes.HasPrefix([]byte(uri), []byte("magnet:?")) {
		t.Errorf("magnet link doesn't start with magnet:?: %s", uri)
	}
	if !bytes.Contains([]byte(uri), []byte("x=mem:")) {
		t.Error("magnet link missing x= parameter")
	}

	// Parse back.
	d2, err := FromMagnetLink(uri)
	if err != nil {
		t.Fatal(err)
	}
	if !d2.RootMID.Equal(d.RootMID) {
		t.Errorf("parsed magnet root MID mismatch: got %s, want %s", d2.RootMID, d.RootMID)
	}
	if d2.Name != d.Name {
		t.Errorf("parsed magnet name: got %q, want %q", d2.Name, d.Name)
	}
	if len(d2.BootstrapPeers) != 1 {
		t.Errorf("parsed magnet bootstrap peers: got %d, want 1", len(d2.BootstrapPeers))
	}
	if d2.Erasure == nil || d2.Erasure.DataShards != 10 {
		t.Error("parsed magnet erasure info missing or wrong")
	}
	if d2.MemNSName != "mysite" {
		t.Errorf("parsed magnet memns name: got %q, want %q", d2.MemNSName, "mysite")
	}
}

func TestMagnetLinkErrors(t *testing.T) {
	_, err := FromMagnetLink("not-a-magnet")
	if err == nil {
		t.Fatal("expected error for non-magnet URI")
	}
	_, err = FromMagnetLink("magnet:?dn=test")
	if err == nil {
		t.Fatal("expected error for magnet without x=")
	}
}

func TestSortBlocks(t *testing.T) {
	m1 := mid.FromBytes([]byte("block-a"))
	m2 := mid.FromBytes([]byte("block-b"))
	m3 := mid.FromBytes([]byte("block-c"))
	blocks := []BlockEntry{
		{Index: 3, MID: m3},
		{Index: 1, MID: m1},
		{Index: 2, MID: m2},
	}
	SortBlocks(blocks)
	for i := 1; i < len(blocks); i++ {
		if blocks[i].Index <= blocks[i-1].Index {
			t.Errorf("blocks not sorted at index %d", i)
		}
	}
}
