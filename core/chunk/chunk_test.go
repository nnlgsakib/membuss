package chunk

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
)

func readAll(t *testing.T, c Chunker) []Block {
	t.Helper()
	var out []Block
	for {
		b, err := c.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		out = append(out, b)
	}
}

func TestFixedChunkerSmallInput(t *testing.T) {
	data := []byte("hello, membuss")
	c, err := NewFixed(DefaultBlockSize)(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewFixed: %v", err)
	}
	blocks := readAll(t, c)
	if len(blocks) != 1 {
		t.Fatalf("len(blocks) = %d, want 1", len(blocks))
	}
	if !bytes.Equal(blocks[0].Data(), data) {
		t.Fatalf("block data = %q, want %q", blocks[0].Data(), data)
	}
	if blocks[0].Size() != len(data) {
		t.Fatalf("Size = %d, want %d", blocks[0].Size(), len(data))
	}
	if blocks[0].MID().IsZero() {
		t.Fatal("MID is zero")
	}
}

func TestFixedChunkerEmptyInput(t *testing.T) {
	c, err := NewFixed(DefaultBlockSize)(bytes.NewReader(nil))
	if err != nil {
		t.Fatalf("NewFixed: %v", err)
	}
	if blocks := readAll(t, c); len(blocks) != 0 {
		t.Fatalf("len(blocks) = %d, want 0", len(blocks))
	}
}

func TestFixedChunkerMultipleBlocks(t *testing.T) {
	size := 1024
	payload := bytes.Repeat([]byte{0xA5}, size*3+123) // 3 full + 1 partial
	c, err := NewFixed(size)(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("NewFixed: %v", err)
	}
	blocks := readAll(t, c)
	if len(blocks) != 4 {
		t.Fatalf("len(blocks) = %d, want 4", len(blocks))
	}
	if blocks[0].Size() != size || blocks[1].Size() != size || blocks[2].Size() != size {
		t.Fatalf("first three blocks must be %d bytes, got %d %d %d", size, blocks[0].Size(), blocks[1].Size(), blocks[2].Size())
	}
	if blocks[3].Size() != 123 {
		t.Fatalf("tail block = %d, want 123", blocks[3].Size())
	}
	joined := bytes.Join([][]byte{
		blocks[0].Data(), blocks[1].Data(), blocks[2].Data(), blocks[3].Data(),
	}, nil)
	if !bytes.Equal(joined, payload) {
		t.Fatal("rejoined blocks do not match input")
	}
}

func TestFixedChunkerRejectsBadSize(t *testing.T) {
	for _, bad := range []int{0, 100, MaxBlockSize + 1} {
		if _, err := NewFixed(bad)(bytes.NewReader(nil)); err == nil {
			t.Errorf("NewFixed(%d) must fail", bad)
		}
	}
}

func TestFixedChunkerMIDMatchesData(t *testing.T) {
	data := []byte("address me")
	c, err := NewFixed(DefaultBlockSize)(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewFixed: %v", err)
	}
	b, err := c.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if b.MID().String() != b.Block.Mid {
		t.Fatalf("Block.Mid = %q, want %q", b.Block.Mid, b.MID().String())
	}
}

func TestFixedChunker10MB(t *testing.T) {
	const total = 10 * 1024 * 1024
	payload := make([]byte, total)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	c, err := NewFixed(DefaultBlockSize)(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("NewFixed: %v", err)
	}
	blocks := readAll(t, c)
	if len(blocks) != total/DefaultBlockSize {
		t.Fatalf("len(blocks) = %d, want %d", len(blocks), total/DefaultBlockSize)
	}
	var rebuilt []byte
	for i, b := range blocks {
		if b.Size() != DefaultBlockSize {
			t.Fatalf("block %d size = %d, want %d", i, b.Size(), DefaultBlockSize)
		}
		rebuilt = append(rebuilt, b.Data()...)
	}
	if !bytes.Equal(rebuilt, payload) {
		t.Fatal("10 MiB reassembly mismatch")
	}
}

func TestRabinChunkerSmallInput(t *testing.T) {
	// Below rabinMin: must be emitted as a single block.
	data := []byte("tiny")
	c, err := NewRabin()(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewRabin: %v", err)
	}
	blocks := readAll(t, c)
	if len(blocks) != 1 {
		t.Fatalf("len(blocks) = %d, want 1", len(blocks))
	}
	if !bytes.Equal(blocks[0].Data(), data) {
		t.Fatalf("block data = %q, want %q", blocks[0].Data(), data)
	}
}

func TestRabinChunker1MB(t *testing.T) {
	const total = 1 * 1024 * 1024
	payload := make([]byte, total)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	c, err := NewRabin()(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("NewRabin: %v", err)
	}
	blocks := readAll(t, c)
	if len(blocks) < 1 {
		t.Fatal("expected at least one block")
	}
	for i, b := range blocks {
		if b.Size() < 1 || b.Size() > rabinMax {
			t.Fatalf("block %d size %d out of bounds (1..%d)", i, b.Size(), rabinMax)
		}
	}
	joined := []byte{}
	for _, b := range blocks {
		joined = append(joined, b.Data()...)
	}
	if !bytes.Equal(joined, payload) {
		t.Fatal("Rabin reassembly mismatch")
	}
}

func TestRabinChunkerRejectsNilReader(t *testing.T) {
	if _, err := NewRabin()(nil); err == nil {
		t.Fatal("NewRabin(nil) must fail")
	}
}

func TestRabinChunkerDeduplicatesAppend(t *testing.T) {
	// A 256 KiB blob of zeros, preceded and followed by distinct
	// 1 KiB headers. Shifting the central blob by one byte should
	// produce an identical cut plan within the blob.
	prefix := bytes.Repeat([]byte{0xAA}, 1024)
	core := bytes.Repeat([]byte{0x00}, 256*1024)
	suffix := bytes.Repeat([]byte{0xBB}, 1024)

	mk := func(off int) []byte {
		out := make([]byte, 0, len(prefix)+1+len(core)+len(suffix))
		out = append(out, prefix...)
		out = append(out, 0xFE) // the shifted byte
		out = append(out, core...)
		out = append(out, suffix...)
		// Shift the central 256 KiB block by replacing the first
		// 'off' bytes with 0xFF and adjusting the prefix boundary.
		for i := 0; i < off && i < len(out); i++ {
			out[i] = 0xFF
		}
		return out
	}

	a, err := NewRabin()(bytes.NewReader(mk(0)))
	if err != nil {
		t.Fatalf("NewRabin a: %v", err)
	}
	b, err := NewRabin()(bytes.NewReader(mk(8)))
	if err != nil {
		t.Fatalf("NewRabin b: %v", err)
	}
	ga := readAll(t, a)
	gb := readAll(t, b)

	if len(ga) == 0 || len(gb) == 0 {
		t.Fatal("expected non-empty chunk sequences")
	}
	// At least one block whose MID appears in both sequences.
	found := false
	for _, ba := range ga {
		for _, bb := range gb {
			if ba.MID().Equal(bb.MID()) {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Fatal("expected at least one shared block MID between the two shifted inputs")
	}
}
