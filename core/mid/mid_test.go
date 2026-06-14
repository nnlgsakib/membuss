package mid

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/multiformats/go-multihash"
)

func TestFromBytesDeterministic(t *testing.T) {
	data := []byte("hello, membuss")
	a := FromBytes(data)
	b := FromBytes(data)
	if !a.Equal(b) {
		t.Fatal("FromBytes must be deterministic for the same input")
	}
	if a.IsZero() {
		t.Fatal("FromBytes must produce a non-zero MID")
	}
}

func TestFromBytesDifferentContent(t *testing.T) {
	a := FromBytes([]byte("one"))
	b := FromBytes([]byte("two"))
	if a.Equal(b) {
		t.Fatal("distinct content must produce distinct MIDs")
	}
}

func TestStringHasPrefix(t *testing.T) {
	m := FromBytes([]byte("payload"))
	s := m.String()
	if len(s) < len(Prefix) || s[:len(Prefix)] != Prefix {
		t.Fatalf("String() = %q, want prefix %q", s, Prefix)
	}
}

func TestRoundTrip(t *testing.T) {
	cases := [][]byte{
		[]byte(""),
		[]byte("a"),
		[]byte("the quick brown fox jumps over the lazy dog"),
		bytes.Repeat([]byte{0xAB}, 1024),
	}
	for i, data := range cases {
		orig := FromBytes(data)
		parsed, err := Parse(orig.String())
		if err != nil {
			t.Fatalf("case %d: Parse(%q): %v", i, orig.String(), err)
		}
		if !orig.Equal(parsed) {
			t.Fatalf("case %d: round-trip mismatch:\n  orig=%x\n  back=%x", i, orig.HashBytes(), parsed.HashBytes())
		}
	}
}

func TestParseRejectsMissingPrefix(t *testing.T) {
	if _, err := Parse(""); err == nil {
		t.Fatal("Parse(\"\") must fail")
	}
	if _, err := Parse("QmExample"); err == nil {
		t.Fatal("Parse without mem prefix must fail")
	}
}

func TestParseRejectsBadMultihash(t *testing.T) {
	if _, err := Parse(Prefix + "not-a-multihash"); err == nil {
		t.Fatal("Parse with bad multihash must fail")
	}
}

func TestDigestMatchesSHA256(t *testing.T) {
	data := []byte("verify me")
	m := FromBytes(data)
	digest, err := m.DigestBytes()
	if err != nil {
		t.Fatalf("DigestBytes: %v", err)
	}
	want := sha256.Sum256(data)
	if !bytes.Equal(digest, want[:]) {
		t.Fatalf("DigestBytes = %x, want %x", digest, want[:])
	}
}

func TestCodecIsRaw(t *testing.T) {
	m := FromBytes([]byte("hi"))
	if m.Codec() != CodecRaw {
		t.Fatalf("Codec = %#x, want CodecRaw (%#x)", m.Codec(), CodecRaw)
	}
}

func TestHashBytesIsCopy(t *testing.T) {
	m := FromBytes([]byte("data"))
	a := m.HashBytes()
	a[0] = 0xFF
	b := m.HashBytes()
	if b[0] == 0xFF {
		t.Fatal("HashBytes must return a defensive copy")
	}
}

func TestFromMultihashRejectsEmpty(t *testing.T) {
	if _, err := FromMultihash(CodecRaw, nil); err == nil {
		t.Fatal("FromMultihash with nil must fail")
	}
}

func TestFromMultihashRoundTrip(t *testing.T) {
	sum := sha256.Sum256([]byte("k"))
	mh, err := multihash.Encode(sum[:], multihash.SHA2_256)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	m, err := FromMultihash(CodecDAGPB, mh)
	if err != nil {
		t.Fatalf("FromMultihash: %v", err)
	}
	if m.Codec() != CodecDAGPB {
		t.Fatalf("Codec = %#x, want CodecDAGPB", m.Codec())
	}
	if !bytes.Equal(m.HashBytes(), mh) {
		t.Fatalf("HashBytes = %x, want %x", m.HashBytes(), mh)
	}
}

func TestEqualSelf(t *testing.T) {
	m := FromBytes([]byte("self"))
	if !m.Equal(m) {
		t.Fatal("MID must be equal to itself")
	}
}
