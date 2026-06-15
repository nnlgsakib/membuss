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

// TestStringIsCIDv1Length asserts the public MID string
// has the canonical CIDv1 + base32 length. The Phase 14
// format is "mem" + "b" + 58 base32 chars = 62 chars for
// a sha2-256 + raw codec.
func TestStringIsCIDv1Length(t *testing.T) {
	m := FromBytes([]byte("hello"))
	s := m.String()
	const want = 3 + 1 + 58 // 3 prefix + 1 multibase + 58 base32
	if len(s) != want {
		t.Fatalf("String() length = %d, want %d (%q)", len(s), want, s)
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

func TestParseRejectsBadCID(t *testing.T) {
	if _, err := Parse(Prefix + "not-a-cid"); err == nil {
		t.Fatal("Parse with bad CID must fail")
	}
}

func TestParseRejectsLegacyBase58(t *testing.T) {
	// A legacy Phase 13-format MID (mem + base58 of the
	// raw sha256 digest) must NOT parse under the new
	// scheme. This is the test that proves the migration
	// is needed for upgrade-in-place scenarios.
	legacy := "mem" + "QmZ4tDuvesekSs4qM5ZBmpkQSSnCzEQQTwnUpYzbMeYxKL"
	if _, err := Parse(legacy); err == nil {
		t.Fatalf("Parse(%q) must fail (legacy base58 form)", legacy)
	}
}

func TestParseRejectsCIDv0(t *testing.T) {
	// A real CIDv0 (Qm...) prefixed with "mem" must be
	// rejected because we are v1-only.
	v0 := "mem" + "QmdfTbBqBPakXso1i6kbju8xf5XfkmtcsY1HerbdrpXcF8"
	if _, err := Parse(v0); err == nil {
		t.Fatalf("Parse(%q) must fail (CIDv0 is not supported)", v0)
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

// TestCIDAccessor reports the go-cid Cid form, which is
// what callers (DHT, IPNS bridges) use to interoperate
// with the broader IPFS ecosystem.
func TestCIDAccessor(t *testing.T) {
	m := FromBytes([]byte("hi"))
	c := m.CID()
	if c.Version() != 1 {
		t.Fatalf("CID version = %d, want 1", c.Version())
	}
	if c.Prefix().Codec != uint64(CodecRaw) {
		t.Fatalf("CID codec = %#x, want %#x", c.Prefix().Codec, CodecRaw)
	}
}

// TestVersionFieldIsV1 confirms the exposed Version field
// is set to 1 for freshly built MIDs.
func TestVersionFieldIsV1(t *testing.T) {
	m := FromBytes([]byte("phase14"))
	if m.Version != 1 {
		t.Fatalf("Version = %d, want 1", m.Version)
	}
}

// TestStringStableAcrossFormats verifies that an MID
// round-tripped through String() and Parse() returns the
// exact same hash envelope.
func TestStringStableAcrossFormats(t *testing.T) {
	m := FromBytes([]byte("a"))
	s := m.String()
	back, err := Parse(s)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !bytes.Equal(m.HashBytes(), back.HashBytes()) {
		t.Fatalf("hash envelope changed across roundtrip")
	}
}
