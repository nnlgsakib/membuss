// Package mid implements the Membuss content identifier (MID).
//
// A MID is a content-addressed identifier that follows the
// IPFS CIDv1 shape: it is a multibase-encoded (base32lower)
// CIDv1 wrapping a multihash envelope. The public string
// form is the literal "mem" prefix followed by the multibase
// letter and the lower-case base32 alphabet:
//
//	mem + "b" + base32lower(CIDv1 bytes)
//
// The CIDv1 byte layout is:
//
//	<version=0x01> <varint(codec)> <multihash>
//
// The multihash envelope is:
//
//	<hash-fn-code=0x12 (sha2-256)> <length=0x20> <32-byte-digest>
//
// so a raw block (codec 0x55) is:
//
//	01 55 12 20 <32 bytes>          (total 36 bytes)
//
// which encodes to a 58-character base32 string. The
// "mem" prefix + the multibase 'b' prefix + the 58
// characters produces the canonical ~61-character public
// MID (matching the IPFS CIDv1 + base32 length).
package mid

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// Prefix is the literal string that begins every public MID.
const Prefix = "mem"

// CIDv1 version byte.
const cidVersion1 = 1

// CodecRaw identifies a block whose data is the raw content
// payload. It mirrors the IPFS "raw" codec (0x55) and is the
// codec used for every leaf chunk emitted by core/chunk.
const CodecRaw uint64 = 0x55

// CodecDAGPB identifies a DAG internal node. It mirrors the
// IPFS dag-pb codec (0x70) and is the codec used for every
// non-leaf DAGNode emitted by core/dag.
const CodecDAGPB uint64 = 0x70

// DefaultHash is the multihash code used to hash content.
const DefaultHash = multihash.SHA2_256

// MID is a content identifier: a CIDv1-encoded codec +
// multihash digest.
//
// The zero value is invalid; use FromBytes, FromMultihash,
// or Parse to construct one. The String form is the public,
// network-facing identifier and is what appears on the wire
// and in the gateway.
type MID struct {
	// Version is the CIDv1 version byte. Always 1 for
	// freshly built MIDs. Parsed MIDs report whatever the
	// input carried; only Version==1 is accepted by Parse.
	Version uint8
	// Codec is the multicodec tag for the body. 0x55 for
	// raw blocks, 0x70 for dag-pb, etc.
	codec uint64
	// Hash is the raw multihash envelope, including the
	// hash-fn code byte, the length byte, and the digest.
	Hash []byte
	// cid is the parsed go-cid form. Cached so callers that
	// want the rich cid.Cid API do not have to re-parse.
	cid cid.Cid
}

// FromBytes returns the MID for the given content bytes. The
// content is hashed with SHA-256, wrapped as a multihash,
// embedded in a CIDv1, and tagged with the raw codec (0x55).
func FromBytes(data []byte) MID {
	sum := sha256.Sum256(data)
	mh, err := multihash.Encode(sum[:], DefaultHash)
	if err != nil {
		// SHA2_256 is always encodable; this branch is unreachable.
		panic(fmt.Sprintf("mid: encode multihash: %v", err))
	}
	return FromCodecAndHash(cidVersion1, CodecRaw, mh)
}

// FromMultihash wraps a pre-built multihash envelope with the
// given codec. The caller retains ownership of mh; this
// function copies it.
func FromMultihash(codec uint64, mh []byte) (MID, error) {
	return fromCodecAndHashErr(cidVersion1, codec, mh)
}

// fromCodecAndHashErr is the same as FromCodecAndHash but
// surfaces validation errors instead of panicking.
func fromCodecAndHashErr(version uint8, codec uint64, mh []byte) (MID, error) {
	if len(mh) == 0 {
		return MID{}, errors.New("mid: empty multihash")
	}
	if _, err := multihash.Decode(mh); err != nil {
		return MID{}, fmt.Errorf("mid: decode multihash: %w", err)
	}
	if err := validateEnvelope(mh); err != nil {
		return MID{}, fmt.Errorf("mid: validate multihash: %w", err)
	}
	out := make([]byte, len(mh))
	copy(out, mh)
	return build(version, codec, out)
}

// FromCodecAndHash constructs a MID from a version, codec, and
// multihash envelope. It panics if the multihash is invalid;
// the FromBytes hot path uses this and SHA-256 is always
// encodable, so the panic is unreachable there.
func FromCodecAndHash(version uint8, codec uint64, mh []byte) MID {
	m, err := fromCodecAndHashErr(version, codec, mh)
	if err != nil {
		panic(err.Error())
	}
	return m
}

// build is the shared constructor used by FromCodecAndHash and
// the parser. It copies the multihash and parses the go-cid.
func build(version uint8, codec uint64, mh []byte) (MID, error) {
	if version != cidVersion1 {
		return MID{}, fmt.Errorf("mid: unsupported CID version %d", version)
	}
	c := cid.NewCidV1(codec, mh)
	return MID{
		Version: version,
		codec:   codec,
		Hash:    mh,
		cid:     c,
	}, nil
}

// Parse parses a public MID string ("mem" + multibase +
// CIDv1 bytes) and returns the corresponding MID. It rejects
// anything that is not a CIDv1 wrapping a sha2-256
// multihash.
func Parse(s string) (MID, error) {
	if !strings.HasPrefix(s, Prefix) {
		return MID{}, fmt.Errorf("mid: missing %q prefix", Prefix)
	}
	encoded := strings.TrimPrefix(s, Prefix)
	if encoded == "" {
		return MID{}, errors.New("mid: empty encoded body")
	}
	parsed, err := cid.Decode(encoded)
	if err != nil {
		return MID{}, fmt.Errorf("mid: cid parse: %w", err)
	}
	if parsed.Version() != cidVersion1 {
		return MID{}, fmt.Errorf("mid: unsupported CID version %d", parsed.Version())
	}
	mh := parsed.Hash()
	if err := validateEnvelope(mh); err != nil {
		return MID{}, fmt.Errorf("mid: validate multihash: %w", err)
	}
	return MID{
		Version: uint8(parsed.Version()),
		codec:   parsed.Prefix().Codec,
		Hash:    append([]byte(nil), mh...),
		cid:     parsed,
	}, nil
}

// MustParse is the panicking form of Parse; it is intended
// for constants and test fixtures only.
func MustParse(s string) MID {
	m, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return m
}

// validateEnvelope sanity-checks a multihash decoded from
// bytes. It rejects unknown hash codes and length
// mismatches, which the multihash library can otherwise
// silently accept.
func validateEnvelope(mh []byte) error {
	if len(mh) == 0 {
		return errors.New("empty multihash")
	}
	d, err := multihash.Decode(mh)
	if err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if d.Code != DefaultHash {
		return fmt.Errorf("unsupported hash code %#x", d.Code)
	}
	if len(d.Digest) != d.Length {
		return fmt.Errorf("length mismatch: header says %d, body has %d", d.Length, len(d.Digest))
	}
	return nil
}

// Codec returns the codec tag associated with this MID.
func (m MID) Codec() uint64 { return m.codec }

// HashBytes returns a copy of the multihash envelope.
func (m MID) HashBytes() []byte {
	out := make([]byte, len(m.Hash))
	copy(out, m.Hash)
	return out
}

// DigestBytes returns the raw hash digest (decoded from the
// multihash envelope).
func (m MID) DigestBytes() ([]byte, error) {
	d, err := multihash.Decode(m.Hash)
	if err != nil {
		return nil, fmt.Errorf("mid: decode multihash: %w", err)
	}
	return d.Digest, nil
}

// Bytes returns the multihash envelope. It is equivalent to
// HashBytes and is the form used in protobuf message bodies
// and in the BadgerDB key layout.
func (m MID) Bytes() []byte { return m.HashBytes() }

// CID returns the underlying go-cid Cid. The returned value
// is suitable for use with the ipfs/go-cid APIs. It shares
// memory with the receiver; do not mutate.
func (m MID) CID() cid.Cid { return m.cid }

// String returns the public, network-facing form of this MID:
// the "mem" prefix followed by the multibase (base32lower)
// encoding of the CIDv1 bytes.
func (m MID) String() string {
	c := cid.NewCidV1(m.codec, m.Hash)
	return Prefix + c.String()
}

// Equal reports whether m and other refer to the same
// content. Two MIDs are equal iff their codec and
// multihash envelope match. The CIDv1 version is always 1
// for freshly built MIDs, so it does not need to be
// checked separately.
func (m MID) Equal(other MID) bool {
	if m.codec != other.codec {
		return false
	}
	if len(m.Hash) != len(other.Hash) {
		return false
	}
	for i := range m.Hash {
		if m.Hash[i] != other.Hash[i] {
			return false
		}
	}
	return true
}

// IsZero reports whether m is the zero value, which is not
// a valid MID.
func (m MID) IsZero() bool { return len(m.Hash) == 0 }
