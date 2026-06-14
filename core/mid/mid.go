// Package mid implements the Membuss content identifier (MID).
//
// A MID is the string "mem" followed by a multibase-encoded
// multihash. The multihash is constructed by hashing the content
// with SHA-256, then wrapping the digest with the multihash
// envelope:
//
//	code:   0x12 (sha2-256)
//	length: 0x20 (32)
//	digest: 32 bytes
//
// The wrapped multihash is then encoded with multibase and prefixed
// with the literal string "mem" to produce the public MID string.
//
// Example: mem1z4a2bd9f...
package mid

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
)

// Prefix is the literal string that begins every public MID.
const Prefix = "mem"

// CodecRaw identifies a block whose data is the raw content payload.
// It mirrors the IPFS "raw" codec (0x55) and is the codec used for
// all leaf chunks emitted by core/chunk.
const CodecRaw uint64 = 0x55

// CodecDAGPB identifies a DAG internal node whose links are stored
// in the dag-pb style. It mirrors the IPFS dag-pb codec (0x70) and
// is the codec used for every non-leaf DAGNode emitted by core/dag.
const CodecDAGPB uint64 = 0x70

// DefaultHash is the multihash code used to hash content.
const DefaultHash = multihash.SHA2_256

// MID is a content identifier: a codec + a multihash digest.
//
// The zero value is invalid; use Parse or FromBytes to construct
// one. The String form is the public, network-facing identifier.
type MID struct {
	codec uint64
	hash  []byte // multihash envelope, not the raw digest
}

// FromBytes returns the MID for the given content bytes. The
// content is hashed with SHA-256, wrapped as a multihash, and
// tagged with the raw codec (0x55).
func FromBytes(data []byte) MID {
	sum := sha256.Sum256(data)
	mh, err := multihash.Encode(sum[:], DefaultHash)
	if err != nil {
		// SHA2_256 is always encodable; this branch is unreachable.
		panic(fmt.Sprintf("mid: encode multihash: %v", err))
	}
	return MID{codec: CodecRaw, hash: mh}
}

// FromMultihash wraps a pre-built multihash envelope with the given
// codec. The caller retains ownership of the multihash byte slice;
// this function copies it.
func FromMultihash(codec uint64, mh []byte) (MID, error) {
	if len(mh) == 0 {
		return MID{}, errors.New("mid: empty multihash")
	}
	decoded, err := multihash.Decode(mh)
	if err != nil {
		return MID{}, fmt.Errorf("mid: decode multihash: %w", err)
	}
	if err := validateDecoded(decoded); err != nil {
		return MID{}, fmt.Errorf("mid: validate multihash: %w", err)
	}
	out := make([]byte, len(mh))
	copy(out, mh)
	return MID{codec: codec, hash: out}, nil
}

// Parse parses a public MID string ("mem" + multibase + multihash)
// and returns the corresponding MID.
func Parse(s string) (MID, error) {
	if !strings.HasPrefix(s, Prefix) {
		return MID{}, fmt.Errorf("mid: missing %q prefix", Prefix)
	}
	encoded := strings.TrimPrefix(s, Prefix)
	if encoded == "" {
		return MID{}, errors.New("mid: empty encoded body")
	}
	_, body, err := multibase.Decode(encoded)
	if err != nil {
		return MID{}, fmt.Errorf("mid: multibase decode: %w", err)
	}
	decoded, err := multihash.Decode(body)
	if err != nil {
		return MID{}, fmt.Errorf("mid: multihash decode: %w", err)
	}
	if err := validateDecoded(decoded); err != nil {
		return MID{}, fmt.Errorf("mid: validate multihash: %w", err)
	}
	out := make([]byte, len(body))
	copy(out, body)
	return MID{codec: CodecRaw, hash: out}, nil
}

// MustParse is the panicking form of Parse; it is intended for
// constants and test fixtures only.
func MustParse(s string) MID {
	m, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return m
}

// validateDecoded sanity-checks a multihash decoded from bytes.
// The check rejects unknown hash codes and length mismatches,
// which the multihash library can otherwise silently accept.
func validateDecoded(d *multihash.DecodedMultihash) error {
	if d == nil {
		return errors.New("nil multihash")
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
	out := make([]byte, len(m.hash))
	copy(out, m.hash)
	return out
}

// DigestBytes returns the raw hash digest (decoded from the
// multihash envelope).
func (m MID) DigestBytes() ([]byte, error) {
	d, err := multihash.Decode(m.hash)
	if err != nil {
		return nil, fmt.Errorf("mid: decode multihash: %w", err)
	}
	return d.Digest, nil
}

// Bytes returns the multihash envelope. It is equivalent to
// HashBytes and is the form used in protobuf message bodies.
func (m MID) Bytes() []byte { return m.HashBytes() }

// String returns the public, network-facing form of this MID:
// the "mem" prefix followed by the multibase encoding of the
// multihash envelope.
func (m MID) String() string {
	encoded, err := multibase.Encode(multibase.Base32, m.hash)
	if err != nil {
		// Base32 + a non-empty multihash is always encodable.
		panic(fmt.Sprintf("mid: encode base32: %v", err))
	}
	return Prefix + encoded
}

// Equal reports whether m and other refer to the same content.
func (m MID) Equal(other MID) bool {
	if m.codec != other.codec {
		return false
	}
	if len(m.hash) != len(other.hash) {
		return false
	}
	for i := range m.hash {
		if m.hash[i] != other.hash[i] {
			return false
		}
	}
	return true
}

// IsZero reports whether m is the zero value, which is not a
// valid MID.
func (m MID) IsZero() bool { return len(m.hash) == 0 }
