package memns

import (
	"bytes"
	"encoding/binary"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/nnlgsakib/membuss/core/keyring"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

// CanonicalBytes returns the deterministic serialization for signing a MemNSRecord.
// It concats: value bytes, big-endian uint64 sequence, big-endian int64 validity.
func CanonicalBytes(record *membusspb.MemNSRecord) []byte {
	val := record.Value
	buf := make([]byte, len(val)+8+8)
	copy(buf, val)
	binary.BigEndian.PutUint64(buf[len(val):], record.Sequence)
	binary.BigEndian.PutUint64(buf[len(val)+8:], uint64(record.Validity))
	return buf
}

// CanonicalLogBytes returns the deterministic serialization for signing a MemLogEntry.
// It concats: big-endian uint64 sequence, value bytes, big-endian int64 timestamp.
func CanonicalLogBytes(seq uint64, value []byte, timestamp int64) []byte {
	buf := make([]byte, 8+len(value)+8)
	binary.BigEndian.PutUint64(buf[0:8], seq)
	copy(buf[8:8+len(value)], value)
	binary.BigEndian.PutUint64(buf[8+len(value):], uint64(timestamp))
	return buf
}

// BuildRecord constructs a new MemNSRecord and signs it using the provided key.
func BuildRecord(
	key *keyring.Key,
	value string,
	seq uint64,
	ttl time.Duration,
	routes []*membusspb.MemRoute,
	message string,
) (*membusspb.MemNSRecord, error) {
	now := time.Now()
	validity := now.Add(ttl).UnixNano()

	pubBytes, err := crypto.MarshalPublicKey(key.PubKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}

	record := &membusspb.MemNSRecord{
		Value:        []byte(value),
		Sequence:     seq,
		Validity:     validity,
		PublicKey:    pubBytes,
		Ttl:          uint64(ttl.Nanoseconds()),
		ValidityType: membusspb.ValidityType_EOL,
		Routes:       routes,
		Meta:         make(map[string]string),
	}

	// Always store owner key in metadata to facilitate delegate verification
	record.Meta["owner_key"] = base64.StdEncoding.EncodeToString(pubBytes)

	// Generate and sign the changelog entry for this publish
	ts := now.UnixNano()
	logBytes := CanonicalLogBytes(seq, []byte(value), ts)
	sig, err := key.PrivKey.Sign(logBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to sign changelog entry: %w", err)
	}

	entry := &membusspb.MemLogEntry{
		Sequence:  seq,
		Value:     []byte(value),
		Timestamp: ts,
		Signature: sig,
		Message:   message,
	}

	record.Changelog = &membusspb.MemLog{
		Entries: []*membusspb.MemLogEntry{entry},
	}

	// Sign the main record value + sequence + validity
	canonical := CanonicalBytes(record)
	recordSig, err := key.PrivKey.Sign(canonical)
	if err != nil {
		return nil, fmt.Errorf("failed to sign record: %w", err)
	}
	record.Signature = recordSig

	return record, nil
}

// VerifyRecord cryptographically validates a MemNSRecord structure.
func VerifyRecord(record *membusspb.MemNSRecord) error {
	if record.Sequence == 0 {
		return errors.New("sequence must be > 0")
	}
	if record.Validity <= time.Now().UnixNano() {
		return errors.New("record expired")
	}

	pubKey, err := crypto.UnmarshalPublicKey(record.PublicKey)
	if err != nil {
		return fmt.Errorf("invalid public key in record: %w", err)
	}

	canonical := CanonicalBytes(record)
	ok, err := pubKey.Verify(canonical, record.Signature)
	if err != nil {
		return fmt.Errorf("signature verification error: %w", err)
	}
	if !ok {
		return errors.New("invalid signature")
	}

	// Verify signer matching if owner_key exists in metadata
	if record.Meta != nil {
		if ownerBase64, ok := record.Meta["owner_key"]; ok {
			ownerBytes, err := base64.StdEncoding.DecodeString(ownerBase64)
			if err == nil {
				isOwner := bytes.Equal(ownerBytes, record.PublicKey)
				isDelegate := false
				for _, d := range record.Delegates {
					if bytes.Equal(d, record.PublicKey) {
						isDelegate = true
						break
					}
				}
				if !isOwner && !isDelegate {
					return errors.New("signer is not the owner and not in delegates list")
				}
			}
		}
	}

	return nil
}
