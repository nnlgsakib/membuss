package dht

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	membusspb "github.com/nnlgsakib/membuss/proto"
	"google.golang.org/protobuf/proto"
)

type membussValidator struct{}

func (membussValidator) Validate(key string, value []byte) error {
	if strings.HasPrefix(key, "/memns/") {
		return validateMemNS(key, value)
	}
	if strings.HasPrefix(key, "/membuss/") {
		return validateMembuss(key, value)
	}
	return fmt.Errorf("unsupported namespace for key: %q", key)
}

func (membussValidator) Select(key string, values [][]byte) (int, error) {
	if len(values) == 0 {
		return 0, errors.New("no values")
	}

	if strings.HasPrefix(key, "/memns/") {
		bestIdx := -1
		var bestSeq uint64
		var bestVal int64

		for idx, val := range values {
			var record membusspb.MemNSRecord
			if err := proto.Unmarshal(val, &record); err != nil {
				continue
			}
			if bestIdx == -1 || record.Sequence > bestSeq || (record.Sequence == bestSeq && record.Validity > bestVal) {
				bestIdx = idx
				bestSeq = record.Sequence
				bestVal = record.Validity
			}
		}
		if bestIdx != -1 {
			return bestIdx, nil
		}
	}

	return 0, nil
}

func validateMemNS(key string, value []byte) error {
	var record membusspb.MemNSRecord
	if err := proto.Unmarshal(value, &record); err != nil {
		return fmt.Errorf("failed to unmarshal memns record: %w", err)
	}

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

	// Calculate canonical bytes for verification
	val := record.Value
	canonical := make([]byte, len(val)+8+8)
	copy(canonical, val)
	binary.BigEndian.PutUint64(canonical[len(val):], record.Sequence)
	binary.BigEndian.PutUint64(canonical[len(val)+8:], uint64(record.Validity))

	ok, err := pubKey.Verify(canonical, record.Signature)
	if err != nil {
		return fmt.Errorf("signature verification error: %w", err)
	}
	if !ok {
		return errors.New("invalid signature")
	}

	// Verify delegate/owner matches
	var ownerBytes []byte
	if record.Meta != nil {
		if ownerBase64, ok := record.Meta["owner_key"]; ok {
			if ob, err := base64.StdEncoding.DecodeString(ownerBase64); err == nil {
				ownerBytes = ob
			}
		}
	}
	if len(ownerBytes) == 0 {
		ownerBytes = record.PublicKey
	}

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

	// Verify name matches public key
	h := sha256.Sum256(ownerBytes)
	var bi big.Int
	bi.SetBytes(h[:])
	expectedName := "k" + bi.Text(36)
	if strings.TrimPrefix(key, "/memns/") != expectedName {
		return fmt.Errorf("record owner public key does not match key name %q", key)
	}

	return nil
}

func validateMembuss(key string, value []byte) error {
	// For testing/development
	if strings.HasPrefix(key, "/membuss/test/") {
		if len(value) == 0 {
			return errors.New("empty test value")
		}
		return nil
	}

	if key == "/membuss/anchors/v1" || key == "/membuss/relays/v1" {
		type wire struct {
			ID    string   `json:"id"`
			Addrs []string `json:"addrs"`
		}
		var w wire
		if err := json.Unmarshal(value, &w); err != nil {
			return fmt.Errorf("invalid json payload: %w", err)
		}
		if _, err := peer.Decode(w.ID); err != nil {
			return fmt.Errorf("invalid peer ID: %w", err)
		}
		if len(w.Addrs) == 0 {
			return errors.New("no addresses provided")
		}
		for _, addrStr := range w.Addrs {
			if _, err := multiaddr.NewMultiaddr(addrStr); err != nil {
				return fmt.Errorf("invalid multiaddr %q: %w", addrStr, err)
			}
		}
		return nil
	}

	return fmt.Errorf("unsupported key path: %q", key)
}
