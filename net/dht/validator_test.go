package dht

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	membusspb "github.com/nnlgsakib/membuss/proto"
	"google.golang.org/protobuf/proto"
)

func signRecordForTest(priv crypto.PrivKey, pub crypto.PubKey, value string, seq uint64, validity int64) (*membusspb.MemNSRecord, error) {
	pubBytes, err := crypto.MarshalPublicKey(pub)
	if err != nil {
		return nil, err
	}
	record := &membusspb.MemNSRecord{
		Value:     []byte(value),
		Sequence:  seq,
		Validity:  validity,
		PublicKey: pubBytes,
		Meta:      make(map[string]string),
	}
	record.Meta["owner_key"] = base64.StdEncoding.EncodeToString(pubBytes)

	// Canonical bytes
	val := record.Value
	canonical := make([]byte, len(val)+8+8)
	copy(canonical, val)
	binary.BigEndian.PutUint64(canonical[len(val):], record.Sequence)
	binary.BigEndian.PutUint64(canonical[len(val)+8:], uint64(record.Validity))

	sig, err := priv.Sign(canonical)
	if err != nil {
		return nil, err
	}
	record.Signature = sig
	return record, nil
}

func TestMembussValidator_ValidateMemNS(t *testing.T) {
	v := membussValidator{}

	priv, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pubBytes, err := crypto.MarshalPublicKey(pub)
	if err != nil {
		t.Fatalf("marshal public key failed: %v", err)
	}

	// Derive the expected key name
	h := sha256.Sum256(pubBytes)
	var bi big.Int
	bi.SetBytes(h[:])
	keyName := "/memns/k" + bi.Text(36)

	t.Run("Valid Record", func(t *testing.T) {
		validity := time.Now().Add(1 * time.Hour).UnixNano()
		rec, err := signRecordForTest(priv, pub, "/mem/mem1abc", 1, validity)
		if err != nil {
			t.Fatalf("failed to sign record: %v", err)
		}
		raw, _ := proto.Marshal(rec)

		if err := v.Validate(keyName, raw); err != nil {
			t.Errorf("expected valid record to pass, got: %v", err)
		}
	})

	t.Run("Expired Record", func(t *testing.T) {
		validity := time.Now().Add(-1 * time.Hour).UnixNano()
		rec, err := signRecordForTest(priv, pub, "/mem/mem1abc", 1, validity)
		if err != nil {
			t.Fatalf("failed to sign record: %v", err)
		}
		raw, _ := proto.Marshal(rec)

		if err := v.Validate(keyName, raw); err == nil {
			t.Error("expected expired record to fail validation")
		} else if !strings.Contains(err.Error(), "expired") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("Invalid Key Name matching", func(t *testing.T) {
		validity := time.Now().Add(1 * time.Hour).UnixNano()
		rec, err := signRecordForTest(priv, pub, "/mem/mem1abc", 1, validity)
		if err != nil {
			t.Fatalf("failed to sign record: %v", err)
		}
		raw, _ := proto.Marshal(rec)

		if err := v.Validate("/memns/kothername", raw); err == nil {
			t.Error("expected key name mismatch to fail validation")
		} else if !strings.Contains(err.Error(), "does not match key name") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("Invalid Sequence Number", func(t *testing.T) {
		validity := time.Now().Add(1 * time.Hour).UnixNano()
		rec, err := signRecordForTest(priv, pub, "/mem/mem1abc", 0, validity)
		if err != nil {
			t.Fatalf("failed to sign record: %v", err)
		}
		raw, _ := proto.Marshal(rec)

		if err := v.Validate(keyName, raw); err == nil {
			t.Error("expected sequence 0 to fail validation")
		} else if !strings.Contains(err.Error(), "sequence must be > 0") {
			t.Errorf("unexpected error message: %v", err)
		}
	})

	t.Run("Invalid Signature", func(t *testing.T) {
		validity := time.Now().Add(1 * time.Hour).UnixNano()
		rec, err := signRecordForTest(priv, pub, "/mem/mem1abc", 1, validity)
		if err != nil {
			t.Fatalf("failed to sign record: %v", err)
		}
		rec.Signature = []byte("corrupted signature")
		raw, _ := proto.Marshal(rec)

		if err := v.Validate(keyName, raw); err == nil {
			t.Error("expected bad signature to fail validation")
		}
	})

	t.Run("Delegate Validation", func(t *testing.T) {
		delPriv, delPub, _ := crypto.GenerateEd25519Key(rand.Reader)
		delPubBytes, _ := crypto.MarshalPublicKey(delPub)

		validity := time.Now().Add(1 * time.Hour).UnixNano()
		rec, err := signRecordForTest(delPriv, delPub, "/mem/mem1abc", 1, validity)
		if err != nil {
			t.Fatalf("failed to sign record: %v", err)
		}

		// Set owner key in metadata to the main owner public key, but list delegate in Delegates
		rec.Meta["owner_key"] = base64.StdEncoding.EncodeToString(pubBytes)
		rec.Delegates = [][]byte{delPubBytes}

		// Re-sign record by delegate
		val := rec.Value
		canonical := make([]byte, len(val)+8+8)
		copy(canonical, val)
		binary.BigEndian.PutUint64(canonical[len(val):], rec.Sequence)
		binary.BigEndian.PutUint64(canonical[len(val)+8:], uint64(rec.Validity))

		sig, _ := delPriv.Sign(canonical)
		rec.Signature = sig
		raw, _ := proto.Marshal(rec)

		// Key name should correspond to owner
		if err := v.Validate(keyName, raw); err != nil {
			t.Errorf("expected delegate signature to pass validation, got: %v", err)
		}

		// If delegate is not in delegates list
		rec.Delegates = nil
		sig, _ = delPriv.Sign(canonical)
		rec.Signature = sig
		raw, _ = proto.Marshal(rec)
		if err := v.Validate(keyName, raw); err == nil {
			t.Error("expected delegate not in delegates list to fail validation")
		}
	})
}

func TestMembussValidator_ValidateMembuss(t *testing.T) {
	v := membussValidator{}

	t.Run("Valid Anchor Record", func(t *testing.T) {
		payload := map[string]interface{}{
			"id": "12D3KooWQz98jQ9S5H9gWnd6XRkH53SUL4jsjUvHCLQJ2JCyWy4A",
			"addrs": []string{
				"/ip4/127.0.0.1/tcp/4001",
			},
		}
		raw, _ := json.Marshal(payload)

		if err := v.Validate("/membuss/anchors/v1", raw); err != nil {
			t.Errorf("expected valid anchor record to pass, got: %v", err)
		}
	})

	t.Run("Invalid JSON", func(t *testing.T) {
		if err := v.Validate("/membuss/anchors/v1", []byte("{bad json")); err == nil {
			t.Error("expected bad JSON to fail validation")
		}
	})

	t.Run("Invalid Peer ID", func(t *testing.T) {
		payload := map[string]interface{}{
			"id":    "invalid_peer_id",
			"addrs": []string{"/ip4/127.0.0.1/tcp/4001"},
		}
		raw, _ := json.Marshal(payload)
		if err := v.Validate("/membuss/anchors/v1", raw); err == nil {
			t.Error("expected invalid Peer ID to fail validation")
		}
	})

	t.Run("Empty Address List", func(t *testing.T) {
		payload := map[string]interface{}{
			"id":    "12D3KooWQz98jQ9S5H9gWnd6XRkH53SUL4jsjUvHCLQJ2JCyWy4A",
			"addrs": []string{},
		}
		raw, _ := json.Marshal(payload)
		if err := v.Validate("/membuss/anchors/v1", raw); err == nil {
			t.Error("expected empty address list to fail validation")
		}
	})

	t.Run("Invalid Multiaddress", func(t *testing.T) {
		payload := map[string]interface{}{
			"id":    "12D3KooWQz98jQ9S5H9gWnd6XRkH53SUL4jsjUvHCLQJ2JCyWy4A",
			"addrs": []string{"/invalid/addr"},
		}
		raw, _ := json.Marshal(payload)
		if err := v.Validate("/membuss/anchors/v1", raw); err == nil {
			t.Error("expected invalid multiaddress to fail validation")
		}
	})

	t.Run("Valid Test Record", func(t *testing.T) {
		if err := v.Validate("/membuss/test/kv/1", []byte("hello")); err != nil {
			t.Errorf("expected test record to pass, got: %v", err)
		}
		if err := v.Validate("/membuss/test/kv/1", []byte("")); err == nil {
			t.Error("expected empty test record value to fail")
		}
	})
}

func TestMembussValidator_Select(t *testing.T) {
	v := membussValidator{}

	priv, pub, _ := crypto.GenerateEd25519Key(rand.Reader)

	// Create three records with different sequences and EOLs
	rec1, _ := signRecordForTest(priv, pub, "val1", 1, 1000)
	raw1, _ := proto.Marshal(rec1)

	rec2, _ := signRecordForTest(priv, pub, "val2", 2, 2000)
	raw2, _ := proto.Marshal(rec2)

	rec3, _ := signRecordForTest(priv, pub, "val3", 2, 3000)
	raw3, _ := proto.Marshal(rec3)

	t.Run("Select Best Sequence", func(t *testing.T) {
		values := [][]byte{raw1, raw2}
		idx, err := v.Select("/memns/testname", values)
		if err != nil {
			t.Fatalf("select failed: %v", err)
		}
		if idx != 1 {
			t.Errorf("expected to select record index 1 (highest sequence), got: %d", idx)
		}
	})

	t.Run("Select Best Validity EOL", func(t *testing.T) {
		values := [][]byte{raw2, raw3}
		idx, err := v.Select("/memns/testname", values)
		if err != nil {
			t.Fatalf("select failed: %v", err)
		}
		if idx != 1 {
			t.Errorf("expected to select record index 1 (later validity), got: %d", idx)
		}
	})
}
