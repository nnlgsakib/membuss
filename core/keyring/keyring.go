package keyring

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	membusspb "github.com/nnlgsakib/membuss/proto"
	"google.golang.org/protobuf/proto"
)

// Key represents a MemNS signing key pair.
type Key struct {
	Name      string
	PrivKey   crypto.PrivKey
	PubKey    crypto.PubKey
	MemNSName string // "/memns/" + base36(sha256(pubkey bytes))
}

// KeyInfo is the metadata structure returned by List and the API.
type KeyInfo struct {
	Name      string    `json:"name"`
	MemNSName string    `json:"memns_name"`
	CreatedAt time.Time `json:"created_at"`
	PublicKey string    `json:"public_key"` // base64 encoded marshaled public key
}

// KeyRing manages private and public keys under the data directory.
type KeyRing struct {
	dataDir string
}

// NewKeyRing initializes a new KeyRing instance.
func NewKeyRing(dataDir string) *KeyRing {
	return &KeyRing{dataDir: dataDir}
}

// DeriveMemNSName derives a /memns/ name from a libp2p public key.
func DeriveMemNSName(pub crypto.PubKey) (string, error) {
	pubBytes, err := crypto.MarshalPublicKey(pub)
	if err != nil {
		return "", fmt.Errorf("failed to marshal public key: %w", err)
	}
	h := sha256.Sum256(pubBytes)
	var i big.Int
	i.SetBytes(h[:])
	return "/memns/k" + i.Text(36), nil
}

// Generate generates a new key pair and stores it in the keyring directory.
func (kr *KeyRing) Generate(name string, keyType string) (*Key, error) {
	if name == "self" {
		return nil, errors.New("cannot generate key named 'self' (reserved for node identity)")
	}

	var priv crypto.PrivKey
	var pub crypto.PubKey
	var err error

	switch keyType {
	case "rsa":
		priv, pub, err = crypto.GenerateKeyPair(crypto.RSA, 2048)
	default:
		// Default to Ed25519
		priv, pub, err = crypto.GenerateEd25519Key(rand.Reader)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	keyringDir := filepath.Join(kr.dataDir, "keyring")
	if err := os.MkdirAll(keyringDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create keyring directory: %w", err)
	}

	privBytes, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	pubBytes, err := crypto.MarshalPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}

	privPath := filepath.Join(keyringDir, name+".key")
	pubPath := filepath.Join(keyringDir, name+".pub")

	if err := os.WriteFile(privPath, privBytes, 0600); err != nil {
		return nil, fmt.Errorf("failed to write private key: %w", err)
	}

	if err := os.WriteFile(pubPath, pubBytes, 0644); err != nil {
		return nil, fmt.Errorf("failed to write public key: %w", err)
	}

	memNSName, err := DeriveMemNSName(pub)
	if err != nil {
		return nil, err
	}

	return &Key{
		Name:      name,
		PrivKey:   priv,
		PubKey:    pub,
		MemNSName: memNSName,
	}, nil
}

// Get loads a key by name.
func (kr *KeyRing) Get(name string) (*Key, error) {
	var privBytes []byte
	var err error

	if name == "self" {
		privPath := filepath.Join(kr.dataDir, "identity.key")
		privBytes, err = os.ReadFile(privPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read identity.key: %w", err)
		}
	} else {
		privPath := filepath.Join(kr.dataDir, "keyring", name+".key")
		privBytes, err = os.ReadFile(privPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read key %q: %w", name, err)
		}
	}

	privKey, err := crypto.UnmarshalPrivateKey(privBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	pubKey := privKey.GetPublic()
	memNSName, err := DeriveMemNSName(pubKey)
	if err != nil {
		return nil, err
	}

	return &Key{
		Name:      name,
		PrivKey:   privKey,
		PubKey:    pubKey,
		MemNSName: memNSName,
	}, nil
}

// List lists all keys in the keyring, including "self".
func (kr *KeyRing) List() ([]*KeyInfo, error) {
	var list []*KeyInfo

	// Check self key
	selfPath := filepath.Join(kr.dataDir, "identity.key")
	if fi, err := os.Stat(selfPath); err == nil {
		key, err := kr.Get("self")
		if err == nil {
			pubBytes, _ := crypto.MarshalPublicKey(key.PubKey)
			list = append(list, &KeyInfo{
				Name:      "self",
				MemNSName: key.MemNSName,
				CreatedAt: fi.ModTime(),
				PublicKey: base64.StdEncoding.EncodeToString(pubBytes),
			})
		}
	}

	// Check keyring directory
	keyringDir := filepath.Join(kr.dataDir, "keyring")
	entries, err := os.ReadDir(keyringDir)
	if err == nil {
		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) == ".key" {
				name := entry.Name()[:len(entry.Name())-len(".key")]
				if name == "self" {
					continue
				}
				keyPath := filepath.Join(keyringDir, entry.Name())
				fi, err := os.Stat(keyPath)
				if err == nil {
					key, err := kr.Get(name)
					if err == nil {
						pubBytes, _ := crypto.MarshalPublicKey(key.PubKey)
						list = append(list, &KeyInfo{
							Name:      name,
							MemNSName: key.MemNSName,
							CreatedAt: fi.ModTime(),
							PublicKey: base64.StdEncoding.EncodeToString(pubBytes),
						})
					}
				}
			}
		}
	}

	return list, nil
}

// Export PEM encodes a private key.
func (kr *KeyRing) Export(name string) ([]byte, error) {
	key, err := kr.Get(name)
	if err != nil {
		return nil, err
	}

	privBytes, err := crypto.MarshalPrivateKey(key.PrivKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	block := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privBytes,
	}

	return pem.EncodeToMemory(block), nil
}

// Import imports a PEM encoded private key under a given name.
func (kr *KeyRing) Import(name string, pemBytes []byte) error {
	if name == "self" {
		return errors.New("cannot overwrite node identity key 'self' via import")
	}

	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return errors.New("invalid PEM data")
	}

	if block.Type != "PRIVATE KEY" {
		return fmt.Errorf("invalid PEM block type %q, expected \"PRIVATE KEY\"", block.Type)
	}

	privKey, err := crypto.UnmarshalPrivateKey(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to unmarshal private key: %w", err)
	}

	pubKey := privKey.GetPublic()
	pubBytes, err := crypto.MarshalPublicKey(pubKey)
	if err != nil {
		return fmt.Errorf("failed to marshal public key: %w", err)
	}

	keyringDir := filepath.Join(kr.dataDir, "keyring")
	if err := os.MkdirAll(keyringDir, 0700); err != nil {
		return fmt.Errorf("failed to create keyring directory: %w", err)
	}

	privPath := filepath.Join(keyringDir, name+".key")
	pubPath := filepath.Join(keyringDir, name+".pub")

	if err := os.WriteFile(privPath, block.Bytes, 0600); err != nil {
		return fmt.Errorf("failed to write private key: %w", err)
	}

	if err := os.WriteFile(pubPath, pubBytes, 0644); err != nil {
		return fmt.Errorf("failed to write public key: %w", err)
	}

	return nil
}

// Delete removes a key by name.
func (kr *KeyRing) Delete(name string) error {
	if name == "self" {
		return errors.New("cannot delete node identity key 'self'")
	}

	keyringDir := filepath.Join(kr.dataDir, "keyring")
	privPath := filepath.Join(keyringDir, name+".key")
	pubPath := filepath.Join(keyringDir, name+".pub")

	err1 := os.Remove(privPath)
	err2 := os.Remove(pubPath)

	if err1 != nil && os.IsNotExist(err1) && err2 != nil && os.IsNotExist(err2) {
		return fmt.Errorf("key %q not found", name)
	}

	return nil
}

// SaveRecord saves a published MemNSRecord to disk.
func (kr *KeyRing) SaveRecord(name string, record *membusspb.MemNSRecord) error {
	keyringDir := filepath.Join(kr.dataDir, "keyring")
	if err := os.MkdirAll(keyringDir, 0700); err != nil {
		return err
	}
	data, err := proto.Marshal(record)
	if err != nil {
		return err
	}
	recordPath := filepath.Join(keyringDir, name+".record")
	return os.WriteFile(recordPath, data, 0600)
}

// LoadRecord loads a saved MemNSRecord from disk.
func (kr *KeyRing) LoadRecord(name string) (*membusspb.MemNSRecord, error) {
	keyringDir := filepath.Join(kr.dataDir, "keyring")
	recordPath := filepath.Join(keyringDir, name+".record")
	data, err := os.ReadFile(recordPath)
	if err != nil {
		return nil, err
	}
	record := &membusspb.MemNSRecord{}
	if err := proto.Unmarshal(data, record); err != nil {
		return nil, err
	}
	return record, nil
}

// ListRecords returns all saved MemNSRecord where we have a corresponding key.
func (kr *KeyRing) ListRecords() ([]*membusspb.MemNSRecord, error) {
	var records []*membusspb.MemNSRecord
	keys, err := kr.List()
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		rec, err := kr.LoadRecord(k.Name)
		if err == nil {
			records = append(records, rec)
		}
	}
	return records, nil
}

