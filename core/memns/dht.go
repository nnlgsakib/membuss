package memns

import (
	"context"
	"fmt"
	"strings"

	"github.com/nnlgsakib/membuss/core/keyring"
	"github.com/nnlgsakib/membuss/net/dht"
	membusspb "github.com/nnlgsakib/membuss/proto"
	"google.golang.org/protobuf/proto"
)

// PublishDHT writes the signed MemNSRecord to the DHT.
func PublishDHT(ctx context.Context, dhtClient *dht.MemDHT, key *keyring.Key, record *membusspb.MemNSRecord) error {
	name := key.MemNSName
	if strings.HasPrefix(name, "/memns/") {
		name = name[7:]
	}
	dhtKey := "/memns/" + name

	data, err := proto.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	if err := dhtClient.PutValue(ctx, dhtKey, data); err != nil {
		return fmt.Errorf("dht put value error: %w", err)
	}

	return nil
}

// ResolveDHT fetches and cryptographically verifies a MemNSRecord from the DHT.
func ResolveDHT(ctx context.Context, dhtClient *dht.MemDHT, name string) (*membusspb.MemNSRecord, error) {
	keyhash := name
	if strings.HasPrefix(keyhash, "/memns/") {
		keyhash = keyhash[7:]
	}
	dhtKey := "/memns/" + keyhash

	raw, err := dhtClient.GetValue(ctx, dhtKey)
	if err != nil {
		return nil, fmt.Errorf("dht get value error: %w", err)
	}

	record := &membusspb.MemNSRecord{}
	if err := proto.Unmarshal(raw, record); err != nil {
		return nil, fmt.Errorf("failed to unmarshal record: %w", err)
	}

	if err := VerifyRecord(record); err != nil {
		return nil, fmt.Errorf("record verification failed: %w", err)
	}

	return record, nil
}
