package dht

import (
	"context"
	"testing"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p"
	"github.com/nnlgsakib/membuss/core/mid"
)

func TestProviderRecord_ActiveRemoval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := ds.NewMapDatastore()
	h, err := libp2p.New(libp2p.NoTransports, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	d, err := New(ctx, Config{
		Host:      h,
		ModeName:  "server",
		Datastore: dstore,
	})
	if err != nil {
		t.Fatalf("failed to create dht: %v", err)
	}

	testMID := mid.FromBytes([]byte("active-removal-test"))
	ps := d.dht.ProviderStore()
	c := midToCID(testMID)
	err = ps.AddProvider(ctx, c.Hash(), h.Peerstore().PeerInfo(h.ID()))
	if err != nil {
		t.Fatalf("failed to add provider record: %v", err)
	}

	// Close DHT to flush the AddProvider write to the datastore
	_ = d.Close()

	// Create a temporary MemDHT instance sharing the same datastore to perform the removal
	tmpDHT, err := New(ctx, Config{
		Host:      h,
		ModeName:  "server",
		Datastore: dstore,
	})
	if err != nil {
		t.Fatalf("failed to create temp dht: %v", err)
	}

	// Call active removal on the new instance
	err = tmpDHT.RemoveProviderRecord(testMID)
	if err != nil {
		t.Fatalf("RemoveProviderRecord failed: %v", err)
	}

	// Close the temporary DHT to flush the deletion to the datastore
	_ = tmpDHT.Close()

	// Verify datastore is empty
	qRes, err := dstore.Query(ctx, query.Query{KeysOnly: false})
	if err != nil {
		t.Fatalf("datastore query failed: %v", err)
	}
	defer qRes.Close()
	
	var remaining []string
	for entry := range qRes.Next() {
		remaining = append(remaining, entry.Key)
	}
	if len(remaining) != 0 {
		t.Errorf("expected datastore to be empty after active removal, got keys: %v", remaining)
	}
}

func TestProviderRecord_TTLAndCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := ds.NewMapDatastore()
	h, err := libp2p.New(libp2p.NoTransports, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	recordTTL := 400 * time.Millisecond
	cleanupInt := 100 * time.Millisecond

	d, err := New(ctx, Config{
		Host:                    h,
		ModeName:                "server",
		Datastore:               dstore,
		ProviderRecordTTL:       recordTTL,
		ProviderAddrTTL:         recordTTL,
		ProviderCleanupInterval: cleanupInt,
	})
	if err != nil {
		t.Fatalf("failed to create dht: %v", err)
	}

	testMID := mid.FromBytes([]byte("ttl-cleanup-test"))
	ps := d.dht.ProviderStore()
	c := midToCID(testMID)
	err = ps.AddProvider(ctx, c.Hash(), h.Peerstore().PeerInfo(h.ID()))
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Wait for the record to expire and the cleanup loop to run
	time.Sleep(recordTTL + cleanupInt + 200*time.Millisecond)

	// Close DHT to flush all datastore batch operations
	_ = d.Close()

	// Verify datastore is empty (swept/cleaned up)
	qRes, err := dstore.Query(ctx, query.Query{KeysOnly: true})
	if err != nil {
		t.Fatalf("datastore query failed: %v", err)
	}
	defer qRes.Close()
	count := countKeys(qRes)
	if count != 0 {
		t.Errorf("expected datastore to be clean after TTL expiration, got %d keys", count)
	}
}

func TestProviderRecord_PersistenceSanity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dstore := ds.NewMapDatastore()
	h, err := libp2p.New(libp2p.NoTransports, libp2p.NoListenAddrs)
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	d, err := New(ctx, Config{
		Host:      h,
		ModeName:  "server",
		Datastore: dstore,
	})
	if err != nil {
		t.Fatalf("failed to create dht: %v", err)
	}

	testMID := mid.FromBytes([]byte("persistence-test"))
	ps := d.dht.ProviderStore()
	c := midToCID(testMID)
	err = ps.AddProvider(ctx, c.Hash(), h.Peerstore().PeerInfo(h.ID()))
	if err != nil {
		t.Fatalf("failed to add provider: %v", err)
	}

	// Close DHT to flush all datastore batch operations
	_ = d.Close()

	// Verify datastore is NOT empty (record persisted)
	qRes, err := dstore.Query(ctx, query.Query{KeysOnly: true})
	if err != nil {
		t.Fatalf("datastore query failed: %v", err)
	}
	defer qRes.Close()
	count := countKeys(qRes)
	if count == 0 {
		t.Error("expected datastore to contain the provider record")
	}
}

func countKeys(res query.Results) int {
	count := 0
	for range res.Next() {
		count++
	}
	return count
}
