package memns

import (
	"context"
	"fmt"
	"strings"
	"sync"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/nnlgsakib/membuss/core/keyring"
	membusspb "github.com/nnlgsakib/membuss/proto"
	"google.golang.org/protobuf/proto"
)

// PubSubManager manages libp2p GossipSub topics and subscriptions for MemNS.
type PubSubManager struct {
	ps     *pubsub.PubSub
	mu     sync.Mutex
	topics map[string]*pubsub.Topic
}

// NewPubSubManager initializes GossipSub on top of the given libp2p host.
func NewPubSubManager(h host.Host) (*PubSubManager, error) {
	ctx := context.Background()
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize GossipSub: %w", err)
	}
	return &PubSubManager{
		ps:     ps,
		topics: make(map[string]*pubsub.Topic),
	}, nil
}

// GetTopic returns or joins a GossipSub topic for the given MemNS name.
func (pm *PubSubManager) GetTopic(name string) (*pubsub.Topic, error) {
	keyhash := name
	if strings.HasPrefix(keyhash, "/memns/") {
		keyhash = keyhash[7:]
	}
	topicName := fmt.Sprintf("/membuss/memns/1.0.0/%s", keyhash)

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if t, exists := pm.topics[topicName]; exists {
		return t, nil
	}

	t, err := pm.ps.Join(topicName)
	if err != nil {
		return nil, fmt.Errorf("failed to join GossipSub topic %s: %w", topicName, err)
	}
	pm.topics[topicName] = t
	return t, nil
}

// PublishPub publishes a serialized MemNSRecord to the appropriate GossipSub topic.
func (pm *PubSubManager) PublishPub(ctx context.Context, key *keyring.Key, record *membusspb.MemNSRecord) error {
	t, err := pm.GetTopic(key.MemNSName)
	if err != nil {
		return err
	}

	data, err := proto.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal record: %w", err)
	}

	if err := t.Publish(ctx, data); err != nil {
		return fmt.Errorf("failed to publish to GossipSub: %w", err)
	}

	return nil
}

// SubscribePub subscribes to the GossipSub topic for a name and sends valid incoming records to ch.
func (pm *PubSubManager) SubscribePub(ctx context.Context, name string, ch chan<- *membusspb.MemNSRecord) error {
	t, err := pm.GetTopic(name)
	if err != nil {
		return err
	}

	sub, err := t.Subscribe()
	if err != nil {
		return fmt.Errorf("failed to subscribe to GossipSub topic: %w", err)
	}

	go func() {
		defer sub.Cancel()
		var lastSeq uint64

		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return // subscription cancelled or context done
			}

			record := &membusspb.MemNSRecord{}
			if err := proto.Unmarshal(msg.Data, record); err != nil {
				continue
			}

			if err := VerifyRecord(record); err != nil {
				continue
			}

			if record.Sequence > lastSeq {
				lastSeq = record.Sequence
				select {
				case ch <- record:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return nil
}
