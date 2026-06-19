package memns

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/nnlgsakib/membuss/net/dht"
	membusspb "github.com/nnlgsakib/membuss/proto"
)

type contextKey string

// ConditionsKey is the context key for passing route selection conditions (e.g. country, user-agent)
const ConditionsKey contextKey = "memns-conditions"

// DNSResolverAPI defines the interface for the DNS resolver to avoid circular dependency.
type DNSResolverAPI interface {
	Resolve(ctx context.Context, domain string) (string, error)
}

// Resolver coordinates resolving MemNS names, domains, and paths.
type Resolver struct {
	dhtClient   *dht.MemDHT
	pubsub      *PubSubManager
	cache       *RecordCache
	dns         DNSResolverAPI
	pubsubCache map[string]*membusspb.MemNSRecord
	pubsubMu    sync.RWMutex
}

// NewResolver instantiates a new Resolver.
func NewResolver(dhtClient *dht.MemDHT, pm *PubSubManager, cache *RecordCache) *Resolver {
	return &Resolver{
		dhtClient:   dhtClient,
		pubsub:      pm,
		cache:       cache,
		pubsubCache: make(map[string]*membusspb.MemNSRecord),
	}
}

// SetDNSResolver sets the DNS resolver instance.
func (r *Resolver) SetDNSResolver(dns DNSResolverAPI) {
	r.dns = dns
}

// DHTClient returns the underlying DHT client.
func (r *Resolver) DHTClient() *dht.MemDHT {
	return r.dhtClient
}

// PubSub returns the underlying PubSub manager.
func (r *Resolver) PubSub() *PubSubManager {
	return r.pubsub
}

// DNS returns the underlying DNS resolver.
func (r *Resolver) DNS() DNSResolverAPI {
	return r.dns
}

// SelectRoute chooses a route target from record.Routes based on weights and conditions,
// falling back to defaultValue if none match or routes list is empty.
func SelectRoute(ctx context.Context, routes []*membusspb.MemRoute, defaultValue string) string {
	if len(routes) == 0 {
		return defaultValue
	}

	var activeRoutes []*membusspb.MemRoute
	conditions, hasConditions := ctx.Value(ConditionsKey).(map[string]string)

	for _, r := range routes {
		if len(r.Conditions) > 0 {
			if !hasConditions {
				continue
			}
			match := true
			for k, v := range r.Conditions {
				if val, ok := conditions[k]; !ok || val != v {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}
		activeRoutes = append(activeRoutes, r)
	}

	if len(activeRoutes) == 0 {
		return defaultValue
	}

	var totalWeight uint32
	for _, r := range activeRoutes {
		totalWeight += r.Weight
	}

	if totalWeight == 0 {
		// Pick uniformly
		idx := rand.Intn(len(activeRoutes))
		return string(activeRoutes[idx].Target)
	}

	w := uint32(rand.Intn(int(totalWeight)))
	var cumulative uint32
	for _, r := range activeRoutes {
		cumulative += r.Weight
		if w < cumulative {
			return string(r.Target)
		}
	}

	return string(activeRoutes[len(activeRoutes)-1].Target)
}

// Resolve resolves a name or domain recursively to its ultimate target path/MID.
func (r *Resolver) Resolve(ctx context.Context, nameOrDomain string) (string, error) {
	return r.resolveDepth(ctx, nameOrDomain, 0)
}

func (r *Resolver) resolveDepth(ctx context.Context, nameOrDomain string, depth int) (string, error) {
	if depth >= 10 {
		return "", errors.New("memns: loop detected, max resolution depth reached")
	}

	// If it's already a direct MID path, IPFS path, or HTTPS URL, return as-is
	if strings.HasPrefix(nameOrDomain, "/mem/") {
		return nameOrDomain, nil
	}
	if strings.HasPrefix(nameOrDomain, "/ipfs/") {
		return nameOrDomain, nil
	}
	if strings.HasPrefix(nameOrDomain, "https://") {
		return nameOrDomain, nil
	}

	// If it's a raw MID, return it as a direct MID path
	if strings.HasPrefix(nameOrDomain, "mem") && !strings.Contains(nameOrDomain, "/") && !strings.Contains(nameOrDomain, ".") {
		return "/mem/" + nameOrDomain, nil
	}

	// If it looks like a domain, resolve via MemLink
	if !strings.HasPrefix(nameOrDomain, "/memns/") && !strings.HasPrefix(nameOrDomain, "k51") && strings.Contains(nameOrDomain, ".") {
		if r.dns == nil {
			return "", errors.New("memns: dns resolver not configured")
		}
		val, err := r.dns.Resolve(ctx, nameOrDomain)
		if err != nil {
			return "", err
		}
		return r.resolveDepth(ctx, val, depth+1)
	}

	// Parse MemNS name
	name := nameOrDomain
	if strings.HasPrefix(name, "/memns/") {
		name = name[7:]
	}

	// 1. Try local LRU cache
	rec := r.cache.Get(name)
	if rec != nil {
		resolvedVal := SelectRoute(ctx, rec.Routes, string(rec.Value))
		return r.resolveDepth(ctx, resolvedVal, depth+1)
	}

	// 2. Try PubSub cache
	r.pubsubMu.RLock()
	rec = r.pubsubCache[name]
	r.pubsubMu.RUnlock()

	if rec != nil && rec.Validity > time.Now().UnixNano() {
		resolvedVal := SelectRoute(ctx, rec.Routes, string(rec.Value))
		return r.resolveDepth(ctx, resolvedVal, depth+1)
	}

	// 3. Fallback to DHT
	rec, err := ResolveDHT(ctx, r.dhtClient, name)
	if err != nil {
		return "", err
	}

	// Cache the resolved record
	r.cache.Add(name, rec)

	// Subscribe to pubsub for real-time updates
	if r.pubsub != nil {
		r.subscribeToName(name)
	}

	resolvedVal := SelectRoute(ctx, rec.Routes, string(rec.Value))
	return r.resolveDepth(ctx, resolvedVal, depth+1)
}

func (r *Resolver) subscribeToName(name string) {
	r.pubsubMu.Lock()
	defer r.pubsubMu.Unlock()

	if _, ok := r.pubsubCache[name]; ok {
		return // already subscribed
	}

	r.pubsubCache[name] = nil
	ch := make(chan *membusspb.MemNSRecord, 10)
	subCtx, cancel := context.WithCancel(context.Background())
	_ = cancel

	err := r.pubsub.SubscribePub(subCtx, name, ch)
	if err != nil {
		delete(r.pubsubCache, name)
		return
	}

	go func() {
		for {
			select {
			case rec, ok := <-ch:
				if !ok {
					return
				}
				r.pubsubMu.Lock()
				current := r.pubsubCache[name]
				if current == nil || rec.Sequence > current.Sequence {
					r.pubsubCache[name] = rec
					r.cache.Add(name, rec)
				}
				r.pubsubMu.Unlock()
			case <-subCtx.Done():
				return
			}
		}
	}()
}
