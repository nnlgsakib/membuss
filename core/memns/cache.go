package memns

import (
	"sync"
	"time"

	membusspb "github.com/nnlgsakib/membuss/proto"
)

type cacheEntry struct {
	record    *membusspb.MemNSRecord
	expiresAt time.Time
}

// RecordCache is a thread-safe cache with TTL expiration and capacity eviction.
type RecordCache struct {
	mu      sync.RWMutex
	entries map[string]cacheEntry
	limit   int
}

// NewRecordCache creates a new RecordCache with the specified capacity limit.
func NewRecordCache(limit int) *RecordCache {
	return &RecordCache{
		entries: make(map[string]cacheEntry),
		limit:   limit,
	}
}

// Get retrieves a record from the cache if it exists and has not expired.
func (c *RecordCache) Get(name string) *membusspb.MemNSRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.entries[name]
	if !ok {
		return nil
	}
	if time.Now().After(entry.expiresAt) {
		return nil
	}
	return entry.record
}

// Add inserts or updates a record in the cache, evicting expired/excess entries.
func (c *RecordCache) Add(name string, record *membusspb.MemNSRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	// Evict expired entries first
	for k, entry := range c.entries {
		if now.After(entry.expiresAt) {
			delete(c.entries, k)
		}
	}

	// Evict earliest expiring entry if limit is reached
	if len(c.entries) >= c.limit {
		var earliestKey string
		var earliestTime time.Time
		first := true
		for k, entry := range c.entries {
			if first || entry.expiresAt.Before(earliestTime) {
				earliestKey = k
				earliestTime = entry.expiresAt
				first = false
			}
		}
		if earliestKey != "" {
			delete(c.entries, earliestKey)
		}
	}

	ttl := time.Minute // default 1 min
	if record.Ttl > 0 {
		ttl = time.Duration(record.Ttl)
	}
	c.entries[name] = cacheEntry{
		record:    record,
		expiresAt: now.Add(ttl),
	}
}
