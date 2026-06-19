package memlink

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// MemLinkRecord represents the parsed components of a _memlink DNS TXT record.
type MemLinkRecord struct {
	MemNSName string
	MID       string
	TTL       int
	Routes    map[string]int // label:weight
	Delegate  string
}

// MemLinkCacheEntry stores resolved values for domains.
type MemLinkCacheEntry struct {
	value     string
	expiresAt time.Time
}

// DNSResolver handles resolving domains using DNS TXT records.
type DNSResolver struct {
	lookupTXT    func(host string) ([]string, error)
	resolveMemNS func(ctx context.Context, name string) (string, error)
	cache        map[string]*MemLinkCacheEntry
	cacheMu      sync.RWMutex
}

// NewDNSResolver instantiates a new DNSResolver.
func NewDNSResolver(resolveMemNS func(ctx context.Context, name string) (string, error)) *DNSResolver {
	return &DNSResolver{
		lookupTXT:    net.LookupTXT,
		resolveMemNS: resolveMemNS,
		cache:        make(map[string]*MemLinkCacheEntry),
	}
}

// SetLookupTXT overrides the default DNS lookup function (primarily for testing).
func (r *DNSResolver) SetLookupTXT(f func(host string) ([]string, error)) {
	r.lookupTXT = f
}

// ParseTXTRecord parses a _memlink TXT record value.
func ParseTXTRecord(txt string) (*MemLinkRecord, error) {
	record := &MemLinkRecord{
		Routes: make(map[string]int),
	}

	clean := strings.ReplaceAll(txt, ";", " ")
	tokens := strings.Fields(clean)

	found := false
	for _, token := range tokens {
		parts := strings.SplitN(token, "=", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(parts[0])
		v := strings.TrimSpace(parts[1])

		switch k {
		case "memns":
			record.MemNSName = v
			found = true
		case "mem":
			record.MID = v
			found = true
		case "memns-ttl":
			var ttl int
			if _, err := fmt.Sscan(v, &ttl); err == nil {
				record.TTL = ttl
			}
		case "memns-routes":
			routeParts := strings.Split(v, ",")
			for _, rp := range routeParts {
				pair := strings.SplitN(rp, ":", 2)
				if len(pair) == 2 {
					label := strings.TrimSpace(pair[0])
					var weight int
					if _, err := fmt.Sscan(pair[1], &weight); err == nil {
						record.Routes[label] = weight
					}
				}
			}
		case "memns-delegate":
			record.Delegate = v
		}
	}

	if !found {
		return nil, errors.New("no mem or memns key found in TXT record")
	}

	return record, nil
}

// LookupTXTRecord retrieves the raw TXT record for a domain.
func (r *DNSResolver) LookupTXTRecord(domain string) (string, error) {
	txts, err := r.lookupTXT("_memlink." + domain)
	if err != nil {
		return "", err
	}
	for _, txt := range txts {
		_, err := ParseTXTRecord(txt)
		if err == nil {
			return txt, nil
		}
	}
	return "", errors.New("no valid memlink txt record found")
}

// Resolve resolves a domain via _memlink TXT records.
func (r *DNSResolver) Resolve(ctx context.Context, domain string) (string, error) {
	r.cacheMu.RLock()
	entry, exists := r.cache[domain]
	r.cacheMu.RUnlock()

	if exists && time.Now().Before(entry.expiresAt) {
		return entry.value, nil
	}

	txts, err := r.lookupTXT("_memlink." + domain)
	if err != nil {
		return "", fmt.Errorf("dns lookup failed for _memlink.%s: %w", domain, err)
	}

	var parsedRecord *MemLinkRecord
	for _, txt := range txts {
		rec, err := ParseTXTRecord(txt)
		if err == nil {
			parsedRecord = rec
			break
		}
	}

	if parsedRecord == nil {
		return "", fmt.Errorf("no valid mem/memns TXT record found for %s", domain)
	}

	var resolved string
	if parsedRecord.MID != "" {
		resolved = parsedRecord.MID
	} else if parsedRecord.MemNSName != "" {
		resolved, err = r.resolveMemNS(ctx, parsedRecord.MemNSName)
		if err != nil {
			if parsedRecord.Delegate != "" {
				resolved, err = r.resolveMemNS(ctx, parsedRecord.Delegate)
			}
			if err != nil {
				return "", fmt.Errorf("failed to resolve memns name: %w", err)
			}
		}
	} else {
		return "", fmt.Errorf("empty TXT record contents for %s", domain)
	}

	ttl := 300 // default 300s
	if parsedRecord.TTL > 0 {
		ttl = parsedRecord.TTL
	}

	r.cacheMu.Lock()
	r.cache[domain] = &MemLinkCacheEntry{
		value:     resolved,
		expiresAt: time.Now().Add(time.Duration(ttl) * time.Second),
	}
	r.cacheMu.Unlock()

	return resolved, nil
}
