package explorer

import (
	"log/slog"
	"net"
	"sync"

	"github.com/oschwald/maxminddb-golang"
)

// GeoResult holds approximate geolocation for an IP address.
type GeoResult struct {
	Country  string  `json:"country"`
	City     string  `json:"city"`
	Lat      float64 `json:"lat"`
	Lon      float64 `json:"lon"`
}

var defaultGeo = &GeoResult{Country: "Unknown", City: "Unknown"}

// GeoResolver performs IP geolocation using a local MaxMind
// GeoLite2-City database. Results are cached permanently in
// memory because IP addresses do not change location.
type GeoResolver struct {
	db    *maxminddb.Reader
	cache sync.Map // string → *GeoResult
}

// NewGeoResolver opens the MMDB file at the given path.
// Returns nil (and logs a warning) when the file does not
// exist or is invalid; callers treat nil as "geo disabled".
func NewGeoResolver(path string) *GeoResolver {
	if path == "" {
		return nil
	}
	slog.Info("geo: loaded", "path", path)
	db, err := maxminddb.Open(path)
	if err != nil {
		slog.Warn("geo: invalid mmdb", "path", path, "err", err)
		return nil
	}
	return &GeoResolver{db: db}
}

// Lookup returns the approximate location for ip. Cached
// permanently after the first call. Never returns an error;
// unknown IPs get GeoResult with "Unknown" placeholders.
func (g *GeoResolver) Lookup(ip string) *GeoResult {
	if g == nil || g.db == nil {
		return defaultGeo
	}
	if v, ok := g.cache.Load(ip); ok {
		return v.(*GeoResult)
	}
	r := g.lookupDB(ip)
	g.cache.Store(ip, r)
	return r
}

func (g *GeoResolver) lookupDB(ip string) *GeoResult {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return defaultGeo
	}
	var record struct {
		Country struct {
			Names map[string]string `maxminddb:"names"`
		} `maxminddb:"country"`
		City struct {
			Names map[string]string `maxminddb:"names"`
		} `maxminddb:"city"`
		Location struct {
			Latitude  float64 `maxminddb:"latitude"`
			Longitude float64 `maxminddb:"longitude"`
		} `maxminddb:"location"`
	}
	if err := g.db.Lookup(parsed, &record); err != nil {
		return defaultGeo
	}
	r := &GeoResult{
		Country: record.Country.Names["en"],
		City:    record.City.Names["en"],
		Lat:     record.Location.Latitude,
		Lon:     record.Location.Longitude,
	}
	if r.Country == "" {
		r.Country = "Unknown"
	}
	if r.City == "" {
		r.City = "Unknown"
	}
	return r
}

// Close releases the database reader.
func (g *GeoResolver) Close() {
	if g != nil && g.db != nil {
		g.db.Close()
	}
}
