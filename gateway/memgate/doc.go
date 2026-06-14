// Package memgate implements the public Mem-Gate HTTP
// gateway. It serves Membuss content over plain HTTP, acting
// as a CDN edge that fronts the local Mem-Store and the
// network's Memex protocol.
//
// Mem-Gate is intentionally read-oriented: any node can run
// it, and it does not require a private key. The operator
// enables it by binding it to config.GatewayAddr.
//
// The package exposes a single type, MemGate, that bundles
// the HTTP router, the LRU content cache, and the Backend
// interface the handlers dispatch into. The daemon supplies
// a production Backend; tests supply a memBackend.
package memgate
