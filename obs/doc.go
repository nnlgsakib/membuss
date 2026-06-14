// Package obs collects the Phase 10 observability primitives:
// Prometheus metrics (obs/metrics) and structured logging
// (obs/logging). Both are designed to be no-op-safe: a nil or
// default value is a valid call target.
package obs
