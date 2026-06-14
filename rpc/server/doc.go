// Package server implements the MembussNode gRPC service. The
// daemon hosts it on the address configured in config.GRPCAddr
// and the CLI dials it for every operator command.
//
// The server is decoupled from concrete subsystems (Mem-Store,
// Memex, DHT, etc.) through the Backend interface. Tests inject
// an in-memory Backend; production wires up the real one in
// cmd/membuss.
package server
