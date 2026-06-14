// gRPC server setup for the Membuss daemon. Kept in a small
// dedicated file so test code can swap in a different
// constructor.
package main

import "google.golang.org/grpc"

// serverGRPCServer is an alias for *grpc.Server kept in a
// dedicated file so test code (and any future instrumentation
// such as tracing interceptors) has a single construction
// point.
type serverGRPCServer = grpc.Server

// newGRPCServer returns a fresh grpc.Server with the daemon's
// default options.
func newGRPCServer() *grpc.Server {
	return grpc.NewServer()
}
