// Package descriptor implements BitTorrent-style .mbuss
// descriptor files. A descriptor captures everything needed
// to find and download a MID DAG from the network without
// prior knowledge of the content: all block hashes, erasure
// coding parameters, bootstrap peer lists, and optional
// creator signatures.
//
// File format (little-endian):
//
//	Magic   [4]byte  "MEMB"
//	Version uint8    1
//	Payload []byte   protobuf-encoded DescriptorPayload
//	Check   [32]byte SHA-256 of Payload
package descriptor
