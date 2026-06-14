You are building "Membuss" — a decentralized, content-addressed
distributed storage and delivery network written in Go.

Core design goals:
- Content is addressed by MID (Mem ID): a multihash-based identifier
  with "mem" prefix, e.g. mem1z4a2...
- Files are chunked, hashed, and structured as Merkle DAGs
- Erasure coding (Reed-Solomon) is applied before distribution
  so content survives partial node failure
- Content is sharded across the network using consistent hashing
- Peers discover each other via Mem-DHT (Kademlia) + PEX
  (Peer Exchange gossip protocol)
- Data is transferred via Memex — a custom block exchange protocol
  over libp2p streams
- Anchor Nodes optionally sync the full network content so that
  content remains available even when original providers go offline
- Storage backend is BadgerDB
- All internal structs are serialized with Protocol Buffers
- CLI communicates with the local daemon via gRPC
- Two separate HTTP APIs exist:
    1. Mem-Gate: public gateway + CDN layer (serves content by MID
       over HTTP, handles caching and range requests)
    2. Node API: local control API for adding, sealing, querying
- A built-in web explorer runs on the gateway for browsing MIDs,
  DAG structure, peer info, and network stats
- The system is designed for future extensions: distributed version
  control, IPNS-style mutable pointers, and more

Stack:
- Language: Go
- Networking: libp2p (TCP + QUIC transports)
- Storage: BadgerDB
- Serialization: Protocol Buffers
- CLI ↔ Daemon: gRPC
- Gateway: net/http with chi router
- Erasure coding: klauspost/reedsolomon

<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan
<!-- SPECKIT END -->
