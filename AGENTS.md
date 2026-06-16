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

## graphify

This project has a knowledge graph at graphify-out/ with god nodes, community structure, and cross-file relationships.

When the user types `/graphify`, invoke the `skill` tool with `skill: "graphify"` before doing anything else.

Rules:
- For codebase questions, first run `graphify query "<question>"` when graphify-out/graph.json exists. Use `graphify path "<A>" "<B>"` for relationships and `graphify explain "<concept>"` for focused concepts. These return a scoped subgraph, usually much smaller than GRAPH_REPORT.md or raw grep output.
- Dirty graphify-out/ files are expected after hooks or incremental updates; dirty graph files are not a reason to skip graphify. Only skip graphify if the task is about stale or incorrect graph output, or the user explicitly says not to use it.
- If graphify-out/wiki/index.md exists, use it for broad navigation instead of raw source browsing.
- Read graphify-out/GRAPH_REPORT.md only for broad architecture review or when query/path/explain do not surface enough context.
- After modifying code, run `graphify update .` to keep the graph current (AST-only, no API cost).
