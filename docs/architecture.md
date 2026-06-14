# Architecture

Membuss is a decentralized, content-addressed distributed storage and
delivery network. This document is the canonical, code-agnostic
overview; component details live in the package doc comments.

## High-level model

1. A client hands a file to the daemon.
2. The chunker splits the file into fixed-size blocks.
3. Each block is hashed with a multihash and prefixed with the "mem"
   codec, producing a MID (Mem ID).
4. The DAG builder emits a Merkle DAG whose leaves are MIDs and whose
   internal nodes are UnixFS-style (or unixfs-equivalent) link tables.
5. The erasure coder expands the leaf set into data + parity shards so
   the content survives partial node failure.
6. The sharder uses consistent hashing over the peer set to pick
   replication targets.
7. The libp2p host announces provider records to the DHT (Mem-DHT)
   and gossips peer metadata via PEX.
8. Other nodes fetch blocks on demand over Memex, a custom block
   exchange protocol that runs over libp2p streams.
9. Mem-Herald periodically re-announces local provider records.
10. Anchor Nodes, if enabled, mirror all announced content for
    durability beyond the original providers.

## Surfaces

- **CLI** (`cmd/membuss-cli`) — thin client; talks to the daemon over
  gRPC.
- **Daemon** (`cmd/membuss`) — long-running process that owns the
  libp2p host, DHT, store, and gateway.
- **Mem-Gate** (`gateway/memgate`) — public HTTP gateway / CDN
  serving content by MID.
- **Node API** (`api`) — local HTTP API for add / seal / query.
- **Web Explorer** (`gateway/explorer`) — static-asset UI served by
  Mem-Gate for browsing MIDs, DAGs, peers, and network stats.

## Storage

- **Mem-Store** (`core/store`) — BadgerDB-backed blockstore keyed by MID.
- All internal structs on disk and over the wire are Protocol Buffers.

## Configuration

The daemon is configured by a single YAML file (`membuss.yaml` by
default). See `config/config.go` for the schema; a starter file is in
the repository root.

## Repository layout

See the top-level README for the canonical folder map.
