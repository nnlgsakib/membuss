# Membuss

> Decentralized, content-addressed distributed storage and delivery
> network written in Go.

Membuss is a peer-to-peer system where files are chunked, hashed,
structured as Merkle DAGs, erasure-coded, and sharded across peers
using consistent hashing. Content is addressed by a self-describing
**MID** (Mem ID) – a multihash-based identifier with the
`mem` codec prefix (e.g. `mem1z4a2bd9f...`). MIDs are content-derived,
so the same bytes always hash to the same address anywhere on the
network.

## What makes Membuss different

- **`mem` prefix.** MIDs are unambiguously Membuss addresses. There
  is no collision with Git hashes or any other multihash codec.
- **Anchor Nodes.** A first-class "full mirror" mode that ensures content persists even when original providers go offline.
  Anchor nodes ask connected peers for their sealed MIDs via a direct
  libp2p content-exchange protocol, fetch any content they don't have,
  and seal it locally.
- **Erasure coding on by default.** Every block is Reed-Solomon
  encoded (`10 data + 4 parity`) before it leaves a node, so any 4
  shards can be lost without losing the original.
- **Memex block exchange.** A custom, focused want/have/block
  protocol built on libp2p streams — focused on efficient block
  transfer with explicit retry, batch wants, and a WantManager that lets
  multiple in-process fetchers share incoming blocks.
- **Mem-PEX gossip.** A dedicated peer-exchange protocol so nodes
  discover each other even when bootstrap peers are unreachable.
- **DNSLink Integration.** Resolve custom domains dynamically to Membuss addresses using standard DNS TXT record lookups (`dnslink=/memns/k51...` or `dnslink=/mem/...`), making it a true replacement for hosting mutable IPFS-style web projects.
- **Dynamic Range Requests.** Fast, lazy HTTP `Range` requests resolve and stream specific Merkle DAG child blocks containing only the requested offset range on-demand, enabling high-performance audio/video streaming.
- **BadgerDB Value Log GC.** Periodic background garbage collection automatically cleans fragmented value logs inside the local store, recovering disk space from deleted/overwritten blocks.
- **Built-in explorer UI.** Every Membuss node serves a web UI at
  `/explorer/` for browsing MIDs, walking DAGs, inspecting peers,
  and seeing what anchor nodes are doing.

## Architecture Overview

```
                                Membuss node
                              +----------------------------+
                              |                            |
+-------------------+         |    gRPC  +---------+       |
| User (terminal)   |  ---->  |  <---->  |  Daemon |       |
| membuss-cli       |         |          +---------+       |
+-------------------+         |              |            |
                              |   +----------+-----------+|
                              |   v          v           v|
                              | Mem-Store  Memex Engine  Mem-DHT  Mem-PEX  Mem-Herald  Anchor Engine
                              | (Badger)  /membuss/      /membuss/ /membuss/ /membuss/   /membuss/
                              |           memex/1.0.0     dht/1.0.0 pex/1.0.0 herald     anchors/v1
                              |    |        |            |        |         |          |
                              |    +--------+------------+--------+---------+----------+
                              |             |            |        |         |
                              |             v            v        v         v
                              |         libp2p host (TCP + QUIC, Noise, yamux)
                              |             |
                              +-------------|----------------+
                                            |
                                       Network peers
                                            |
+-------------------+                     v
| Browser           |  ---->   Mem-Gate  :8080  (HTTP, Range, ETag, /explorer/)
| /explorer/        |
| /mem/{mid}        |
+-------------------+
```

Two independent HTTP surfaces run side by side from the same daemon:

- **Mem-Gate** (`gateway_addr`, default `127.0.0.1:8080`) —
  public, read-only CDN. Serves `/mem/{MID}`, `/explorer/...`, range
  requests, DAG JSON.
- **Node API** (`api_addr`, default `127.0.0.1:5001`) —
  local control plane. Add/seal/stat, `/api/v1/...`, `/metrics`.
  Optionally protected by `X-Membuss-Key`.

## Core Concepts

### MID (Mem ID)

A `mem`-prefixed multihash. Construction:
`raw bytes -> SHA-256 -> multihash wrap -> multibase encode -> prepend "mem"`.
Example: `mem1z4a2bd9f...`. MIDs are immutable, self-describing, and
content-derived. The Codec field distinguishes leaves (`raw`) from
internal nodes (`dag-pb`).

### Merkle DAG

A directed acyclic graph of content-addressed nodes. Leaf nodes hold
raw block bytes; internal nodes hold only links (children MIDs) plus
the metadata needed to navigate them. The root node's MID is the
identifier you hand to other nodes. Any identical content produces
identical structure, so deduplication is automatic.

### Erasure Coding

Every block is split into `k=10` data shards and `m=4` parity shards
using Reed-Solomon (`klauspost/reedsolomon`). Content survives the
loss of any 4 of the 14 shards. The shards themselves are content-
addressed; the original MID's ErasureManifest is stored alongside
the data so peers know how to reassemble.

### Anchor Nodes

A first-class "full mirror" role. When a node has `anchor_mode: true`
it subscribes to the DHT, fetches every newly-announced piece via
Memex, seals its local copy, and publishes itself to the DHT under
`/membuss/anchors/v1` so other nodes prefer it as a fallback
provider. Anchor nodes are Membuss's answer to 
persistence problem.

### Memex (Block Exchange)

A custom, focused want/have/block protocol on libp2p streams
(`/membuss/memex/1.0.0`). A requester opens a stream to a provider,
sends a `WantList`, and reads blocks back. A requester pulls from
multiple providers in parallel, walks the DAG as children are
discovered, and assembles the reassembled content into an
`io.Reader` when complete. Includes exponential-backoff retry and
per-session timeouts.

### Mem-DHT (Kademlia)

A wrapped `go-libp2p-kad-dht` instance, scoped to the protocol
prefix `/membuss/dht/1.0.0`. Used for three things: (1) provider
announcements ("I have this MID"), (2) provider discovery ("who
has this MID?"), and (3) small value records under a permissive
`membuss` validator (anchor registry, etc.).

### Mem-PEX (Peer Exchange)

A dedicated gossip protocol at `/membuss/pex/1.0.0`. Every 30 s
each node picks 5 random connected peers, exchanges its local peer
table, and merges new entries. New peers with advertised
multiaddrs are dialed asynchronously so the network keeps growing
even when bootstrap is offline.

### Mem-Herald (Reprovider)

A background loop that re-announces the node's sealed MIDs to the
DHT on a configurable interval (default 12 h). Three strategies:
`roots` (only sealed root MIDs, the cheap default), `all` (every
block, used by anchor nodes), and `shards` (only erasure shards
the node is responsible for).

## Getting Started

### Prerequisites

- **Go 1.22+** (the module declares `go 1.25.10`; any 1.22+ works)
- A reachable UDP+TCP port for libp2p (default `4001` for both)
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` only if you
  intend to regenerate the protobuf bindings (not required to build
  or run)

### Build

```bash
git clone https://github.com/nnlgsakib/membuss
cd membuss
make build
# -> bin/membuss and bin/membuss-cli
```

Or directly:

```bash
go build -o bin/membuss     ./cmd/membuss
go build -o bin/membuss-cli ./cmd/membuss-cli
```

### Run a node

The daemon reads `membuss.yaml` from the current directory by
default. A starter config is at `membuss.yaml`; otherwise generate
one with `bin/membuss --help` and a hand-written YAML, or just use
the defaults and pass `--in-memory` for a one-shot local node.

```bash
# in-memory node with sensible defaults
./bin/membuss --in-memory --build dev

# banner shows the bound addresses:
#   grpc_addr:      127.0.0.1:50051
#   gateway_addr:   127.0.0.1:<port>
#   api_addr:       127.0.0.1:<port>
```

### Add a file

```bash
./bin/membuss-cli add README.md
# -> mem1z4a2bd9fzsg7n6n8y9z4m9z9y8z7x6w5v4u3t2s1r0q9p8o7n6m5
```

`add` reads the file, chunks it, builds the Merkle DAG, stores every
block in BadgerDB, seals the root MID, and announces it to the DHT.

### Fetch it

```bash
./bin/membuss-cli get mem1z4a2bd9fzsg7n6n8y9z4m9z9y8z7x6w5v4u3t2s1r0q9p8o7n6m5 -o copy.md
diff README.md copy.md   # identical
```

`get` walks the DHT for providers, opens parallel Memex streams,
retrieves the DAG, verifies every block, and writes the reassembled
content to the output file (or stdout if `-o` is omitted).

### Explore in the browser

While the daemon is running, open:

```
http://127.0.0.1:8080/explorer/
```

for the home page (search bar, node stats), then
`http://127.0.0.1:8080/explorer/mid/<MID>` for the MID detail
page with provider list, DAG tree, seal/unseal controls, and a
download link to `/mem/{MID}`.

## Configuration Reference

All settings live in a single YAML file passed via `--config`.
Every field is optional; defaults are applied before the YAML is
unmarshalled.

| Key | Type | Default | Description |
|---|---|---|---|
| `listen_addrs` | `[]string` | `/ip4/0.0.0.0/tcp/4001`, `/ip4/0.0.0.0/udp/4001/quic-v1` | libp2p multiaddrs to bind. |
| `bootstrap_peers` | `[]string` | `[]` | libp2p peer IDs (full multiaddr or plain ID) for the DHT bootstrap loop. |
| `data_dir` | `string` | `./data` | BadgerDB directory. Created if missing. |
| `gateway_addr` | `string` | `127.0.0.1:8080` | Mem-Gate HTTP listen address (`:0` for OS-assigned). |
| `api_addr` | `string` | `127.0.0.1:5001` | Node API HTTP listen address. |
| `grpc_addr` | `string` | `127.0.0.1:50051` | gRPC listen address for `membuss-cli`. |
| `anchor_mode` | `bool` | `false` | Enable the Anchor Node full-sync engine. |
| `reprovide_interval` | `duration` | `12h` | Mem-Herald reprovide loop period. |
| `log_level` | `string` | `info` | slog level: `debug`, `info`, `warn`, `error`. |
| `gateway_tls.cert_file` | `string` | _(empty)_ | PEM cert (leaf + chain) for Mem-Gate HTTPS. |
| `gateway_tls.key_file` | `string` | _(empty)_ | PEM key for Mem-Gate HTTPS. |
| `api_tls.cert_file` | `string` | _(empty)_ | PEM cert for Node API HTTPS. |
| `api_tls.key_file` | `string` | _(empty)_ | PEM key for Node API HTTPS. |
| `api_key` | `string` | _(empty)_ | Shared secret required in `X-Membuss-Key` header on `/api/v1/*`. Empty = auth disabled. |
| `gateway_rate_limit_per_min` | `int` | `100` | Per-source-IP request budget for Mem-Gate. `0` disables. |
| `metrics_enabled` | `bool` | `true` | Mount `/metrics` on the Node API. |
| `memex_retry_backoff.initial` | `duration` | `100ms` | First retry delay for Memex sessions. |
| `memex_retry_backoff.max` | `duration` | `30s` | Cap on a single backoff sleep. |
| `memex_retry_backoff.factor` | `float` | `2.0` | Multiplier applied to the previous delay. |
| `memex_retry_backoff.max_attempts` | `int` | `4` | Max retries per retrieval. `0` = unlimited. |
| `bootstrap_backoff.initial` | `duration` | `500ms` | First retry delay for DHT bootstrap. |
| `bootstrap_backoff.max` | `duration` | `60s` | Cap on a single DHT bootstrap backoff sleep. |
| `bootstrap_backoff.factor` | `float` | `2.0` | Multiplier applied to the previous delay. |
| `bootstrap_backoff.max_attempts` | `int` | `0` | Max retries per bootstrap peer. `0` = unlimited. |

## CLI Reference

The CLI is a thin client over gRPC; everything resolves to a single
RPC to the local daemon.

| Command | Args | Description |
|---|---|---|
| `membuss-cli add <file>` | `<file>` | Upload a file. Returns the root MID. |
| `membuss-cli get <MID> [-o file]` | `<MID>`, `-o` | Fetch the content of a MID. `-o` writes to a file; otherwise stdout. |
| `membuss-cli seal <MID>` | `<MID>` | Pin a MID and all reachable blocks. |
| `membuss-cli unseal <MID>` | `<MID>` | Remove the pin. |
| `membuss-cli stat <MID>` | `<MID>` | Show size, block count, and seal status. |
| `membuss-cli peers` | _(none)_ | List peers known to the local PEX table. |
| `membuss-cli dht peek <MID>` | `<MID>` | Ask the local DHT who provides a MID. |
| `membuss-cli gc` | _(none)_ | Run garbage collection on the local store. |
| `membuss-cli anchor status` | _(none)_ | Show the local Anchor Node engine stats. |
| `membuss-cli ping [message]` | `[message]` | Connectivity probe; echoes the message + daemon build. |
| `membuss-cli daemon start` | _(none)_ | Run the daemon in the foreground (delegates to `cmd/membuss`). |

Every read command supports `--json` for machine-readable output.

## API Reference

Two HTTP surfaces. Both are mounted by the daemon and use the chi
router with the standard middleware stack (RequestID, RealIP, Logger,
Recoverer, Timeout).

### Mem-Gate — public gateway (`gateway_addr`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/healthz` | Liveness probe. Returns `{"ok":true}`. |
| `GET` | `/mem/{mid}` | Fetch the resolved content. |
| `HEAD` | `/mem/{mid}` | Existence + `Content-Length`. |
| `GET` | `/mem/{mid}?format=raw` | Raw block bytes (no DAG walk). |
| `GET` | `/mem/{mid}?format=dag-json` | The DAG node as JSON: `{mid, size, links}`. |
| `GET` | `/mem/{mid}/{path}` | DAG path traversal. |
| `GET` | `/explorer/` | Explorer home. |
| `GET` | `/explorer/mid/{mid}` | MID detail page. |
| `GET` | `/explorer/mid/{mid}/dag` | DAG tree view. |
| `GET` | `/explorer/peers` | Connected peers table. |
| `GET` | `/explorer/anchors` | Anchor nodes list. |
| `GET` | `/explorer/node` | Local node info. |
| `POST` | `/explorer/search` | Redirect (303) to `/explorer/mid/{MID}`. |
| `GET` | `/explorer/static/{file}` | CSS/JS. `Cache-Control: max-age=300`. |

Range requests (`Range: bytes=N-M`) return `206 Partial Content`.
`Cache-Control: public, immutable, max-age=31536000` and
`ETag: <MID>`.

### Node API — local control plane (`api_addr`)

| Method | Path | Description |
|---|---|---|
| `GET` | `/metrics` | Prometheus scrape endpoint (no auth). |
| `POST` | `/api/v1/add` | Upload content (raw or multipart). Returns `{"mid", "size", "blocks"}`. |
| `GET` | `/api/v1/get/{mid}` | Fetch the content of a MID. |
| `POST` | `/api/v1/seal/{mid}` | Pin a MID recursively. |
| `DELETE` | `/api/v1/seal/{mid}` | Unpin a MID. |
| `GET` | `/api/v1/stat/{mid}` | Size, block count, sealed flag. |
| `GET` | `/api/v1/peers` | Connected peers (optional `?limit=N`). |
| `GET` | `/api/v1/node/info` | Local peer ID, addrs, version, build. |
| `POST` | `/api/v1/gc` | Run garbage collection. Returns bytes freed. |
| `GET` | `/api/v1/healthz` | Liveness probe. |

All `/api/v1/*` routes require `X-Membuss-Key: <api_key>` when
`api_key` is set in config; empty key = auth disabled. JSON envelope
on every response: `{"ok": true, "data": ...}` or
`{"ok": false, "error": "..."}`.

### gRPC — CLI to Daemon (`grpc_addr`)

The `MembussNode` service exposes `Add`, `Get` (streaming), `Seal`,
`Unseal`, `Stat`, `Peers`, `DHTPeek`, `GC`, `AnchorStatus`, plus a
unary `Ping`. Definitions live in `rpc/proto/membuss.proto`; the
generated Go bindings are in `proto/`.

## Running an Anchor Node

Set `anchor_mode: true` in the daemon's YAML config (or pass
`--no-anchor=false` from the CLI). On boot the engine will:

1. Run Mem-Herald with `strategy=all` so every block the anchor
   holds is announced to the DHT.
2. Subscribe to the DHT and continuously pull newly-announced
   content into the local store via Memex.
3. Publish itself to the DHT under `/membuss/anchors/v1` so other
   nodes prefer it as a fallback provider.

**Recommended hardware for a public anchor:**

- **Storage**: at least 1 TiB free on the data directory; SSDs
  strongly recommended. The engine never deletes content, so disk
  fills monotonically.
- **RAM**: 4 GiB minimum, 16 GiB comfortable (BadgerDB block cache
  + Memex want managers + multiple in-flight fetches).
- **CPU**: 2+ cores. Erasure encoding/decoding and SHA-256 are
  single-threaded per request, but the anchor fans out many
  concurrent retrievals.
- **Bandwidth**: 100 Mbps symmetric minimum. The anchor will pull
  a steady stream of new content as it is announced.

A good first anchor starts small: an inexpensive VPS with 1 TB of
block storage and 1 Gbps of bandwidth, configured with
`anchor_mode: true` and a short `reprovide_interval` (e.g. 1 h).

## Architecture Decisions

### Why BadgerDB?

Pure Go, embedded (no external process), LSM-tree design that
matches Membuss's write-heavy workload (every block is a write).
The Go ecosystem has no other embeddable LSM KV store with
production-quality crash recovery. BoltDB was considered but is
B+-tree, which gives worse write amplification. We deliberately
avoid SQLite/LevelDB/RocksDB to keep the binary statically linked.

### Why Reed-Solomon `10+4`?

`10+4` gives 28.6% storage overhead for the ability to lose any
4 of 14 shards (28.6% of the data) without losing the original.
The trade-off is decoding cost: every retrieval from a partial
state has to do 14-shard RS reconstruction, which is O(n log n)
in the shard size. `10+4` is the sweet spot used by production
erasure-coded systems (Ceph default, MinIO, Backblaze) and
matches the read latency budgets we have measured in
`net/memex/bench_test.go`.

### Why rendezvous hashing?

Rendezvous (a.k.a. highest-random-weight) hashing gives the same
stable mapping as consistent-hash rings, but with O(1) peer
lookup for a given key: score = `hash(peerID + MID)`, take the top
N. No ring to maintain, no virtual nodes to rebalance. The
`core/shard.HashRing` type is a thin wrapper that exposes a
familiar `AddPeer` / `RemovePeer` / `Assign(MID, replicas)`
interface on top of the rendezvous core.

### Why libp2p?

Two reasons. First, it is the only mature, multi-transport,
multi-platform P2P stack in any language, with first-class Go
support. Second, the future plans (mobile clients, browser peers
via WebTransport, private networks) all have known libp2p
incarnations we can adopt without rewriting the protocols.

### Why a separate Gateway and Node API?

They have different threat models. Mem-Gate is **public,
read-only, and content-cacheable**: it must serve range requests,
set `Cache-Control: immutable`, never write to the store. The
Node API is **local-only, mutating, and operator-controlled**: it
must authenticate, never expose upload bandwidth to the internet,
and is allowed to assume a trusted caller. Putting both behind
one router would force us to invent a unified auth model that
satisfies neither. Two surfaces, two policies, two log streams.

## Roadmap

These are the next concrete directions, in rough priority order:

- **Mutable MID pointers.** A signed record published
  to the DHT under a stable key that resolves to a current MID.
  Lets users have a permanent address that points to a mutable
  current root.
- **Distributed version control.** A DAG-of-DAGs structure on top
  of the existing Merkle DAG for history, branching, and merges
  of content-addressed commits. Mostly a library on top of
  `core/dag`; the network layer is already there.
- **Private networks.** A pre-shared-key bootstrap list plus a
  `membuss` namespace validator restricted to known peer IDs.
  Trivially supported by the existing libp2p + DHT plumbing.
- **Mobile client.** A pure-Go client library suitable for
  Android (gomobile) and iOS (gobind) that speaks the existing
  libp2p and Memex protocols. No new server-side work needed
  beyond a smaller-footprint daemon.
- **Content search.** Optional: a local full-text index over
  sealed MIDs, exposed as `/api/v1/search` and
  `/explorer/search`.
- **Streaming / live content.** A Memex extension for block
  ranges fetched as they become available, suitable for
  audio/video.

## Running Manually (Without Docker)

You can build and run both the Membuss daemon and CLI natively on your host machine without containerization.

### 1. Prerequisites
- **Go**: Version 1.21 or higher installed on your system.
- **make**: (Optional) For Unix-like systems, a standard `make` utility.

### 2. Compilation
To compile the binaries directly to the `bin/` directory, run:
```bash
make build
```
This compiles:
- `bin/membuss` (or `bin/membuss.exe` on Windows): The peer daemon.
- `bin/membuss-cli` (or `bin/membuss-cli.exe` on Windows): The control CLI.

*(Alternative compile without `make`)*:
```bash
go build -o bin/membuss ./cmd/membuss
go build -o bin/membuss-cli ./cmd/membuss-cli
```

### 3. Running the Daemon
The daemon runs as a background engine and manages the local BadgerDB blockstore, Kademlia DHT table, Memex block transfer streams, and HTTP gateways.

To start the daemon using the default configuration file:
```bash
# Unix/macOS
./bin/membuss -config membuss.yaml

# Windows (PowerShell)
.\bin\membuss.exe -config membuss.yaml
```

#### Configuration (`membuss.yaml`)
You can configure bind addresses, data directories, and anchor modes in the config file. Key fields include:
- `listen_addrs`: Multiaddresses libp2p binds to (default: port `4001`).
- `data_dir`: Directory for BadgerDB files (default: `./data`).
- `gateway_addr`: Public Mem-Gate HTTP server address (default: `127.0.0.1:8080`).
- `api_addr`: Local Node control API address (default: `127.0.0.1:5001`).
- `grpc_addr`: Local gRPC address CLI uses to communicate (default: `127.0.0.1:50051`).
- `anchor_mode`: When enabled, local node acts as a full-sync anchor engine mirroring announcements (default: `false`).

### 4. Operating via CLI
With the daemon running, use the CLI in another terminal window to manage and seal content:

* **Add File**: Ingest a single file into the local store and obtain its MID.
  ```bash
  ./bin/membuss-cli add <filepath>
  ```
* **Add Directory**: Ingest a folder hierarchy as a UnixFS-equivalent MemFS structure.
  ```bash
  ./bin/membuss-cli add-dir <dirpath>
  ```
* **Pin/Seal Content**: Seal content recursively to prevent Garbage Collection (GC) sweeps.
  ```bash
  ./bin/membuss-cli seal <MID>
  ```
* **Unseal Content**: Remove the recursive seal pin from a MID.
  ```bash
  ./bin/membuss-cli unseal <MID>
  ```
* **Stat Metadata**: Inspect block sizes, codecs, and providers.
  ```bash
  ./bin/membuss-cli stat <MID>
  ```
* **List Peers**: View all discovered network table peers.
  ```bash
  ./bin/membuss-cli peers
  ```

### 5. Accessing Web Interfaces
- **Web Explorer & Gateway**: Open `http://localhost:8080/explorer/` in your browser. This built-in dashboard lets you browse sealed MIDs, inspect active DHT providers, watch Merkle DAG structures, and view real-time resolver downloads.
- **Node Local API**: The control endpoint runs at `http://localhost:5001`.

## Docker

A multi-stage `Dockerfile` and a single-service `docker-compose.yml`
ship in the repo root for one-line deploys.

```
make docker-build
make docker-compose-up
make docker-compose-logs
make docker-compose-down
```

Image: distroless `nonroot`, ~25 MB, `VOLUME /var/lib/membuss`,
`HEALTHCHECK` on gRPC. Ports: `4001/tcp+udp` (libp2p), `5001`
(Node API), `8080` (Mem-Gate), `50051` (gRPC).

All `config.yaml` fields can be overridden by environment variables
that the entrypoint renders into a temp config before exec'ing the
daemon:

- `MEMBUSS_LISTEN_ADDRS` (default `/ip4/0.0.0.0/tcp/4001,/ip4/0.0.0.0/udp/4001/quic-v1`)
- `MEMBUSS_GATEWAY_ADDR` (default `0.0.0.0:8080`)
- `MEMBUSS_API_ADDR` (default `0.0.0.0:5001`)
- `MEMBUSS_GRPC_ADDR` (default `0.0.0.0:50051`)
- `MEMBUSS_DATA_DIR` (default `/var/lib/membuss`)
- `MEMBUSS_BOOTSTRAP_PEERS` (default empty)
- `MEMBUSS_LOG_LEVEL` (default `info`)
- `MEMBUSS_ANCHOR_MODE` (default `false`)
- `MEMBUSS_NO_ANCHOR` (default `true`)
- `MEMBUSS_BLOOM_CAPACITY` (default `10000000`)
- `MEMBUSS_BLOOM_FP_RATE` (default `0.001`)

For anything not in the list, mount a custom config over
`/etc/membuss/config.yaml`.

`make docker-push IMAGE=membuss:0.1.0 REGISTRY=ghcr.io/me` tags and
pushes the local image.

## Development

```bash
make build          # build daemon + CLI
make test           # go test ./... -race -count=1
make proto          # regenerate protobuf Go bindings
make lint           # golangci-lint (skipped if not installed)
make run-daemon     # run the daemon with ./membuss.yaml
```

Constitution: `.specify/memory/constitution.md`.
Detailed design: `docs/architecture.md`.
Generated protobuf bindings: `proto/`.
