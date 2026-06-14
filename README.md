# Membuss

> Decentralized, content-addressed distributed storage and delivery
> network. Files are chunked, hashed, structured as Merkle DAGs,
> erasure-coded, and sharded across peers using consistent hashing.
> Content is addressed by **MID** (Mem ID), a multihash-based
> identifier with the `mem` codec prefix (e.g. `mem1z4a2...`).

## Status

**Phase 0 — Project Initialization & Architecture Skeleton.** The
repository contains the canonical folder layout, the `config` package,
stub `doc.go` files for every subsystem, a starter `membuss.yaml`,
the Makefile, codegen scripts, and the base `.proto` definition. No
subsystem logic is implemented yet.

See `docs/architecture.md` for the high-level design and
`.specify/memory/constitution.md` for the binding engineering
principles (code quality, testing, UX, performance).

## Requirements

- Go 1.22 or newer
- (Optional) `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` for
  regenerating protobuf bindings
- (Optional) `golangci-lint` for `make lint`

## Quickstart

```bash
# 1. Build the daemon and CLI
make build

# 2. (Optional) Edit the starter config
$EDITOR membuss.yaml

# 3. Run the daemon
make run-daemon
```

On Windows, use the PowerShell equivalents:

```powershell
make build
make run-daemon
```

## CLI

```bash
./bin/membuss-cli --config membuss.yaml
```

The CLI is currently a stub that resolves the config path. Real
subcommands (`add`, `cat`, `pin`, `peers`, etc.) land in later
phases.

## Configuration

The daemon reads a single YAML file. All fields are optional; missing
fields fall back to the defaults compiled into `config.Default()`.

| Key                 | Type              | Default                     | Description |
|---------------------|-------------------|-----------------------------|-------------|
| `listen_addrs`      | `[]string`        | TCP 4001 + QUIC 4001        | libp2p multiaddrs the host binds to |
| `bootstrap_peers`   | `[]string`        | `[]`                        | libp2p peer multiaddrs to dial on startup |
| `data_dir`          | `string`          | `./data`                    | BadgerDB + block-store directory |
| `gateway_addr`      | `string`          | `127.0.0.1:8080`            | Mem-Gate HTTP listen address |
| `api_addr`          | `string`          | `127.0.0.1:5001`            | Local Node API listen address |
| `grpc_addr`         | `string`          | `127.0.0.1:50051`           | gRPC listen address for the CLI |
| `anchor_mode`       | `bool`            | `false`                     | Enable Anchor Node full-sync engine |
| `reprovide_interval`| `duration`        | `12h`                       | How often Mem-Herald re-announces provider records |

## Repository layout

```
membuss/
├── cmd/
│   ├── membuss/          # main daemon entry point
│   └── membuss-cli/      # CLI entry point
├── core/
│   ├── mid/              # MID generation, parsing, validation
│   ├── chunk/            # chunking engine
│   ├── dag/              # Merkle DAG builder and resolver
│   ├── store/            # BadgerDB blockstore (Mem-Store)
│   ├── erasure/          # Reed-Solomon erasure coding
│   ├── shard/            # consistent hash sharding logic
├── net/
│   ├── host/             # libp2p host setup
│   ├── dht/              # Mem-DHT (Kademlia wrapper)
│   ├── pex/              # PEX peer exchange gossip
│   ├── memex/            # Memex block exchange protocol
│   ├── herald/           # Mem-Herald reprovider loop
├── anchor/               # Anchor Node full-sync engine
├── gateway/
│   ├── memgate/          # Mem-Gate HTTP gateway + CDN
│   └── explorer/         # built-in web explorer UI
├── api/                  # Node local REST API
├── rpc/
│   ├── proto/            # .proto definitions
│   └── server/           # gRPC server
├── config/               # config loader (YAML)
├── proto/                # generated protobuf Go files
├── scripts/              # build, codegen scripts
├── docs/                 # architecture docs
├── membuss.yaml          # starter config
├── Makefile
├── go.mod
└── README.md
```

## Make targets

| Target          | Description |
|-----------------|-------------|
| `make build`        | Compile the daemon and CLI into `bin/` |
| `make proto`        | Regenerate protobuf Go bindings into `proto/` |
| `make test`         | Run `go test ./... -race -count=1` |
| `make lint`         | Run `golangci-lint` (skipped if not installed) |
| `make run-daemon`   | Run the daemon with `./membuss.yaml` |
| `make tidy`         | `go mod tidy` |
| `make clean`        | Remove `bin/` and generated `*.pb.go` files |

## Protobuf

Wire schemas live in `rpc/proto/`. Generated Go bindings land in
`proto/` and are produced by `make proto` (PowerShell or bash script
under `scripts/`).

The base service (`Node.Ping`) is intentionally minimal in Phase 0
and is expanded in later phases.

## Contributing

This project is governed by `.specify/memory/constitution.md`. Read
it before opening a PR. The four non-negotiable principles are:

1. **Code Quality** — `gofmt`, `golangci-lint`, doc comments,
   context-aware APIs, no goroutine leaks.
2. **Testing Standards** — test-first, ≥ 80% package coverage (≥ 95%
   on critical paths), `-race -count=1` in CI, fuzz tests for
   parsers/decoders.
3. **User Experience Consistency** — stable CLI/HTTP/gRPC surfaces,
   normalized errors, copy-paste-runnable docs.
4. **Performance Requirements** — explicit latency/throughput
   budgets, benchmark gates on releases, Prometheus + slog
   observability.

## License

TBD.
