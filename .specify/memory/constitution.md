<!--
Sync Impact Report
- Version change: 0.0.0 (template) -> 1.0.0
- Modified principles: none (initial ratification)
- Added sections: Core Principles, Technology Constraints, Development Workflow, Governance
- Removed sections: none
- Templates requiring updates: plan-template.md (Constitution Check), spec-template.md (performance/quality sections), tasks-template.md (test-first and perf-task categorization) - propagated below
- Deferred items: none
-->

# Membuss Constitution

## Core Principles

### I. Code Quality (NON-NEGOTIABLE)

All Go code in Membuss MUST be production-grade: idiomatic, readable, and
reviewable without translation. Specifically:

- Code MUST follow the official Go style guide and pass `gofmt`, `go vet`,
  and `golangci-lint` with the project's enforced linters enabled
  (`govet`, `staticcheck`, `revive`, `gocritic`, `errcheck`, `gosec`,
  `bodyclose`).
- Every exported identifier MUST carry a Go doc comment that begins with
  the identifier name and explains its purpose, ownership, and any
  concurrency or lifecycle guarantees.
- Errors MUST be wrapped with `%w` and a small amount of context
  (`fmt.Errorf("op: %w", err)`); sentinel errors MUST be defined as
  package-level `var ErrX = errors.New(...)` and matched with
  `errors.Is` / `errors.As`.
- Public APIs MUST be context-aware: any blocking call (I/O, libp2p
  stream, BadgerDB transaction, gRPC handler, HTTP handler) MUST accept
  a `context.Context` as its first parameter and honor cancellation.
- Concurrency primitives (goroutines, channels, `sync` types) MUST be
  documented for ownership, lifetime, and shutdown signal. Goroutine
  leaks are treated as bugs.
- Configuration MUST be 12-factor: read from env, files, or flags, never
  hard-coded. Sensible defaults MUST be safe-by-default.
- Dependencies MUST be vendored or pinned with `go mod tidy` reproducible
  builds; new third-party dependencies require a written justification in
  the PR description.

**Rationale**: Membuss is a long-lived distributed system. Code that
cannot be read, linted, and reasoned about in isolation is a liability
for every future change to the protocol, storage layer, or network stack.

### II. Testing Standards (NON-NEGOTIABLE)

Testing is a first-class engineering activity. Red-Green-Refactor is
strictly enforced for every non-trivial change.

- **Test-First Discipline**: For any new feature, bug fix, or behavior
  change, tests MUST be authored and committed *before* the
  implementation. A PR that lands implementation without corresponding
  tests is rejected. This is not a guideline — it is a hard gate.
- **Coverage**: Unit test coverage MUST be >= 80% for new or modified
  packages. Critical-path packages (chunking, erasure coding, Merkle
  DAG, Mem-DHT routing, Memex block exchange, MID address
  parsing/verification) MUST have >= 95% coverage and MUST include
  fuzz tests for all parsers, decoders, and protocol message handlers.
- **Test Tiers** — every PR MUST include the appropriate tier:
  - *Unit*: pure logic (MID encoding, DAG traversal, RS encode/decode,
    consistent-hash placement, protobuf marshaling) — fast, hermetic.
  - *Contract*: protobuf/gRPC service contracts, Memex stream protocol
    messages, HTTP/gateway API contracts — verify the wire format
    matches the spec.
  - *Integration*: multi-process or multi-node scenarios (DHT
    discovery, PEX gossip, chunk replication, range requests through
    Mem-Gate, sealing/anchoring) — run in CI on a dedicated job.
  - *E2E*: full lifecycle (add -> chunk -> distribute -> fetch via
    gateway) using ephemeral testnets — run nightly and on release
    branches.
- **Determinism**: Tests MUST be deterministic. No reliance on wall
  clock, real network, or shared global state. Use injected clocks,
  libp2p mock hosts, and per-test BadgerDB temp directories.
- **Property-based / Fuzzing**: Encoders, decoders, Merkle DAG builders,
  and RS streams MUST be exercised with property-based or fuzz tests
  using `testing.F`.
- **CI Gate**: `go test ./... -race -count=1` MUST pass on every PR,
  along with `go test -fuzz=...` for the fuzz targets on a schedule.

**Rationale**: A decentralized storage system is judged by the
correctness of its invariants — content addressing, DAG soundness,
erasure coding recovery, and protocol compliance. A single off-by-one
in a chunker or a single non-deterministic Merkle build silently
corrupts user data. There is no path to correctness without tests
that run before the code that could break them.

### III. User Experience Consistency

The user-facing surface (CLI, Mem-Gate HTTP, Node API, web explorer)
MUST feel like a single, coherent product.

- **CLI**: `membuss` (and its subcommands) MUST follow the
  `verb-noun` convention, support `--json` output for every read
  command, and produce human-friendly colored output by default with
  `NO_COLOR` respected. Errors MUST be actionable — they MUST include
  a short cause and, when possible, a hint to the next step.
- **Mem-Gate HTTP**: Public URLs MUST be stable and predictable
  (`/ipfs/{mid}`, `/api/v0/...`, `/dag/{mid}`, `/explorer/...`).
  Responses MUST advertise capabilities through standard headers
  (`Accept-Ranges: bytes`, `ETag`, `Cache-Control`, `Content-Type`
  derived from the chunk's codec or DAG node metadata). Range requests
  MUST work for any DAG node that has a byte representation.
- **Node API**: The local control API MUST be a gRPC service with
  REST/JSON gateway bindings for scripting. It MUST be backwards
  compatible within a major version; breaking changes require
  deprecation in one minor release before removal.
- **Web Explorer**: A single static-asset explorer MUST be served by
  Mem-Gate at `/explorer/`. It MUST render MID details, DAG graphs,
  peer lists, network stats, and local storage usage, all backed by
  the same APIs the CLI uses. No second source of truth.
- **Errors & Status Codes**: Errors across the CLI, gRPC, and HTTP
  surfaces MUST be normalized. A defined `membuss error` vocabulary
  (NotFound, InvalidMID, ChunkUnavailable, QuotaExceeded,
  PermissionDenied, Internal) MUST be mapped consistently to gRPC
  status codes, HTTP status codes, and CLI exit codes.
- **Docs**: Every user-facing change MUST be accompanied by a change
  to the relevant doc under `docs/`. Examples in docs MUST be
  copy-paste-runnable.

**Rationale**: Membuss will be operated by humans and automated agents
in parallel. Inconsistent surfaces turn a small protocol change into
a multi-day migration. Consistency is a forcing function for
interoperability with future tools, SDKs, and the planned IPNS-style
mutable pointers and DVCS layers.

### IV. Performance Requirements

Performance is a feature. Specific, measurable budgets MUST hold at
every release; regressions block the release.

- **Chunking & Hashing**: A single local add of a 1 GiB file MUST
  complete end-to-end (chunk -> hash -> DAG -> RS encode) in under
  60 seconds on the reference machine (4 vCPU, 8 GiB RAM, SSD).
- **Mem-Gate Latency**: p50 byte-range fetch latency for warm
  content (already in local BadgerDB) MUST be under 10 ms;
  p95 MUST be under 50 ms; p99 MUST be under 150 ms. Cold fetches
  over libp2p MUST have p95 under 1 s for content <= 1 MiB and
  must degrade gracefully (progress events, range streaming) for
  larger content.
- **Throughput**: Sustained ingress (add) and egress (gateway) MUST
  reach at least 200 MB/s on the reference machine when the network
  is healthy, and MUST NOT fall below 50 MB/s under 10% packet loss.
- **DHT & Discovery**: DHT lookup (provider record resolution for an
  MID) MUST return at least one provider in under 2 s p95 on a
  healthy 100-node testnet; PEX gossip convergence MUST reach 95% of
  reachable peers within 30 s.
- **Resource Caps**: The daemon MUST be configurable with hard caps
  for memory, BadgerDB size, open libp2p streams, and concurrent
  Memex transfers. Defaults MUST fit a 512 MiB / 1 vCPU / 10 GB disk
  machine so the system is usable on commodity hardware.
- **Profiling & Benchmarks**: Hot paths (chunking, hashing, RS,
  Merkle DAG, libp2p stream, BadgerDB put/get, HTTP gateway) MUST
  ship Go benchmarks under `*_bench_test.go`. CI MUST run benchmarks
  on the release branch and FAIL on > 10% regression against the
  stored baseline.
- **Observability**: Every request path MUST emit structured logs
  (slog) and expose Prometheus metrics: request count, latency
  histogram, error rate, bytes in/out, active streams, DHT table
  size, RS shard count, and storage usage.

**Rationale**: A "decentralized" label is not a license to be slow.
If chunking a 1 GiB file takes 5 minutes, no one will seal large
content; if Mem-Gate adds 200 ms to every byte, no one will use it
as a CDN. Performance is the moat against dropping back to
centralized storage.

## Technology Constraints

The stack is fixed by the design goals in `AGENTS.md`. New
dependencies MUST be justified and MUST NOT replace the chosen
technologies without a constitution amendment.

- **Language**: Go (latest stable, currently 1.22+). Modules are
  pinned with `go.mod` / `go.sum`. Build with `go build ./...`.
- **Networking**: libp2p is the only allowed peer-to-peer stack.
  Transports MUST be TCP and QUIC. Yamux (or an equivalent muxer
  that supports backpressure) is required. Noise or TLS is required
  for security transport.
- **Storage**: BadgerDB is the only allowed embedded KV store. No
  SQLite, no BoltDB, no leveldb, no in-memory mocks in production
  code paths.
- **Serialization**: Protocol Buffers (proto3) for ALL internal
  structs and ALL wire protocols (Memex, gRPC, gRPC-gateway JSON).
  No JSON-in-proto, no struct tags-only JSON for cross-process
  boundaries. JSON is permitted only at the Mem-Gate HTTP edge and
  CLI `--json` output, derived from the protobuf definitions.
- **Erasure Coding**: `klauspost/reedsolomon` for all RS operations.
  Default shard parameters: data shards = 10, parity shards = 4
  (tunable, but every change must be reflected in benchmarks).
- **CLI ↔ Daemon**: gRPC with reflection enabled for local
  scripting. The CLI is a thin client; no business logic in the CLI
  binary.
- **Gateway**: `net/http` with the `go-chi/chi` router. No
  framework substitution. Range requests, conditional requests, and
  streaming responses are first-class.
- **External Network**: The project MUST NOT introduce hard
  dependencies on centralized services (no proprietary CDNs, no
  auth-as-a-service, no analytics endpoints). Bootstrap nodes MAY
  be used for initial peer discovery.

## Development Workflow

The team follows a Spec-Driven Development cycle aligned with the
spec-kit tooling (`/speckit-specify`, `/speckit-plan`, `/speckit-tasks`,
`/speckit-implement`).

- **Spec First**: No code is written for a new feature until
  `spec.md` exists and is approved. The spec MUST list user stories
  with priorities (P1, P2, P3...) and acceptance criteria.
- **Plan Second**: `plan.md` MUST be produced from the spec, MUST
  include a Constitution Check, and MUST pass it before Phase 0
  research. Any violation MUST be recorded in `Complexity Tracking`
  with a written justification.
- **Tasks Third**: `tasks.md` is generated from `plan.md` and is
  organized by user story. Each task references its story ID and
  exact file paths.
- **Implement Fourth**: Tasks are executed in order. The
  test-first gate is enforced inside the implement step.
- **Quality Gates** — a PR is mergeable only when ALL hold:
  1. `gofmt` and `golangci-lint run` are clean.
  2. `go vet ./...` and `go test ./... -race -count=1` pass.
  3. Coverage thresholds are met (>= 80% package, >= 95% critical).
  4. Benchmarks show no > 10% regression vs. baseline.
  5. Protobuf / gRPC / HTTP / CLI docs are updated.
  6. CHANGELOG entry and, if applicable, migration note are added.
  7. At least one reviewer signs off; reviewers MUST run the test
     suite locally for any change to a critical-path package.
- **Versioning**: Public APIs (gRPC, HTTP, CLI) follow
  SemVer. A breaking change to any of these surfaces is a MAJOR
  bump and MUST be preceded by one MINOR release that marks the
  change as deprecated.
- **Commits & History**: Commit messages follow Conventional
  Commits (`feat:`, `fix:`, `perf:`, `refactor:`, `test:`, `docs:`,
  `chore:`). Squash-merge is the default.
- **Security**: Security-sensitive changes (auth, libp2p protocol
  hardening, BadgerDB encryption-at-rest, MID canonicalization)
  require a second reviewer and a threat-model note in the PR.

## Governance

- This constitution supersedes all other written practices for
  Membuss. Where a `plan.md` or PR description conflicts with this
  document, this document wins.
- Amendments require:
  1. A written proposal (issue or PR) describing the change and
     its motivation.
  2. Review by at least one maintainer per affected area
     (networking, storage, CLI, gateway, core).
  3. A `Complexity Tracking` entry in the affected `plan.md` (or a
     new ADR under `docs/adr/`) explaining trade-offs.
  4. A version bump following SemVer for the constitution itself
     (see below).
- **Constitution Versioning** follows SemVer:
  - MAJOR — removal or redefinition of an existing principle, or
    any change that is intentionally backward-incompatible.
  - MINOR — addition of a new principle, section, or materially
    expanded guidance.
  - PATCH — clarifications, wording, typo fixes, non-semantic
    refinements.
- **Compliance Review**: Every release MUST include a
  constitution-compliance check item in its release notes,
  confirming that the four core principles and the quality gates
  above were verified.
- **Runtime Guidance**: Day-to-day development conventions (style,
  naming, commit policy) live in `AGENTS.md` and `docs/`. When they
  conflict with this constitution, this constitution wins until the
  constitution is amended.

**Version**: 1.0.0 | **Ratified**: 2026-06-14 | **Last Amended**: 2026-06-14
