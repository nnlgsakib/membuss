# syntax=docker/dockerfile:1.7
#
# Membuss daemon - multi-stage container build.
#
# Stage 1 compiles statically-linked Linux binaries using the
# same Go toolchain pinned in go.mod, then stage 2 copies only
# the binaries and a small entrypoint into a distroless base
# image that ships a busybox shell. The result is a tiny
# (~25 MB) image with no package manager, no apt, and a
# non-root user - but enough tooling to render config from
# env vars and respond to docker exec / docker debug.

# ---------------------------------------------------------------------------
# Stage 1: builder
# ---------------------------------------------------------------------------
FROM golang:1.25-alpine AS builder

# git is required by `go mod download` for some indirect deps.
# ca-certificates is required so the libp2p host can dial TLS
# bootstrap peers.
RUN apk add --no-cache git ca-certificates binutils

WORKDIR /src

# Pre-copy module manifests so the dependency layer is cached
# separately from the source layer. This means `docker build`
# only re-runs `go mod download` when go.mod / go.sum change.
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source. The build context is constrained
# by .dockerignore to keep this layer small.
COPY . .

# Build static, stripped binaries. CGO_ENABLED=0 gives us a
# fully static binary that runs on the distroless base image
# without a libc dependency. -trimpath strips local paths from
# the binary for reproducible builds. -ldflags "-s -w" drops
# the symbol table and DWARF debug info.
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN mkdir -p /out \
    && go build -trimpath -ldflags "-s -w" -o /out/membuss            ./cmd/membuss \
    && go build -trimpath -ldflags "-s -w" -o /out/membuss-cli        ./cmd/membuss-cli \
    && go build -trimpath -ldflags "-s -w" -o /out/membuss-entrypoint ./cmd/membuss-entrypoint \
    && strip /out/membuss /out/membuss-cli /out/membuss-entrypoint

# ---------------------------------------------------------------------------
# Stage 2: runtime
# ---------------------------------------------------------------------------
# distroless/base-debian12:nonroot ships a busybox shell, the
# ca-certificates bundle, /etc/passwd with the `nonroot` user
# (uid 65532), and tzdata. It is ~25 MB on disk and has no
# package manager - a good fit for a network-facing daemon.
FROM gcr.io/distroless/base-debian12

# Container metadata. org.opencontainers.image.* labels are
# read by `docker inspect` and most container registries.
LABEL org.opencontainers.image.title="membuss" \
      org.opencontainers.image.description="Decentralized, content-addressed storage and delivery network (daemon + CLI)" \
      org.opencontainers.image.source="https://github.com/nnlgsakib/membuss" \
      org.opencontainers.image.licenses="MIT"

# Copy the binaries produced by the builder stage.
COPY --from=builder /out/membuss     /usr/local/bin/membuss
COPY --from=builder /out/membuss-cli /usr/local/bin/membuss-cli

# Ship a container-friendly default config. The entrypoint
# shim is a static Go binary (distroless has no shell, so a
# .sh ENTRYPOINT is silently rejected by the kernel).
COPY deploy/membuss.yaml /etc/membuss/config.yaml
COPY --from=builder /out/membuss-entrypoint /usr/local/bin/membuss-entrypoint

# Data directory. The named volume (`membuss-data`) declared in
# docker-compose.yml is mounted here so the BadgerDB files and
# bloom-filter snapshot survive container restarts.
VOLUME ["/var/lib/membuss"]

# libp2p: 4001 tcp + 4001 udp/quic
# gRPC:    50051
# HTTP:    8080 (gateway), 5001 (node api)
EXPOSE 4001 4001/udp 5001 8080 50051

# The distroless image already runs as uid 65532 (nonroot).
# The data volume must be writable by that uid; docker-compose
# handles this with `user:` or by using a host-side chown.


# Healthcheck pings the gRPC Ping endpoint. It runs every 30s,
# times out after 5s, and is considered healthy after one
# success. It is intentionally short and unprivileged.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["/usr/local/bin/membuss-cli", "--addr", "127.0.0.1:50051", "ping"]

# The container is a long-running daemon. The entrypoint
# shim renders the env-var-driven config, chowns the data
# volume, and execs the binary so the daemon becomes PID 1
# and receives signals directly.
ENTRYPOINT ["/usr/local/bin/membuss-entrypoint"]
CMD ["/usr/local/bin/membuss", "-config", "/etc/membuss/config.yaml"]
