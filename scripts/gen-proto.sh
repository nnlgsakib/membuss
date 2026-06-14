#!/usr/bin/env bash
# Generate Go bindings for all .proto files in rpc/proto/.
#
# Requirements: protoc, protoc-gen-go, protoc-gen-go-grpc.
# Install with:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROTO_DIR="${ROOT_DIR}/rpc/proto"
OUT_DIR="${ROOT_DIR}/proto"

mkdir -p "${OUT_DIR}"

protoc \
  --proto_path="${PROTO_DIR}" \
  --go_out="${OUT_DIR}" --go_opt=paths=source_relative \
  --go-grpc_out="${OUT_DIR}" --go-grpc_opt=paths=source_relative \
  $(find "${PROTO_DIR}" -name '*.proto')
