#!/bin/sh
# Container entrypoint for membuss.
#
# The distroless base image has no shell, so this script lives
# in the build context and is consumed by a tiny busybox stage
# in the Dockerfile. Its job is to translate the MEMBUSS_*
# environment variables into a rendered config.yaml, write it
# to a temp file, and exec the daemon with -config pointing
# at it. Every env var is optional; if unset, the baked-in
# /etc/membuss/config.yaml is used as the base so the container
# works out of the box.
#
# Exported variables (all optional):
#   MEMBUSS_LISTEN_ADDRS       comma-separated multiaddrs
#   MEMBUSS_GATEWAY_ADDR       host:port
#   MEMBUSS_API_ADDR           host:port
#   MEMBUSS_GRPC_ADDR          host:port
#   MEMBUSS_DATA_DIR           absolute path
#   MEMBUSS_BOOTSTRAP_PEERS    comma-separated multiaddrs
#   MEMBUSS_LOG_LEVEL          debug|info|warn|error
#   MEMBUSS_ANCHOR_MODE        "true" / "false"
#   MEMBUSS_BLOOM_CAPACITY     integer
#   MEMBUSS_BLOOM_FP_RATE      float
#   MEMBUSS_NO_ANCHOR          "true" forces the -no-anchor flag
#   MEMBUSS_EXTRA_FLAGS        passed verbatim to the daemon

set -eu

# Default every MEMBUSS_* env var to empty so an unset var
# does not blow up under `set -u`. set_yaml / set_yaml_list
# no-op on empty values.
: "${MEMBUSS_LISTEN_ADDRS:=}"
: "${MEMBUSS_GATEWAY_ADDR:=}"
: "${MEMBUSS_API_ADDR:=}"
: "${MEMBUSS_GRPC_ADDR:=}"
: "${MEMBUSS_DATA_DIR:=}"
: "${MEMBUSS_BOOTSTRAP_PEERS:=}"
: "${MEMBUSS_LOG_LEVEL:=}"
: "${MEMBUSS_ANCHOR_MODE:=}"
: "${MEMBUSS_BLOOM_CAPACITY:=}"
: "${MEMBUSS_BLOOM_FP_RATE:=}"
: "${MEMBUSS_NO_ANCHOR:=false}"
: "${MEMBUSS_EXTRA_FLAGS:=}"

BASE_CONFIG="${MEMBUSS_BASE_CONFIG:-/etc/membuss/config.yaml}"
RENDERED_CONFIG="$(mktemp -t membuss-config.XXXXXX.yaml)"
trap "rm -f \"$RENDERED_CONFIG\"" EXIT INT TERM

# Start from the base config; if any env var is set we re-render
# the affected fields on top. This keeps the file readable for
# `docker exec ... cat $RENDERED_CONFIG` debugging.
cp "$BASE_CONFIG" "$RENDERED_CONFIG"

# Helper: replace a YAML scalar value at the top level. The base
# file is well-formed and indentation-stable so a sed on the
# `^key: value$` line is sufficient.
set_yaml() {
    key="$1"
    value="$2"
    if [ -n "$value" ]; then
        # Escape forward slashes/backslashes for sed and drop
        # any embedded newlines (env vars are single-line).
        esc=$(printf "%s" "$value" | sed -e "s/[\/&]/\\&/g")
        if grep -qE "^${key}:" "$RENDERED_CONFIG"; then
            sed -i "s|^${key}:.*|${key}: ${esc}|" "$RENDERED_CONFIG"
        else
            printf "\n%s: %s\n" "$key" "$value" >> "$RENDERED_CONFIG"
        fi
    fi
}

# Multi-line list fields (listen_addrs, bootstrap_peers) need a
# different strategy: replace the next non-empty, non-comment
# line that follows the key.
set_yaml_list() {
    key="$1"
    value="$2"
    if [ -z "$value" ]; then return; fi
    # Build the new block: `key:\n  - v1\n  - v2\n`.
    new_block="${key}:"
    # shellcheck disable=SC2086
    IFS=","
    for v in $value; do
        # Trim whitespace.
        v=$(printf "%s" "$v" | sed "s/^[[:space:]]*//;s/[[:space:]]*$//")
        new_block="${new_block}\n  - ${v}"
    done
    unset IFS
    # Use awk for the substitution so newlines land correctly.
    awk -v key="$key" -v newblock="$new_block" '
        BEGIN { done=0 }
        /^[[:space:]]*#/ { print; next }
        $0 ~ "^"key":" && !done {
            printf "%s\n", newblock
            # Skip the old value (it may be a scalar or a list).
            done=1
            next
        }
        done==1 && /^[[:space:]]*-[[:space:]]/ { next }
        done==1 && /^[[:space:]]*$/ { done=2; print; next }
        done==1 { done=2 }
        { print }
    ' "$RENDERED_CONFIG" > "${RENDERED_CONFIG}.new"
    mv "${RENDERED_CONFIG}.new" "$RENDERED_CONFIG"
}

set_yaml_list listen_addrs    "$MEMBUSS_LISTEN_ADDRS"
set_yaml_list bootstrap_peers "$MEMBUSS_BOOTSTRAP_PEERS"
set_yaml       gateway_addr   "$MEMBUSS_GATEWAY_ADDR"
set_yaml       api_addr       "$MEMBUSS_API_ADDR"
set_yaml       grpc_addr      "$MEMBUSS_GRPC_ADDR"
set_yaml       data_dir       "$MEMBUSS_DATA_DIR"
set_yaml       log_level      "$MEMBUSS_LOG_LEVEL"
set_yaml       anchor_mode    "$MEMBUSS_ANCHOR_MODE"
set_yaml       bloom_capacity "$MEMBUSS_BLOOM_CAPACITY"
set_yaml       bloom_fp_rate  "$MEMBUSS_BLOOM_FP_RATE"

# Ensure the data directory exists and is writable by the
# current uid. distroless runs as uid 65532 by default.
if [ -n "$MEMBUSS_DATA_DIR" ]; then
    mkdir -p "$MEMBUSS_DATA_DIR"
fi

# Echo the rendered config to the docker log so operators can
# see what the daemon is actually using.
echo "---- membuss rendered config ----"
cat "$RENDERED_CONFIG"
echo "---------------------------------"

# Build the -no-anchor flag if requested. This mirrors the
# `membuss -no-anchor` CLI flag; useful for read-only nodes.
extra=""
if [ "${MEMBUSS_NO_ANCHOR:-false}" = "true" ]; then
    extra="-no-anchor"
fi
if [ -n "${MEMBUSS_EXTRA_FLAGS:-}" ]; then
    extra="$extra $MEMBUSS_EXTRA_FLAGS"
fi

# exec replaces the shell so the daemon is PID 1 and receives
# signals directly from the kernel.
# shellcheck disable=SC2086
exec /usr/local/bin/membuss -config "$RENDERED_CONFIG" $extra
