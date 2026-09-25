#!/usr/bin/env bash
# Check that every built-in `--network` endpoint is still served.
#
# For each endpoint in firehose-parquet/src/networks_generated.rs this sends an
# unauthenticated Firehose `EndpointInfo/Info` gRPC call with curl. That covers
# DNS, TLS (certificate and hostname), HTTP/2, and a gRPC answer from the
# Firehose service. `grpc-status: 16` (Unauthenticated) counts as served, since
# the check runs without credentials.
#
# usage: scripts/check_network_endpoints.sh [path/to/networks_generated.rs]
set -euo pipefail

registry="${1:-firehose-parquet/src/networks_generated.rs}"
[[ -f "$registry" ]] || { echo "registry file not found: $registry" >&2; exit 2; }

pairs=$(grep -E '^[[:space:]]+(chain_name|default_endpoint):' "$registry" \
  | sed -E 's/.*: "(.*)",/\1/' | paste -d' ' - -)
[[ -n "$pairs" ]] || { echo "no built-in networks found in $registry" >&2; exit 2; }

total=0
failed=()
while read -r name endpoint; do
  total=$((total + 1))
  headers=$(printf '\0\0\0\0\0' | curl --silent --show-error --http2 \
    --connect-timeout 10 --max-time 20 --retry 2 --retry-all-errors --retry-delay 5 \
    --output /dev/null --dump-header - \
    -X POST -H 'content-type: application/grpc' -H 'te: trailers' --data-binary @- \
    "${endpoint}/sf.firehose.v2.EndpointInfo/Info" 2>&1 | tr -d '\r') || true
  status=$(printf '%s\n' "$headers" | sed -nE 's/^grpc-status: *([0-9]+).*/\1/p' | tail -1)
  case "$status" in
    0 | 16)
      echo "ok    $name $endpoint (grpc-status $status)"
      ;;
    *)
      reason=$(printf '%s\n' "$headers" | grep -E '^(curl: |grpc-message:|HTTP/)' | uniq | head -2 | tr '\n' ' ' || true)
      echo "FAIL  $name $endpoint ${reason:-no gRPC response}"
      failed+=("$name")
      ;;
  esac
done <<< "$pairs"

if ((${#failed[@]} > 0)); then
  echo "${#failed[@]} of $total built-in network endpoints failed: ${failed[*]}" >&2
  echo "Refresh the registry (docs/network-registry-integration.md) or remove the aliases." >&2
  exit 1
fi
echo "all $total built-in network endpoints answered"
