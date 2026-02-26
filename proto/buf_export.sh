#!/usr/bin/env bash
# Pulls the latest proto definitions from the Buf Build Registry (https://buf.build)
# and writes them as flat, single-file protos named after each block type.
#
# Prerequisites:
#   - buf CLI (https://buf.build/docs/installation)
#
# Usage:
#   ./proto/buf_export.sh

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="$SCRIPT_DIR"
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

echo "Exporting proto definitions from Buf Build Registry..."

# ── Firehose core ──────────────────────────────────────────
buf export buf.build/streamingfast/firehose -o "$TMP_DIR/firehose"
cp "$TMP_DIR/firehose/sf/firehose/v2/firehose.proto" "$PROTO_DIR/firehose.proto"
echo "  ✓ firehose.proto"

# ── Ethereum (EVM) ────────────────────────────────────────
buf export buf.build/streamingfast/firehose-ethereum -o "$TMP_DIR/ethereum"
cp "$TMP_DIR/ethereum/sf/ethereum/type/v2/type.proto" "$PROTO_DIR/ethereum.proto"
echo "  ✓ ethereum.proto"

# ── Bitcoin ───────────────────────────────────────────────
buf export buf.build/streamingfast/firehose-bitcoin -o "$TMP_DIR/bitcoin"
cp "$TMP_DIR/bitcoin/sf/bitcoin/type/v1/type.proto" "$PROTO_DIR/bitcoin.proto"
echo "  ✓ bitcoin.proto"

# ── Solana ────────────────────────────────────────────────
buf export buf.build/streamingfast/firehose-solana -o "$TMP_DIR/solana"
cp "$TMP_DIR/solana/sf/solana/type/v1/type.proto" "$PROTO_DIR/solana.proto"
echo "  ✓ solana.proto"

# ── NEAR ──────────────────────────────────────────────────
buf export buf.build/streamingfast/firehose-near -o "$TMP_DIR/near"
cp "$TMP_DIR/near/sf/near/type/v1/type.proto" "$PROTO_DIR/near.proto"
echo "  ✓ near.proto"

# ── Antelope ──────────────────────────────────────────────
buf export buf.build/pinax/firehose-antelope -o "$TMP_DIR/antelope"
cp "$TMP_DIR/antelope/sf/antelope/type/v1/type.proto" "$PROTO_DIR/antelope.proto"
echo "  ✓ antelope.proto"

# ── Cosmos ────────────────────────────────────────────────
buf export buf.build/streamingfast/firehose-cosmos -o "$TMP_DIR/cosmos"
cp "$TMP_DIR/cosmos/sf/cosmos/type/v2/block.proto" "$PROTO_DIR/cosmos.proto"
echo "  ✓ cosmos.proto"


# ── Tron ──────────────────────────────────────────────────
buf export buf.build/streamingfast/firehose-tron -o "$TMP_DIR/tron"
cp "$TMP_DIR/tron/sf/tron/type/v1/block.proto" "$PROTO_DIR/tron.proto"
# Tron depends on core protocol protos
mkdir -p "$PROTO_DIR/core"
cp "$TMP_DIR/tron/core/Tron.proto"             "$PROTO_DIR/core/Tron.proto"
cp "$TMP_DIR/tron/core/Discover.proto"          "$PROTO_DIR/core/Discover.proto"
cp "$TMP_DIR/tron/core/common.proto"            "$PROTO_DIR/core/common.proto"
echo "  ✓ tron.proto (+ core/)"

# ── Beacon ────────────────────────────────────────────────
buf export buf.build/pinax/firehose-beacon -o "$TMP_DIR/beacon"
cp "$TMP_DIR/beacon/sf/beacon/type/v1/type.proto" "$PROTO_DIR/beacon.proto"
echo "  ✓ beacon.proto"

echo ""
echo "Done. All proto files updated in $PROTO_DIR"
