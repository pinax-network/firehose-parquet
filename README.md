# firehose-parquet

A production-grade Rust toolkit that consumes [StreamingFast Firehose](https://firehose.streamingfast.io/) v2 gRPC streams and writes **Apache Parquet** files. A single unified binary (`firehose-to-parquet`) supports multiple blockchain types with automatic chain detection.

## Supported Chains

| `--block-type` | Endpoint Example | Tables |
|---|---|---|
| `evm` | `eth.firehose.pinax.network:443` | blocks, transactions, logs |
| `evm --extended` | | + calls, balance_changes, code_changes, storage_changes, nonce_changes, gas_changes, account_creations |
| `solana` | `solana.firehose.pinax.network:443` | blocks, transactions, messages, instructions, rewards |
| `bitcoin` | `btc.firehose.pinax.network:443` | blocks, transactions, inputs, outputs |
| `beacon` | `beacon.firehose.pinax.network:443` | blocks, attestations, deposits, proposer_slashings, attester_slashings, voluntary_exits, execution_payload, blob_sidecars |
| `tron` | `tron.firehose.pinax.network:443` | blocks, transactions, logs, internal_transactions |
| `cosmos` | `cosmoshub.firehose.pinax.network:443` | blocks, transactions, events, messages |
| `antelope` | `eos.firehose.pinax.network:443` | blocks, transactions, actions, db_ops |
| `near` | `near.firehose.pinax.network:443` | blocks, chunks, transactions, receipts, state_changes |

> **Tip:** Use `--block-type auto` (the default) to auto-detect the chain from the Firehose stream's protobuf `type_url`.

## Features

- **Single binary** — one `firehose-to-parquet` binary handles all chains via `--block-type` with auto-detection
- **Multi-chain** — pluggable `BlockMapper` trait with per-chain mapper modules
- **Canonical identity columns** — `block_num`, `block_id`, `parent_num`, `parent_id`, `lib_num`, `timestamp` on every table (from Firehose `BlockMetadata`)
- **gRPC streaming** — connects to any Firehose v2 endpoint via tonic, with TLS and API key / JWT auth
- **Automatic retry / resume** — exponential back-off on connection errors; resumes from the last cursor
- **Partitioning** — `none`, `block_range`, `date`, or `hour` layouts
- **File rollover** — flush by row count, byte size, or time interval
- **Fork handling** — `--final-blocks-only` (default) or include `fork_step` column (`NEW`/`UNDO`/`FINAL`)
- **Byte encoding** — configurable encoding for binary fields: `binary` (raw), `hex`, `base58`, `tron_base58`, `auto`
- **Compression** — zstd (default), snappy, gzip, or none
- **Arrow-native pipeline** — column builders produce `RecordBatch`es that flush to Parquet

## Quick Start

```bash
# Build
cargo build --release --workspace

# Stream Solana blocks to Parquet (auto-detect chain)
./target/release/firehose-to-parquet \
  --endpoint https://solana.firehose.pinax.network:443 \
  --start-block 200000000 \
  --stop-block 200001000 \
  --output ./output \
  --partition date \
  --compression zstd

# Stream EVM blocks with extended traces (explicit block type)
./target/release/firehose-to-parquet \
  --block-type evm \
  --endpoint https://eth.firehose.pinax.network:443 \
  --start-block 19000000 \
  --stop-block 19001000 \
  --extended \
  --bytes-encoding hex
```

## CLI Reference

```
REQUIRED:
  -e, --endpoint <URL>       Firehose gRPC endpoint URL

CHAIN SELECTION:
  --block-type <TYPE>        auto (default) | evm | solana | bitcoin | beacon | tron | cosmos | antelope | near
                             "auto" detects the chain from the Firehose stream

AUTHENTICATION:
  --api-key-envvar <NAME>    Env var name for API key (default: SUBSTREAMS_API_KEY)
  --api-token-envvar <NAME>  Env var name for JWT token (default: SUBSTREAMS_API_TOKEN)
  --public                   Skip authentication (for public endpoints)

CONNECTION:
  (TLS is automatically enabled for https:// endpoints, disabled for http://)

BLOCK RANGE:
  -s, --start-block <NUM>    Start block number (inclusive)
  -t, --stop-block <NUM>     Stop block number (inclusive, 0 = stream forever)
  -c, --cursor <PATH>        Path to cursor file for resuming a previous session

OUTPUT:
  --output <DIR>             Output directory (default: "output")
  --compression <CODEC>      zstd (default) | snappy | gzip | none

PARTITIONING:
  --partition <MODE>         none (default) | block_range | date | hour
  --block-range-size <NUM>   Block range size when partition=block_range (default: 10000)

FILE ROLLOVER:
  --flush-rows <NUM>         Max rows per file (default: 50000)
  --flush-bytes <NUM>        Max bytes per file (default: 134217728 = 128MB)
  --flush-interval-secs <N>  Time-based flush interval (disabled by default)

ENCODING:
  --bytes-encoding <MODE>    binary | hex | base58 | tron_base58 | auto
                             "auto" resolves to a chain-appropriate default (hex for EVM, base58 for Solana, etc.)

FORK HANDLING:
  --final-blocks-only        Only process finalized blocks (default: true)
                             When false, adds fork_step column to all tables

EVM-SPECIFIC:
  --extended                 Enable extended trace tables (calls, balance_changes, etc.)

OTHER:
  --dry-run                  Decode and map but don't write files
  --log-level <LEVEL>        info (default) | debug | trace

AWS S3 OUTPUT:
  --output s3://bucket/path  Write Parquet files to an S3 bucket
  --aws-access-key-id <KEY>  AWS access key ID
  --aws-secret-access-key <SECRET>  AWS secret access key
  --aws-session-token <TOKEN>       AWS session token (optional)
  --aws-region <REGION>             AWS region (e.g. us-east-1)
  --aws-endpoint-url <URL>          Custom S3 endpoint (for S3-compatible services)
```

## Output Directory Layout

```
output/
├── blocks/
│   ├── date=2026-02-25/
│   │   ├── part-000001.parquet
│   │   └── part-000002.parquet
│   └── date=2026-02-26/
│       └── part-000001.parquet
├── transactions/
│   └── ...
└── logs/
    └── ...
```

## Canonical Identity Columns

Every table across all chains includes these 6 columns (from Firehose `BlockMetadata`):

| Column | Type | Description |
|---|---|---|
| `block_num` | UInt64 | Block number |
| `block_id` | Utf8 | Block ID (hex for EVM, base58 for Solana) |
| `parent_num` | UInt64 | Parent block number |
| `parent_id` | Utf8 | Parent block ID |
| `lib_num` | UInt64 | Last irreversible block number |
| `timestamp` | Int64 | Block time (unix seconds) |

## Byte Encoding

| Mode | Description | Best For |
|---|---|---|
| `binary` | Raw bytes (Arrow `Binary`) | Parquet-native workflows (DuckDB, Spark) |
| `hex` | `0x`-prefixed hex strings | EVM ecosystem tools |
| `base58` | Base58 strings | Solana ecosystem tools |
| `tron_base58` | Tron Base58Check addresses (hex fallback) | Tron-specific tools |
| `auto` | Chain-appropriate default | General use |

### Auto Encoding Per Chain

| Chain | `auto` resolves to |
|---|---|
| EVM | `hex` |
| Solana | `base58` |
| Bitcoin | `hex` |
| Tron | `tron_base58` |
| Beacon | `hex` |
| Cosmos | `hex` |
| Antelope | `hex` |
| NEAR | `hex` |

## Environment Variables

CLI flags can also be set via environment variables. Copy `.env.example` to `.env`:

```bash
# Authentication — set the env vars that the CLI reads by default
SUBSTREAMS_API_KEY=your-api-key-here
SUBSTREAMS_API_TOKEN=your-jwt-token-here

# AWS S3 output (optional)
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
# AWS_REGION=us-east-1
```

See `.env.example` for the full list of supported environment variables.

## CLI Architecture

The binary is built with [`clap`](https://docs.rs/clap) v4 using derive macros, chosen for its idiomatic Rust approach, excellent documentation, built-in shell completion support, and widespread community adoption.

### Key crates

| Crate | Purpose |
|---|---|
| `clap` (derive) | `#[derive(Parser)]` / `#[derive(Args)]` argument parsing with type-safe enums and defaults |
| `clap_complete` | Generates shell completions for Bash, Zsh, Fish, Elvish, and PowerShell |
| `anyhow` | Ergonomic error handling with context in binary crates |
| `thiserror` | Structured error types in the core library |
| `tracing` / `tracing-subscriber` | Structured, filterable logging |
| `tokio` | Async runtime for gRPC streaming |

### Shared CLI module

Common arguments, parsing helpers, and completions are defined once in `firehose-parquet::cli`:

```rust
use firehose_parquet::cli::{CommonArgs, Commands, build_config, init_tracing};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    #[command(flatten)]
    common: CommonArgs,

    // binary-specific flags (block_type, extended, bytes_encoding)
}
```

### Shell completions

The binary supports the `completions` subcommand:

```bash
# Bash
firehose-to-parquet completions bash > ~/.local/share/bash-completion/completions/firehose-to-parquet

# Zsh
firehose-to-parquet completions zsh > ~/.zfunc/_firehose-to-parquet

# Fish
firehose-to-parquet completions fish > ~/.config/fish/completions/firehose-to-parquet.fish
```

## Repository Structure

```
firehose-parquet/
├── Cargo.toml                              # workspace root
├── .env.example                            # environment variables template
├── .github/workflows/ci.yml               # CI pipeline (build + test)
├── proto/                                  # Protobuf definitions (flat layout)
│   ├── firehose.proto                      # Firehose streaming protocol
│   ├── ethereum.proto
│   ├── solana.proto
│   ├── bitcoin.proto
│   ├── beacon.proto
│   ├── tron.proto
│   ├── cosmos.proto
│   ├── antelope.proto
│   └── near.proto
├── crates/
│   ├── firehose-protos/                    # Centralized proto compilation
│   ├── firehose-parquet/                   # Core library
│   │   └── src/
│   │       ├── cli.rs                      # Shared CLI args, completions, helpers
│   │       ├── config.rs                   # Config, Partition, Compression enums
│   │       ├── encode.rs                   # BytesColumn, encoding helpers
│   │       ├── grpc.rs                     # Firehose gRPC client
│   │       ├── traits.rs                   # BlockMapper trait, BlockIdentity
│   │       └── writer.rs                   # Parquet writer, partitioning
│   └── blocks/                             # Block type definitions + unified binary
│       └── src/
│           ├── bin/
│           │   └── main.rs                 # Single unified binary (firehose-to-parquet)
│           ├── evm/                        # Per-chain mapper, schema, proto
│           ├── solana/
│           ├── bitcoin/
│           ├── beacon/
│           ├── tron/
│           ├── cosmos/
│           ├── antelope/
│           └── near/
└── target/                                 # build output
```

## Development

```bash
# Build
cargo build --workspace

# Test
cargo test --workspace

# Build release
cargo build --release --workspace

# Install
cargo install --path crates/blocks
```

## License

[MIT](LICENSE)
