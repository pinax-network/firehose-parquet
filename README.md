# firehose-parquet

A production-grade Rust toolkit that consumes [StreamingFast Firehose](https://firehose.streamingfast.io/) v2 gRPC streams and writes **Apache Parquet** files. Supports multiple blockchain types with a shared core architecture.

## Supported Chains

| Chain | Binary | Endpoint Example | Tables |
|---|---|---|---|
| **Solana** | `firehose-solana-to-parquet` | `solana.firehose.pinax.network:443` | blocks, transactions, messages, instructions, rewards |
| **EVM** | `firehose-evm-to-parquet` | `eth.firehose.pinax.network:443` | blocks, transactions, logs (+7 extended tables) |
| **Bitcoin** | `firehose-bitcoin-to-parquet` | `btc.firehose.pinax.network:443` | blocks, transactions, inputs, outputs |
| **Beacon** | `firehose-beacon-to-parquet` | `beacon.firehose.pinax.network:443` | blocks, attestations, deposits, voluntary_exits, blob_sidecars, ... |
| **Tron** | `firehose-tron-to-parquet` | `tron.firehose.pinax.network:443` | blocks, transactions, logs, internal_transactions |
| **Cosmos** | `firehose-cosmos-to-parquet` | `cosmoshub.firehose.pinax.network:443` | blocks, transactions, events, messages |
| **Antelope** | `firehose-antelope-to-parquet` | `eos.firehose.pinax.network:443` | blocks, transactions, actions, db_ops |
| **NEAR** | `firehose-near-to-parquet` | `near.firehose.pinax.network:443` | blocks, chunks, transactions, receipts, state_changes |

## Features

- **Multi-chain** — pluggable `BlockMapper` trait with per-chain binary crates
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
# Build all binaries
cargo build --release --workspace

# Stream Solana blocks to Parquet
./target/release/firehose-solana-to-parquet \
  --endpoint https://solana.firehose.pinax.network:443 \
  --api-key $FIREHOSE_API_KEY \
  --start-block 200000000 \
  --stop-block 200001000 \
  --output ./output \
  --partition date \
  --compression zstd

# Stream EVM blocks with extended traces
./target/release/firehose-evm-to-parquet \
  --endpoint https://eth.firehose.pinax.network:443 \
  --api-key $FIREHOSE_API_KEY \
  --start-block 19000000 \
  --stop-block 19001000 \
  --extended \
  --encode-bytes hex
```

## CLI Reference

All binaries share these common flags:

```
REQUIRED:
  --endpoint <URL>           Firehose gRPC endpoint URL

AUTHENTICATION (one of):
  --api-key <KEY>            API key (also: FIREHOSE_API_KEY env var)
  --jwt-token <TOKEN>        JWT bearer token (also: SUBSTREAMS_API_TOKEN env var)

BLOCK RANGE:
  --start-block <NUM>        Start block number (inclusive)
  --stop-block <NUM>         Stop block number (inclusive, 0 = stream forever)
  --cursor <STRING>          Resume cursor from a previous session

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
  --encode-bytes <MODE>      binary | hex | base58 | tron_base58 | auto
                             Default varies by chain (hex for EVM, binary for Solana)

FORK HANDLING:
  --final-blocks-only        Only process finalized blocks (default: true)
                             When false, adds fork_step column to all tables

OTHER:
  --dry-run                  Decode and map but don't write files
  --log-level <LEVEL>        info (default) | debug | trace
```

### Chain-Specific Flags

| Binary | Extra Flags |
|---|---|
| `firehose-evm-to-parquet` | `--extended` — enable extended trace tables (calls, balance_changes, etc.) |

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
| NEAR | `base58` |

## Environment Variables

Copy `.env.example` to `.env`:

```bash
# Authentication (one of these is typically required)
FIREHOSE_API_KEY=your-api-key-here
SUBSTREAMS_API_TOKEN=your-jwt-token-here
```

## CLI Architecture

All binaries are built with [`clap`](https://docs.rs/clap) v4 using derive macros, chosen for its idiomatic Rust approach, excellent documentation, built-in shell completion support, and widespread community adoption.

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

    // chain-specific flags here …
}
```

### Shell completions

Every binary supports the `completions` subcommand:

```bash
# Bash
firehose-evm-to-parquet completions bash > ~/.local/share/bash-completion/completions/firehose-evm-to-parquet

# Zsh
firehose-evm-to-parquet completions zsh > ~/.zfunc/_firehose-evm-to-parquet

# Fish
firehose-evm-to-parquet completions fish > ~/.config/fish/completions/firehose-evm-to-parquet.fish
```

## Repository Structure

```
firehose-parquet/
├── Cargo.toml                              # workspace root
├── .env.example                            # environment variables template
├── .github/workflows/ci.yml               # CI pipeline (build + test)
├── proto/                                  # Protobuf definitions
│   └── sf/
│       ├── firehose/v2/firehose.proto      # Firehose streaming protocol
│       ├── solana/type/v1/type.proto
│       ├── ethereum/type/v2/type.proto
│       ├── bitcoin/type/v1/type.proto
│       ├── beacon/type/v1/type.proto
│       ├── tron/type/v1/block.proto
│       ├── cosmos/type/v2/type.proto
│       ├── antelope/type/v1/type.proto
│       └── near/type/v1/type.proto
├── crates/
│   ├── firehose-parquet/                   # Core library
│   │   └── src/
│   │       ├── cli.rs                      # Shared CLI args, completions, helpers
│   │       ├── config.rs                   # Config, Partition, Compression enums
│   │       ├── encode.rs                   # BytesColumn, encoding helpers
│   │       ├── grpc.rs                     # Firehose gRPC client
│   │       ├── traits.rs                   # BlockMapper trait, BlockIdentity
│   │       └── writer.rs                   # Parquet writer, partitioning
│   ├── firehose-protos/                    # Centralized proto compilation
│   └── blocks/                             # Block type definitions + binary targets
│       └── src/
│           ├── bin/                         # Per-chain CLI binaries
│           │   ├── evm.rs
│           │   ├── solana.rs
│           │   ├── bitcoin.rs
│           │   ├── beacon.rs
│           │   ├── tron.rs
│           │   ├── cosmos.rs
│           │   ├── antelope.rs
│           │   └── near.rs
│           ├── evm/                         # Per-chain mapper, schema, proto
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

# Install a specific binary
cargo install --path crates/blocks --bin firehose-evm-to-parquet
```

## License

[MIT](LICENSE)
