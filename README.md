# firehose-parquet

A production-grade Rust CLI that consumes a [StreamingFast Firehose](https://firehose.streamingfast.io/) v2 gRPC stream of **Solana** blocks and writes **Apache Parquet** files using Apache Arrow RecordBatches as the intermediate format.

## Features

- **gRPC streaming** — connects to any Firehose v2 endpoint via tonic, with TLS and bearer-token auth.
- **Automatic retry / resume** — exponential back-off on connection errors; resumes from the last cursor.
- **Five normalised output tables** — `blocks`, `transactions`, `messages`, `instructions`, `rewards`.
- **Arrow-native pipeline** — per-table column builders produce `RecordBatch`es that flush to Parquet at configurable row/byte thresholds.
- **Partitioning** — flat or block-range based directory layouts (`output/<table>/block_range=…/part-000001.parquet`).
- **Compression** — zstd (default), snappy, gzip, or none.
- **Structured logging** via `tracing`.

## Repository structure

```
firehose-parquet/
├── Cargo.toml                          # workspace root
├── proto/
│   ├── sf/firehose/v2/firehose.proto   # Firehose v2 service definition
│   └── sf/solana/type/v1/type.proto    # Solana block types
├── crates/
│   ├── firehose-parquet/               # library crate (core pipeline)
│   │   ├── build.rs                    # tonic-build proto codegen
│   │   └── src/
│   │       ├── lib.rs                  # re-exports + generated proto modules
│   │       ├── config.rs               # Config / Partition / Compression types
│   │       ├── grpc.rs                 # Firehose gRPC client with retry
│   │       ├── schema.rs              # Arrow schema definitions (5 tables)
│   │       ├── mapper.rs              # Protobuf → Arrow builder mapping
│   │       └── writer.rs             # Parquet writer with partitioning
│   └── firehose-solana-to-parquet/    # binary crate (CLI)
│       └── src/main.rs
└── README.md
```

## Prerequisites

| Tool | Version |
|------|---------|
| Rust | stable (≥ 1.75) |
| `protoc` | ≥ 3.21 |

Install `protoc`:

```bash
# Ubuntu / Debian
sudo apt-get install -y protobuf-compiler

# macOS
brew install protobuf
```

## Build

```bash
cargo build --release
```

The `build.rs` in `crates/firehose-parquet` automatically compiles the `.proto` files under `proto/` via `tonic-build` + `prost`.

## Run

```bash
cargo run --release --bin firehose-solana-to-parquet -- \
  --endpoint https://mainnet.sol.streamingfast.io:443 \
  --api-token "$SF_API_TOKEN" \
  --start-block 200000000 \
  --stop-block  200001000 \
  --output ./output \
  --compression zstd \
  --flush-rows 50000 \
  --final-blocks-only
```

### All CLI flags

| Flag | Default | Description |
|------|---------|-------------|
| `--endpoint` | *(required)* | Firehose gRPC endpoint URL |
| `--api-token` | — | Bearer token for auth |
| `--start-block` | — | Start block (inclusive) |
| `--stop-block` | — | Stop block (inclusive, 0 = stream forever) |
| `--cursor` | — | Resume from an opaque Firehose cursor |
| `--output` | `output` | Root directory for Parquet files |
| `--partition` | `none` | `none` or `block_range` |
| `--block-range-size` | `10000` | Range size when `--partition=block_range` |
| `--flush-rows` | `50000` | Flush when any table exceeds this row count |
| `--flush-bytes` | `134217728` (128 MB) | *(reserved for future use)* |
| `--compression` | `zstd` | `zstd`, `snappy`, `gzip`, `none` |
| `--log-level` | `info` | `info`, `debug`, `trace` |
| `--dry-run` | `false` | Decode + map without writing files |
| `--final-blocks-only` | `true` | Only process irreversible blocks |

## Output tables & schemas

### `blocks`

| Column | Arrow type | Nullable | Notes |
|--------|-----------|----------|-------|
| `slot` | UInt64 | no | Primary key |
| `parent_slot` | UInt64 | no | |
| `block_height` | UInt64 | yes | From `BlockHeight` message |
| `blockhash` | Utf8 | no | Base-58 encoded |
| `previous_blockhash` | Utf8 | no | |
| `block_time` | Int64 | yes | Unix timestamp (seconds) |
| `num_transactions` | UInt32 | no | |
| `num_rewards` | UInt32 | no | |

### `transactions`

| Column | Arrow type | Nullable | Notes |
|--------|-----------|----------|-------|
| `slot` | UInt64 | no | FK → blocks |
| `transaction_index` | UInt32 | no | Position within block |
| `signature` | Binary | no | First signature (64 bytes) |
| `num_signatures` | UInt32 | no | |
| `fee` | UInt64 | no | Lamports |
| `err` | Binary | yes | Serialised `TransactionError` |
| `success` | Boolean | no | `true` when `err` is null |
| `compute_units_consumed` | UInt64 | yes | |
| `log_messages` | List\<Utf8\> | yes | Program log lines |
| `pre_balances` | List\<UInt64\> | yes | |
| `post_balances` | List\<UInt64\> | yes | |

### `messages`

| Column | Arrow type | Nullable | Notes |
|--------|-----------|----------|-------|
| `slot` | UInt64 | no | FK → blocks |
| `transaction_index` | UInt32 | no | FK → transactions |
| `message_index` | UInt32 | no | Always 0 (Solana has 1 msg/tx) |
| `num_required_signatures` | UInt32 | no | |
| `num_readonly_signed_accounts` | UInt32 | no | |
| `num_readonly_unsigned_accounts` | UInt32 | no | |
| `recent_blockhash` | Binary | no | 32 bytes |
| `versioned` | Boolean | no | |
| `account_keys` | List\<Binary\> | no | 32-byte public keys |

### `instructions`

| Column | Arrow type | Nullable | Notes |
|--------|-----------|----------|-------|
| `slot` | UInt64 | no | FK → blocks |
| `transaction_index` | UInt32 | no | FK → transactions |
| `instruction_index` | UInt32 | no | Global index within tx (top-level + inner) |
| `program_id_index` | UInt32 | no | Index into `account_keys` |
| `accounts` | Binary | no | Byte array of account indices |
| `data` | Binary | no | Instruction payload |
| `is_inner` | Boolean | no | `false` = top-level, `true` = CPI |
| `inner_index` | UInt32 | yes | Parent instruction index (for inner) |
| `stack_height` | UInt32 | yes | CPI stack depth |

### `rewards`

| Column | Arrow type | Nullable | Notes |
|--------|-----------|----------|-------|
| `slot` | UInt64 | no | FK → blocks |
| `reward_index` | UInt32 | no | Position within block |
| `pubkey` | Utf8 | no | Base-58 public key |
| `lamports` | Int64 | no | Reward amount (can be negative for rent) |
| `post_balance` | UInt64 | no | Balance after reward |
| `reward_type` | Int32 | no | Enum: 0=Unspecified, 1=Fee, 2=Rent, 3=Staking, 4=Voting |
| `commission` | Utf8 | yes | Validator commission string |

## Schema mapping rules

| Protobuf type | Arrow type | Notes |
|---------------|-----------|-------|
| `bytes` | `Binary` | Raw byte arrays |
| `string` | `Utf8` | |
| `uint64` | `UInt64` | |
| `int64` | `Int64` | |
| `uint32` | `UInt32` | |
| `int32` | `Int32` | |
| `bool` | `Boolean` | |
| `repeated T` | `List<T>` | For inline repeated fields |
| `optional T` | nullable column | Arrow nullability |
| nested message | normalised table | Flattened into the 5 output tables |
| `enum` | `Int32` | Stored as the i32 wire value |

## Tests

```bash
cargo test
```

Tests cover:
- Schema column counts for all 5 tables
- Mapping a synthetic block end-to-end (correct row counts per table)
- Flush/reset cycle (builders are reusable)
- Empty block edge case
- Parquet round-trip (write → read back, schema equality)
- Block-range partitioning directory layout
- OutputWriter writes all 5 tables

## Proto sources

The `.proto` files under `proto/` are sourced from:

- **Firehose v2**: <https://buf.build/streamingfast/firehose/docs/main:sf.firehose.v2>
- **Solana types**: <https://buf.build/streamingfast/firehose-solana/docs/main:sf.solana.type.v1>

## License

MIT — see [LICENSE](LICENSE).
