# Development

How to build and test the workspace, how it is organized, and how the CLI is put together. [Repository navigation](repo-navigation.md) maps every module and where to edit for common tasks.

## Build and test

```bash
# Build
cargo build --workspace

# Test
cargo test --workspace

# Build release (`fireparq`, the maintenance job included)
cargo build --release -p blocks

# Install
cargo install --path blocks
```

All code is Rust; building and testing need no Python, and nothing links
DataFusion (a CI step checks it). The DuckDB tests use the DuckDB CLI when it is
installed (`FIREPARQ_DUCKDB`, required in CI by `FIREPARQ_REQUIRE_DUCKDB`), and
the Delta maintenance tests run `fireparq maintenance` of the binary under
test.

## Repository structure

A summary of the workspace; [docs/repo-navigation.md](repo-navigation.md)
maps every module and where to edit for common tasks.

```
firehose-parquet/
├── Cargo.toml                              # workspace root (firehose-protos, firehose-parquet, blocks, maintenance)
├── Dockerfile                              # multi-stage Docker build of `fireparq`
├── .env.example                            # environment variables template (drift-tested against the CLI)
├── .github/workflows/                      # ci, advisories, docker-publish, release, network-endpoints
├── proto/                                  # chain and Firehose .proto files, plus proto/core/ dependencies
├── firehose-protos/                        # compiles proto/*.proto (build.rs) and exposes the modules
├── scripts/                                # generate_networks.rs, check_network_endpoints.sh
├── maintenance/                            # `fireparq maintenance`: the Delta maintenance job (row-order
│                                           #   compaction, delta-rs VACUUM, checkpoints, log cleanup)
├── deploy/examples/                        # example Kubernetes manifests (the maintenance CronJob)
├── docs/                                   # user guides, chain notes, design, schema reference, release notes, audit records
├── firehose-parquet/                       # core library
│   └── src/
│       ├── cli.rs, cli/                    # shared Clap args and subcommands; configuration, paths,
│       │                                   #   inspect and validate helpers
│       ├── ingest/                         # all-table transactions, output authority, cursor mirror, recovery
│       ├── date_partition.rs               # the date=YYYY-MM-DD key, formatted and parsed in one place
│       ├── writer.rs, writer/              # Arrow -> Parquet part encoding, partition routing, protected parts
│       ├── delta/                          # Delta tables: type mapping, log stores, stats, commits
│       ├── dataset_lock/, dataset_lock_s3.rs  # local directory and bucket-wide S3 ownership
│       ├── durable_state.rs, durable_state_s3.rs  # versioned control records
│       ├── recovery.rs                     # `fireparq recovery`
│       ├── maintenance/                    # shared read-only listings (startup and recovery checks)
│       ├── grpc.rs, grpc/                  # Firehose stream client, auth, reconnects
│       ├── s3.rs, s3/                      # AWS config, bounded uploads
│       ├── auth.rs                         # provider-scoped credential selection
│       ├── networks.rs, networks_generated.rs  # built-in --network names (generated)
│       ├── config.rs, flush.rs, cursor.rs  # config model, flush sizing, cursor Parquet format
│       ├── encode.rs, encode/              # identifier encodings
│       └── artifacts.rs, metrics.rs, traits.rs  # artifact names, Prometheus, BlockMapper trait
├── blocks/                                 # chain mappers + unified binary
│   ├── src/
│   │   ├── bin/main.rs                     # `fireparq` entrypoint and command dispatch
│   │   ├── bin/ingestion/                  # `build`: mod.rs, setup.rs (endpoint/resume), runtime.rs
│   │   ├── chain.rs                        # ChainKind / ChainProfile per chain family
│   │   └── evm/, solana/, bitcoin/, beacon/, tron/, cosmos/, antelope/, near/, sec/, hypercore/
│   │                                       # per chain: proto.rs, schema.rs, mapper.rs
│   └── tests/                              # integration tests, including `fireparq` runs against a mock Firehose
└── target/                                 # build output
```

## CLI architecture

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
| `prometheus-client` | Prometheus metrics exposition |
| `object_store` | S3-compatible object storage (data + cursor) |

### Shared CLI module

Subcommands, shared arguments (`AwsArgs`, `CommonArgs`, `BuildArgs`), parsing
helpers and completions are defined once in `firehose_parquet::cli`. The binary
in `blocks/src/bin/main.rs` only adds the global logging flags and dispatches:

```rust
use firehose_parquet::cli::{build_config, init_tracing, Commands};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>, // Build(BuildArgs), Validate, Inspect, Recovery, Completions

    #[command(flatten)]
    global: GlobalArgs, // --log-level, --verbose, --env-file
}
```

`BuildArgs` flattens `CommonArgs` (connection, range, output, flush and AWS
flags) and adds the chain flags (`--network`, `--block-type`,
`--without-extended`, `--without-votes`, the failed-transaction flags and
`--cursor-override`). `Commands::Build` runs `ingestion::run_ingestion` in
`blocks/src/bin/ingestion/`.
