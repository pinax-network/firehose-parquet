# firehose-parquet

`fireparq`, the binary of this repository, streams blockchain blocks from
[StreamingFast Firehose](https://firehose.streamingfast.io/) v2 gRPC endpoints,
such as Pinax's, into **Delta Lake** tables: one table per chain table, with
Parquet data files partitioned by UTC day and a `_delta_log/`, on local disk or
S3. It writes finalized blocks only by default. The recommended deployment is a
final-only lake with one bucket per network and one continuous writer per
bucket. Ingestion is crash-safe: every flush commits all tables in one
protected transaction, and a restart finishes an interrupted one exactly once
before it streams again. DuckDB (`delta_scan`) and Polars (`scan_delta`) read
the tables directly, and `fireparq maintenance`, the same binary, compacts,
vacuums and checkpoints them beside the writer.

## Supported chains

| `--block-type` | Example `--network` → endpoint | Tables, columns and types | Notes |
|---|---|---|---|
| `evm` | `mainnet` → `eth.firehose.pinax.network:443` | [EVM schema](docs/schemas/evm.md) | [EVM notes](docs/chains/evm.md). `--without-extended` drops the call, state-change and `system_*` tables |
| `solana` | `solana-mainnet-beta` → `solana.firehose.pinax.network:443` | [Solana schema](docs/schemas/solana.md) | [Solana notes](docs/chains/solana.md). `--without-votes` drops `vote_transactions` |
| `bitcoin` | `btc` → `bitcoin.firehose.pinax.network:443` | [Bitcoin schema](docs/schemas/bitcoin.md) | [Bitcoin notes](docs/chains/bitcoin.md) |
| `beacon` | `mainnet-cl` → `eth-cl.firehose.pinax.network:443` | [Beacon schema](docs/schemas/beacon.md) | [Beacon notes](docs/chains/beacon.md) (fork coverage) |
| `tron` | `tron` → `mainnet.tron.streamingfast.io:443` | [Tron schema](docs/schemas/tron.md) | [Tron notes](docs/chains/tron.md) |
| `cosmos` | no built-in name; `--endpoint https://mainnet.injective.streamingfast.io:443` | [Cosmos schema](docs/schemas/cosmos.md) | [Cosmos notes](docs/chains/cosmos.md) |
| `antelope` | `eos` → `eos.firehose.pinax.network:443` | [Antelope schema](docs/schemas/antelope.md) | [Antelope notes](docs/chains/antelope.md) |
| `near` | `near-mainnet` → `mainnet.near.streamingfast.io:443` | [NEAR schema](docs/schemas/near.md) | [NEAR notes](docs/chains/near.md) |

> **Tip:** Use `--block-type auto` (the default) to auto-detect the chain from the Firehose stream's protobuf `type_url`.

The generated [schema reference](docs/schemas/README.md) lists every table,
column, Delta type and nullability. The [chain notes](docs/chains/README.md)
explain semantics, joins and queries. Select columns by name, not position: the
non-final `fork_step` and `stream_ordinal` are not always the last columns,
because later additions follow them on several Solana, Antelope, NEAR and Tron
tables.

## Features

- **Delta Lake tables**, partitioned by `date` (`date=YYYY-MM-DD`) and read through their logs ([reading the tables](docs/reading-tables.md)).
- **Exactly-once crash recovery**: all-table transactions and an output authority under `.fireparq-ingest/` ([cursor and resume](docs/cursor-and-resume.md)).
- **One consistent schema across chains**: canonical identity columns on every table, checked Delta types and per-chain encodings ([output layout](docs/output-layout.md)).
- **Finalized output by default**; non-final streams write an append-only event history with a documented live view ([non-final streams](docs/non-final-streams.md)).
- **Failed transactions modeled explicitly** on every chain ([failed transaction filtering](docs/chains/failed-transactions.md)).
- **Adaptive flushing**: an interval at the chain head, full-size files while catching up ([flush interval and catch-up](docs/cli.md#flush-interval-and-catch-up)).
- **Operations**: Prometheus metrics with `/health` and `/ready` ([metrics](docs/metrics.md)), and the maintenance job with example CronJobs ([Delta maintenance](docs/delta-maintenance.md)).

The [full feature list](docs/features.md) links each feature to its page.

## Install

Each GitHub release attaches the `fireparq` binary for Linux and macOS
(`x86_64` and `aarch64`), with build provenance attestations:

```bash
curl -LO https://github.com/pinax-network/firehose-parquet/releases/download/v1.1.2/fireparq-linux-x86_64.tar.gz
tar xzf fireparq-linux-x86_64.tar.gz
./fireparq-linux-x86_64/fireparq --version
```

Each release also publishes the image `ghcr.io/pinax-network/firehose-parquet`
to GitHub Container Registry, with `fireparq` as its entrypoint
([Docker](docs/getting-started.md#docker)); this release is tagged `1.1.2`,
`1.1`, `1` and `latest`. The writer runs it as `fireparq build`, the
[Delta maintenance](docs/delta-maintenance.md) job as `fireparq maintenance`.

To build from source, run `cargo install --path blocks` (and
`cargo install --path maintenance` for the maintenance job). See
[Getting started](docs/getting-started.md) for every archive and more examples.

## Quick start

Credentials are selected by endpoint provider ([authentication](docs/authentication.md)).
`--output` is the dataset root; `{chain}` names a directory after the
endpoint's chain name.

```bash
export PINAX_API_KEY=your-pinax-api-key

# Local: 1,000 Ethereum mainnet blocks into ./output/mainnet
fireparq build \
  --network mainnet \
  --start-block 20000000 \
  --stop-block 20001000 \
  --output './output/{chain}'

# S3: one bucket per network, the dataset at the bucket root. S3 output needs
# an explicit s3:// URI and both keys (AWS_ENDPOINT_URL_S3 for other stores).
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
fireparq build \
  --network mainnet \
  --start-block 20000000 \
  --stop-block 20001000 \
  --output s3://ethereum-mainnet
```

Without `--stop-block`, `build` keeps following finalized blocks, and a restart
resumes from the dataset's own authority. Read every table through its Delta
log, never by globbing its files. With DuckDB 1.5 or later:

```bash
duckdb -c "INSTALL delta; LOAD delta;
  SELECT count(*), min(block_num), max(block_num), min(date), max(date)
  FROM delta_scan('output/mainnet/blocks')"
```

With Polars (`pip install polars deltalake`):

```python
import polars as pl

blocks = pl.scan_delta("output/mainnet/blocks")
print(blocks.select(
    rows=pl.len(),
    first_block=pl.col("block_num").min(),
    last_block=pl.col("block_num").max(),
).collect())
```

[Reading the tables](docs/reading-tables.md) covers S3 reads, consistent reads
across tables and engine compatibility.

## Recommended deployment

A **final-only lake**: one bucket per network with the dataset at the bucket
root, one continuous final-only `build` per network (a single replica: a second
writer on the bucket fails with `bucket ownership is held`), and the
`fireparq maintenance` job hourly beside it, with a weekly full VACUUM.

```bash
OUTPUT=s3://ethereum-mainnet \
FLUSH_INTERVAL_SECS=60 \
METRICS_PORT=9102 \
fireparq build --network mainnet
```

[Recommended deployment](docs/deployment.md) has the settings, memory sizing,
alerts and Kubernetes notes, and
[`deploy/examples/delta-maintenance-cronjob.yaml`](deploy/examples/delta-maintenance-cronjob.yaml)
the maintenance CronJobs.

## Documentation

| Topic | Page | Covers |
|---|---|---|
| Using fireparq | [Getting started](docs/getting-started.md) | Install, `build` examples, Docker |
| | [Authentication](docs/authentication.md) | Provider-scoped Firehose credentials and explicit selectors |
| | [CLI reference](docs/cli.md) | Flags, network aliases, environment variables, flush sizing, `inspect`, `validate`, `recovery`, completions |
| | [Non-final streams and reorgs](docs/non-final-streams.md) | `--final-blocks-only=false`, `fork_step`, `stream_ordinal`, the canonical live view |
| | [Features](docs/features.md) | The full feature list |
| Output and schemas | [Output layout](docs/output-layout.md) | Dataset root and `{chain}`, directory tree, single-network buckets, identity columns, encodings, file metadata |
| | [Reading the tables](docs/reading-tables.md) | DuckDB and Polars reads, the frontier rule, engine compatibility |
| | [Schema reference](docs/schemas/README.md) | Every table, column, Delta type and nullability (generated) |
| | [Partition vocabulary](docs/partition-vocabulary.md) | The `date=YYYY-MM-DD` key and the CLI terms for it |
| Chain-specific notes | [Chain notes](docs/chains/README.md) | Index of the chain pages |
| | [Failed transaction filtering](docs/chains/failed-transactions.md) | Flags, failure conditions and outcome columns per chain |
| | [EVM](docs/chains/evm.md), [Solana](docs/chains/solana.md), [Bitcoin](docs/chains/bitcoin.md), [Beacon](docs/chains/beacon.md), [Tron](docs/chains/tron.md), [Cosmos](docs/chains/cosmos.md), [Antelope](docs/chains/antelope.md), [NEAR](docs/chains/near.md) | Joins, ordering and example queries per chain |
| Operations | [Recommended deployment](docs/deployment.md) | Final-only lake, one writer per network, settings and alerts |
| | [Cursor and resume](docs/cursor-and-resume.md) | Output authority, crash recovery, S3 cursors, ownership, shutdown |
| | [Delta maintenance](docs/delta-maintenance.md) | `fireparq maintenance`: compaction in the writer's row order, VACUUM, checkpoints, CronJobs |
| | [Prometheus metrics](docs/metrics.md) | Metrics, `/health` and `/ready` |
| | [Network registry integration](docs/network-registry-integration.md) | Built-in `--network` aliases, provider policy, endpoint check |
| Design and audit | [Delta Lake design](docs/design/delta-lake.md) | The v1.0.0 Delta Lake output design (#643) |
| | [Audit records](docs/audit/README.md) | Implementation and validation records of the September 2026 audit (#463) |
| Development | [Development](docs/development.md) | Build and test, repository structure, CLI architecture |
| | [Repository navigation](docs/repo-navigation.md) | Module map, data flow and where to edit |
| Releases | [v1.1.2](docs/releases/v1.1.2.md), [v1.1.1](docs/releases/v1.1.1.md), [v1.1.0](docs/releases/v1.1.0.md), [v1.0.7](docs/releases/v1.0.7.md), [v1.0.6](docs/releases/v1.0.6.md), [v1.0.5](docs/releases/v1.0.5.md), [v1.0.4](docs/releases/v1.0.4.md), [v1.0.3](docs/releases/v1.0.3.md), [v1.0.2](docs/releases/v1.0.2.md), [v1.0.1](docs/releases/v1.0.1.md), [v1.0.0](docs/releases/v1.0.0.md) | Release notes; v1.0.0 has the [upgrade guide](docs/releases/v1.0.0.md#upgrade-guide-read-first) from v0.7 |
| | [Unreleased](docs/releases/unreleased.md), [all releases](docs/releases/) | Changes since the last release, and older notes |

## License

[MIT](LICENSE)
