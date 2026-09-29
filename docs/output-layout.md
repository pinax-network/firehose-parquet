# Output layout

Where `build` writes a dataset, the columns every table shares, how identifiers are encoded, and the metadata each data file carries. [Reading the tables](reading-tables.md) covers queries.

## Output directory layout

`--output` is the dataset root, used exactly as given: `build` writes one
network's dataset directly into it. To name a directory after the network, put
the `{chain}` placeholder anywhere in the path or S3 key prefix:

| `--output` (mainnet endpoint) | Dataset root |
|---|---|
| `./output` | `./output` |
| `./output/{chain}` | `./output/mainnet` |
| `s3://ethereum-mainnet` or `s3://ethereum-mainnet/` | `s3://ethereum-mainnet` (the bucket root) |
| `s3://datasets/{chain}` | `s3://datasets/mainnet` |
| `s3://datasets/v1/{chain}/raw` | `s3://datasets/v1/mainnet/raw` |
| `./output/{{chain}}` | `./output/{chain}` (escaped braces) |

- `{chain}` expands to the endpoint's canonical `chain_name` from EndpointInfo.
  EndpointInfo with a nonempty chain name is required either way: the name is
  also recorded in the `firehose-parquet.chain_name` file metadata and in the
  protected dataset identity. It must be one path segment of ASCII letters,
  digits, `-`, `_` and `.`; every built-in network qualifies.
- `{chain}` is the only variable. `{{` and `}}` are literal braces. An unknown
  variable, an unterminated `{` or an unmatched
  `}` is an error before the endpoint is contacted.
- The S3 bucket name is literal, so `s3://{chain}/...` is refused. Credentials,
  the `S3_BUCKET` check and the bucket-wide owner are settled per bucket before
  the endpoint is contacted. Put the placeholder in the key prefix.
- An S3 root drops trailing `/` separators, so `s3://bucket/` and `s3://bucket`
  are the same bucket root. A local root is used as given.
- The resolved root is bound when the dataset is created. A later run whose
  `--output` resolves inside or above the dataset is refused before any Blocks
  request (`selected ingestion output overlaps another protected root`). For
  example, a dataset created at `s3://b` cannot be resumed as `s3://b/{chain}`,
  and one created at `s3://b/{chain}` cannot be resumed as `s3://b`. Any other
  root is a different dataset and must start empty.
- v0.7.x appended `<chain_name>` to `--output`. To keep that layout, add
  `/{chain}`: `--output ./output` becomes `--output './output/{chain}'`.

```
<root>/
├── .fireparq-ingest/          # authoritative checkpoint and transaction journal (do not edit)
├── _fireparq/                 # fireparq's artifacts, never table data
│   └── cursor.parquet         # optional mirror of the checkpoint (absent with --cursor none)
├── blocks/                    # one Delta table
│   ├── _delta_log/            # its log: the table's only file index
│   │   ├── 00000000000000000000.json
│   │   └── ...
│   ├── date=2026-02-25/
│   │   ├── part-v1-<stream>-<first>-<last>-<txn>-<index>.parquet
│   │   └── part-v1-<stream>-<first>-<last>-<txn>-<index>.parquet
│   └── date=2026-02-26/
│       └── part-v1-<stream>-<first>-<last>-<txn>-<index>.parquet
├── transactions/
│   └── ...
└── logs/
    └── ...
```

`build` names each part deterministically from its stream, the first and last
accepted event of its transaction, the transaction ID and the part index.

The dataset root holds only the table directories, `_fireparq/` and
dot-prefixed control state (`.fireparq-ingest/`, and at a bucket root the
`.fireparq-owner-v1.json` record and `.fireparq-owner-probes-v1/`). Readers
open one table at a time through its Delta log, so fireparq's own files are
never read as table data. Each table directory holds its `_delta_log/`, the
`date=` directories of its data files and, while a flush is in progress,
hidden `.fireparq-txn-*.tmp` staging files that no log references.
Releases before v1.0.0 wrote `cursor.parquet` at the dataset root; a dataset
whose mirror was bound at that old default keeps `--cursor cursor.parquet`.

Every table is partitioned by UTC day: `build` writes its data files to
`<table>/date=YYYY-MM-DD/part-*.parquet`, and there is no other layout. `date`
is the table's Delta partition column: the log records it for every file, and
the data files do not store it (#643). It comes from the same whole-second
block time as each row's `timestamp`, and `build` refuses to write a row whose
time is in another day. (Earlier releases wrote `year=YYYY/month=MM/` and a
`date=DD` or `day=DD` day of the month. v1.0.0 writes only
`date=YYYY-MM-DD`.)

## Single-network buckets

With one bucket per network, the bucket root is the dataset root:

```bash
OUTPUT=s3://<bucket> fireparq build --network mainnet
# s3://<bucket>/.fireparq-ingest/, s3://<bucket>/_fireparq/cursor.parquet,
# s3://<bucket>/<table>/_delta_log/, s3://<bucket>/<table>/date=YYYY-MM-DD/part-*.parquet
```

- Other commands take the root or its tables directly:
  `recovery status s3://<bucket>`, `validate s3://<bucket>/blocks`, and
  `inspect` on one file such as `s3://<bucket>/_fireparq/cursor.parquet`.
- The bucket root then lists only the table prefixes, `_fireparq/` and the
  dot-prefixed control state. Read each table with
  `delta_scan('s3://<bucket>/<table>')` or `pl.scan_delta`.
- The first `build` needs an empty bucket; only the bucket owner record may
  already exist there. S3 ownership is
  bucket-wide in any case, so a bucket per network also gives each concurrently
  running `build` its own owner.
- To keep several networks in one bucket instead, use
  `--output 's3://<bucket>/{chain}'`. They then share the bucket-wide owner, so
  their mutating commands run one at a time.

## Canonical identity columns

Every table across all chains includes these 7 columns (from Firehose `BlockMetadata`):

| Column | Type | Description |
|---|---|---|
| `block_num` | long | Block number |
| `block_id` | string | Block ID (format depends on block type; see [Output Encoding by Block Type](#output-encoding-by-block-type)) |
| `parent_num` | long | Parent block number |
| `parent_id` | string | Parent block ID |
| `lib_num` | long | Last irreversible block number |
| `timestamp` | timestamp | Block time, UTC. Parquet logical type `TIMESTAMP(MICROS, isAdjustedToUTC=true)`; keeps sub-second precision to the millisecond where the chain has it (e.g. Antelope's 500 ms blocks) |
| `date` | date (partition) | UTC day of the block time: the `date=YYYY-MM-DD` directory, not stored in the files |

The `date=YYYY-MM-DD` directory is derived from the whole-second block time, so a block at `23:59:59.500` lands in the same day as one at `23:59:59.000`. Solana tables keep `timestamp` null when `block_time` is missing; such rows are routed to the day of the last known block time, which is their `date`.

## Output encoding by block type

`fireparq` determines output encoding from the resolved block type/profile. Operators do not need to set a separate encoding flag. The effective values written for each file are exposed in Parquet metadata as `firehose-parquet.bytes_encoding` and `firehose-parquet.block_id_encoding`.

| Block type / profile | Block encoding | Transaction / hash encoding | Address / other binary field encoding | Notes |
|---|---|---|---|---|
| `evm` | `hex_0x` | `hex` | `hex` | `block_id` is `0x`-prefixed hex. Transaction hashes, log topics, and addresses are `0x`-prefixed hex. |
| `bitcoin` | `hex_0x` | Upstream text | Upstream text | Canonical IDs use `0x`-prefixed hex. Native hashes/txids/scripts/witnesses remain the original protobuf strings, normally Bitcoin Core hex without `0x`; addresses keep their native text format. |
| `solana` | `base58` | `base58` | Identifiers: `base58`; payloads: `binary`; indices: `array<short>` | Block IDs and binary identifiers stay base58. Instruction/error/return payloads and account-index lists have fixed types; see [Solana Payloads](chains/solana.md#payloads-and-account-indices). |
| `near` | `base58` | `base58` | `base58` | Block IDs, transaction hashes, receipt IDs, and key-like binary fields stay base58. `receipt_actions.args` is raw `binary` under every encoding. |
| `antelope` | `hex_no_prefix` | `hex_no_prefix` | `hex_no_prefix` | Uses lowercase hex without `0x` for both block IDs and other binary fields. |
| `cosmos` | `hex_0x` | `hex` | `hex` | Block IDs are `0x`-prefixed hex. Other binary identifiers are `0x`-prefixed hex. |
| `tron` | `hex_no_prefix` | `hex_no_prefix` | `tron_base58` for addresses; `hex_no_prefix` for other binary fields | Address-like fields use Tron Base58Check. Canonical hashes, topics, and other non-address bytes remain lowercase hex without `0x`. |
| `beacon` | `hex_0x` | `hex` | `hex` | Block roots and other binary identifiers are `0x`-prefixed hex. |
| `tron-evm` (`evm` Tron-style profile) | `hex_no_prefix` | `hex_no_prefix` | `tron_base58` for addresses; `hex_no_prefix` for other binary fields | Same operator-facing contract as `tron`: address-like fields use Tron Base58Check, while canonical hashes/topics stay lowercase hex without `0x`. |

## Parquet file metadata

Every Parquet file `build` writes embeds key-value metadata in the file footer under the `firehose-parquet.*` namespace. This allows consumers to identify the source pipeline, encoding, and chain without external sidecar files. The files that the [maintenance job](delta-maintenance.md)'s OPTIMIZE writes do not carry it: a table's identity is in its Delta table properties (`fireparq.descriptor`, `fireparq.chain`, `fireparq.blockType`), which every `build` start checks.

| Key | Example Value |
|---|---|
| `firehose-parquet.version` | `1.0.2` |
| `firehose-parquet.block_type` | `evm` |
| `firehose-parquet.bytes_encoding` | `hex` |
| `firehose-parquet.endpoint` | `https://eth.firehose.pinax.network:443` |
| `firehose-parquet.chain_name` | `eth-mainnet` |
| `firehose-parquet.chain_name_aliases` | `ethereum,eth` |
| `firehose-parquet.first_streamable_block_num` | `0` |
| `firehose-parquet.first_streamable_block_id` | `0x0000...` |
| `firehose-parquet.block_id_encoding` | `hex_0x` |
| `firehose-parquet.block_features` | `extended,base` |
| `firehose-parquet.compression` | `zstd` |
| `firehose-parquet.partition` | `date` |
| `firehose-parquet.synthetic_timestamps` | `true` |
| `firehose-parquet.synthetic_timestamp_policy` | `last_known_partition_routing` |
| `firehose-parquet.with_votes` | `true` |

Table files carry the pipeline, chain, encoding and compression keys.
`partition` (always `date`) is written to `_fireparq/cursor.parquet`, which also
records `extended`, `final_blocks_only` and `include_failed_transactions`. `with_votes` appears on Solana output, and the
`synthetic_*` keys only when synthetic routing is used.

`firehose-parquet.bytes_encoding` and `firehose-parquet.block_id_encoding` describe the emitted output contract, not just the upstream Firehose endpoint. See [Output Encoding by Block Type](#output-encoding-by-block-type) for the operator-facing defaults by supported chain/profile.

On Solana, the `firehose-parquet.synthetic_*` metadata keys mark routing as
using a synthetic last-known timestamp anchor: canonical `timestamp` stays
chain-sourced and null when `block_time` is missing, and such a row's `date` is
its routing day.

Endpoint `block_id_encoding` remains a fallback only when the chain does not resolve to a known block-type/profile contract.

### Reading metadata

```python
import pyarrow.parquet as pq

meta = pq.read_metadata("output/mainnet/blocks/date=2026-01-15/part-v1-<...>.parquet")
for i in range(meta.metadata.count()):
    key = meta.metadata.keys()[i]
    if key.startswith("firehose-parquet."):
        print(f"{key} = {meta.metadata.values()[i]}")
```

```sql
-- DuckDB: one data file, named by the table's log (never a glob)
SELECT key, value
FROM parquet_kv_metadata('output/mainnet/blocks/date=2026-01-15/part-v1-<...>.parquet')
WHERE key LIKE 'firehose-parquet.%';
```

## Parquet lookup metadata

Ingestion writes bounded Bloom filters for selected scalar
hash, signature and account/address columns. Readers that support these filters
can skip row groups for equality lookups; positive matches still require row
filtering. Filters do not answer `IS NULL` predicates. Row groups contain at most
65,536 rows, with at most eight filters per group. This changes physical layout,
not table schemas or row order. Dictionary encoding retains its existing policy.
The retained-data benchmark measured 0.74–1.92% larger files and faster missing-key
lookups; readers without Bloom pruning may only see the storage overhead.

Complete ingestion parts declare ascending `block_num` only when every observed
height proves that order. Ingestion neither sorts nor reconstructs
reversible-chain history. The maintenance job's compacted files keep neither the
Bloom filters nor the sort metadata unless its `WriterProperties` enable them;
the `add.stats` of every file (`block_num` and `timestamp` bounds) still prune
reads.

Use `--compression zstd:6` to select an explicit Zstandard level; `zstd` and
`zstd:3` retain level 3. Zero is rejected as ambiguous. Explicit non-default
levels participate in protected transaction identity; recover any pending work
with a supporting version before downgrading. See the
[lookup properties and measurements](audit/519-parquet-lookup-properties.md).
