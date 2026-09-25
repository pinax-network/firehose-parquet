# NEAR public-source qualification of recovered PR #559 (#506)

Status: the single approved public capture and independent offline comparison
passed. Full repository validation and final review are in progress. The earlier
single Firehose request remains quota-blocked and was not retried. #506, #507 and
#550 remain open until their respective acceptance/merge gates are satisfied.

## Recovery and current compatibility

The original PR head `8056a07d358bf1ddbe9a091d862377fd26cae478` was merged into a
new isolated checkout based on main `5de4f16`. The original Claude checkout and
branch remain clean at that exact head. The preserved original binary patch
(`git diff 8056a07^ 8056a07 --binary`) has SHA-256
`a608b627ac404c3d98c6d14e9087265aab02ab9642c3f4994cedbf54adedc620`.
Recovery merge `4ebdb8e` keeps current fallible identity preparation, owned Bytes
mapping, complete seven-table estimates, protected-output schema inventory, and
all subsequent unrelated main changes. Adaptations are byte-buffer fixture
conversions and combining documentation; the intended #506 row semantics remain.

The new columns and tables require a fresh output dataset or explicit rebuild.
Protected ingestion binds its complete schema inventory and refuses to resume an
incompatible existing dataset. Cross-block lineage remains unknown without the
required earlier transactions/receipts. The new tables preserve receipt rows
independently of transaction filtering; they do not prove that every submitted
action executed successfully. #550's outcome/filter policy remains separate.

## Selected source and exact scope

NEAR Lake S3 is excluded: its documented access uses AWS credentials and
Requester Pays. No S3 object/list/probe was requested. The proposed alternative
is FASTNEAR's documented unauthenticated finalized full-block endpoint, which
preserves the block and shard indexer contexts rather than substituting later
aggregate transaction status:

- [NearData server](https://github.com/fastnear/neardata-server/tree/7324d9131e7e8285306606ba05822b007e44c53a): README and `src/api.rs` document finalized `/v0/block/{height}` and archival redirects.
- [BlockWithTxHashes](https://github.com/fastnear/libs/blob/8d9142c0bf86ea1c3f884bf362fd8bc127c69f92/primitives/src/block_with_tx_hash.rs): original block/shard context plus an optional externally enriched transaction hash.
- [nearcore indexer types](https://github.com/near/nearcore/blob/74a6829e947512019773059d8b5905b4e6cb6236/chain/indexer-primitives/src/lib.rs): chunk receipts can come from a previous chunk; they are not substituted for executed receipt outcomes.
- [Lake S3 reader](https://github.com/near/near-lake-framework-rs/blob/e611fc888b3bcf579b21943e93b4a492484af23a/lake-framework/src/s3_fetchers.rs): explicit `RequestPayer::Requester`.
- [Official RPC providers](https://docs.near.org/api/rpc/providers): the exact public archival RPC used solely for the LIB hash-to-height lookup.

The independently reviewed capture permits at most three sequential HTTP
application request attempts:

1. GET `https://mainnet.neardata.xyz/v0/block/150000000`.
2. Only if that returns exactly HTTP 302, one manual GET to its Location, whose
   host must match `a[0-9]+.mainnet.neardata.xyz`, HTTPS port 443, with the exact
   same `/v0/block/150000000` path and no userinfo/query/fragment. A second redirect
   or any other destination stops the capture.
3. Only after the full document and its complete supported producer projection
   validate, one JSON-RPC `block` POST to `https://archival-rpc.mainnet.near.org`.
   Its `params.block_id` is the document's exact nonzero `last_final_block` hash.
   The reply must identify that hash at a strictly older height. This supplies
   the production reader's LIB enrichment; it is not a second selected sample.

Missing/zero parent height stops the capture: no extra parent lookup is allowed.
No retries, fallback, health/head probes, adjacent blocks, transaction-status
calls, Firehose calls, extra shard reads, batch requests or credentials are used.
Failure does not authorize another sample.

## Actual transport controls

[506-near-public.py](506-near-public.py) uses direct stdlib `HTTPSConnection`
with verified TLS, no proxy resolution, cookie jar, netrc, auth or retry layer.
The TLS context does not honor ambient `SSLKEYLOGFILE`. Only fixed Accept,
Accept-Encoding, Connection and optional JSON Content-Type headers are sent.
HTTP redirects are handled explicitly by the above three-call state machine.
Unexpected content encoding, transfer encoding, duplicate HTTP headers or
ambiguous Content-Length/Transfer-Encoding framing fail closed.

The `capture` command launches one worker through `subprocess.run(timeout=60)`;
that supervisor kills and reaps a stalled worker even if DNS is blocked inside a
C call. The worker also has an overall 60-second SIGALRM and each request has a
20-second deadline/alarm covering connection, headers and body. Reads use
`read1`, accumulate at most the cap plus one rejection byte, and enforce:
32 MiB full document, 64 KiB redirect body, 2 MiB anchor response. Large declared
Content-Length, truncated bodies and streamed cap breaches all fail.

Before a network attempt and after its status/headers, an atomic local JSON
checkpoint records the stage, exact URL/method, bounded request count and UTC
times. Failure records contain exception class and status, without cookies,
authorization or response previews. A killed worker leaves its last complete
attempted-stage record. Bounded raw document/anchor bytes and SHA-256/size are
retained even if a later conversion fails. No failure path makes another call.
The hard supervisor deadline is not extended to finish evidence writes.

## Exact producer projection and its losses

The converter follows
[near-firehose-indexer codec](https://github.com/streamingfast/near-firehose-indexer/blob/144071c93685c057293459329e9ca35b07aba641/src/codec/mod.rs)
and the
[Firehose reader](https://github.com/streamingfast/firehose-near/blob/98d869ceab00a764a9addb18053aecf4c5ae1f33/codec/consolereader.go).
It validates duplicate-free JSON with exact integer/decimal values, fixed-size
base58 IDs/keys/signatures, canonical base64, u64/u32/u128 bounds, consistent
nanosecond clocks, complete shard/header/mask membership and unique transaction
and receipt-outcome IDs. Receipt/outcome IDs and transaction/outcome IDs must
agree, and optional transaction receipts must appear among produced IDs.
Source outcome block hashes are copied, not rewritten to the selected block.

Every supported header, chunk, transaction, action, receipt and execution-outcome
field is projected to the repository's protobuf descriptor. Unknown variants or
fields in the constructed protobuf are errors. The nine local action variants,
including nested non-delegate actions, and the local structured/enum error types
are supported. Newer producer global-contract/gas-key/MLDSA variants absent from
the local protobuf fail explicitly; they are not quietly reinterpreted.

Intentional source losses are reproduced, not silently repaired:

- BigInt is fixed 16-byte **big-endian unsigned u128**, including zero.
- At selected height 150000000, receipt outcomes are sorted by decoded ID bytes
  per shard, because the pinned producer restores that ordering below193444226.
  Other list/shard order is preserved.
- `state_changes` is always empty: the pinned producer drops per-shard changes.
  Raw NearData changes remain in the source document. This cannot close #507.
- NearData's enriched receipt `tx_hash` is not a Firehose field and is ignored by
  conversion. It cannot make the mapper's same-block-only lineage look complete.
- `local_receipts`/`instant_receipts` do not enter the producer's chunk receipt
  list. Null approval signatures are dropped; execution metadata becomes V1;
  block ordinal and epoch-sync-data bytes remain their producer defaults.
- ReceiptData `null` and empty base64 both become protobuf empty bytes, exactly
  like the producer's `data.unwrap_or(vec![])`. The distinction survives only in
  the original JSON. Optional receipt message absence remains distinct.
- Error-detail reductions follow the producer, including an empty account ID for
  DeleteAccountStaking and default DelegateActionAccessKeyError. Unsupported
  top-level/nested error categories stop conversion.
- `last_ds_final_block_height` stays zero. `last_final_block_height` comes only
  from the exact older-hash anchor reply, never from height-minus-two or a head.

The retained old producer golden JSON files are already converted, with no
transaction/receipt events in the retained block set. They do not provide an
independent original-JSON oracle for the affected tables. Synthetic fixtures are
labeled accordingly; no live-event coverage is claimed from those files.

## Offline evidence before capture

At recovery merge `4ebdb8e` plus this audit tooling:

- 17 focused NEAR mapper tests pass: all action kinds, receipt/log joins and
  indices, failed-transaction position gaps, null same-block origins, raw args,
  encoding, flush/reset, and cross-block cache absence.
- Six cross-chain schema-contract tests pass, including the expanded seven-table
  NEAR fixture across every supported encoding, Parquet round trips and owned vs
  borrowed protobuf mapping.
- 16 offline Python tests pass: every supported action; descriptor serialization;
  receipt-data null/empty behavior; selected structured failures and every local
  reduced invalid-transaction/function/receipt error; integer/encoding bounds;
  ID/shard/mask rejection; duplicate JSON/numeric rejection; exact two/three-call
  plan; forbidden and second redirects; no proxy/cookie replay; streamed/header
  limits; truncation; real request/process alarm; hard supervisor kill; partial
  anchor-failure and forcibly killed-worker evidence.

Reproduction (requires `uv`, a cached protobuf environment and `protoc`; for first setup, omit `--offline` once to resolve the Python dependency):

```sh
uv run --offline --with protobuf python docs/audit/506-near-public-tests.py
cargo test --locked -p blocks near:: --lib -j4
cargo test --locked -p blocks schema_contract_tests --lib -j4
cargo fmt --all -- --check
```

Shared-target Cargo runs in this audit are serialized under a whole-command
lock. No new external chain request is part of these tests. The coordinator independently reran all 16 tests at tooling commit `0ac6643`,
reviewed the final transport/provenance code, and released exactly the stated
capture. No further data read is authorized by missing coverage.

## Executed capture: exactly three requests

The normal supervised `capture` command ran once, from 2026-09-25
22:15:43.618813 UTC to22:15:48.048240 UTC (4.43seconds). It received:

1. HTTP302 from the exact original height150000000 URL, with an empty body.
2. HTTP200 from the permitted `https://a2.mainnet.neardata.xyz/v0/block/150000000`
   archive URL, with334,930 original JSON bytes.
3. HTTP200 from the exact archival RPC, with16,891 bytes for the source's exact
   `last_final_block` hash. Its verified height is149999998; the source parent
   height is149999999.

There were no retries, fallback, credentials, additional selected heights or
Firehose calls. All subsequent work is offline. Raw artifacts and request
provenance are retained at
`/tmp/fireparq-506-neardata-150000000-20260925`:

| Artifact | Bytes | SHA-256 |
|---|---:|---|
| `neardata.json` |334930|`794e00c1c14d952e2e2f84c05807c6cd68b913cc61e818bd981d0be6d6a9220c`|
| `lib-anchor.json` |16891|`b3757b2ed5458321bc484e6fd98fffed9e4b0b2d6c7cd92d981b9cce2e3dee14`|
| `block.pb` |108483|`8bec28c855cc0f4b95e59bcbec36cb3621669f0479c2c12dd06689ecee7afca5`|

The eight shards all have chunks. The21 transaction inclusion outcomes are
literally `SuccessReceiptId`; this is not an aggregate claim about the later
receipt tree. The70 receipt outcomes are68 `SuccessValue`,1 `SuccessReceiptId`,
and1 `Failure`. The failure is `ActionError::DelegateActionInvalidNonce` at
index0, with no logs. All70 executed receipts are Action receipts:2 AddKey,
32 Transfer,15 Delegate and21 FunctionCall actions. Their54 logs include42
NEP-141 events:8 `ft_transfer` and34 `ft_mint`. The README event query, with only
its local file paths adapted, returns exactly those counts.

**No receipt has an origin derivable from a transaction in this same block.**
All70 new mapper tx_hash values therefore stay NULL, despite all70 NearData
records containing externally enriched hashes. Positive same-block lineage,
cross-block unresolved lineage, Data receipts, failed transaction inclusion,
and the other action kinds have synthetic fixture coverage only. The221 native
per-shard state changes remain in raw JSON but are deliberately absent from the
producer-compatible protobuf and Parquet. They do not qualify #507.

## Independent comparison and current integration

Recovery is integrated with actual main `81f5b79` as `e8a3347`. A separate
current-main baseline checkout at81f5b79 and the candidate both use the same
[offline replay example](../../blocks/examples/replay_near.rs), with no baseline
production edits. Stable binaries were copied inside the Cargo lock before
running. Each reads only a saved protobuf and writes a fresh local directory.

[506-compare-near-public.py](506-compare-near-public.py) derives expected rows
from original NearData JSON and the exact anchor reply. It does not import the
converter, read the converted protobuf for expected values, or derive expected
row selection/positions from mapper output. It independently applies the pinned
legacy receipt-ID ordering and explicitly checks the ignored external tx_hash
and state-change boundaries.

For every one of five encodings × two transaction filters × optional fork-step
column settings (20cases), the live comparison passes:

- 4,480 row occurrences and83,920 raw-source value comparisons across the seven
  declared tables (state_changes empty).
- 29,800 existing-column value comparisons against main81f5b79. Every legacy
  Arrow field, order, type, nullability and metadata is preserved when the new
  fields are projected out.
- Actual physical Parquet schemas on every nonempty baseline/candidate part,
  including recursive List child nullability, dictionary indices/value types,
  UTC millisecond timestamps, and field/schema metadata. Empty tables have
  manifest-schema checks only.
- No rows retained after mapper flush.

Report: `/tmp/fireparq-506-live-comparison.json`; local datasets:
`/tmp/fireparq-506-live-before` and `/tmp/fireparq-506-live-after`.
The synthetic all-action/data-receipt document also passes converter→mapper→
Parquet comparison over20cases:360row occurrences,6,780raw-source values and
1,570legacy values (`/tmp/fireparq-506-synthetic-comparison.json`). Four separate
oracle/schema tests check exact sample expectations, same-block origins versus
filtering, raw args under all encodings, and rejection of nested type/nullability
or metadata changes.

```sh
uv run --offline --with protobuf --with pyarrow python docs/audit/506-compare-near-public-tests.py
cargo run --locked -p blocks --example replay_near -- --block /path/block.pb --output /fresh/output
uv run --offline --with pyarrow python docs/audit/506-compare-near-public.py \
  --before /path/baseline --after /path/candidate --raw /path/raw --report /path/report.json
```

## Acceptance boundary

The live-block mapper comparison passes under the explicitly reviewed
NearData/indexer-JSON plus pinned-producer-conversion boundary. It does not prove
Firehose transport/cursor behavior, globally complete lineage, #507 state-change
rows or #550 execution/filter semantics. Those remain separately open.
Current-main workspace tests/build/standard CI example and independent final
review remain required before publishing or merging the recovered PR.
