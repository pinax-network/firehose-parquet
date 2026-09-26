# Offline EVM golden-block regression (#499)

Issue: <https://github.com/pinax-network/firehose-parquet/issues/499>

## Diagnosis and implementation

The existing mapper tests largely construct synthetic protobuf messages. Earlier
live raw-to-output audits were temporary and could not protect later CI runs.
Retain one original finalized Ethereum mainnet block and a cursor-free public
metadata sidecar under `blocks/tests/fixtures/evm-mainnet/`. A fixed JSON oracle
records all 20 table counts and 283 selected values, with an original protobuf
source path on each of 28 selected rows.

The oracle was produced and reviewed from Python protobuf decoding of the raw
payload, before running the new Rust mapper test. It uses direct repeated-list
counts and the documented persistent-change rule for this block's single
reverted transaction. The initial inspection used the wrong guessed order for
its three balance-change enum names and Python's wrong spelling for the raw
`blockIndex` field. The raw descriptor/value inspection corrected those oracle
construction mistakes; the mapper implementation was unchanged. No expected
value was copied from a mapper result to make the test pass.

The test covers standard/extended output, Binary/Hex schemas and fork-step
presence. It validates the payload checksum, raw block/header identity against
metadata, exact counts and selected values, and canonical identity columns on
every emitted row. The selected values cover byte order, leading zeroes, decimal
integers, nulls, enums, blob hash lists, log indices, nested calls, withdrawals,
system changes and persistent gas charges on a reverted transaction.

The capture example uses existing provider-scoped auth and Firehose streaming,
with an exact one-block range, checked payload identity/type/time, a 16 MiB cap
and a 45-second deadline. It writes only into a new staging directory after a
successful bounded capture. It does not generate expected output or retain a
cursor. The documented refresh process requires independent raw-field review.

## Validation and limits

- One finalized block was read from the public Pinax mainnet endpoint, with no
  production output writes. Original payload: 2,094,485 bytes, SHA-256
  `dad74257d32c66a056404add2a5f3360c288faedf0a026e5698394cd9d231b2a`.
- The independent decode found 182 transactions, 557 receipt logs, 16 withdrawals
  and four system calls. The applicable selected values and exact table counts passed
  in all eight test configurations on the first mapper-test run (0.43 seconds);
  extended mode covers all 283 values.
- Eight tables have no source rows in this fixture. Their absence is checked,
  but nonempty mappings and other Ethereum forks still need appropriate fixtures
  or existing synthetic tests. This is the issue's requested EVM starting point,
  not live qualification of every chain or all upstream data shapes.
- Current-main integration (including #475) passed **788 workspace tests**, with
  four intentionally ignored tests, plus formatting, binary and capture-example
  builds. The golden test itself takes about half a second and needs no network.
- Independent review decoded the retained raw block again and verified all 20
  counts (5,049 extended rows), 283 selected values, the checksum and canonical
  identity/date. No oracle change was needed after the mapper test ran.
- Review caught a credential-routing defect in the new helper's implicit default
  selectors: a custom endpoint alone could have authorized ambient Pinax headers.
  Selectors are now optional with no defaults and the shared resolver scopes
  ambient credentials. An actual loopback subprocess regression passed with all
  four Pinax/legacy fixture variables present and no credential headers sent.
  CI explicitly runs this example test (one passed, one child fixture skipped
  by the parent harness and invoked separately). No live recapture was needed;
  the original bounded capture used the intended Pinax endpoint and an isolated
  environment. The final review has no remaining blockers.

## Follow-up: owned mapping path and EIP-7702 fixture

A later validation pass found two gaps. The test only called the borrowed
`map_block`, while production has mapped owned buffers through
`map_block_bytes` since #518/#606. The retained block also had no
`set_code_authorizations`, `code_changes` or `gas_changes` rows and no failed
`SET_CODE` transaction.

- Every configuration now maps each fixture through both entry points
  (`Bytes::from_static` for the owned path) and requires identical tables,
  schemas and rows before the oracle checks.
- `blocks/tests/fixtures/evm-mainnet-26000004/` retains mainnet block
  26,000,004, captured once with the refresh helper and an explicitly selected
  Pinax key. The 5,304,418-byte payload (SHA-256
  `ded7e47245becdf20d4b213a43d29cf3828140313e54fd8c849286b039178893`) is stored
  with `zstd -19` in 762,362 bytes. It has seven `SET_CODE` transactions with 16
  authorizations, 10 code changes, and reverted transaction 130 with two accepted
  authorizations from one authority.
- Its oracle (20 table counts, 14,001 extended rows, 304 selected values on 27
  rows) came from an independent Python protobuf decode and the documented
  persistence rules, retained as [`499-evm-golden-oracle.py`](499-evm-golden-oracle.py),
  which regenerates the committed file byte for byte. The mapper test passed
  against it on the first run. A
  targeted test checks transaction 130's raw shape and its persisted rows:
  three balance changes, three nonce changes (sender plus one per accepted
  authorization) and no code or storage changes.
- Neither fixture records a gas change in any call, so `gas_changes` still has
  no real-data coverage; synthetic tests remain the only check for that mapping.
