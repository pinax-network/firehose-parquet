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
  and four system calls. All 283 selected values and exact table counts passed
  in all eight test configurations on the first mapper-test run (0.43 seconds).
- Eight tables have no source rows in this fixture. Their absence is checked,
  but nonempty mappings and other Ethereum forks still need appropriate fixtures
  or existing synthetic tests. This is the issue's requested EVM starting point,
  not live qualification of every chain or all upstream data shapes.
- Full current-main integration and independent review are recorded before merge.
