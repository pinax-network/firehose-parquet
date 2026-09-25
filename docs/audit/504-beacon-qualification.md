# Issue #504: Beacon mapping qualification

## Outcome

The two remaining live-data gaps in PR #560 are qualified. Two exact mainnet
slots, selected from public explorers before making Firehose requests, contain
one Deneb BLS-to-execution change and one Fusaka attester slashing. Their mapped
Parquet rows match the decoded Firehose protobuf, including byte values and the
complete, ordered attesting-index lists. No production mapping changes were needed.

This supplements the PR's previous live Capella and Fusaka comparisons for
withdrawals, execution requests, graffiti, and committee bits. It does not claim
exhaustive historical coverage or add Capella BLS changes: the source
`CapellaBody` protobuf still does not expose that field.

## Source and isolation

- Original PR head: `26d96f90f730185cc07faff0965be8064076dd3c`.
- Isolated branch: `codex/beacon-live-qualification`. The original Claude checkout
  and local `audit/504-beacon-coverage` branch were preserved at that head.
- Live sample binary: `8c22970bafd63c3432ad4b40b261dcc0d90e00b4`, merging the
  original PR with main `62d7b10f0cc95a55cd619a4141b7d41f97e728e5`.
- Full-test integration: `5e703dcd995b6b8505fb92e365f437d3e06744a4`, including main
  `f7fb4df5e8d51782c66a78f339a66de09c408c17` (#573 S3/cursor resolution and #574
  audit documentation). Those merges do not change Beacon mapping or schemas.
- Final current-main integration: `55d13d0af636f77c9c084e6330c857620f0c052b`,
  including main `c25b9e9262a3609fa396b4496291485404436432`. The additional #575
  changes are documentation and EVM schema comments only; runtime code is unchanged.
- Qualification: 2026-09-25, approximately 15:40–15:43 UTC.

## Target selection and bounded requests

| Target | Public reference | Source result |
|---|---|---|
| Slot 10597349, 2024-12-12 08:30:11 UTC | [beaconcha.in slot 10597349](https://beaconcha.in/slot/10597349), indexed result identifying one BLS address change | `DENEB`, one `blsToExecutionChanges` entry |
| Slot 15038051, 2026-08-21 02:50:35 UTC | [Beaconscan slashing list](https://beaconscan.com/slots-slashed) and [slot 15038051](https://beaconscan.com/slot/15038051) | `FUSAKA`, one `attesterSlashings` entry; validator 1731581 |

The direct beaconcha.in page returned HTTP 403 during the follow-up; its indexed
result supplied the precise candidate, and the actual Firehose response
confirmed the event. Beaconscan's direct page confirmed the second candidate.
No broad Firehose ranges or scans were used.

The destination was only `eth-cl.firehose.pinax.network:443`. Each subprocess
received the intended Pinax key through `PINAX_API_KEY`, plus `PATH`. Ambient
bearer tokens, the repository `.env` token, and StreamingFast credentials were
not supplied. No credential values or raw private resume cursors are included
in this evidence.

For each target `N`, exactly two block-stream requests were made:

1. `grpcurl` decoded a single response with the repository's `firehose.proto`
   and `beacon.proto`; request fields were `final_blocks_only=true`,
   `start_block_num=N`, `stop_block_num=N`. Each call had a 25-second deadline.
2. A copied, isolated `fireparq` binary ran `build --network mainnet-cl
   --start-block N --stop-block N+1 --final-blocks-only --output <fresh-local-dir>`.
   CLI stop is exclusive; the gRPC helper sends `N`, so this also requests only
   the selected slot. Both runs exited successfully without retries or probes.

Raw responses contained respectively 1,225,236 and 145,970 bytes of decoded JSON
before removing the cursor. Both had exactly one response. Fresh output
directories contained exactly one `blocks` row each.

## Comparison

[504-compare-beacon.py](504-compare-beacon.py) performs an offline comparison of
cursor-stripped source JSON with DuckDB reads of actual Parquet files. Run:

```sh
python3 docs/audit/504-compare-beacon.py <evidence-directory>
```

Input names are `raw-10597349.json`, `raw-15038051.json`, `out-10597349/`, and
`out-15038051/`. The script performs no network requests. It verifies exact row
multiplicity and column sets, converts protobuf base64 bytes to output hex,
normalizes timestamp time zones, and compares lists without sorting or
deduplicating them. Protobuf-omitted scalar defaults are handled explicitly
(`committeeIndex=0`).

| Parquet data | Raw rows | Parquet rows | Compared columns | Mismatches |
|---|---:|---:|---:|---:|
| Deneb block identity/body summary | 1 | 1 | All 17 | 0 |
| BLS-to-execution changes | 1 | 1 | All 13 | 0 |
| Fusaka block identity/body summary | 1 | 1 | All 17 | 0 |
| Attester slashings | 1 | 1 | All 25 | 0 |

The seven canonical columns are block number/ID, parent number/ID, LIB number,
timestamp, and UTC date. The script also verifies response metadata against the
decoded block slot, roots, parent slot, and timestamp.

The BLS row checks slot, zero-based change index, validator index (`44051`),
48-byte BLS public key, 20-byte execution address, and 96-byte signature.
The slashing row checks slot, zero-based slashing index, and both attestations'
slot, committee index, beacon block root, source epoch/root, target epoch/root,
and complete attesting indices. Ordered lists contain **438** and **1** entries;
their intersection is exactly **[1731581]**, matching Beaconscan's validator.
Indexed-attestation signatures are not existing Parquet columns and are not
claimed as mapped by this PR.

Both block rows additionally match proposer, parent slot, block/parent/state/body
roots, signature, fork name, and graffiti. Each target table has one row in its
sample; these samples do not demonstrate multi-entry BLS ordering.

Cursor-stripped raw JSON SHA-256 values (local generated evidence, not committed):

- Slot 10597349: `056949b5d8f87fd805bc91938016d9b97ab86aaa2c5fe91bc10a6a5d53c65be3`.
- Slot 15038051: `5f116ae270d20e0543917b820ba69018beb5c7ee2775de8529b0619f03c8e978`.

## Current-main validation

Cargo commands use a whole-process file lock around the shared target directory
and `CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0`, with `-j4 --locked`.
The live executable was copied while holding that same lock, so another branch's
build could not replace it between compilation and copying.

- `cargo test --workspace --locked -j4`: **722 passed, 0 failed, 3 ignored**
  (117 + 186 + 1 + 415 + 3). Doc tests also passed.
- `cargo build --workspace --locked -j4`: passed.
- `cargo fmt --all --check`: passed.
- Offline comparison script: all four row comparisons passed as above.
- `git diff --check`: passed.

The build retains the existing unused `transactions_processed` assignment
warning in the terminal ingestion path. No new warning or production-source
change was introduced by this qualification follow-up.
