# Beacon notes

Semantics of the Beacon tables beyond their columns, which the [Beacon schema reference](../schemas/beacon.md) lists.

## Beacon chain tables

Each Beacon table gets rows from the fork that introduced its data. Blocks from earlier forks add no rows to it, so a range from before that fork writes no file for the table. The [Beacon schema reference](../schemas/beacon.md) lists every column; the table below gives each table's source and first fork.

| Table | Rows from | Source |
|---|---|---|
| `blocks` | Phase0 | Block header, plus the body's `graffiti` |
| `attestations` | Phase0 | `body.attestations`; `committee_bits` from Electra |
| `deposits` | Phase0 | Deposits from the Eth1 bridge (`body.deposits`) |
| `proposer_slashings`, `attester_slashings`, `voluntary_exits` | Phase0 | `body.proposer_slashings`, `body.attester_slashings`, `body.voluntary_exits` |
| `execution_payload` | Bellatrix | `body.execution_payload` |
| `withdrawals` | Capella | `execution_payload.withdrawals` (up to 16 per block) |
| `bls_to_execution_changes` | Deneb | `body.bls_to_execution_changes`. The Firehose Capella body has no such field, so changes included in Capella blocks are not available. |
| `blob_sidecars` | Deneb | `body.embedded_blobs` |
| `deposit_requests` | Electra | `execution_requests.deposits` (EIP-6110) |
| `withdrawal_requests` | Electra | `execution_requests.withdrawals` (EIP-7002) |
| `consolidation_requests` | Electra | `execution_requests.consolidations` (EIP-7251) |

`execution_payload.base_fee_per_gas` is an exact unsigned decimal string in wei
per gas. It is independent of the selected byte encoding; use a checked numeric cast for
arithmetic (the full uint256 range needs up to 78 decimal digits).
`blob_sidecars.blob` is always `binary`; hashes, roots, commitments, and proofs
retain the selected byte encoding. `blocks.spec` is a `string` of generated enum
names, with `UNKNOWN` for unrecognized numeric values.
Missing nested messages produce null descendants, while present zero values
and empty byte/list values remain present. Absent bodies or execution payloads
produce no child rows.

These schema changes require a fresh output root or a verified conversion of
existing files. Old fee bytes have different byte orders by payload type:
Bellatrix/Capella are fixed little-endian, Deneb and later are big-endian.
Historical fake zeros for missing messages cannot be repaired without source
replay. See [the producer evidence and migration procedure](../audit/505-beacon-values.md).

Amounts (`amount`) are in Gwei. `block_slot` joins `blocks.slot`. `withdrawals.withdrawal_index` is the chain-wide withdrawal index; `change_index` and `request_index` are positions within the block.

- **Attestations after Electra.** EIP-7549 moved the committee out of the signed data: `committee_index` is always `0`, and `committee_bits` (8 bytes, a 64-bit bitvector) says which committees an aggregate covers. Bit `i` is bit `i % 8` of byte `i / 8`. `aggregation_bits` then spans those committees in index order. `committee_bits` is null before Electra.
- **Deposits after Electra.** New deposits reach the chain as `deposit_requests`, whose `deposit_index` is the deposit contract index (the `index` of EIP-6110). `deposits` only holds deposits from the Eth1 bridge, which stop once its backlog is processed. `deposits.deposit_index` is the position within the block.
- **Withdrawal requests.** `amount` `0` requests a full exit; any other value is a partial withdrawal.
- **Consolidation requests.** A request whose `source_pubkey` equals its `target_pubkey` switches the validator to compounding withdrawal credentials.
- **Attester slashings.** `attestation_1_attesting_indices` and `attestation_2_attesting_indices` (`array<long>`) are the two conflicting attestations' validators. The slashed validators are in both lists:

  ```sql
  SELECT block_slot, list_intersect(attestation_1_attesting_indices, attestation_2_attesting_indices) AS slashed
  FROM delta_scan('output/mainnet-cl/attester_slashings');
  ```

- **Graffiti.** `blocks.graffiti` is the proposer's raw 32 bytes, usually zero-padded text. It is null only for a block without a body.
