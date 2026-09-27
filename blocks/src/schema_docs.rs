//! The Markdown schema reference in `docs/schemas/`, rendered from the
//! production mappers (`ChainKind::create_mapper`) so that it cannot drift from
//! the code.
//!
//! Regenerate it with `cargo run -p blocks --example dump_schemas`. The
//! `committed_schema_docs_match_the_code` test fails when the committed files
//! differ from what this module renders.
//!
//! Tables and columns come from an empty flush of each mapper. Descriptions are
//! the code comments beside the fields in `blocks/src/<chain>/schema.rs` (and
//! the canonical fields in `firehose_parquet::traits`), copied into the tables
//! below: update both together.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use arrow::datatypes::{DataType, Field, SchemaRef};
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::timestamp_millis_utc_type;

use crate::chain::{ChainKind, MapperOptions};

/// The command that rewrites `docs/schemas/`.
pub const REGENERATE_COMMAND: &str = "cargo run -p blocks --example dump_schemas";

/// The canonical identity columns every table starts with.
const CANONICAL_COLUMNS: [&str; 7] = [
    "block_num",
    "block_id",
    "parent_num",
    "parent_id",
    "lib_num",
    "timestamp",
    "date",
];

const FORK_STEP: &str = firehose_parquet::traits::FORK_STEP_COLUMN;
const STREAM_ORDINAL: &str = firehose_parquet::traits::STREAM_ORDINAL_COLUMN;

/// Columns that exist only on non-final streams, in their file order.
const NON_FINAL_COLUMNS: [&str; 2] = [FORK_STEP, STREAM_ORDINAL];

/// One rendered file of `docs/schemas/`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDoc {
    pub file_name: String,
    pub contents: String,
}

/// `docs/schemas/` in this workspace.
pub fn docs_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("the blocks crate is inside the workspace")
        .join("docs/schemas")
}

/// Render `README.md` and one file per chain family.
pub fn render_all() -> Result<Vec<SchemaDoc>> {
    let references = ChainKind::ALL
        .into_iter()
        .map(chain_reference)
        .collect::<Result<Vec<_>>>()?;
    check_descriptions(&references)?;

    let mut docs = vec![SchemaDoc {
        file_name: "README.md".to_string(),
        contents: render_index(&references),
    }];
    docs.extend(references.iter().map(|reference| SchemaDoc {
        file_name: format!("{}.md", reference.kind.label()),
        contents: render_chain(reference),
    }));
    Ok(docs)
}

/// Render every file into `dir`, returning the paths written.
pub fn write_all(dir: &Path) -> Result<Vec<PathBuf>> {
    std::fs::create_dir_all(dir).with_context(|| format!("create {}", dir.display()))?;
    render_all()?
        .into_iter()
        .map(|doc| {
            let path = dir.join(&doc.file_name);
            std::fs::write(&path, doc.contents)
                .with_context(|| format!("write {}", path.display()))?;
            Ok(path)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Descriptions harvested from the code comments beside each field
// ---------------------------------------------------------------------------

fn chain_title(kind: ChainKind) -> &'static str {
    match kind {
        ChainKind::Evm => "EVM",
        ChainKind::Bitcoin => "Bitcoin",
        ChainKind::Solana => "Solana",
        ChainKind::Near => "NEAR",
        ChainKind::Antelope => "Antelope",
        ChainKind::Cosmos => "Cosmos",
        ChainKind::Tron => "Tron",
        ChainKind::Beacon => "Beacon",
    }
}

/// Chain-wide notes, from the section comments of each `schema.rs`.
fn chain_notes(kind: ChainKind) -> &'static [&'static str] {
    match kind {
        ChainKind::Evm => &[
            "Extended tables are populated only from blocks at the EXTENDED detail level; \
             the `system_*` tables hold block-level events without `tx_hash`.",
        ],
        ChainKind::Tron => &[
            "Hash columns (block, transaction and internal transaction hashes, `tx_trie_root` \
             and log topics) are always lowercase hex without `0x`, whatever the encoding \
             (`tron_reserved_encoding` in `blocks/src/tron/schema.rs`).",
        ],
        ChainKind::Bitcoin
        | ChainKind::Solana
        | ChainKind::Near
        | ChainKind::Antelope
        | ChainKind::Cosmos
        | ChainKind::Beacon => &[],
    }
}

const TABLE_DESCRIPTIONS: &[(ChainKind, &str, &str)] = &[
    (
        ChainKind::Evm,
        "system_calls",
        "System table: block-level events without `tx_hash`.",
    ),
    (
        ChainKind::Evm,
        "system_balance_changes",
        "System table: block-level events without `tx_hash`.",
    ),
    (
        ChainKind::Evm,
        "system_code_changes",
        "System table: block-level events without `tx_hash`.",
    ),
    (
        ChainKind::Evm,
        "system_storage_changes",
        "System table: block-level events without `tx_hash`.",
    ),
    (
        ChainKind::Evm,
        "system_nonce_changes",
        "System table: block-level events without `tx_hash`.",
    ),
    (
        ChainKind::Evm,
        "system_gas_changes",
        "System table: block-level events without `tx_hash`.",
    ),
    (
        ChainKind::Evm,
        "system_account_creations",
        "System table: block-level events without `tx_hash`.",
    ),
    (
        ChainKind::Solana,
        "token_balances",
        "Token balance snapshots (pre/post) per transaction.",
    ),
    (
        ChainKind::Solana,
        "account_lookups",
        "Address table lookups from versioned transactions.",
    ),
    (
        ChainKind::Near,
        "receipt_actions",
        "One row per action of an executed action receipt.",
    ),
    (
        ChainKind::Near,
        "execution_logs",
        "One row per log line of an executed receipt (`ExecutionOutcome.logs`). NEP-297 events \
         such as NEP-141 and NEP-171 are logs starting with `EVENT_JSON:`.",
    ),
    (
        ChainKind::Near,
        "state_changes",
        "One row per entry of the block's `state_changes` (#507). The pinned producer never \
         fills that list, so this table is empty on its output.",
    ),
    (
        ChainKind::Beacon,
        "withdrawals",
        "Capella+: `execution_payload.withdrawals`, one row per withdrawal.",
    ),
    (
        ChainKind::Beacon,
        "bls_to_execution_changes",
        "Deneb+ (the Firehose Capella body does not carry them): signed BLS-to-execution \
         withdrawal credential changes.",
    ),
    (
        ChainKind::Beacon,
        "deposit_requests",
        "Electra+: EIP-6110 deposit requests from `execution_requests.deposits`.",
    ),
    (
        ChainKind::Beacon,
        "withdrawal_requests",
        "Electra+: EIP-7002 withdrawal requests from `execution_requests.withdrawals`.",
    ),
    (
        ChainKind::Beacon,
        "consolidation_requests",
        "Electra+: EIP-7251 consolidation requests from `execution_requests.consolidations`.",
    ),
];

const NEAR_RECEIPT_STATUS: &str = "The parent receipt's own outcome, as `receipts.status` \
    (#550). A `Failure` receipt's actions did not take effect.";
const PARENT_TRANSACTION_OUTCOME: &str = "Parent transaction outcome (#550).";
const YOCTO_NEAR: &str = "yoctoNEAR, as a decimal string.";

/// `(chain, table, column, description)`.
const COLUMN_DESCRIPTIONS: &[(ChainKind, &str, &str, &str)] = &[
    // EVM
    (
        ChainKind::Evm,
        "logs",
        "log_index",
        "Firehose transaction-relative `Log.index` (guaranteed at EXTENDED detail).",
    ),
    (
        ChainKind::Evm,
        "logs",
        "block_index",
        "Firehose receipt `Log.blockIndex`, corresponding to JSON-RPC `logIndex`.",
    ),
    // Solana
    (
        ChainKind::Solana,
        "rewards",
        "source",
        "`block` for block-level rewards, `transaction` for per-transaction rewards.",
    ),
    (
        ChainKind::Solana,
        "token_balances",
        "balance_type",
        "`pre` or `post`.",
    ),
    // Antelope
    (
        ChainKind::Antelope,
        "transactions",
        "transaction_success",
        "Whether the transaction's effects persisted (#550).",
    ),
    (
        ChainKind::Antelope,
        "actions",
        "transaction_status",
        PARENT_TRANSACTION_OUTCOME,
    ),
    (
        ChainKind::Antelope,
        "actions",
        "transaction_success",
        PARENT_TRANSACTION_OUTCOME,
    ),
    (
        ChainKind::Antelope,
        "db_ops",
        "transaction_status",
        PARENT_TRANSACTION_OUTCOME,
    ),
    (
        ChainKind::Antelope,
        "db_ops",
        "transaction_success",
        PARENT_TRANSACTION_OUTCOME,
    ),
    // NEAR
    (
        ChainKind::Near,
        "transactions",
        "transaction_index",
        "Position in the block: chunks in shard order, then each chunk's transactions in \
         order. Transactions dropped by the failed-transaction filter keep their index, so \
         the written values can have gaps.",
    ),
    (
        ChainKind::Near,
        "transactions",
        "status",
        "The transaction's own outcome: its inclusion and conversion into a receipt. \
         `SuccessReceiptId` does not mean the contract calls succeeded; the final outcome \
         follows `receipts.success_receipt_id` (#507).",
    ),
    (ChainKind::Near, "transactions", "tokens_burnt", YOCTO_NEAR),
    (
        ChainKind::Near,
        "transactions",
        "receipt_ids",
        "The outcome's `receipt_ids`.",
    ),
    (
        ChainKind::Near,
        "transactions",
        "converted_into_receipt_id",
        "The receipt the transaction was converted into: the first (and only) entry of \
         `receipt_ids`. Joins `receipts.receipt_id`.",
    ),
    (
        ChainKind::Near,
        "receipts",
        "receipt_index",
        "Position of the execution outcome in the block: shards in order, then each shard's \
         executed receipts in order.",
    ),
    (
        ChainKind::Near,
        "receipts",
        "tx_hash",
        "The originating transaction, when it is in the same block; null otherwise.",
    ),
    (
        ChainKind::Near,
        "receipts",
        "signer_id",
        "`ReceiptAction.signer_id`: the signer of the transaction that started the receipt \
         chain. Null for data receipts.",
    ),
    (ChainKind::Near, "receipts", "tokens_burnt", YOCTO_NEAR),
    (
        ChainKind::Near,
        "receipts",
        "receipt_ids",
        "Receipts created by this execution (the outcome's `receipt_ids`).",
    ),
    (
        ChainKind::Near,
        "receipts",
        "success_receipt_id",
        "For a `SuccessReceiptId` outcome, the receipt whose outcome becomes this receipt's \
         result; null otherwise (#507).",
    ),
    (
        ChainKind::Near,
        "receipt_actions",
        "action_index",
        "Position of the action in the receipt's action list.",
    ),
    (
        ChainKind::Near,
        "receipt_actions",
        "method_name",
        "FunctionCall only.",
    ),
    (
        ChainKind::Near,
        "receipt_actions",
        "args",
        "FunctionCall only. Raw bytes (usually JSON) whatever the encoding.",
    ),
    (
        ChainKind::Near,
        "receipt_actions",
        "gas",
        "FunctionCall only: the gas attached to the call.",
    ),
    (
        ChainKind::Near,
        "receipt_actions",
        "deposit",
        "FunctionCall and Transfer only: yoctoNEAR, as a decimal string.",
    ),
    (
        ChainKind::Near,
        "receipt_actions",
        "receipt_status",
        NEAR_RECEIPT_STATUS,
    ),
    (
        ChainKind::Near,
        "execution_logs",
        "log_index",
        "Position of the log in the outcome's logs.",
    ),
    (
        ChainKind::Near,
        "execution_logs",
        "executor_id",
        "The account whose code emitted the log.",
    ),
    (
        ChainKind::Near,
        "execution_logs",
        "receipt_status",
        NEAR_RECEIPT_STATUS,
    ),
    (
        ChainKind::Near,
        "state_changes",
        "state_change_index",
        "Position in the block's `state_changes`, counting skipped entries.",
    ),
    (
        ChainKind::Near,
        "state_changes",
        "cause_tx_hash",
        "`TransactionProcessing` causes only.",
    ),
    (
        ChainKind::Near,
        "state_changes",
        "cause_receipt_hash",
        "Receipt causes: ActionReceiptProcessingStarted, ActionReceiptGasReward, \
         ReceiptProcessing and PostponedReceipt. Joins `receipts.receipt_id`.",
    ),
    (
        ChainKind::Near,
        "state_changes",
        "data_key",
        "`DataUpdate` and `DataDeletion`: the storage key.",
    ),
    (
        ChainKind::Near,
        "state_changes",
        "data_value",
        "`DataUpdate`: the stored value.",
    ),
    (
        ChainKind::Near,
        "state_changes",
        "amount",
        "`AccountUpdate`: yoctoNEAR balance, as a decimal string.",
    ),
    (
        ChainKind::Near,
        "state_changes",
        "locked",
        "`AccountUpdate`: yoctoNEAR balance, as a decimal string.",
    ),
    (
        ChainKind::Near,
        "state_changes",
        "storage_usage",
        "`AccountUpdate`: storage in bytes.",
    ),
    // Tron
    (
        ChainKind::Tron,
        "transactions",
        "expiration_ms",
        "Transaction expiration time in unix milliseconds, named apart from the canonical \
         block `timestamp` column. Raw `Int64` rather than a Timestamp type.",
    ),
    (
        ChainKind::Tron,
        "transactions",
        "tx_timestamp_ms",
        "Transaction creation time in unix milliseconds. Set by the sender and not validated \
         on chain (0 and other units occur), so it stays raw `Int64` rather than a Timestamp \
         type.",
    ),
    (
        ChainKind::Tron,
        "transactions",
        "transaction_success",
        "Transaction outcome (#550).",
    ),
    (
        ChainKind::Tron,
        "logs",
        "transaction_success",
        PARENT_TRANSACTION_OUTCOME,
    ),
    (
        ChainKind::Tron,
        "internal_transactions",
        "transaction_success",
        PARENT_TRANSACTION_OUTCOME,
    ),
    (
        ChainKind::Tron,
        "contracts",
        "transaction_success",
        PARENT_TRANSACTION_OUTCOME,
    ),
    (
        ChainKind::Tron,
        "internal_call_values",
        "transaction_success",
        PARENT_TRANSACTION_OUTCOME,
    ),
    // Beacon
    (
        ChainKind::Beacon,
        "blocks",
        "graffiti",
        "Null only when the block has no body.",
    ),
    (
        ChainKind::Beacon,
        "attestations",
        "committee_bits",
        "EIP-7549 (Electra+): the committees the attestation aggregates. Null before \
         Electra, where `committee_index` identifies the committee.",
    ),
];

/// The canonical identity columns (`firehose_parquet::traits`).
fn canonical_description(column: &str) -> Option<&'static str> {
    Some(match column {
        "block_num" => "Block number (Firehose block metadata).",
        "block_id" => "Block id (hash), in the chain's byte encoding.",
        "parent_num" => "Parent block number (Firehose block metadata).",
        "parent_id" => "Parent block id, in the chain's byte encoding.",
        "lib_num" => "Last irreversible block number reported with the block.",
        "timestamp" => "Block time, UTC, millisecond precision.",
        "date" => "UTC date of `timestamp`.",
        FORK_STEP => {
            "**Non-final streams only** (`--final-blocks-only=false`): the Firehose fork \
             step of the block, `NEW`, `UNDO` or `FINAL`."
        }
        STREAM_ORDINAL => {
            "**Non-final streams only** (`--final-blocks-only=false`): accepted-event \
             ordinal of the stream event (`NEW`, `UNDO` or `FINAL`) that produced the row. \
             Strictly increasing in delivery order and durable across reconnects and \
             restarts; every row of one event, in every table, has the same value."
        }
        _ => return None,
    })
}

fn column_description(kind: ChainKind, table: &str, column: &str) -> Option<&'static str> {
    canonical_description(column).or_else(|| {
        COLUMN_DESCRIPTIONS
            .iter()
            .find(|(k, t, c, _)| *k == kind && *t == table && *c == column)
            .map(|(_, _, _, description)| *description)
    })
}

fn table_description(kind: ChainKind, table: &str) -> Option<&'static str> {
    TABLE_DESCRIPTIONS
        .iter()
        .find(|(k, t, _)| *k == kind && *t == table)
        .map(|(_, _, description)| *description)
}

/// Every description must name a column (or table) that exists, so a renamed or
/// removed field fails generation instead of leaving a dead entry.
fn check_descriptions(references: &[ChainReference]) -> Result<()> {
    let has_table = |kind: ChainKind, table: &str| {
        references
            .iter()
            .filter(|reference| reference.kind == kind)
            .flat_map(|reference| &reference.tables)
            .find(|t| t.name == table)
    };
    for (kind, table, _) in TABLE_DESCRIPTIONS {
        ensure!(
            has_table(*kind, table).is_some(),
            "schema_docs: table description for {kind}.{table} names no table"
        );
    }
    for (kind, table, column, _) in COLUMN_DESCRIPTIONS {
        ensure!(
            has_table(*kind, table).is_some_and(|t| t.columns.iter().any(|c| c.name == *column)),
            "schema_docs: column description for {kind}.{table}.{column} names no column"
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Reference model, built from the mappers
// ---------------------------------------------------------------------------

struct ChainReference {
    kind: ChainKind,
    encoding: EncodeBytes,
    /// The encoding on Tron-style endpoints, when it differs from `encoding`.
    tron_style_encoding: Option<EncodeBytes>,
    tables: Vec<TableReference>,
}

struct TableReference {
    name: String,
    /// Omitted with `--without-extended`.
    extended_only: bool,
    /// Omitted with `--without-votes`.
    votes_only: bool,
    /// An earlier table with the same schema.
    same_as: Option<String>,
    /// Every column in file order, `fork_step` and `stream_ordinal` included.
    columns: Vec<ColumnReference>,
}

struct ColumnReference {
    name: String,
    data_type: DataType,
    nullable: bool,
    /// Binary data written as text in the chain's byte encoding.
    encoded: bool,
}

impl TableReference {
    fn column_count_without_non_final_columns(&self) -> usize {
        self.columns
            .iter()
            .filter(|c| !NON_FINAL_COLUMNS.contains(&c.name.as_str()))
            .count()
    }
}

/// The mapper options that could change a chain's tables or schemas.
#[derive(Clone, Copy, Debug)]
struct Toggles {
    extended: bool,
    with_votes: bool,
    include_failed_transactions: bool,
    synthetic_partition_routing: bool,
}

impl Toggles {
    /// Production defaults: `--without-extended` and `--without-votes` unset.
    fn defaults(kind: ChainKind) -> Self {
        Self {
            extended: true,
            with_votes: true,
            include_failed_transactions: kind.profile().failed_transactions_by_default,
            synthetic_partition_routing: false,
        }
    }

    fn options(self, include_fork_step: bool, encoding: &EncodeBytes) -> MapperOptions {
        MapperOptions {
            extended: self.extended,
            with_votes: self.with_votes,
            include_fork_step,
            encode_bytes: encoding.clone(),
            synthetic_partition_routing: self.synthetic_partition_routing,
            include_failed_transactions: self.include_failed_transactions,
        }
    }
}

type Tables = Vec<(String, SchemaRef)>;

/// Every table's schema in the mapper's declared order, from an empty flush.
fn mapper_tables(kind: ChainKind, options: MapperOptions) -> Result<Tables> {
    let context = format!("{kind} {options:?}");
    let mut mapper = kind.create_mapper(options);
    let names: Vec<String> = mapper
        .table_names()
        .into_iter()
        .map(str::to_string)
        .collect();
    let mut batches = mapper
        .flush()
        .with_context(|| format!("{context}: flush"))?;
    let flushed: BTreeSet<&String> = batches.keys().collect();
    ensure!(
        flushed == names.iter().collect::<BTreeSet<_>>() && names.len() == flushed.len(),
        "{context}: flushed tables {flushed:?} differ from table_names() {names:?}"
    );
    Ok(names
        .into_iter()
        .map(|name| {
            let schema = batches.remove(&name).expect("checked above").schema();
            (name, schema)
        })
        .collect())
}

fn table_names(tables: &Tables) -> Vec<&str> {
    tables.iter().map(|(name, _)| name.as_str()).collect()
}

fn chain_reference(kind: ChainKind) -> Result<ChainReference> {
    let profile = kind.profile();
    let encoding = kind.default_bytes_encoding(false);
    let defaults = Toggles::defaults(kind);

    let full = mapper_tables(kind, defaults.options(true, &encoding))?;
    let without_extended = mapper_tables(
        kind,
        Toggles {
            extended: false,
            ..defaults
        }
        .options(true, &encoding),
    )?;
    let without_votes = mapper_tables(
        kind,
        Toggles {
            with_votes: false,
            ..defaults
        }
        .options(true, &encoding),
    )?;
    let extended_only = |name: &str| !table_names(&without_extended).contains(&name);
    let votes_only = |name: &str| !table_names(&without_votes).contains(&name);

    // Only `extended` and `with_votes` may change the set of tables, and no
    // option may change a table's schema; otherwise this reference would
    // misdescribe some configuration.
    for extended in [false, true] {
        for with_votes in [false, true] {
            for include_failed_transactions in [false, true] {
                for synthetic_partition_routing in [false, true] {
                    let toggles = Toggles {
                        extended,
                        with_votes,
                        include_failed_transactions,
                        synthetic_partition_routing,
                    };
                    let tables = mapper_tables(kind, toggles.options(true, &encoding))?;
                    let expected: Tables = full
                        .iter()
                        .filter(|(name, _)| {
                            (extended || !extended_only(name)) && (with_votes || !votes_only(name))
                        })
                        .cloned()
                        .collect();
                    ensure!(
                        tables == expected,
                        "{kind} {toggles:?}: tables or schemas differ from the documented \
                         defaults; teach schema_docs to describe this option"
                    );
                }
            }
        }
    }

    let tron_style = kind.default_bytes_encoding(true);
    let tron_style_encoding = if tron_style == encoding {
        None
    } else {
        let tables = mapper_tables(kind, defaults.options(true, &tron_style))?;
        ensure!(
            tables == full,
            "{kind}: the Tron-style encoding {tron_style:?} changes the Arrow schema"
        );
        Some(tron_style)
    };

    let final_only = mapper_tables(kind, defaults.options(false, &encoding))?;
    let binary = mapper_tables(kind, defaults.options(true, &EncodeBytes::Binary))?;
    ensure!(
        table_names(&final_only) == table_names(&full)
            && table_names(&binary) == table_names(&full),
        "{kind}: fork_step, stream_ordinal or the encoding changes the set of tables"
    );

    let mut tables = Vec::with_capacity(full.len());
    for (index, (name, schema)) in full.iter().enumerate() {
        let context = format!("{kind}.{name}");
        let fields = schema.fields();

        let leading: Vec<&str> = fields
            .iter()
            .take(CANONICAL_COLUMNS.len())
            .map(|f| f.name().as_str())
            .collect();
        ensure!(
            leading == CANONICAL_COLUMNS,
            "{context}: does not start with the canonical columns"
        );
        for column in ["timestamp", "date"] {
            ensure!(
                schema.field_with_name(column)?.is_nullable() == profile.nullable_timestamps,
                "{context}: {column} nullability differs from ChainProfile::nullable_timestamps"
            );
        }
        ensure!(
            schema.field_with_name("timestamp")?.data_type() == &timestamp_millis_utc_type(),
            "{context}: canonical timestamp type"
        );

        for column in NON_FINAL_COLUMNS {
            ensure!(
                fields.iter().filter(|f| f.name() == column).count() == 1,
                "{context}: expected one {column} column on non-final streams"
            );
        }
        let fork_step = schema.index_of(FORK_STEP)?;
        ensure!(
            schema.index_of(STREAM_ORDINAL)? == fork_step + 1,
            "{context}: stream_ordinal must directly follow fork_step"
        );
        ensure!(
            schema.field(fork_step + 1).data_type() == &DataType::UInt64
                && !schema.field(fork_step + 1).is_nullable(),
            "{context}: stream_ordinal must be a non-null UInt64"
        );
        let without_fork_step: Vec<&Field> = fields
            .iter()
            .filter(|f| !NON_FINAL_COLUMNS.contains(&f.name().as_str()))
            .map(|f| f.as_ref())
            .collect();
        let final_fields: Vec<&Field> = final_only[index]
            .1
            .fields()
            .iter()
            .map(|f| f.as_ref())
            .collect();
        ensure!(
            without_fork_step == final_fields,
            "{context}: final-only schema is not the non-final schema without fork_step and stream_ordinal"
        );

        let binary_fields = binary[index].1.fields();
        ensure!(
            binary_fields.len() == fields.len()
                && binary_fields
                    .iter()
                    .zip(fields)
                    .all(|(b, f)| b.name() == f.name()),
            "{context}: the binary encoding changes the columns"
        );
        let columns = fields
            .iter()
            .zip(binary_fields)
            .map(|(field, binary_field)| ColumnReference {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
                encoded: field.data_type() != binary_field.data_type(),
            })
            .collect();

        tables.push(TableReference {
            name: name.clone(),
            extended_only: extended_only(name),
            votes_only: votes_only(name),
            same_as: full[..index]
                .iter()
                .find(|(_, earlier)| earlier == schema)
                .map(|(earlier, _)| earlier.clone()),
            columns,
        });
    }

    Ok(ChainReference {
        kind,
        encoding,
        tron_style_encoding,
        tables,
    })
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

fn encoding_label(encoding: &EncodeBytes) -> &'static str {
    match encoding {
        EncodeBytes::Binary => "binary",
        EncodeBytes::Hex => "hex",
        EncodeBytes::HexNoPrefix => "hex_no_prefix",
        EncodeBytes::Base58 => "base58",
        EncodeBytes::TronBase58 => "tron_base58",
    }
}

/// How each encoding writes a byte value (`firehose_parquet::encode`).
fn encoding_description(encoding: &EncodeBytes) -> &'static str {
    match encoding {
        EncodeBytes::Binary => "raw bytes",
        EncodeBytes::Hex => "lowercase hex with a `0x` prefix",
        EncodeBytes::HexNoPrefix => "lowercase hex without a `0x` prefix",
        EncodeBytes::Base58 => "Base58",
        EncodeBytes::TronBase58 => {
            "Tron Base58Check for 20- and 21-byte values such as addresses, lowercase hex \
             without `0x` for other lengths"
        }
    }
}

/// A human-readable Arrow type, stable across Arrow releases.
fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Timestamp(unit, Some(tz)) => format!("Timestamp({unit:?}, \"{tz}\")"),
        DataType::Timestamp(unit, None) => format!("Timestamp({unit:?})"),
        DataType::Dictionary(key, value) => {
            format!("Dictionary({}, {})", type_name(key), type_name(value))
        }
        DataType::List(item) => format!("List<{}>", nested_type_name(item)),
        DataType::Struct(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|field| format!("{}: {}", field.name(), nested_type_name(field)))
                .collect();
            format!("Struct<{}>", fields.join(", "))
        }
        other => other.to_string(),
    }
}

fn nested_type_name(field: &Field) -> String {
    let nullability = if field.is_nullable() { "" } else { "non-null " };
    format!("{nullability}{}", type_name(field.data_type()))
}

fn escape_cell(text: &str) -> String {
    text.replace('|', "\\|")
}

fn table_condition(table: &TableReference) -> Option<&'static str> {
    match (table.extended_only, table.votes_only) {
        (false, false) => None,
        (true, false) => Some("Extended output only: omitted with `--without-extended`."),
        (false, true) => Some("Votes only: omitted with `--without-votes`."),
        (true, true) => Some(
            "Extended output with votes only: omitted with `--without-extended` or \
             `--without-votes`.",
        ),
    }
}

/// `20 (14 extended only)`.
fn table_count_summary(reference: &ChainReference) -> String {
    let total = reference.tables.len();
    let extended = reference.tables.iter().filter(|t| t.extended_only).count();
    let votes = reference.tables.iter().filter(|t| t.votes_only).count();
    let mut conditional = Vec::new();
    if extended > 0 {
        conditional.push(format!("{extended} extended only"));
    }
    if votes > 0 {
        conditional.push(format!("{votes} votes only"));
    }
    if conditional.is_empty() {
        total.to_string()
    } else {
        format!("{total} ({})", conditional.join(", "))
    }
}

fn generated_notice() -> String {
    format!(
        "<!-- Generated by `{REGENERATE_COMMAND}` from blocks/src/schema_docs.rs. \
         Do not edit by hand. -->\n"
    )
}

fn render_index(references: &[ChainReference]) -> String {
    let mut out = generated_notice();
    out.push_str(
        "\n# Output schema reference\n\n\
         One file per chain family lists every table `fireparq` writes and every column \
         with its Arrow type, nullability and, where the code documents it, a description. \
         The files are rendered from the production mappers (`ChainKind::create_mapper` in \
         `blocks/src/chain.rs`) with each chain's default options and byte encoding, so \
         they match what a v1.0.0 build writes.\n\n",
    );
    out.push_str(
        "| Chain | `--block-type` | Tables | Byte encoding | Reference |\n\
         |---|---|---|---|---|\n",
    );
    for reference in references {
        let label = reference.kind.label();
        let _ = writeln!(
            out,
            "| {} | `{label}` | {} | `{}` | [{label}.md]({label}.md) |",
            chain_title(reference.kind),
            table_count_summary(reference),
            encoding_label(&reference.encoding),
        );
    }
    out.push_str(
        "\n## Conventions\n\n\
         - Every table starts with the canonical block identity columns `block_num`, \
         `block_id`, `parent_num`, `parent_id`, `lib_num`, `timestamp` and `date`, shared \
         by all chains (`firehose_parquet::traits`). `timestamp` is \
         `Timestamp(Millisecond, \"UTC\")` and `date` is `Date32`; both are nullable only \
         on chains whose blocks may lack a timestamp (Solana).\n\
         - `fork_step` (`Utf8`, `NEW`, `UNDO` or `FINAL`) and `stream_ordinal` (`UInt64`) \
         exist only on non-final streams (`--final-blocks-only=false`). `stream_ordinal` \
         is the accepted-event ordinal of the stream event that produced the row: strictly \
         increasing in delivery order, durable across reconnects and restarts, and the \
         same for every row of one event in every table. The references list both where \
         they sit in that mode; with the default `--final-blocks-only=true` they are \
         absent and the other columns keep their order. They follow each table's \
         original columns, `stream_ordinal` directly after `fork_step`; columns added \
         later may come after them (for example `transaction_success`), so they are not \
         always the last columns.\n\
         - Select columns by name, not by position: new columns are sometimes inserted \
         in the middle of a table, and `fork_step` and `stream_ordinal` change the \
         positions of the columns after them.\n\
         - Byte encoding: binary values (hashes, addresses, keys) are written as text in the \
         chain's encoding, fixed per chain in v1.0.0 (`ChainProfile` in \
         `blocks/src/chain.rs`). Their type is suffixed with the encoding, for example \
         `Utf8` (hex); other `Utf8` columns hold chain-native text and do not depend on the \
         encoding. `Binary` columns hold raw bytes whatever the encoding.\n\
         - Enum columns are `Dictionary(Int32, Utf8)` holding stable protobuf labels (the \
         Parquet enum convention in `docs/repo-navigation.md`).\n\
         - Type notation: `List<T>` is an Arrow list whose items may be null, \
         `List<non-null T>` one whose items may not; `Struct<name: T, ...>` spells out the \
         struct fields the same way. Nullable refers to the column itself.\n\
         - Conditional tables are marked: EVM extended tables are omitted with \
         `--without-extended`, Solana `vote_transactions` with `--without-votes`. Every \
         other table is always part of the mapper's output; a table only gets files for \
         flushes in which it has rows.\n\n\
         ## Regenerating\n\n",
    );
    let _ = writeln!(
        out,
        "`{REGENERATE_COMMAND}` rewrites every file in this directory from \
         `blocks/src/schema_docs.rs`. The test \
         `schema_docs::tests::committed_schema_docs_match_the_code` (part of \
         `cargo test -p blocks`) fails when a committed file differs from the code, for \
         example after a schema change. Column descriptions are the code comments beside \
         each field in `blocks/src/<chain>/schema.rs`, copied into `schema_docs.rs`: \
         update both together. Columns without such a comment have no description."
    );
    out
}

fn render_chain(reference: &ChainReference) -> String {
    let kind = reference.kind;
    let profile = kind.profile();
    let encoding = encoding_label(&reference.encoding);
    let mut out = generated_notice();

    let _ = writeln!(out, "\n# {} schema\n", chain_title(kind));
    let _ = writeln!(
        out,
        "Generated from the `{}` mapper; do not edit by hand. Regenerate with \
         `{REGENERATE_COMMAND}` (see [README](README.md) for the conventions).\n",
        kind.label()
    );
    let _ = writeln!(out, "- Block type: `--block-type {}`.", kind.label());
    let _ = write!(
        out,
        "- Byte encoding: `{encoding}` ({}), fixed for this chain in v1.0.0.",
        encoding_description(&reference.encoding)
    );
    if let Some(tron_style) = &reference.tron_style_encoding {
        let _ = write!(
            out,
            " Tron-style endpoints (`tron`, `tron-evm` chain names) use `{}` ({}) instead, \
             with the same Arrow types.",
            encoding_label(tron_style),
            encoding_description(tron_style)
        );
    }
    out.push('\n');
    // In order of first appearance: `block_id` makes `Utf8` first.
    let mut encoded_types: Vec<String> = Vec::new();
    for column in reference.tables.iter().flat_map(|table| &table.columns) {
        let encoded_type = format!("`{}` ({encoding})", type_name(&column.data_type));
        if column.encoded && !encoded_types.contains(&encoded_type) {
            encoded_types.push(encoded_type);
        }
    }
    let _ = writeln!(
        out,
        "- Columns typed {} hold binary values written as text in that encoding.",
        encoded_types.join(" or ")
    );
    let _ = writeln!(
        out,
        "- `fork_step` and `stream_ordinal` are listed where they sit on non-final streams \
         (`--final-blocks-only=false`); with the default `--final-blocks-only=true` they \
         are absent."
    );
    if profile.nullable_timestamps {
        let _ = writeln!(
            out,
            "- Blocks may lack a timestamp, so the canonical `timestamp` and `date` are \
             nullable."
        );
    }
    for note in chain_notes(kind) {
        let _ = writeln!(out, "- {note}");
    }

    out.push_str(
        "\n## Tables\n\n\
         | Table | Columns (without `fork_step`, `stream_ordinal`) | Written |\n\
         |---|---|---|\n",
    );
    for table in &reference.tables {
        let _ = writeln!(
            out,
            "| [`{name}`](#{name}) | {} | {} |",
            table.column_count_without_non_final_columns(),
            table_condition(table).unwrap_or("Always."),
            name = table.name,
        );
    }

    for table in &reference.tables {
        let _ = writeln!(out, "\n## `{}`\n", table.name);
        let mut paragraphs = Vec::new();
        if let Some(condition) = table_condition(table) {
            paragraphs.push(condition.to_string());
        }
        if let Some(description) = table_description(kind, &table.name) {
            paragraphs.push(description.to_string());
        }
        if let Some(same_as) = &table.same_as {
            paragraphs.push(format!("Same columns as `{same_as}`."));
        }
        for paragraph in paragraphs {
            let _ = writeln!(out, "{paragraph}\n");
        }
        out.push_str(
            "| Column | Type | Nullable | Description |\n\
             |---|---|---|---|\n",
        );
        for column in &table.columns {
            let encoded = if column.encoded {
                format!(" ({encoding})")
            } else {
                String::new()
            };
            let description = column_description(kind, &table.name, &column.name)
                .map(|description| format!(" {} ", escape_cell(description)))
                .unwrap_or_else(|| " ".to_string());
            let _ = writeln!(
                out,
                "| `{}` | `{}`{encoded} | {} |{description}|",
                column.name,
                type_name(&column.data_type),
                if column.nullable { "yes" } else { "no" },
            );
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{antelope, beacon, bitcoin, cosmos, evm, near, solana, tron};

    /// The first line where `committed` and `rendered` differ, for the failure message.
    fn first_difference(committed: &str, rendered: &str) -> String {
        let mut committed_lines = committed.lines();
        let mut rendered_lines = rendered.lines();
        for line in 1.. {
            match (committed_lines.next(), rendered_lines.next()) {
                (Some(a), Some(b)) if a == b => continue,
                (None, None) => break,
                (a, b) => {
                    return format!(
                        "line {line}: committed {:?}, rendered {:?}",
                        a.unwrap_or("<end of file>"),
                        b.unwrap_or("<end of file>")
                    )
                }
            }
        }
        "trailing whitespace differs".to_string()
    }

    /// Modeled on the `.env.example` drift test: the committed reference must be
    /// exactly what the code renders.
    #[test]
    fn committed_schema_docs_match_the_code() {
        let dir = docs_dir();
        let rendered = render_all().expect("render the schema reference");
        let mut problems = Vec::new();
        for doc in &rendered {
            let path = dir.join(&doc.file_name);
            match std::fs::read_to_string(&path) {
                Ok(committed) => {
                    let committed = committed.replace("\r\n", "\n");
                    if committed != doc.contents {
                        problems.push(format!(
                            "{} is out of date ({})",
                            doc.file_name,
                            first_difference(&committed, &doc.contents)
                        ));
                    }
                }
                Err(err) => problems.push(format!("{} is missing ({err})", doc.file_name)),
            }
        }
        let expected: BTreeSet<&str> = rendered.iter().map(|d| d.file_name.as_str()).collect();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".md") && !expected.contains(name.as_str()) {
                    problems.push(format!("{name} is not generated by schema_docs; delete it"));
                }
            }
        }
        assert!(
            problems.is_empty(),
            "docs/schemas does not match the mapper schemas; run `{REGENERATE_COMMAND}` \
             and commit the result:\n  {}",
            problems.join("\n  ")
        );
    }

    /// The rendered tables are exactly each mapper's `table_names()`, for the
    /// default options and with each table-selecting flag.
    #[test]
    fn rendered_tables_match_every_mapper_configuration() {
        let docs = render_all().expect("render the schema reference");
        for kind in ChainKind::ALL {
            let reference = chain_reference(kind).unwrap();
            let encoding = kind.default_bytes_encoding(false);
            for extended in [false, true] {
                for with_votes in [false, true] {
                    let toggles = Toggles {
                        extended,
                        with_votes,
                        ..Toggles::defaults(kind)
                    };
                    let mapper = kind.create_mapper(toggles.options(false, &encoding));
                    let rendered: Vec<&str> = reference
                        .tables
                        .iter()
                        .filter(|t| (extended || !t.extended_only) && (with_votes || !t.votes_only))
                        .map(|t| t.name.as_str())
                        .collect();
                    assert_eq!(rendered, mapper.table_names(), "{kind} {toggles:?}");
                }
            }

            let markdown = &docs
                .iter()
                .find(|doc| doc.file_name == format!("{}.md", kind.label()))
                .expect("a file per chain")
                .contents;
            for table in &reference.tables {
                assert!(
                    markdown.contains(&format!("\n## `{}`\n", table.name)),
                    "{kind}: {} has no section",
                    table.name
                );
            }
        }
    }

    /// The same table lists as the schema constants the cross-chain contract
    /// test counts (`schema_contract_tests::expected_table_count`).
    #[test]
    fn rendered_tables_match_the_schema_table_name_constants() {
        let names = |kind: ChainKind, keep: fn(&TableReference) -> bool| -> Vec<String> {
            chain_reference(kind)
                .unwrap()
                .tables
                .iter()
                .filter(|t| keep(t))
                .map(|t| t.name.clone())
                .collect()
        };
        let all = |_: &TableReference| true;
        let base = |t: &TableReference| !t.extended_only && !t.votes_only;
        let expect = |constants: &[&str]| -> Vec<String> {
            constants.iter().map(|name| name.to_string()).collect()
        };

        assert_eq!(
            names(ChainKind::Evm, all),
            expect(&evm::schema::EXTENDED_TABLE_NAMES)
        );
        assert_eq!(
            names(ChainKind::Evm, base),
            expect(&evm::schema::BASE_TABLE_NAMES)
        );
        assert_eq!(
            names(ChainKind::Solana, all),
            expect(&solana::schema::WITH_VOTES_TABLE_NAMES)
        );
        assert_eq!(
            names(ChainKind::Solana, base),
            expect(&solana::schema::BASE_TABLE_NAMES)
        );
        for (kind, constants) in [
            (ChainKind::Antelope, &antelope::schema::TABLE_NAMES[..]),
            (ChainKind::Beacon, &beacon::schema::TABLE_NAMES[..]),
            (ChainKind::Bitcoin, &bitcoin::schema::TABLE_NAMES[..]),
            (ChainKind::Cosmos, &cosmos::schema::TABLE_NAMES[..]),
            (ChainKind::Near, &near::schema::TABLE_NAMES[..]),
            (ChainKind::Tron, &tron::schema::TABLE_NAMES[..]),
        ] {
            assert_eq!(names(kind, all), expect(constants), "{kind}");
            assert_eq!(names(kind, base), expect(constants), "{kind}");
        }
    }

    #[test]
    fn type_names_spell_out_nested_types() {
        use std::sync::Arc;
        let item = |data_type, nullable| Arc::new(Field::new("item", data_type, nullable));
        assert_eq!(
            type_name(&timestamp_millis_utc_type()),
            "Timestamp(Millisecond, \"UTC\")"
        );
        assert_eq!(
            type_name(&firehose_parquet::traits::enum_data_type()),
            "Dictionary(Int32, Utf8)"
        );
        assert_eq!(
            type_name(&DataType::List(item(DataType::UInt8, false))),
            "List<non-null UInt8>"
        );
        assert_eq!(
            type_name(&DataType::List(item(DataType::Utf8, true))),
            "List<Utf8>"
        );
        let fee = DataType::Struct(
            vec![
                Field::new("denom", DataType::Utf8, false),
                Field::new("memo", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(
            type_name(&DataType::List(item(fee, false))),
            "List<non-null Struct<denom: non-null Utf8, memo: Utf8>>"
        );
    }
}
