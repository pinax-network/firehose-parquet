//! The Markdown schema reference in `docs/schemas/`, rendered from the
//! production mappers (`ChainKind::create_mapper`) so that it cannot drift from
//! the code.
//!
//! Regenerate it with `cargo run -p blocks --example dump_schemas`. The
//! `committed_schema_docs_match_the_code` test fails when the committed files
//! differ from what this module renders.
//!
//! Tables and columns come from an empty flush of each mapper. The column types
//! are the Delta types of the data files, mapped from the mapper's Arrow types
//! at the flush boundary with each chain's `ChainProfile::delta_types()`
//! (#643, `firehose_parquet::delta::types`); each file ends with that chain's
//! mapping. Descriptions are the code comments beside the fields in
//! `blocks/src/<chain>/schema.rs` (and the canonical fields in
//! `firehose_parquet::traits`), copied into the tables below: update both
//! together.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use arrow::datatypes::{DataType, Field, SchemaRef};
use firehose_parquet::delta::types::{Conversion, DecimalColumn, PARTITION_COLUMN};
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
        ChainKind::Bitcoin => &[
            "`outputs.value_sats` is a checked `long`, not a `decimal(20,0)`: consensus caps \
             it at 2.1·10^15 satoshis (`MAX_MONEY`).",
        ],
        ChainKind::Solana
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
        "timestamp" => "Block time, UTC, millisecond precision (stored in microseconds).",
        "date" => {
            "Partition column: the UTC date of the block time, stored in the Delta log \
             (`partitionValues.date`) and the `date=YYYY-MM-DD` directory, not in the data \
             files."
        }
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
    /// `ChainProfile::decimal_columns`.
    decimal_columns: &'static [DecimalColumn],
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
    /// The Delta type (`Date32` for the `date` partition column).
    data_type: DataType,
    nullable: bool,
    /// Binary data written as text in the chain's byte encoding.
    encoded: bool,
    /// The Arrow type the mapper builds.
    source_type: DataType,
    /// How the flush maps `source_type` onto `data_type`.
    conversions: BTreeSet<Conversion>,
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
    let types = profile.delta_types();
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
        // The Delta data file schema: the mapper's columns without `date`,
        // with their Delta types.
        let data = types.data_schema(name, schema)?;
        let binary_data = types.data_schema(name, &binary[index].1)?;
        let columns = fields
            .iter()
            .map(|field| {
                let conversions = types.conversions(name, field);
                if field.name() == PARTITION_COLUMN {
                    // The partition value is always set: a Solana row
                    // without a block time takes its routing day.
                    return Ok(ColumnReference {
                        name: field.name().clone(),
                        data_type: field.data_type().clone(),
                        nullable: false,
                        encoded: false,
                        source_type: field.data_type().clone(),
                        conversions,
                    });
                }
                let delta = data.field_with_name(field.name())?;
                let binary_delta = binary_data.field_with_name(field.name())?;
                Ok(ColumnReference {
                    name: field.name().clone(),
                    data_type: delta.data_type().clone(),
                    nullable: delta.is_nullable(),
                    encoded: delta.data_type() != binary_delta.data_type(),
                    source_type: field.data_type().clone(),
                    conversions,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        tables.push(TableReference {
            name: name.clone(),
            extended_only: extended_only(name),
            votes_only: votes_only(name),
            same_as: full[..index]
                .iter()
                .find(|(earlier_name, earlier)| {
                    earlier == schema
                        && types.data_schema(earlier_name, earlier).ok().as_ref() == Some(&data)
                })
                .map(|(earlier, _)| earlier.clone()),
            columns,
        });
    }

    Ok(ChainReference {
        kind,
        encoding,
        tron_style_encoding,
        tables,
        decimal_columns: profile.decimal_columns,
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

/// A human-readable Arrow type, stable across Arrow releases: the types the
/// mappers build.
fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Timestamp(unit, Some(tz)) => format!("Timestamp({unit:?}, \"{tz}\")"),
        DataType::Timestamp(unit, None) => format!("Timestamp({unit:?})"),
        DataType::Dictionary(key, value) => {
            format!("Dictionary({}, {})", type_name(key), type_name(value))
        }
        DataType::List(item) => format!("List<{}>", nested_type_name(item, type_name)),
        DataType::Struct(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|field| format!("{}: {}", field.name(), nested_type_name(field, type_name)))
                .collect();
            format!("Struct<{}>", fields.join(", "))
        }
        other => other.to_string(),
    }
}

/// The Delta protocol name of a data file type: `long`, `decimal(20,0)`,
/// `timestamp`, `array<non-null short>`, `struct<name: T, ...>`.
fn delta_type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::List(item) => format!("array<{}>", nested_type_name(item, delta_type_name)),
        DataType::Struct(fields) => {
            let fields: Vec<String> = fields
                .iter()
                .map(|field| {
                    format!(
                        "{}: {}",
                        field.name(),
                        nested_type_name(field, delta_type_name)
                    )
                })
                .collect();
            format!("struct<{}>", fields.join(", "))
        }
        other => firehose_parquet::delta::types::delta_type_name(other),
    }
}

fn nested_type_name(field: &Field, name: fn(&DataType) -> String) -> String {
    let nullability = if field.is_nullable() { "" } else { "non-null " };
    format!("{nullability}{}", name(field.data_type()))
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
         with its Delta type, nullability and, where the code documents it, a description, \
         then the chain's type mapping. The files are rendered from the production mappers \
         (`ChainKind::create_mapper` in `blocks/src/chain.rs`) with each chain's default \
         options and byte encoding, mapped onto Delta types as every flush is \
         (`ChainProfile::delta_types`), so they match what a v1.0.0 build writes.\n\n",
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
         - Each table is one directory directly below the dataset root, which is \
         `build --output` as given (`<output>/<table>/`; with `--output '<prefix>/{chain}'` \
         it is `<prefix>/<chain_name>/<table>/`). The `_fireparq/` directory and the \
         dot-prefixed entries \
         beside the tables hold fireparq's artifacts (the cursor mirror) and control \
         state, not tables. Engines that \
         skip `_` and `.` paths (Spark, Trino, Hive, Delta) ignore them; with DuckDB, \
         read one table with `<root>/<table>/**/*.parquet`.\n\
         - Types are Delta Lake types (#643), the types of the data files: `long`, \
         `integer`, `short`, `decimal(20,0)`, `double`, `boolean`, `string`, `binary`, \
         `date`, `timestamp`, `array<T>` and `struct<...>`. Delta has no unsigned, \
         dictionary or millisecond types, so every flush maps the mapper's Arrow types \
         once, with checked casts, before anything is written \
         (`firehose_parquet::delta::types`): `UInt64` becomes a checked `long` (a value \
         above 9,223,372,036,854,775,807 refuses the flush), or `decimal(20,0)` for the \
         chain's currency amounts and values a sender or signer chooses without a range \
         check (`ChainProfile::decimal_columns`); `UInt32` and `UInt16` become `long`, \
         `UInt8` becomes `short`, dictionaries become `string`, and millisecond \
         timestamps become `timestamp` (microseconds, UTC) with the same instant. Each \
         chain file ends with its mapping.\n\
         - Every table starts with the canonical block identity columns `block_num`, \
         `block_id`, `parent_num`, `parent_id`, `lib_num`, `timestamp` and `date`, shared \
         by all chains (`firehose_parquet::traits`). `block_num`, `parent_num` and \
         `lib_num` are `long`; `timestamp` is a `timestamp` (UTC, whole milliseconds, \
         stored in microseconds), nullable only on chains whose blocks may lack a \
         timestamp (Solana); `date` is the partition column.\n\
         - Every table is partitioned by UTC day: its files are \
         `<table>/date=YYYY-MM-DD/part-*.parquet`. `date` is the partition column only: \
         its value is the directory (the Delta `partitionValues.date`), and the data files \
         have no `date` column. A Solana row without `block_time` has a null `timestamp` \
         and the `date` of its routing day, the last known block time. Partition-aware \
         readers such as DuckDB and Polars read the directory as the `date` column and \
         prune by it.\n\
         - `fork_step` (`string`, `NEW`, `UNDO` or `FINAL`) and `stream_ordinal` (`long`) \
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
         `string` (hex); other `string` columns hold chain-native text and do not depend \
         on the encoding. `binary` columns hold raw bytes whatever the encoding.\n\
         - Enum columns are `string` columns holding stable protobuf labels (the Parquet \
         enum convention in `docs/repo-navigation.md`). The mappers build them as \
         `Dictionary(Int32, Utf8)`, and Parquet still dictionary-encodes their pages.\n\
         - Type notation: `array<T>` is a list whose items may be null, \
         `array<non-null T>` one whose items may not; `struct<name: T, ...>` spells out the \
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
             with the same types.",
            encoding_label(tron_style),
            encoding_description(tron_style)
        );
    }
    out.push('\n');
    // In order of first appearance: `block_id` makes `string` first.
    let mut encoded_types: Vec<String> = Vec::new();
    for column in reference.tables.iter().flat_map(|table| &table.columns) {
        let encoded_type = format!("`{}` ({encoding})", delta_type_name(&column.data_type));
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
            "- Blocks may lack a timestamp, so the canonical `timestamp` is nullable. The \
             `date` partition of such a row is its routing day, the last known block time."
        );
    }
    let _ = writeln!(
        out,
        "- Types are the Delta types of the data files; [Delta type mapping](#delta-type-mapping) \
         lists how each mapper column gets its type."
    );
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
            let partition = if column.conversions.contains(&Conversion::Partition) {
                " (partition)"
            } else {
                ""
            };
            let _ = writeln!(
                out,
                "| `{}` | `{}`{encoded}{partition} | {} |{description}|",
                column.name,
                delta_type_name(&column.data_type),
                if column.nullable { "yes" } else { "no" },
            );
        }
    }
    render_mapping(reference, &mut out);
    out
}

/// One row of a chain's type mapping: every column that goes through the
/// same conversion from the same Arrow type to the same Delta type.
struct MappingRow {
    source: String,
    delta: String,
    conversion: Conversion,
    /// `(column, tables)` in order of first appearance.
    columns: Vec<(String, Vec<String>)>,
}

/// The rows of a chain's type mapping, in [`Conversion`] order, then by first
/// appearance.
fn mapping_rows(reference: &ChainReference) -> Vec<MappingRow> {
    let mut rows: Vec<MappingRow> = Vec::new();
    for table in &reference.tables {
        for column in &table.columns {
            for conversion in &column.conversions {
                let (source, delta) = if *conversion == Conversion::Partition {
                    ("date".to_string(), "partition column".to_string())
                } else {
                    (
                        type_name(&column.source_type),
                        delta_type_name(&column.data_type),
                    )
                };
                let row = match rows.iter_mut().find(|row| {
                    row.conversion == *conversion && row.source == source && row.delta == delta
                }) {
                    Some(row) => row,
                    None => {
                        rows.push(MappingRow {
                            source,
                            delta,
                            conversion: *conversion,
                            columns: Vec::new(),
                        });
                        rows.last_mut().expect("just pushed")
                    }
                };
                match row
                    .columns
                    .iter_mut()
                    .find(|(name, _)| *name == column.name)
                {
                    Some((_, tables)) => tables.push(table.name.clone()),
                    None => row
                        .columns
                        .push((column.name.clone(), vec![table.name.clone()])),
                }
            }
        }
    }
    rows.sort_by_key(|row| row.conversion);
    rows
}

fn render_mapping(reference: &ChainReference, out: &mut String) {
    let every_table: Vec<&str> = reference.tables.iter().map(|t| t.name.as_str()).collect();
    out.push_str(
        "\n## Delta type mapping\n\n\
         The mapper builds Arrow types; every flush maps them onto the Delta types above \
         once, with checked casts, before anything is written (#643, \
         `firehose_parquet::delta::types`). Columns that are not listed are built with \
         their Delta type. A column is listed with its tables unless every table has it \
         (`stream_ordinal` on non-final streams only).\n\n\
         | Mapper Arrow type | Delta type | Conversion | Columns |\n\
         |---|---|---|---|\n",
    );
    for row in mapping_rows(reference) {
        let columns: Vec<String> = row
            .columns
            .iter()
            .map(|(column, tables)| {
                let missing: Vec<String> = every_table
                    .iter()
                    .filter(|table| !tables.iter().any(|t| t == *table))
                    .map(|table| format!("`{table}`"))
                    .collect();
                if missing.is_empty() {
                    format!("`{column}`")
                } else if missing.len() <= 2 && missing.len() < tables.len() {
                    format!("`{column}` (every table but {})", missing.join(" and "))
                } else {
                    let tables: Vec<String> = tables.iter().map(|t| format!("`{t}`")).collect();
                    format!("`{column}` ({})", tables.join(", "))
                }
            })
            .collect();
        let delta = if row.conversion == Conversion::Partition {
            row.delta.clone()
        } else {
            format!("`{}`", row.delta)
        };
        let source = if row.conversion == Conversion::Partition {
            format!("`{}` column (`Date32`)", row.source)
        } else {
            format!("`{}`", row.source)
        };
        let _ = writeln!(
            out,
            "| {source} | {delta} | {} | {} |",
            escape_cell(row.conversion.rule()),
            escape_cell(&columns.join("; ")),
        );
    }
    out.push_str(
        "\n### `decimal(20,0)` columns\n\n\
         The `UInt64` columns stored as `decimal(20,0)` instead of a checked `long` \
         (`ChainProfile::decimal_columns` in `blocks/src/chain.rs`). Every other `UInt64` \
         column is bounded by its protocol.\n\n",
    );
    if reference.decimal_columns.is_empty() {
        out.push_str("None: every `UInt64` column of this chain is a checked `long`.\n");
        return;
    }
    out.push_str("| Column | Why not a checked `long` |\n|---|---|\n");
    for decimal in reference.decimal_columns {
        let _ = writeln!(
            out,
            "| `{}.{}` | {} |",
            decimal.table,
            decimal.column,
            escape_cell(decimal.reason)
        );
    }
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
            type_name(&DataType::List(item(fee.clone(), false))),
            "List<non-null Struct<denom: non-null Utf8, memo: Utf8>>"
        );

        // The Delta names of the data file types.
        assert_eq!(
            delta_type_name(&firehose_parquet::traits::timestamp_micros_utc_type()),
            "timestamp"
        );
        assert_eq!(
            delta_type_name(&DataType::Decimal128(20, 0)),
            "decimal(20,0)"
        );
        assert_eq!(
            delta_type_name(&DataType::List(item(DataType::Int16, false))),
            "array<non-null short>"
        );
        assert_eq!(
            delta_type_name(&DataType::List(item(DataType::Decimal128(20, 0), true))),
            "array<decimal(20,0)>"
        );
        assert_eq!(
            delta_type_name(&DataType::List(item(fee, false))),
            "array<non-null struct<denom: non-null string, memo: string>>"
        );
        assert_eq!(
            delta_type_name(&DataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                None
            )),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None).to_string(),
            "a zone-less timestamp is not a Delta timestamp"
        );
    }

    /// Every chain file ends with its mapping: the partition column, the
    /// profile's `decimal(20,0)` columns with their reasons, and a row for
    /// every conversion its tables go through.
    #[test]
    fn every_chain_documents_its_delta_type_mapping() {
        for kind in ChainKind::ALL {
            let reference = chain_reference(kind).unwrap();
            let markdown = render_chain(&reference);
            assert!(markdown.contains("\n## Delta type mapping\n"), "{kind}");
            for decimal in kind.profile().decimal_columns {
                assert!(
                    markdown.contains(&format!(
                        "| `{}.{}` | {} |",
                        decimal.table, decimal.column, decimal.reason
                    )),
                    "{kind}: {decimal:?}"
                );
            }
            let rows = mapping_rows(&reference);
            let conversions: BTreeSet<Conversion> = rows.iter().map(|row| row.conversion).collect();
            for expected in [
                Conversion::CheckedLong,
                Conversion::LosslessInteger,
                Conversion::TimestampMicros,
                Conversion::Partition,
            ] {
                assert!(conversions.contains(&expected), "{kind}: {expected:?}");
            }
            assert_eq!(
                conversions.contains(&Conversion::Decimal),
                !kind.profile().decimal_columns.is_empty(),
                "{kind}"
            );
            // Every converted column is in exactly the rows of its conversions.
            for table in &reference.tables {
                for column in &table.columns {
                    for conversion in &column.conversions {
                        assert!(
                            rows.iter().any(|row| row.conversion == *conversion
                                && row.columns.iter().any(|(name, tables)| {
                                    *name == column.name && tables.contains(&table.name)
                                })),
                            "{kind}: {}.{} {conversion:?}",
                            table.name,
                            column.name
                        );
                    }
                }
            }
        }
    }
}
