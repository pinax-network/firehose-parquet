//! The Delta root: one append-only, unpartitioned table per reference table,
//! at `<root>/<table>/`.
//!
//! Commits are optimistic and never retried. A run reads each table at its
//! version V, decides which rows changed against V, and commits V+1 with
//! `max_retries(0)` and no post-commit hooks. Any other commit after V fails
//! it, either at delta-rs's check of the latest version or at the conditional
//! create of the commit file (`If-None-Match: *` on S3, a no-clobber link on
//! disk), and the next run diffs against whatever is there, so no row is
//! appended twice. A handle moves only to the version its own commit returned
//! (`FinalizedCommit::snapshot`), never past another writer's.
//!
//! Nothing links DataFusion. An append writes one Parquet file with delta-rs's
//! `PartitionWriter` and commits its `add`; reads stream the pinned snapshot's
//! active files, as the log lists them (never a directory listing); OPTIMIZE
//! ([`crate::compact`]) and VACUUM ([`crate::vacuum`]) are this crate's own.
//! A checkpoint follows delta-rs's rule (after version `v` when
//! `(v + 1) % delta.checkpointInterval == 0`), written explicitly once the
//! commits that crossed it landed.
//!
//! Schemas only grow: a release may append nullable columns after the last
//! one, and a writer adds them to an older table as a metadata-only commit.
//! Anything else is a configuration error.
//!
//! On S3 the log goes through firehose-parquet's `s3_log_client` (one attempt
//! per write, retried reads) and the data files through `s3_data_client`
//! (bounded retries: their names are unique).

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use deltalake_core::arrow::datatypes::SchemaRef;
use deltalake_core::checkpoints::{cleanup_metadata, create_checkpoint};
use deltalake_core::datafile::writer::{PartitionWriter, PartitionWriterConfig};
use deltalake_core::kernel::transaction::{CommitBuilder, CommitProperties, FinalizedCommit};
use deltalake_core::kernel::{Action, Add, LogicalFileView, StructField};
use deltalake_core::logstore::{LogStoreRef, ObjectStoreRef};
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use deltalake_core::parquet::arrow::ProjectionMask;
use deltalake_core::parquet::basic::{Compression, ZstdLevel};
use deltalake_core::parquet::errors::ParquetError;
use deltalake_core::parquet::file::properties::WriterProperties;
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::table::config::TablePropertiesExt as _;
use deltalake_core::table::state::DeltaTableState;
#[cfg(test)]
use deltalake_core::TableProperty;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableError};
use firehose_parquet::delta::store::{s3_data_client, s3_log_client, DeltaStore};
use firehose_parquet::s3::AwsConfig;
use fireparq_maintenance::compact::StoreReader;
use fireparq_maintenance::is_conflict;
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::prefix::PrefixStore;
use serde_json::{json, Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::arrow::{decode, delta_fields, encode, file_schema, TABLE_PROPERTIES};
use crate::clock::Clock;
use crate::log::Logger;
use crate::model::{Cell, Key, Row, TableSpec};
use crate::{compact, vacuum, ConfigError, Env};

/// The release, recorded in every commit (`hlref.version`).
const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The rows of one row group, at most: memory while writing is about one.
const ROW_GROUP_ROWS: usize = 65_536;

/// A VACUUM in the VACUUM hour is skipped when one ran less than this ago.
const VACUUM_INTERVAL_MICROS: i64 = 20 * 3_600_000_000;

/// Where the tables live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Root {
    /// An absolute directory, `.` and `..` collapsed lexically (symlinks are
    /// not resolved), without a trailing `/`. It need not exist yet.
    Local(PathBuf),
    /// `s3://<bucket>[/<prefix>]`. Production uses the bucket root: its
    /// bucket policy grants only the top-level table prefixes.
    S3 { bucket: String, prefix: String },
}

impl Root {
    /// `s3://<bucket>[/<prefix>]`, or anything else as a local directory.
    /// Trailing `/`s are dropped.
    pub fn parse(text: &str) -> Result<Root, ConfigError> {
        if let Some(rest) = text.strip_prefix("s3://") {
            let rest = rest.trim_end_matches('/');
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                return Err(ConfigError(
                    "an s3:// root needs a bucket: s3://<bucket>[/<prefix>]".into(),
                ));
            }
            return Ok(Root::S3 {
                bucket: bucket.to_owned(),
                prefix: prefix.trim_matches('/').to_owned(),
            });
        }
        if text.is_empty() {
            return Err(ConfigError("--root or HLREF_ROOT is required".into()));
        }
        let absolute = std::path::absolute(text)
            .map_err(|_| ConfigError("the root directory has no absolute path".into()))?;
        Ok(Root::Local(lexical(&absolute)))
    }

    /// The root as the `start` event shows it.
    pub fn display(&self) -> String {
        match self {
            Root::Local(path) => path.display().to_string(),
            Root::S3 { bucket, prefix } if prefix.is_empty() => format!("s3://{bucket}"),
            Root::S3 { bucket, prefix } => format!("s3://{bucket}/{prefix}"),
        }
    }
}

/// `path` with `.` and `..` collapsed as Python's `os.path.abspath` does,
/// without looking at the file system.
fn lexical(path: &Path) -> PathBuf {
    let mut normal = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normal.pop();
            }
            other => normal.push(other.as_os_str()),
        }
    }
    normal
}

/// The settings an `s3://` root refuses, in the Python job's order, then its
/// credentials rule. Each message names settings, never their values.
fn s3_guards(aws: &AwsConfig, env: Env<'_>) -> Result<(), ConfigError> {
    let var = |name: &str| env(name).filter(|value| !value.is_empty());
    if var("AWS_S3_ALLOW_UNSAFE_RENAME")
        .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true"))
    {
        return Err(ConfigError(
            "AWS_S3_ALLOW_UNSAFE_RENAME is refused: commits must be conditional puts".into(),
        ));
    }
    // A stale ConfigMap would otherwise send the RGW key's requests to AWS.
    if ["S3_ENDPOINT", "AWS_ENDPOINT_URL"]
        .into_iter()
        .any(|name| var(name).is_some_and(|value| Some(&value) != aws.aws_endpoint_url.as_ref()))
    {
        return Err(ConfigError(
            "S3_ENDPOINT and AWS_ENDPOINT_URL are not read by fireparq hypercore-reference; \
             set AWS_ENDPOINT_URL_S3 (same value)"
                .into(),
        ));
    }
    if aws
        .aws_endpoint_url
        .as_deref()
        .is_some_and(|endpoint| !endpoint.to_ascii_lowercase().starts_with("https://"))
    {
        return Err(ConfigError(
            "AWS_ENDPOINT_URL_S3 must be an https:// URL".into(),
        ));
    }
    // Without keys object_store would ask instance metadata, which the
    // cluster does not have: a long timeout instead of this error.
    let given = |value: &Option<String>| value.as_deref().is_some_and(|value| !value.is_empty());
    if !given(&aws.aws_access_key_id) || !given(&aws.aws_secret_access_key) {
        return Err(ConfigError(
            "an s3:// root needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY".into(),
        ));
    }
    Ok(())
}

/// A client that cannot be built: its error chain, redacted.
fn client_error(log: &Logger, error: &dyn std::fmt::Display) -> ConfigError {
    ConfigError(format!(
        "the S3 client cannot be built: {}",
        log.redact(&format!("{error:#}"))
    ))
}

/// A table of the root, at the version the run read or last committed.
pub struct Table {
    pub spec: &'static TableSpec,
    pub delta: DeltaTable,
}

impl Table {
    /// The handle's version (a loaded table always has one).
    pub fn version(&self) -> u64 {
        self.delta.version().unwrap_or_default()
    }

    /// Moves the handle to the state its own commit returned, which must be
    /// version `expected`; never to a later one another writer made.
    pub(crate) fn advance(
        &mut self,
        committed: &FinalizedCommit,
        expected: u64,
    ) -> DeltaResult<u64> {
        self.delta.state = Some(committed.snapshot());
        let version = self.version();
        if version != expected {
            return Err(DeltaTableError::Generic(format!(
                "the commit landed at version {version}, not {expected}"
            )));
        }
        Ok(version)
    }
}

/// Whether [`Store::open`] may change the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OpenMode {
    /// Create a missing table; add the columns this release appended.
    Write,
    /// Neither: a missing table is `None`, an older one is read as it is.
    Read,
}

/// Why a store operation failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// The table's schema is not one this release can extend: exit status 2.
    Config(String),
    /// A run failure: the message is recorded and the run exits 1.
    Failed(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::Config(message) | StoreError::Failed(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for StoreError {}

/// A committed append.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Appended {
    pub version: u64,
    /// The checkpoint the commit was due for failed: a run failure, though
    /// the rows landed.
    pub checkpoint_error: Option<String>,
}

/// How [`Store::maintain`] compacts and vacuums a table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaintainOptions {
    /// OPTIMIZE once the table has this many active files.
    pub compact_min_files: usize,
    /// OPTIMIZE and VACUUM now, whatever the file count and the hour.
    pub force: bool,
    /// The UTC hour of the daily VACUUM.
    pub vacuum_hour: u8,
    /// OPTIMIZE packs files smaller than this into files up to this size.
    pub target_size: u64,
    /// A full VACUUM keeps what was removed, or written, more recently.
    pub retention_hours: u64,
}

impl Default for MaintainOptions {
    fn default() -> Self {
        Self {
            compact_min_files: 24,
            force: false,
            vacuum_hour: 6,
            target_size: 128 * 1024 * 1024,
            retention_hours: 168,
        }
    }
}

/// What a maintenance run did; a step that did not run is `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Maintained {
    pub files_before: Option<usize>,
    pub files_removed: Option<usize>,
    pub files_added: Option<usize>,
    pub vacuum_deleted: Option<usize>,
    pub version: u64,
}

/// Skip a file whose statistics prove that no row has
/// `column > greater_than` (µs). delta-rs writes a timestamp's maximum
/// truncated to the millisecond, so a file is skipped only when that maximum
/// plus 999 µs is still at or below the bound; missing or unreadable
/// statistics never skip a file.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Prune {
    pub column: &'static str,
    pub greater_than: i64,
}

impl Prune {
    fn skips(&self, file: &LogicalFileView) -> bool {
        let maximum = stats(file)
            .as_ref()
            .and_then(|stats| stats.get("maxValues")?.get(self.column)?.as_str())
            .and_then(stats_micros);
        maximum.is_some_and(|maximum| maximum.saturating_add(999) <= self.greater_than)
    }
}

/// A timestamp statistic (`2026-10-08T12:00:00Z`, `...00.123Z`), in µs.
fn stats_micros(text: &str) -> Option<i64> {
    let moment = OffsetDateTime::parse(text, &Rfc3339).ok()?;
    i64::try_from(moment.unix_timestamp_nanos() / 1000).ok()
}

/// A file's `add.stats`, parsed.
pub(crate) fn stats(file: &LogicalFileView) -> Option<Value> {
    serde_json::from_str(&file.stats()?).ok()
}

/// The first `max` characters of `text`.
pub(crate) fn cut(text: &str, max: usize) -> String {
    text.chars().take(max).collect()
}

/// A failure's kind: the error's variant, a transaction error's own.
pub(crate) fn kind(error: &DeltaTableError) -> String {
    let debug = match error {
        DeltaTableError::Transaction { source } => format!("{source:?}"),
        other => format!("{other:?}"),
    };
    debug
        .split(|c: char| !c.is_alphanumeric())
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// The UTC hour of a time in µs.
fn hour_of(micros: i64) -> u8 {
    micros.div_euclid(3_600_000_000).rem_euclid(24) as u8
}

/// The `(name, Delta type, nullable)` of each field, in order: what a
/// table's schema is compared by.
fn shape<'a>(fields: impl Iterator<Item = &'a StructField>) -> Vec<(String, Value, bool)> {
    fields
        .map(|field| {
            (
                field.name().clone(),
                serde_json::to_value(field.data_type()).unwrap_or(Value::Null),
                field.is_nullable(),
            )
        })
        .collect()
}

#[cfg(test)]
type FailAppend = Box<dyn Fn(&str, usize) -> Option<String> + Send + Sync>;

/// The tables of a root, and how this run commits to them.
pub struct Store {
    delta: DeltaStore,
    /// S3: the data files' client (retried); local: each table's own store.
    data: Option<ObjectStoreRef>,
    root: Root,
    run_id: String,
    mode: &'static str,
    log: Logger,
    clock: Arc<dyn Clock>,
    /// Fails an append of `(table, rows)` before anything is written.
    #[cfg(test)]
    pub(crate) fail_append: Option<FailAppend>,
    /// Fails adding columns with this message.
    #[cfg(test)]
    pub(crate) fail_add_columns: Option<String>,
    /// Creates tables with this `delta.deletedFileRetentionDuration`.
    #[cfg(test)]
    pub(crate) deleted_file_retention: Option<&'static str>,
    /// How often the history was read for the VACUUM due rule.
    #[cfg(test)]
    pub(crate) history_reads: AtomicUsize,
}

impl Store {
    /// The store of `root`. An `s3://` root is checked first: no unsafe
    /// rename, no stale endpoint variable, an `https` endpoint, and both
    /// keys. `log` redacts what delta-rs reports; `clock` decides the VACUUM
    /// hour; `run_id` and `mode` go into every commit.
    pub fn new(
        root: Root,
        aws: &AwsConfig,
        env: Env<'_>,
        run_id: String,
        mode: &'static str,
        log: Logger,
        clock: Arc<dyn Clock>,
    ) -> Result<Store, ConfigError> {
        let (delta, data) = match &root {
            Root::Local(path) => {
                let delta = DeltaStore::local(path)
                    .map_err(|_| ConfigError("the root directory is not absolute".into()))?;
                (delta, None)
            }
            Root::S3 { bucket, prefix } => {
                s3_guards(aws, env)?;
                let client = |error| client_error(&log, &error);
                let log_client = s3_log_client(aws, bucket).map_err(client)?;
                let delta = DeltaStore::s3(bucket, prefix, log_client).map_err(|_| {
                    ConfigError("the s3:// root's prefix is not a valid path".into())
                })?;
                (delta, Some(s3_data_client(aws, bucket).map_err(client)?))
            }
        };
        Ok(Store {
            delta,
            data,
            root,
            run_id,
            mode,
            log,
            clock,
            #[cfg(test)]
            fail_append: None,
            #[cfg(test)]
            fail_add_columns: None,
            #[cfg(test)]
            deleted_file_retention: None,
            #[cfg(test)]
            history_reads: AtomicUsize::new(0),
        })
    }

    /// Where the tables are.
    pub fn root(&self) -> &Root {
        &self.root
    }

    /// `<table>: <what> failed (<kind>: <delta-rs's text, redacted, cut>)`.
    fn failure(&self, spec: &TableSpec, what: &str, error: &DeltaTableError) -> String {
        format!("{}: {what} failed ({})", spec.name, self.describe(error))
    }

    /// An error's kind and text, credentials redacted, cut to 300 characters.
    fn describe(&self, error: &DeltaTableError) -> String {
        let text = self.log.redact(&error.to_string());
        format!("{}: {}", kind(error), cut(&text, 300))
    }

    /// Commit properties: one attempt, no post-commit hooks (checkpoints are
    /// explicit), and the run's provenance (`hlref.*`; nothing reads it).
    pub(crate) fn commit_properties(&self, action: &str) -> CommitProperties {
        self.commit_metadata(action, None)
    }

    /// [`Self::commit_properties`] with `operationMetrics`.
    pub(crate) fn commit_metadata(&self, action: &str, metrics: Option<Value>) -> CommitProperties {
        let mut metadata = vec![
            ("hlref.version".to_owned(), json!(VERSION)),
            ("hlref.run_id".to_owned(), json!(self.run_id)),
            ("hlref.mode".to_owned(), json!(self.mode)),
            ("hlref.action".to_owned(), json!(action)),
        ];
        if let Some(metrics) = metrics {
            metadata.push(("operationMetrics".to_owned(), metrics));
        }
        CommitProperties::default()
            .with_max_retries(0)
            .with_create_checkpoint(false)
            .with_cleanup_expired_logs(Some(false))
            .with_metadata(metadata)
    }

    /// Where `t`'s data files are written and deleted: on S3 the retried
    /// client under the table's prefix, else the table's own store.
    pub(crate) fn data_store(&self, t: &Table) -> ObjectStoreRef {
        match (&self.data, &self.root) {
            (Some(data), Root::S3 { prefix, .. }) => {
                let path: ObjectPath = prefix
                    .split('/')
                    .chain(t.spec.name.split('/'))
                    .filter(|part| !part.is_empty())
                    .collect();
                Arc::new(PrefixStore::new(Arc::clone(data), path))
            }
            _ => t.delta.object_store(),
        }
    }

    /// A writer of one data file of `schema` at the table's root: ZSTD 3,
    /// row groups of at most 65,536 rows, the table's statistics columns.
    pub(crate) fn writer(&self, t: &Table, schema: SchemaRef) -> DeltaResult<PartitionWriter> {
        let properties = t.delta.snapshot()?.table_config();
        let stats_columns = properties
            .data_skipping_stats_columns
            .as_ref()
            .map(|columns| columns.iter().map(ToString::to_string).collect());
        let level = ZstdLevel::try_new(3).expect("3 is a ZSTD level");
        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(level))
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .build();
        let config = PartitionWriterConfig::try_new(
            schema,
            Default::default(),
            Some(writer_properties),
            None,
            None,
            None,
            None,
        )?;
        PartitionWriter::try_with_config(
            self.data_store(t),
            config,
            properties.num_indexed_cols(),
            stats_columns,
        )
    }

    /// The checkpoint delta-rs's rule asks for after commits took the table
    /// from version `old` to `new`, then the expired log entries behind it.
    /// When entries went, the handle is reloaded at its own version `new`, as
    /// delta-rs's post-commit hook does: its snapshot may still name them
    /// (all of them after a week without a commit), and a VACUUM planned on
    /// it reads them. A failure is returned as a run failure: the commits
    /// stand.
    async fn checkpoint(&self, t: &mut Table, old: u64, new: u64) -> Option<String> {
        let interval = t
            .delta
            .snapshot()
            .ok()?
            .table_config()
            .checkpoint_interval();
        if (new + 1) / interval.get() <= (old + 1) / interval.get() {
            return None;
        }
        let written = async {
            create_checkpoint(&t.delta, None).await?;
            if cleanup_metadata(&t.delta, None).await? > 0 {
                let state =
                    DeltaTableState::try_new(t.delta.log_store().as_ref(), Some(new)).await?;
                t.delta.state = Some(state);
            }
            DeltaResult::Ok(())
        }
        .await;
        written.err().map(|error| {
            format!(
                "{}: checkpoint at version {new} failed ({})",
                t.spec.name,
                kind(&error)
            )
        })
    }

    /// The table at its latest version: created empty when missing
    /// ([`OpenMode::Write`]), or `None`. An older table gets this release's
    /// appended columns in `Write` mode and is read as it is in `Read`.
    pub async fn open(
        &self,
        spec: &'static TableSpec,
        mode: OpenMode,
    ) -> Result<Option<Table>, StoreError> {
        let failed =
            |error: &DeltaTableError| StoreError::Failed(self.failure(spec, "opening", error));
        let log_store = self.delta.log_store(spec.name).map_err(|error| {
            StoreError::Failed(format!(
                "{}: opening failed ({})",
                spec.name,
                self.log.redact(&format!("{error:#}"))
            ))
        })?;
        // A listing of `_delta_log/`: delta-rs logs an ERROR loading a
        // missing table (#680).
        let exists = log_store
            .is_delta_table_location()
            .await
            .map_err(|error| failed(&error))?;
        if !exists {
            if mode == OpenMode::Read {
                return Ok(None);
            }
            if let Some(table) = self.create(spec, log_store.clone()).await? {
                return Ok(Some(table));
            }
        }
        let mut table = DeltaTable::new(log_store);
        table.load().await.map_err(|error| failed(&error))?;
        self.evolve(spec, table, mode).await.map(Some)
    }

    /// Creates the table (version 0); `None` when another writer created it
    /// first.
    async fn create(
        &self,
        spec: &'static TableSpec,
        log_store: LogStoreRef,
    ) -> Result<Option<Table>, StoreError> {
        let mut builder = CreateBuilder::new()
            .with_log_store(log_store)
            .with_table_name(spec.delta_name())
            .with_columns(delta_fields(spec))
            .with_save_mode(SaveMode::ErrorIfExists)
            .with_commit_properties(self.commit_properties("CREATE"));
        for (property, value) in TABLE_PROPERTIES {
            #[cfg(test)]
            let value = match (&property, self.deleted_file_retention) {
                (TableProperty::DeletedFileRetentionDuration, Some(retention)) => retention,
                _ => value,
            };
            builder = builder.with_configuration_property(property, Some(value));
        }
        match builder.await {
            Ok(table) => {
                self.log.emit(
                    "created",
                    json!({"table": spec.name, "version": table.version()}),
                );
                Ok(Some(Table { spec, delta: table }))
            }
            Err(error) if is_conflict(&error) || error.to_string().contains("already exists") => {
                Ok(None)
            }
            Err(error) => Err(StoreError::Failed(self.failure(
                spec,
                "creating the table",
                &error,
            ))),
        }
    }

    /// Checks the table's schema against the release's, and adds the
    /// release's appended nullable columns in `Write` mode.
    async fn evolve(
        &self,
        spec: &'static TableSpec,
        table: DeltaTable,
        mode: OpenMode,
    ) -> Result<Table, StoreError> {
        let schema = table
            .snapshot()
            .map_err(|error| StoreError::Failed(self.failure(spec, "opening", &error)))?
            .schema();
        let actual = shape(schema.fields());
        let fields = delta_fields(spec);
        let expected = shape(fields.iter());
        if actual == expected {
            return Ok(Table { spec, delta: table });
        }
        let appended = actual.len() < expected.len()
            && expected[..actual.len()] == actual[..]
            && expected[actual.len()..]
                .iter()
                .all(|(_, _, nullable)| *nullable);
        if !appended {
            return Err(StoreError::Config(format!(
                "{}: the table's schema differs from this release's \
                 (only appended nullable columns can be added)",
                spec.name
            )));
        }
        if mode == OpenMode::Read {
            return Ok(Table { spec, delta: table });
        }
        self.add_columns(spec, table, fields[actual.len()..].to_vec())
            .await
    }

    /// Commits `added` as new last columns: a metadata-only commit that keeps
    /// the table's properties. Old rows read NULL there.
    async fn add_columns(
        &self,
        spec: &'static TableSpec,
        table: DeltaTable,
        added: Vec<StructField>,
    ) -> Result<Table, StoreError> {
        let names: Vec<String> = added.iter().map(|field| field.name().clone()).collect();
        let failed = |kind: String| {
            StoreError::Failed(format!(
                "{}: adding columns {names:?} failed ({kind})",
                spec.name
            ))
        };
        #[cfg(test)]
        if let Some(message) = &self.fail_add_columns {
            return Err(StoreError::Failed(message.clone()));
        }
        let before = table.version().unwrap_or_default();
        let delta = table
            .add_columns()
            .with_fields(added)
            .with_commit_properties(self.commit_properties("ADD COLUMNS"))
            .await
            .map_err(|error| failed(kind(&error)))?;
        let mut t = Table { spec, delta };
        let version = t.version();
        if version != before + 1 {
            return Err(failed(format!(
                "it landed at version {version}, not {}",
                before + 1
            )));
        }
        self.log.emit(
            "columns_added",
            json!({"table": spec.name, "columns": names, "version": version}),
        );
        match self.checkpoint(&mut t, before, version).await {
            Some(error) => Err(StoreError::Failed(error)),
            None => Ok(t),
        }
    }

    /// The pinned snapshot's active files, as its log lists them, in commit
    /// order (modification time, then path).
    pub(crate) async fn active_files(&self, t: &Table) -> Result<Vec<LogicalFileView>, StoreError> {
        let failed = |error: DeltaTableError| {
            StoreError::Failed(self.failure(t.spec, "listing the active files", &error))
        };
        let snapshot = t.delta.snapshot().map_err(failed)?;
        let log_store = t.delta.log_store();
        let mut files: Vec<LogicalFileView> = snapshot
            .snapshot()
            .file_views(log_store.as_ref(), None)
            .try_collect()
            .await
            .map_err(failed)?;
        files.sort_by_cached_key(|file| (file.modification_time(), file.path().into_owned()));
        Ok(files)
    }

    /// Calls `f` with the cells of `columns` of every row of the pinned
    /// snapshot, file by file, a batch at a time. A column a file lacks
    /// (written before the column was added) is NULL. `prune` skips files
    /// whose statistics prove they hold no wanted row; the rows of the files
    /// read are all passed.
    pub async fn for_each_row(
        &self,
        t: &Table,
        columns: &[&str],
        prune: Option<Prune>,
        f: &mut (dyn FnMut(&[Cell]) + Send),
    ) -> Result<(), StoreError> {
        for file in self.active_files(t).await? {
            if prune.is_some_and(|prune| prune.skips(&file)) {
                continue;
            }
            self.read_file(t, &file, columns, f).await?;
        }
        Ok(())
    }

    /// One data file's rows: ranged reads of only the columns asked for.
    async fn read_file(
        &self,
        t: &Table,
        file: &LogicalFileView,
        columns: &[&str],
        f: &mut (dyn FnMut(&[Cell]) + Send),
    ) -> Result<(), StoreError> {
        let failed = |error: ParquetError| self.read_failure(t, &error);
        let size = u64::try_from(file.size()).unwrap_or_default();
        let reader = StoreReader::new(t.delta.object_store(), file.object_store_path(), size);
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(failed)?;
        // A name the file lacks selects nothing.
        let mask = ProjectionMask::columns(builder.parquet_schema(), columns.iter().copied());
        let mut batches = builder.with_projection(mask).build().map_err(failed)?;
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(failed)?;
            let rows = decode(&batch, t.spec, columns).map_err(|error| {
                StoreError::Failed(format!("{}: {}", t.spec.name, cut(&error.to_string(), 500)))
            })?;
            for row in &rows {
                f(row);
            }
        }
        Ok(())
    }

    fn read_failure(&self, t: &Table, error: &ParquetError) -> StoreError {
        let gone = matches!(error, ParquetError::External(source)
            if matches!(source.downcast_ref::<object_store::Error>(),
                        Some(object_store::Error::NotFound { .. })));
        StoreError::Failed(if gone {
            format!(
                "{}: an active data file is gone (a VACUUM after version {}); the next run retries",
                t.spec.name,
                t.version()
            )
        } else {
            format!(
                "{}: reading a data file failed ({})",
                t.spec.name,
                cut(&self.log.redact(&error.to_string()), 300)
            )
        })
    }

    /// Each entity's latest row, with every column but `payload_json` (which
    /// is NULL in it).
    pub async fn latest(&self, t: &Table) -> Result<HashMap<Key, Row>, StoreError> {
        let columns: Vec<&str> = t
            .spec
            .columns
            .iter()
            .map(|column| column.name)
            .filter(|name| *name != "payload_json")
            .collect();
        self.latest_with(t, &columns).await
    }

    /// Each entity's latest row by `observed_at`, with `columns` (the key and
    /// `observed_at` among them); the others are NULL. On a tie the later
    /// file wins, then the later row.
    pub async fn latest_with(
        &self,
        t: &Table,
        columns: &[&str],
    ) -> Result<HashMap<Key, Row>, StoreError> {
        let spec = t.spec;
        let observed = |row: &Row| match spec.position("observed_at").map(|index| &row.cells[index])
        {
            Some(Cell::Timestamp(at)) => Some(*at),
            _ => None,
        };
        let mut latest: HashMap<Key, Row> = HashMap::new();
        self.for_each_row(t, columns, None, &mut |cells| {
            let row = Row::of(spec, columns, cells);
            match latest.entry(row.key()) {
                Entry::Occupied(mut entry) => {
                    if observed(&row) >= observed(entry.get()) {
                        entry.insert(row);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(row);
                }
            }
        })
        .await?;
        Ok(latest)
    }

    /// Commits `rows` as version V+1 of the handle's version V: one data
    /// file, then its `add`. Nothing for no rows. On success the handle is at
    /// V+1, `appended` is logged and a checkpoint due is written.
    pub async fn append(
        &self,
        t: &mut Table,
        rows: Vec<Row>,
    ) -> Result<Option<Appended>, StoreError> {
        if rows.is_empty() {
            return Ok(None);
        }
        #[cfg(test)]
        if let Some(message) = self
            .fail_append
            .as_ref()
            .and_then(|fail| fail(t.spec.name, rows.len()))
        {
            return Err(StoreError::Failed(message));
        }
        let before = t.version();
        // Nothing was committed: an unreferenced file at most.
        let adds = self.write(t, &rows).await.map_err(|error| {
            StoreError::Failed(format!(
                "{}: commit after version {before} failed ({})",
                t.spec.name,
                self.describe(&error)
            ))
        })?;
        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };
        let committed = async {
            let committed = CommitBuilder::from(self.commit_properties("APPEND"))
                .with_actions(adds.into_iter().map(Action::Add).collect())
                .build(Some(t.delta.snapshot()?), t.delta.log_store(), operation)
                .await?;
            t.advance(&committed, before + 1)
        }
        .await;
        let version = committed.map_err(|error| {
            StoreError::Failed(if is_conflict(&error) {
                format!(
                    "{}: commit after version {before} failed (a concurrent commit)",
                    t.spec.name
                )
            } else {
                // The post-commit read of V+1 can fail after V+1 was
                // written: the next run's diff tells.
                format!(
                    "{}: commit after version {before} failed ({}); it may have landed",
                    t.spec.name,
                    self.describe(&error)
                )
            })
        })?;
        self.log.emit(
            "appended",
            json!({"table": t.spec.name, "rows": rows.len(), "version": version}),
        );
        let checkpoint_error = self.checkpoint(t, before, version).await;
        Ok(Some(Appended {
            version,
            checkpoint_error,
        }))
    }

    /// The rows as one data file of the table's current schema.
    async fn write(&self, t: &Table, rows: &[Row]) -> DeltaResult<Vec<Add>> {
        let schema = file_schema(&t.delta)?;
        let batch = encode(&schema, t.spec, rows)?;
        let mut writer = self.writer(t, schema)?;
        if let Err(error) = writer.write(&batch).await {
            let _ = writer.abort().await;
            return Err(error);
        }
        writer.close().await
    }

    /// Compacts and vacuums the table as its only writer. OPTIMIZE runs once
    /// it has `compact_min_files` active files; VACUUM in the runs of the
    /// VACUUM hour, unless one ran less than 20 hours before (a VACUUM that
    /// deletes nothing commits nothing, so the log cannot tell that it ran);
    /// `force` runs both. The history is read only when a VACUUM may be due.
    /// Logs `maintained`, and returns it, when either ran.
    pub async fn maintain(
        &self,
        t: &mut Table,
        options: &MaintainOptions,
    ) -> Result<Option<Maintained>, StoreError> {
        let name = t.spec.name;
        let mut maintained = Maintained::default();
        let files = self.active_files(t).await?;
        if options.force || files.len() >= options.compact_min_files {
            let before = t.version();
            let compacted = compact::optimize(self, t, &files, options.target_size)
                .await
                .map_err(|error| {
                    StoreError::Failed(format!("{name}: OPTIMIZE failed ({})", kind(&error)))
                })?;
            maintained.files_before = Some(files.len());
            maintained.files_removed = Some(compacted.files_removed);
            maintained.files_added = Some(compacted.files_added);
            let after = t.version();
            if let Some(error) = self.checkpoint(t, before, after).await {
                return Err(StoreError::Failed(error));
            }
        }
        let now = self.clock.now_micros();
        let due = options.force || hour_of(now) == options.vacuum_hour;
        let last = if due && !options.force {
            self.last_vacuum(t).await?
        } else {
            None
        };
        if options.force || (due && last.is_none_or(|last| now - last >= VACUUM_INTERVAL_MICROS)) {
            let before = t.version();
            let deleted = vacuum::vacuum(self, t, options.retention_hours)
                .await
                .map_err(|error| {
                    StoreError::Failed(format!("{name}: VACUUM failed ({})", kind(&error)))
                })?;
            maintained.vacuum_deleted = Some(deleted);
            let after = t.version();
            if let Some(error) = self.checkpoint(t, before, after).await {
                return Err(StoreError::Failed(error));
            }
        }
        if maintained.files_before.is_none() && maintained.vacuum_deleted.is_none() {
            return Ok(None);
        }
        maintained.version = t.version();
        let mut fields = Map::new();
        fields.insert("table".into(), json!(name));
        for (key, value) in [
            ("files_before", maintained.files_before),
            ("files_removed", maintained.files_removed),
            ("files_added", maintained.files_added),
            ("vacuum_deleted", maintained.vacuum_deleted),
        ] {
            if let Some(value) = value {
                fields.insert(key.into(), json!(value));
            }
        }
        fields.insert("version".into(), json!(maintained.version));
        self.log.emit("maintained", Value::Object(fields));
        Ok(Some(maintained))
    }

    /// When the newest VACUUM among the last 200 commits ran (µs).
    pub(crate) async fn last_vacuum(&self, t: &Table) -> Result<Option<i64>, StoreError> {
        #[cfg(test)]
        self.history_reads.fetch_add(1, Ordering::SeqCst);
        let mut history = t.delta.history(Some(200));
        while let Some(commit) = history.next().await {
            let commit = commit.map_err(|error| {
                StoreError::Failed(self.failure(t.spec, "reading the history", &error))
            })?;
            if commit
                .operation
                .as_deref()
                .is_some_and(|operation| operation.starts_with("VACUUM"))
            {
                return Ok(commit.timestamp.map(|millis| millis.saturating_mul(1000)));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests;
