//! `fireparq-maintenance`: the Delta maintenance job of a fireparq lake
//! (#643, `docs/design/delta-lake.md` §9).
//!
//! Compaction and cleanup of fireparq's Delta tables are platform-side policy:
//! this job runs delta-rs's own operations (`deltalake-core` 1.0.0, with
//! DataFusion for OPTIMIZE) on a schedule, a Kubernetes CronJob in
//! `deploy/examples/delta-maintenance-cronjob.yaml`, and has no compaction
//! logic of its own. It is safe beside a running `fireparq build`: fireparq's
//! commits are blind appends that rebase over OPTIMIZE, and VACUUM follows the
//! rule of design §4.1. Every step is idempotent, so a failed or conflicting
//! run is simply repeated by the next one.
//!
//! For each table, in this order:
//!
//! 1. OPTIMIZE (compact) each closed `date` partition that has more than one
//!    file, to the table's `delta.targetFileSize`. A date is closed once
//!    `blocks` holds a later date: `blocks` commits last in every fireparq
//!    transaction, so every earlier date is complete in every table.
//! 2. VACUUM. Lite (the default) deletes only files that a `remove` tombstone
//!    older than the retention names, and never a part that fireparq
//!    published but has not committed yet. Full (`FULL_VACUUM=1`, weekly)
//!    also deletes untracked files older than the retention, so it runs only
//!    with the enforced retention of at least 168 h
//!    (`delta.deletedFileRetentionDuration`).
//! 3. A checkpoint, after VACUUM: a checkpoint drops expired tombstones, so a
//!    lite VACUUM after it would never see them and their files would stay as
//!    orphans. The checkpoint is skipped when VACUUM failed, and no OPTIMIZE
//!    or VACUUM commit writes one on its own.
//! 4. Log cleanup: commits older than `delta.logRetentionDuration` behind a
//!    checkpoint.
//!
//! A table that does not exist yet (the writer has not created it) is
//! skipped, not failed (#680).
//!
//! Settings are environment variables ([`Settings::from_env`]; credentials
//! are read, never printed):
//!
//! - `LAKE_ROOT`: the dataset root, `s3://bucket[/prefix]` or a local path;
//!   or `LAKE_BUCKET`, shorthand for `s3://<bucket>` (a lake at the bucket
//!   root).
//! - `LAKE_TABLES` (required): comma-separated table names, for example
//!   `blocks,transactions,logs`.
//! - S3: `S3_ENDPOINT` (for example the in-cluster RGW URL), `AWS_REGION`
//!   (default `us-east-1`), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
//!   optional `AWS_SESSION_TOKEN`, `AWS_ALLOW_HTTP` (default `false`) and
//!   `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` (default `false`, path-style). Log
//!   commits use conditional puts (`If-None-Match: *`), and no request sends
//!   `If-Match`; `SSL_CERT_FILE` names a private CA.
//!   `AWS_S3_ALLOW_UNSAFE_RENAME` is refused.
//! - `FULL_VACUUM`: `1` for a full VACUUM (default `0`, lite).
//! - `VACUUM_RETENTION_HOURS`: default the table's
//!   `delta.deletedFileRetentionDuration` (7 days). A lite VACUUM may go
//!   lower, which only shortens how long readers of older snapshots find
//!   removed files; a full VACUUM refuses anything below 168.
//! - `OPTIMIZE_DATES`: `closed` (default) or `all`, which also compacts the
//!   open date (safe beside the writer, but repeated every run; for a lake
//!   whose writer has stopped).
//! - `OPTIMIZE_TARGET_SIZE`: bytes, default the table's `delta.targetFileSize`.
//! - `OPTIMIZE_ZSTD_LEVEL`: default 3, as fireparq writes.
//! - `OPTIMIZE_MAX_CONCURRENT_TASKS`: default the CPU count.
//! - `DRY_RUN`: `1` reports the plan and the files VACUUM would delete, and
//!   changes nothing.
//!
//! Output ([`run`]): one JSON object per line on stdout (`start`, one `table`
//! or `skipped` line per table, `done`). Exit status: 0 when every table was
//! maintained or skipped (a lost commit race is reported as a conflict and
//! left to the next run), 1 when a table failed, 2 for a configuration error.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::time::Instant;

use deltalake_core::checkpoints::{cleanup_metadata, create_checkpoint};
use deltalake_core::kernel::transaction::{CommitProperties, TransactionError};
use deltalake_core::logstore::{
    default_logstore, logstore_factories, logstore_with, LogStore, LogStoreFactory, LogStoreRef,
    StorageConfig,
};
use deltalake_core::operations::vacuum::VacuumMode;
use deltalake_core::parquet::basic::{Compression, ZstdLevel};
use deltalake_core::parquet::file::properties::WriterProperties;
use deltalake_core::{DeltaResult, DeltaTable, DeltaTableError, FilterOp, FilterValue};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::{ClientOptions, ObjectStore};
use serde_json::{json, Map, Value};
use url::Url;

/// A full VACUUM deletes untracked files, so it never runs with a shorter
/// retention (design §4.1).
pub const MIN_FULL_VACUUM_HOURS: u64 = 168;

/// Where the job reads its settings: the process environment in the binary,
/// an explicit map in tests. `None` is an unset variable.
pub type Env<'a> = &'a dyn Fn(&str) -> Option<String>;

/// A setting that makes the job unsafe or incomplete: exit status 2.
#[derive(Debug)]
pub struct ConfigError(pub String);

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

fn config<T>(message: impl Into<String>) -> Result<T, ConfigError> {
    Err(ConfigError(message.into()))
}

fn var(env: Env<'_>, name: &str, default: &str) -> String {
    env(name).unwrap_or_else(|| default.to_string())
}

fn flag(env: Env<'_>, name: &str, default: &str) -> Result<bool, ConfigError> {
    let value = var(env, name, default).trim().to_lowercase();
    match value.as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" | "" => Ok(false),
        _ => config(format!("{name} must be 0 or 1, got '{value}'")),
    }
}

fn integer(
    env: Env<'_>,
    name: &str,
    minimum: u64,
    maximum: Option<u64>,
) -> Result<Option<u64>, ConfigError> {
    let value = var(env, name, "").trim().to_string();
    if value.is_empty() {
        return Ok(None);
    }
    let Ok(number) = value.parse::<i128>() else {
        return config(format!("{name} must be an integer, got '{value}'"));
    };
    if number < i128::from(minimum) || maximum.is_some_and(|maximum| number > i128::from(maximum)) {
        let maximum = maximum.map_or("...".to_string(), |maximum| maximum.to_string());
        return config(format!(
            "{name} must be in [{minimum}, {maximum}], got {number}"
        ));
    }
    u64::try_from(number)
        .map(Some)
        .or_else(|_| config(format!("{name} is too large: {number}")))
}

/// The S3 client settings of a lake.
#[derive(Clone, Debug, Default)]
pub struct S3Options {
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    /// For example the in-cluster RGW URL; `None` is AWS.
    pub endpoint: Option<String>,
    pub allow_http: bool,
    pub virtual_hosted_style_request: bool,
}

#[derive(Clone, Debug)]
enum Root {
    /// An absolute directory.
    Local(PathBuf),
    /// `s3://<bucket>/<prefix>`; `prefix` has no leading or trailing `/`.
    S3 { bucket: String, prefix: String },
}

/// The Delta tables below one dataset root, opened through delta-rs's
/// default log store: conditional-create commits (`If-None-Match: *` on S3,
/// a no-clobber hard link on local disk), as fireparq commits.
#[derive(Clone)]
pub struct Lake {
    root: Root,
    store: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for Lake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lake({:?})", self.root)
    }
}

impl Lake {
    /// The tables below a local directory (made absolute, need not exist).
    pub fn local(root: &Path) -> std::io::Result<Self> {
        Ok(Self {
            root: Root::Local(std::path::absolute(root)?),
            store: Arc::new(LocalFileSystem::new()),
        })
    }

    /// The tables below `s3://<bucket>/<prefix>`. TLS trusts the platform's
    /// roots, or only `SSL_CERT_FILE`'s when it is set (rustls-native-certs).
    pub fn s3(bucket: &str, prefix: &str, options: &S3Options) -> Result<Self, String> {
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(&options.region)
            // Log commits with `If-None-Match: *`; no DynamoDB lock.
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_virtual_hosted_style_request(options.virtual_hosted_style_request)
            .with_client_options(ClientOptions::new().with_allow_http(options.allow_http));
        if let Some(key) = &options.access_key_id {
            builder = builder.with_access_key_id(key);
        }
        if let Some(secret) = &options.secret_access_key {
            builder = builder.with_secret_access_key(secret);
        }
        if let Some(token) = &options.session_token {
            builder = builder.with_token(token);
        }
        if let Some(endpoint) = &options.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        let store = builder
            .build()
            .map_err(|error| format!("building the S3 client: {error}"))?;
        Ok(Self {
            root: Root::S3 {
                bucket: bucket.to_string(),
                prefix: prefix.trim_matches('/').to_string(),
            },
            store: Arc::new(store),
        })
    }

    /// The URL of `table`'s Delta table: `<root>/<table>/`.
    pub fn table_url(&self, table: &str) -> DeltaResult<Url> {
        let url = match &self.root {
            Root::Local(root) => Url::from_directory_path(root.join(table)).map_err(|()| {
                DeltaTableError::InvalidTableLocation(format!("{}", root.join(table).display()))
            })?,
            Root::S3 { bucket, prefix } if prefix.is_empty() => {
                Url::parse(&format!("s3://{bucket}/{table}/"))
                    .map_err(|error| DeltaTableError::InvalidTableLocation(error.to_string()))?
            }
            Root::S3 { bucket, prefix } => Url::parse(&format!("s3://{bucket}/{prefix}/{table}/"))
                .map_err(|error| DeltaTableError::InvalidTableLocation(error.to_string()))?,
        };
        Ok(url)
    }

    /// The log store of `table`, which need not exist yet.
    pub fn log_store(&self, table: &str) -> DeltaResult<LogStoreRef> {
        let url = self.table_url(table)?;
        if matches!(self.root, Root::S3 { .. }) {
            register_s3_log_store();
        }
        logstore_with(Arc::clone(&self.store), &url, StorageConfig::default())
    }

    /// Opens `table` at its latest version.
    pub async fn open(&self, table: &str) -> DeltaResult<DeltaTable> {
        let mut table = DeltaTable::new(self.log_store(table)?);
        table.load().await?;
        Ok(table)
    }
}

/// delta-rs's `DefaultLogStore` (conditional-put commits) for `s3://` URLs:
/// without `deltalake-aws` (and its AWS SDK) delta-rs registers none.
struct ConditionalPutLogStoreFactory;

impl LogStoreFactory for ConditionalPutLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: Arc<dyn ObjectStore>,
        root_store: Arc<dyn ObjectStore>,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

fn register_s3_log_store() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        logstore_factories().insert(
            Url::parse("s3://").expect("static scheme"),
            Arc::new(ConditionalPutLogStoreFactory),
        );
    });
}

/// The job's settings, from the environment.
#[derive(Debug)]
pub struct Settings {
    /// `LAKE_ROOT` without a trailing `/`, or `s3://<LAKE_BUCKET>`.
    pub root: String,
    pub tables: Vec<String>,
    pub full_vacuum: bool,
    pub dry_run: bool,
    pub retention_hours: Option<u64>,
    pub optimize_all_dates: bool,
    pub target_size: Option<u64>,
    pub zstd_level: i32,
    pub max_concurrent_tasks: Option<usize>,
    s3: Option<S3Options>,
    secrets: Vec<String>,
}

impl Settings {
    /// Reads and checks every setting; nothing is requested yet.
    pub fn from_env(env: Env<'_>) -> Result<Self, ConfigError> {
        let root = var(env, "LAKE_ROOT", "").trim().to_string();
        let bucket = var(env, "LAKE_BUCKET", "").trim().to_string();
        if root.is_empty() == bucket.is_empty() {
            return config("set exactly one of LAKE_ROOT and LAKE_BUCKET");
        }
        let root = if root.is_empty() {
            format!("s3://{bucket}")
        } else {
            root
        };
        let is_s3 = root.starts_with("s3://");
        let root = root.trim_end_matches('/').to_string();
        let no_bucket = is_s3 && root.len() <= "s3://".len();
        if (root.contains("://") && !is_s3) || no_bucket {
            return config("LAKE_ROOT must be s3://bucket[/prefix] or a local path");
        }
        let tables: Vec<String> = var(env, "LAKE_TABLES", "")
            .split(',')
            .map(str::trim)
            .filter(|table| !table.is_empty())
            .map(str::to_string)
            .collect();
        if tables.is_empty() {
            return config("LAKE_TABLES must name the tables, for example blocks,transactions");
        }
        for table in &tables {
            if !table.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return config(format!("not a table name: '{table}'"));
            }
        }
        let full_vacuum = flag(env, "FULL_VACUUM", "0")?;
        let dry_run = flag(env, "DRY_RUN", "0")?;
        let retention_hours = integer(env, "VACUUM_RETENTION_HOURS", 0, None)?;
        if full_vacuum && retention_hours.is_some_and(|hours| hours < MIN_FULL_VACUUM_HOURS) {
            return config(format!(
                "a full VACUUM deletes untracked files, including parts fireparq has not \
                 committed yet, so it needs VACUUM_RETENTION_HOURS >= {MIN_FULL_VACUUM_HOURS}"
            ));
        }
        let dates = var(env, "OPTIMIZE_DATES", "closed").trim().to_lowercase();
        if dates != "closed" && dates != "all" {
            return config(format!(
                "OPTIMIZE_DATES must be closed or all, got '{dates}'"
            ));
        }
        let target_size = integer(env, "OPTIMIZE_TARGET_SIZE", 1, None)?;
        let zstd_level = integer(env, "OPTIMIZE_ZSTD_LEVEL", 1, Some(22))?.unwrap_or(3);
        let max_concurrent_tasks = integer(env, "OPTIMIZE_MAX_CONCURRENT_TASKS", 1, None)?;
        let mut settings = Self {
            root,
            tables,
            full_vacuum,
            dry_run,
            retention_hours,
            optimize_all_dates: dates == "all",
            target_size,
            zstd_level: zstd_level as i32,
            max_concurrent_tasks: max_concurrent_tasks
                .map(|tasks| usize::try_from(tasks).unwrap_or(usize::MAX)),
            s3: None,
            secrets: Vec::new(),
        };
        if is_s3 {
            settings.s3 = Some(settings.s3_options(env)?);
        }
        Ok(settings)
    }

    fn s3_options(&mut self, env: Env<'_>) -> Result<S3Options, ConfigError> {
        if flag(env, "AWS_S3_ALLOW_UNSAFE_RENAME", "0")? {
            return config("AWS_S3_ALLOW_UNSAFE_RENAME is unsafe beside a running writer");
        }
        let key = var(env, "AWS_ACCESS_KEY_ID", "");
        let secret = var(env, "AWS_SECRET_ACCESS_KEY", "");
        if key.is_empty() || secret.is_empty() {
            return config(
                "maintenance writes to S3: set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY",
            );
        }
        let allow_http = flag(env, "AWS_ALLOW_HTTP", "false")?;
        let virtual_hosted_style_request = flag(env, "AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false")?;
        self.secrets = vec![key.clone(), secret.clone()];
        let token = var(env, "AWS_SESSION_TOKEN", "");
        let session_token = (!token.is_empty()).then(|| {
            self.secrets.push(token.clone());
            token
        });
        let endpoint = var(env, "S3_ENDPOINT", "").trim().to_string();
        Ok(S3Options {
            region: var(env, "AWS_REGION", "us-east-1"),
            access_key_id: Some(key),
            secret_access_key: Some(secret),
            session_token,
            endpoint: (!endpoint.is_empty()).then_some(endpoint),
            allow_http,
            virtual_hosted_style_request,
        })
    }

    /// The lake at [`Settings::root`].
    pub fn lake(&self) -> Result<Lake, ConfigError> {
        match (&self.s3, self.root.strip_prefix("s3://")) {
            (Some(options), Some(location)) => {
                let (bucket, prefix) = location.split_once('/').unwrap_or((location, ""));
                Lake::s3(bucket, prefix, options).map_err(|error| ConfigError(self.redact(&error)))
            }
            _ => Lake::local(Path::new(&self.root))
                .map_err(|error| ConfigError(format!("LAKE_ROOT: {error}"))),
        }
    }

    /// `text` with every credential replaced by `***`.
    pub fn redact(&self, text: &str) -> String {
        let mut text = text.to_string();
        for secret in &self.secrets {
            text = text.replace(secret.as_str(), "***");
        }
        text
    }
}

/// `delta.deletedFileRetentionDuration` in hours (default 7 days), or `None`
/// when it is not an `interval <n> <unit>` value.
fn table_retention_hours(configuration: &HashMap<String, String>) -> Option<f64> {
    let value = configuration
        .get("delta.deletedFileRetentionDuration")
        .map_or("interval 7 days", String::as_str);
    let value = value.to_lowercase();
    let mut words = value.split_whitespace();
    if words.next()? != "interval" {
        return None;
    }
    let number = words.next()?;
    if number.is_empty() || !number.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let number: f64 = number.parse().ok()?;
    let unit = words.next()?;
    if words.next().is_some() {
        return None;
    }
    let hours = match unit.strip_suffix('s').unwrap_or(unit) {
        "second" => 1.0 / 3600.0,
        "minute" => 1.0 / 60.0,
        "hour" => 1.0,
        "day" => 24.0,
        "week" => 168.0,
        _ => return None,
    };
    Some(number * hours)
}

/// The number of active files per `date` partition.
fn active_files_per_date(table: &DeltaTable) -> DeltaResult<BTreeMap<String, u64>> {
    let snapshot = table.snapshot()?;
    let files = snapshot.log_data();
    let mut counts = BTreeMap::new();
    if files.num_files() == 0 {
        return Ok(counts);
    }
    if !snapshot
        .metadata()
        .partition_columns()
        .iter()
        .any(|column| column == "date")
    {
        return Err(DeltaTableError::Generic(
            "the table is not partitioned by date".into(),
        ));
    }
    for file in files.iter() {
        let date = file
            .partition_values_map()
            .get("date")
            .cloned()
            .flatten()
            .unwrap_or_else(|| "None".into());
        *counts.entry(date).or_insert(0) += 1;
    }
    Ok(counts)
}

/// The newest `date` of `blocks` (still being written), or `None`.
async fn open_date_of(lake: &Lake) -> DeltaResult<Option<String>> {
    let table = lake.open("blocks").await?;
    Ok(active_files_per_date(&table)?.into_keys().next_back())
}

/// Whether opening a table failed because it does not exist yet: no
/// `_delta_log/` commit at all (delta-rs's "No files in log segment"), or no
/// local table directory (the kernel's "Invalid table location").
pub fn is_missing_table(error: &DeltaTableError) -> bool {
    if matches!(
        error,
        DeltaTableError::NotATable(_) | DeltaTableError::InvalidTableLocation(_)
    ) {
        return true;
    }
    let text = error.to_string();
    text.contains("No files in log segment")
        || (matches!(error, DeltaTableError::KernelError(_))
            && text.contains("Invalid table location"))
}

/// A lost commit race: reported as a conflict and left to the next run.
fn is_conflict(error: &DeltaTableError) -> bool {
    matches!(
        error,
        DeltaTableError::VersionAlreadyExists(_)
            | DeltaTableError::Transaction {
                source: TransactionError::CommitConflict(_)
                    | TransactionError::MaxCommitAttempts(_)
                    | TransactionError::VersionAlreadyExists(_),
            }
    )
}

/// No checkpoint and no log cleanup after an OPTIMIZE or VACUUM commit: the
/// job checkpoints once, after VACUUM.
fn no_post_commit_hooks() -> CommitProperties {
    CommitProperties::default()
        .with_create_checkpoint(false)
        .with_cleanup_expired_logs(Some(false))
}

fn seconds(started: Instant) -> f64 {
    (started.elapsed().as_secs_f64() * 1000.0).round() / 1000.0
}

/// One table's report while it is maintained.
struct Report<'a> {
    settings: &'a Settings,
    fields: Map<String, Value>,
    compacted: Vec<Value>,
    conflicts: Vec<String>,
    errors: Vec<String>,
}

impl Report<'_> {
    fn failed(&mut self, step: &str, error: &DeltaTableError) {
        let text = self.settings.redact(&format!("{step}: {error}"));
        if is_conflict(error) {
            self.conflicts.push(text);
        } else {
            self.errors.push(text);
        }
    }

    fn refused(&mut self, step: &str, reason: &str) {
        self.errors
            .push(self.settings.redact(&format!("{step}: {reason}")));
    }

    fn set(&mut self, key: &str, value: Value) {
        self.fields.insert(key.to_string(), value);
    }

    fn finish(mut self, started: Instant) -> Map<String, Value> {
        self.set("compacted", Value::Array(self.compacted.clone()));
        self.set("conflicts", json!(self.conflicts));
        self.set("errors", json!(self.errors));
        self.set("seconds", json!(seconds(started)));
        self.fields
    }
}

enum Outcome {
    Maintained(Map<String, Value>),
    /// The table does not exist yet; the reason.
    Skipped(String),
}

/// One table. Never fails: every error is in the report.
async fn maintain(
    settings: &Settings,
    lake: &Lake,
    name: &str,
    open_date: Option<&str>,
) -> Outcome {
    let started = Instant::now();
    let mut report = Report {
        settings,
        fields: Map::new(),
        compacted: Vec::new(),
        conflicts: Vec::new(),
        errors: Vec::new(),
    };
    report.set("table", json!(name));
    let mut table = match lake.open(name).await {
        Ok(table) => table,
        Err(error) if is_missing_table(&error) => {
            return Outcome::Skipped(settings.redact(&error.to_string()))
        }
        Err(error) => {
            report.failed("open", &error);
            return Outcome::Maintained(report.finish(started));
        }
    };
    report.set("version_before", json!(table.version()));
    let files = match active_files_per_date(&table) {
        Ok(files) => files,
        Err(error) => {
            report.failed("open", &error);
            return Outcome::Maintained(report.finish(started));
        }
    };

    // 1. OPTIMIZE closed dates (or all, OPTIMIZE_DATES=all) with several files.
    let dates: Vec<String> = files
        .iter()
        .filter(|(date, count)| {
            **count > 1
                && (settings.optimize_all_dates
                    || open_date.is_some_and(|open| date.as_str() < open))
        })
        .map(|(date, _)| date.clone())
        .collect();
    report.set("dates_to_compact", json!(dates));
    let level = ZstdLevel::try_new(settings.zstd_level).expect("checked to be 1..=22");
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(level))
        .build();
    for date in dates.iter().filter(|_| !settings.dry_run) {
        let filters = [("date", FilterOp::Eq, FilterValue::Scalar(date.as_str()))];
        let mut optimize = table
            .clone()
            .optimize()
            .with_filters(&filters)
            .with_writer_properties(properties.clone())
            .with_commit_properties(no_post_commit_hooks());
        if let Some(size) = settings.target_size.and_then(NonZeroU64::new) {
            optimize = optimize.with_target_size(size);
        }
        if let Some(tasks) = settings.max_concurrent_tasks {
            optimize = optimize.with_max_concurrent_tasks(tasks);
        }
        match optimize.await {
            Ok((optimized, metrics)) => {
                table = optimized;
                report.compacted.push(json!({
                    "date": date,
                    "files_removed": metrics.num_files_removed,
                    "files_added": metrics.num_files_added,
                }));
            }
            Err(error) => report.failed(&format!("optimize {date}"), &error),
        }
    }

    // 2. VACUUM, lite unless FULL_VACUUM=1 (weekly, >= 168 h enforced).
    let table_hours = table
        .snapshot()
        .ok()
        .and_then(|snapshot| table_retention_hours(snapshot.metadata().configuration()));
    let hours = settings.retention_hours;
    let mut vacuum = Map::new();
    vacuum.insert(
        "mode".into(),
        json!(if settings.full_vacuum { "full" } else { "lite" }),
    );
    vacuum.insert("retention_hours".into(), json!(hours));
    let mut vacuumed = false;
    if settings.full_vacuum
        && table_hours.is_none_or(|table_hours| table_hours < MIN_FULL_VACUUM_HOURS as f64)
    {
        report.refused(
            "vacuum",
            &format!(
                "refusing a full VACUUM: delta.deletedFileRetentionDuration is below \
                 {MIN_FULL_VACUUM_HOURS} h"
            ),
        );
    } else {
        // A full VACUUM always enforces the table's retention; a lite one may
        // go below it (it never deletes an untracked part).
        let enforce = settings.full_vacuum
            || hours.is_none()
            || table_hours.is_some_and(|table_hours| hours.unwrap() as f64 >= table_hours);
        let mut builder = table
            .clone()
            .vacuum()
            .with_dry_run(settings.dry_run)
            .with_enforce_retention_duration(enforce)
            .with_mode(if settings.full_vacuum {
                VacuumMode::Full
            } else {
                VacuumMode::Lite
            })
            .with_commit_properties(no_post_commit_hooks());
        if let Some(hours) = hours {
            builder = builder.with_retention_period(chrono::Duration::hours(
                i64::try_from(hours).unwrap_or(i64::MAX / 3_600_000),
            ));
        }
        match builder.await {
            Ok((after, metrics)) => {
                table = after;
                vacuum.insert("files_deleted".into(), json!(metrics.files_deleted.len()));
                vacuumed = true;
            }
            Err(error) => report.failed("vacuum", &error),
        }
    }
    report.set("vacuum", Value::Object(vacuum));

    // 3. Checkpoint, only after a successful VACUUM (design §4.1), then
    // 4. log cleanup behind it.
    if vacuumed && !settings.dry_run {
        match create_checkpoint(&table, None).await {
            Ok(()) => {
                report.set("checkpoint_version", json!(table.version()));
                if let Err(error) = cleanup_metadata(&table, None).await {
                    report.failed("checkpoint", &error);
                }
            }
            Err(error) => report.failed("checkpoint", &error),
        }
    }
    report.set("version_after", json!(table.version()));
    Outcome::Maintained(report.finish(started))
}

/// `value` with every object's keys in sorted order, whatever map serde_json
/// was built with.
fn sorted(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let entries: BTreeMap<String, Value> = map
                .into_iter()
                .map(|(key, value)| (key, sorted(value)))
                .collect();
            Value::Object(entries.into_iter().collect())
        }
        Value::Array(values) => Value::Array(values.into_iter().map(sorted).collect()),
        other => other,
    }
}

fn emit(out: &mut dyn Write, event: &str, mut fields: Map<String, Value>) {
    fields.insert("event".into(), json!(event));
    // A closed stdout must not stop maintenance.
    let _ = writeln!(out, "{}", sorted(Value::Object(fields)));
    let _ = out.flush();
}

fn object(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        _ => unreachable!("json! object"),
    }
}

/// Runs the job with the settings in `env`, writing its JSON lines to `out`;
/// returns the exit status: 0 when every table was maintained or skipped, 1
/// when a table failed, 2 for a configuration error.
pub async fn run(env: Env<'_>, out: &mut dyn Write) -> i32 {
    let started = Instant::now();
    let (settings, lake) = match Settings::from_env(env).and_then(|settings| {
        let lake = settings.lake()?;
        Ok((settings, lake))
    }) {
        Ok(ready) => ready,
        Err(error) => {
            emit(out, "config_error", object(json!({"error": error.0})));
            return 2;
        }
    };
    emit(
        out,
        "start",
        object(json!({
            "root": settings.root,
            "tables": settings.tables.len(),
            "full_vacuum": settings.full_vacuum,
            "retention_hours": settings.retention_hours,
            "optimize_dates": if settings.optimize_all_dates { "all" } else { "closed" },
            "dry_run": settings.dry_run,
            "deltalake": deltalake_core::crate_version(),
            "version": env!("CARGO_PKG_VERSION"),
        })),
    );
    let mut failed: Vec<String> = Vec::new();
    let mut skipped: Vec<String> = Vec::new();
    let mut conflicts = 0;
    let open_date = match open_date_of(&lake).await {
        Ok(open_date) => open_date,
        // No `blocks` yet: no date is closed, and its line says skipped.
        Err(error) if is_missing_table(&error) => None,
        // Without blocks no date is closed.
        Err(error) => {
            emit(
                out,
                "blocks_error",
                object(json!({"error": settings.redact(&error.to_string())})),
            );
            failed.push("blocks".into());
            None
        }
    };
    for name in &settings.tables {
        match maintain(&settings, &lake, name, open_date.as_deref()).await {
            Outcome::Skipped(reason) => {
                emit(
                    out,
                    "skipped",
                    object(json!({
                        "table": name,
                        "reason": format!("the table does not exist yet: {reason}"),
                        "open_date": open_date,
                    })),
                );
                skipped.push(name.clone());
            }
            Outcome::Maintained(mut report) => {
                report.insert("open_date".into(), json!(open_date));
                conflicts += report["conflicts"].as_array().map_or(0, Vec::len);
                let errors = report["errors"].as_array().is_some_and(|e| !e.is_empty());
                emit(out, "table", report);
                if errors && !failed.contains(name) {
                    failed.push(name.clone());
                }
            }
        }
    }
    emit(
        out,
        "done",
        object(json!({
            "tables": settings.tables.len(),
            "failed": failed,
            "skipped": skipped,
            "conflicts": conflicts,
            "seconds": seconds(started),
        })),
    );
    i32::from(!failed.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_intervals_parse_like_delta() {
        let hours = |value: &str| {
            table_retention_hours(&HashMap::from([(
                "delta.deletedFileRetentionDuration".to_string(),
                value.to_string(),
            )]))
        };
        assert_eq!(table_retention_hours(&HashMap::new()), Some(168.0));
        assert_eq!(hours("interval 7 days"), Some(168.0));
        assert_eq!(hours("  INTERVAL 2 Weeks "), Some(336.0));
        assert_eq!(hours("interval 1 hour"), Some(1.0));
        assert_eq!(hours("interval 2 seconds"), Some(2.0 / 3600.0));
        for invalid in [
            "7 days",
            "interval days",
            "interval -1 days",
            "interval 1 fortnight",
        ] {
            assert_eq!(hours(invalid), None, "{invalid}");
        }
    }

    #[test]
    fn credentials_are_redacted() {
        let env: BTreeMap<&str, &str> = BTreeMap::from([
            ("LAKE_BUCKET", "ethereum-mainnet"),
            ("LAKE_TABLES", "blocks"),
            ("AWS_ACCESS_KEY_ID", "the-key"),
            ("AWS_SECRET_ACCESS_KEY", "the-secret"),
            ("AWS_SESSION_TOKEN", "the-token"),
        ]);
        let lookup = |name: &str| env.get(name).map(|value| value.to_string());
        let settings = Settings::from_env(&lookup).unwrap();
        assert_eq!(settings.root, "s3://ethereum-mainnet");
        assert_eq!(
            settings.redact("the-key the-secret the-token"),
            "*** *** ***"
        );
    }
}
