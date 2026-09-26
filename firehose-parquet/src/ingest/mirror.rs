//! Non-authoritative cursor mirrors, reconciled only from mandatory authority.
//! No legacy cursor load participates in protected resume or initialization.

use super::state::{
    AuthorityState, BlockFamily, Checkpoint, Digest, MirrorBinding, PartitionPolicy,
    StreamDescriptor,
};
use crate::cursor::CursorState;
use crate::dataset_lock::{DatasetOwnership, LocalOwnership};
use crate::dataset_lock_s3::{usable_version, S3Ownership};
use crate::metrics::{ErrorLabels, PipelineMetrics};
use crate::writer::ParquetFileMetadata;
use anyhow::{bail, ensure, Context, Result};
use arrow::array::Array;
use bytes::Bytes;
use futures::StreamExt;
use object_store::{path::Path as ObjectPath, PutMode, UpdateVersion};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::{reader::FileReader, serialized_reader::SerializedFileReader};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const MIRROR_VERSION: u32 = 1;
const ENVELOPE_KEY: &str = "fireparq.ingest.mirror";
const MAX_MIRROR_BYTES: usize = 4 * 1024 * 1024;
const MAX_ROW_BYTES: i64 = 512 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MirrorOutcome {
    Disabled,
    Unchanged,
    Repaired,
}

// Intentionally no Debug: the checkpoint includes the private opaque cursor.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    format_version: u32,
    stream_id: Digest,
    checkpoint: Checkpoint,
    updated_at: String,
    sha256: Digest,
}
impl Envelope {
    fn new(authority: &AuthorityState) -> Result<Self> {
        let mut value = Self {
            format_version: MIRROR_VERSION,
            stream_id: authority.descriptor.id()?,
            checkpoint: authority.checkpoint.clone(),
            updated_at: time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .map_err(|_| anyhow::anyhow!("could not format mirror update time"))?,
            sha256: authority.checkpoint.id.clone(),
        };
        value.sha256 = value.digest()?;
        Ok(value)
    }
    fn digest(&self) -> Result<Digest> {
        Digest::hash(
            "cursor-mirror",
            &(
                self.format_version,
                &self.stream_id,
                &self.checkpoint,
                &self.updated_at,
            ),
        )
    }
    fn validate(&self, descriptor: &StreamDescriptor) -> Result<()> {
        ensure!(
            self.format_version == MIRROR_VERSION,
            "unsupported protected mirror version"
        );
        ensure!(
            self.stream_id == descriptor.id()?,
            "protected mirror belongs to another stream"
        );
        self.checkpoint
            .validate(descriptor)
            .map_err(|_| anyhow::anyhow!("protected mirror checkpoint is invalid"))?;
        ensure!(
            self.checkpoint.ordinal > 0,
            "protected mirror cannot represent an unaccepted prefix"
        );
        ensure!(
            self.updated_at.len() <= 64
                && time::OffsetDateTime::parse(
                    &self.updated_at,
                    &time::format_description::well_known::Rfc3339
                )
                .is_ok(),
            "protected mirror update time is invalid"
        );
        ensure!(
            self.digest()? == self.sha256,
            "protected mirror checksum mismatch"
        );
        Ok(())
    }
}

pub(crate) struct ProtectedMirror<'a> {
    ownership: &'a DatasetOwnership,
    binding: MirrorBinding,
    metrics: Option<&'a PipelineMetrics>,
    shutdown: Option<&'a AtomicBool>,
}
impl<'a> ProtectedMirror<'a> {
    /// The session derives resolved_service from actual normalized endpoint
    /// configuration, excluding credentials, before this adapter is constructed.
    /// DatasetOwnership supplies zero-transport-retry mutation stores; ownership
    /// guards constructed by other callers must preserve that client contract.
    pub(crate) fn new(
        ownership: &'a DatasetOwnership,
        binding: &MirrorBinding,
        resolved_service: Option<&Digest>,
    ) -> Result<Self> {
        let mirror = Self {
            ownership,
            binding: binding.clone(),
            metrics: None,
            shutdown: None,
        };
        match binding {
            MirrorBinding::Disabled => ensure!(
                resolved_service.is_none(),
                "disabled mirror cannot bind a remote service"
            ),
            MirrorBinding::Local { absolute_path } => {
                ensure!(
                    resolved_service.is_none(),
                    "local mirror cannot bind a remote service"
                );
                mirror.local_path(absolute_path)?;
            }
            MirrorBinding::S3 {
                service,
                bucket,
                key,
            } => {
                ensure!(
                    resolved_service == Some(service),
                    "resolved mirror service differs from stored binding"
                );
                let owner = ownership
                    .remote(bucket)
                    .context("mirror bucket is not held by dataset ownership")?;
                validate_remote_key(key)?;
                ensure!(
                    owner.record().scopes().iter().any(|scope| scope.is_empty()
                        || key == scope
                        || key
                            .strip_prefix(scope)
                            .is_some_and(|rest| rest.starts_with('/'))),
                    "mirror key is outside declared bucket ownership"
                );
            }
        }
        Ok(mirror)
    }
    pub(crate) fn with_metrics(mut self, metrics: &'a PipelineMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }
    pub(crate) fn with_shutdown(mut self, shutdown: &'a AtomicBool) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// Any existing mirror, including a zero-byte or legacy empty cursor, blocks
    /// initialization. This check precedes creation of initial authority.
    pub(crate) async fn require_absent_for_initialization(&self) -> Result<()> {
        let present = match &self.binding {
            MirrorBinding::Disabled => false,
            MirrorBinding::Local { absolute_path } => {
                let _lock = self.local_owner()?.lock_control_mutation()?;
                read_local(&self.local_path(absolute_path)?)?.is_some()
            }
            MirrorBinding::S3 { bucket, key, .. } => {
                let owner = self
                    .ownership
                    .remote(bucket)
                    .context("missing mirror ownership")?;
                let _lock = owner.lock_control_mutation().await;
                read_remote(owner, key).await?.is_some()
            }
        };
        ensure!(!present, "existing mirror prevents protected initialization; legacy cursors cannot establish authority");
        Ok(())
    }

    /// Every failed reconciliation increments `cursor_save_failures_total` and
    /// `errors_total{kind="cursor_save"}` once per failed attempt, whatever the
    /// stage: local write attempts, pre-publication S3 reads/validation/owner
    /// checks, and ambiguous or cancelled S3 publication alike.
    pub(crate) async fn reconcile(&self, authority: &AuthorityState) -> Result<MirrorOutcome> {
        let failures = FailureCount::new(self.metrics);
        let outcome = self.reconcile_counted(authority, &failures).await?;
        failures.resolve();
        Ok(outcome)
    }

    async fn reconcile_counted(
        &self,
        authority: &AuthorityState,
        failures: &FailureCount<'_>,
    ) -> Result<MirrorOutcome> {
        authority.validate()?;
        ensure!(
            authority.descriptor.mirror == self.binding,
            "authority mirror binding differs from resolved adapter"
        );
        match &self.binding {
            MirrorBinding::Disabled => Ok(MirrorOutcome::Disabled),
            MirrorBinding::Local { absolute_path } => {
                let mut had_write = false;
                for attempt in 0..3 {
                    let mut attempted_write = false;
                    let result = self.local_attempt(absolute_path, authority, &mut attempted_write);
                    had_write |= attempted_write;
                    match result {
                        Ok(outcome) => {
                            if had_write {
                                record_success(self.metrics, authority);
                                return Ok(MirrorOutcome::Repaired);
                            }
                            return Ok(outcome);
                        }
                        Err(error) => {
                            failures.record();
                            if !attempted_write {
                                return Err(error);
                            }
                            if attempt == 2 {
                                return Err(error.context("protected mirror persistence failed after three local attempts"));
                            }
                            self.retry_backoff(Duration::from_secs(1 << attempt))
                                .await?;
                        }
                    }
                }
                unreachable!()
            }
            MirrorBinding::S3 { bucket, key, .. } => {
                let owner = self
                    .ownership
                    .remote(bucket)
                    .context("missing mirror ownership")?;
                let _lock = owner.lock_control_mutation().await;
                ensure!(
                    !owner.is_mutation_uncertain(),
                    "unresolved remote mutation prevents mirror repair"
                );
                let existing = read_remote(owner, key).await?;
                let current = existing
                    .as_ref()
                    .map(|(bytes, _)| decode(bytes.clone(), &authority.descriptor))
                    .transpose()?;
                if !needs_repair(authority, current.as_ref())? {
                    return Ok(MirrorOutcome::Unchanged);
                }
                let bytes = encode(authority)?;
                let mode =
                    existing.map_or(PutMode::Create, |(_, version)| PutMode::Update(version));
                let mut options = crate::writer::s3_put_options("no-store, no-cache, max-age=0");
                options.mode = mode;
                ensure!(
                    S3Ownership::status(owner.object_store()).await?.as_ref()
                        == Some(owner.record()),
                    "mirror ownership changed before publication"
                );
                let mut attempt = RemoteAttempt {
                    owner,
                    failures,
                    resolved: false,
                };
                let result = tokio::time::timeout(REQUEST_TIMEOUT, owner.object_store().put_opts(&ObjectPath::from(key.as_str()), bytes.clone().into(), options)).await
                    .map_err(|_| anyhow::anyhow!("protected mirror upload timed out"))?
                    .map_err(|_| anyhow::anyhow!("protected mirror conditional upload failed; retain owner for quiescent recovery"))?;
                let version = UpdateVersion {
                    e_tag: result.e_tag,
                    version: result.version,
                };
                ensure!(
                    usable_version(&version),
                    "mirror upload returned no usable version"
                );
                let (observed, observed_version) = read_remote(owner, key)
                    .await?
                    .context("acknowledged mirror is missing")?;
                ensure!(
                    observed == bytes && observed_version == version,
                    "mirror readback differs from acknowledged checkpoint"
                );
                decode(observed, &authority.descriptor)?;
                attempt.resolved = true;
                record_success(self.metrics, authority);
                Ok(MirrorOutcome::Repaired)
            }
        }
    }

    fn local_attempt(
        &self,
        absolute_path: &str,
        authority: &AuthorityState,
        attempted_write: &mut bool,
    ) -> Result<MirrorOutcome> {
        let _lock = self.local_owner()?.lock_control_mutation()?;
        let path = self.local_path(absolute_path)?;
        let bytes = read_local(&path)?;
        let current = bytes
            .map(|bytes| decode(bytes, &authority.descriptor))
            .transpose()?;
        if !needs_repair(authority, current.as_ref())? {
            if current.is_some() {
                sync_private_file(&path)?;
            }
            return Ok(MirrorOutcome::Unchanged);
        }
        let bytes = encode(authority)?;
        *attempted_write = true;
        self.ownership.revalidate_local_paths()?;
        write_local(&path, &bytes)?;
        Ok(MirrorOutcome::Repaired)
    }
    fn local_owner(&self) -> Result<&LocalOwnership> {
        self.ownership
            .local()
            .context("mirror has no local ownership scope")
    }
    fn local_path(&self, value: &str) -> Result<PathBuf> {
        self.ownership.revalidate_local_paths()?;
        let path = Path::new(value);
        ensure!(
            value.len() <= 4096
                && path.is_absolute()
                && path.file_name().is_some()
                && !value.split('/').any(|part| part == "." || part == "..")
                && !crate::artifacts::is_control_path(value),
            "invalid protected mirror local binding"
        );
        let parent = path.parent().context("mirror has no parent")?;
        let mut existing = parent;
        let resolved = loop {
            match fs::canonicalize(existing) {
                Ok(mut root) => {
                    root.push(parent.strip_prefix(existing)?);
                    break root;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    existing = existing
                        .parent()
                        .context("mirror has no existing ancestor")?
                }
                Err(_) => bail!("resolving protected mirror parent failed"),
            }
        };
        ensure!(
            self.local_owner()?
                .roots()
                .iter()
                .any(|root| resolved.starts_with(root)),
            "mirror path is outside held local ownership"
        );
        let result = resolved.join(path.file_name().unwrap());
        match fs::symlink_metadata(&result) {
            Ok(meta) => ensure!(
                meta.is_file() && !meta.file_type().is_symlink(),
                "protected mirror is not a regular file"
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => bail!("inspecting protected mirror failed"),
        }
        // Preserve the lexical alias: durability requires syncing both the
        // target ancestry and the directory entries that name that alias.
        Ok(path.to_path_buf())
    }
    async fn retry_backoff(&self, delay: Duration) -> Result<()> {
        let until = tokio::time::Instant::now() + delay;
        loop {
            ensure!(
                !self
                    .shutdown
                    .is_some_and(|flag| flag.load(Ordering::SeqCst)),
                "protected mirror retry interrupted by shutdown"
            );
            let remaining = until.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(());
            }
            tokio::time::sleep(remaining.min(Duration::from_millis(50))).await;
        }
    }
}

fn needs_repair(authority: &AuthorityState, current: Option<&Envelope>) -> Result<bool> {
    if authority.checkpoint.ordinal == 0 {
        ensure!(
            current.is_none(),
            "empty authority cannot adopt an existing mirror"
        );
        return Ok(false);
    }
    let Some(current) = current else {
        return Ok(true);
    };
    let stored = &current.checkpoint;
    let target = &authority.checkpoint;
    if stored == target {
        return Ok(false);
    }
    ensure!(
        stored.ordinal <= target.ordinal,
        "protected mirror is ahead of mandatory authority"
    );
    if stored.ordinal < target.ordinal {
        return Ok(true);
    }
    let completion_extended = match (stored.completed_stop, target.completed_stop) {
        (None, Some(_)) => true,
        (Some(old), Some(new)) => new > old,
        _ => false,
    };
    ensure!(
        target.previous.as_ref() == Some(&stored.id)
            && stored.event == target.event
            && stored.routing == target.routing
            && completion_extended,
        "same-ordinal mirror conflicts with mandatory authority"
    );
    Ok(true)
}

pub(super) fn resume_parameters(authority: &AuthorityState) -> Result<CursorState> {
    authority.validate()?;
    if authority.checkpoint.event.is_some() {
        return expected_state(&Envelope::new(authority)?, &authority.descriptor);
    }
    let mut metadata = ParquetFileMetadata::new();
    for (key, value) in expected_config(&authority.descriptor) {
        metadata.add(key, value);
    }
    Ok(CursorState {
        start_block: Some(authority.descriptor.origin_start),
        extended: authority.descriptor.extended,
        final_blocks_only: authority.descriptor.final_blocks_only,
        include_failed_transactions: authority.descriptor.include_failed_transactions,
        file_metadata: metadata,
        ..Default::default()
    })
}

fn expected_state(envelope: &Envelope, descriptor: &StreamDescriptor) -> Result<CursorState> {
    let event = envelope
        .checkpoint
        .event
        .as_ref()
        .context("mirror has no accepted event")?;
    let mut metadata = ParquetFileMetadata::new();
    for (key, value) in expected_config(descriptor) {
        metadata.add(key, value);
    }
    metadata.add(
        ENVELOPE_KEY,
        serde_json::to_string(envelope)
            .map_err(|_| anyhow::anyhow!("encoding protected mirror checkpoint failed"))?,
    );
    metadata.add("fireparq.ingest.stream_id", envelope.stream_id.as_str());
    metadata.add(
        "fireparq.ingest.checkpoint_id",
        envelope.checkpoint.id.as_str(),
    );
    metadata.add(
        "fireparq.ingest.ordinal",
        envelope.checkpoint.ordinal.to_string(),
    );
    Ok(CursorState {
        cursor: event.cursor.as_str().to_owned(),
        last_block_num: event.block_num,
        last_block_id: crate::traits::decode_id_bytes(&event.block_id),
        last_timestamp: envelope
            .checkpoint
            .routing
            .anchor
            .as_ref()
            .map(|anchor| anchor.seconds)
            .or(event.source_timestamp),
        updated_at: envelope.updated_at.clone(),
        start_block: Some(descriptor.origin_start),
        stop_block: envelope.checkpoint.completed_stop,
        extended: descriptor.extended,
        final_blocks_only: descriptor.final_blocks_only,
        include_failed_transactions: descriptor.include_failed_transactions,
        file_metadata: metadata,
    })
}
fn expected_config(descriptor: &StreamDescriptor) -> BTreeMap<String, String> {
    let family = match descriptor.family {
        BlockFamily::Evm => "evm",
        BlockFamily::Bitcoin => "bitcoin",
        BlockFamily::Solana => "solana",
        BlockFamily::Near => "near",
        BlockFamily::Antelope => "antelope",
        BlockFamily::Cosmos => "cosmos",
        BlockFamily::Tron => "tron",
        BlockFamily::Beacon => "beacon",
    };
    let partition = match descriptor.partition {
        PartitionPolicy::None => "none",
        PartitionPolicy::BlockRange { .. } => "block_range",
        PartitionPolicy::Date => "date",
        PartitionPolicy::Hour => "hour",
        PartitionPolicy::Minute => "minute",
        PartitionPolicy::Second => "second",
    };
    let mut values: BTreeMap<String, String> = [
        ("chain_name", descriptor.chain.clone()),
        ("block_type", family.into()),
        ("bytes_encoding", descriptor.bytes_encoding.clone()),
        ("partition", partition.into()),
        ("extended", descriptor.extended.to_string()),
        (
            "final_blocks_only",
            descriptor.final_blocks_only.to_string(),
        ),
        (
            "include_failed_transactions",
            descriptor.include_failed_transactions.to_string(),
        ),
        ("with_votes", descriptor.with_votes.to_string()),
        ("mapper_epoch", descriptor.mapper_epoch.clone()),
    ]
    .into_iter()
    .map(|(key, value)| (format!("firehose-parquet.{key}"), value))
    .collect();
    if let PartitionPolicy::BlockRange { size, anchor } = descriptor.partition {
        values.insert("firehose-parquet.block_range_size".into(), size.to_string());
        values.insert(
            "firehose-parquet.block_range_start".into(),
            anchor.to_string(),
        );
    }
    values
}
fn encode(authority: &AuthorityState) -> Result<Bytes> {
    let envelope = Envelope::new(authority)?;
    envelope.validate(&authority.descriptor)?;
    let bytes = crate::cursor::encode_cursor(&expected_state(&envelope, &authority.descriptor)?)
        .map_err(|_| anyhow::anyhow!("encoding protected mirror Parquet failed"))?;
    ensure!(
        bytes.len() <= MAX_MIRROR_BYTES,
        "protected mirror exceeds byte limit"
    );
    Ok(Bytes::from(bytes))
}
fn decode(bytes: Bytes, descriptor: &StreamDescriptor) -> Result<Envelope> {
    ensure!(
        bytes.len() <= MAX_MIRROR_BYTES,
        "protected mirror exceeds byte limit"
    );
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .map_err(|_| anyhow::anyhow!("invalid protected mirror Parquet metadata"))?;
    let metadata = builder.metadata();
    ensure!(
        metadata.file_metadata().num_rows() == 1 && metadata.num_row_groups() == 1,
        "protected mirror must contain exactly one row group and one row"
    );
    let group = metadata.row_group(0);
    let mut uncompressed = 0_i64;
    for column in group.columns() {
        // Protected v1 mirrors use the existing cursor writer's uncompressed
        // pages. Reject compressed pages before any decoder allocation so a
        // dishonest page header cannot bypass the bounded row/file contract.
        ensure!(
            column.compression() == parquet::basic::Compression::UNCOMPRESSED,
            "protected mirror compression is unsupported"
        );
        ensure!(
            column.uncompressed_size() >= 0,
            "invalid mirror column size"
        );
        uncompressed = uncompressed
            .checked_add(column.uncompressed_size())
            .context("mirror column size overflow")?;
    }
    ensure!(
        group.total_byte_size() >= 0
            && group.total_byte_size() <= MAX_ROW_BYTES
            && uncompressed <= MAX_ROW_BYTES,
        "mirror decoded row exceeds byte limit"
    );
    ensure!(
        builder.schema().fields() == crate::cursor::cursor_schema().fields(),
        "protected mirror row schema differs from cursor contract"
    );
    let kvs = metadata
        .file_metadata()
        .key_value_metadata()
        .context("protected mirror metadata is absent")?
        .clone();
    let mut values = BTreeMap::new();
    for kv in &kvs {
        let value = kv
            .value
            .as_deref()
            .context("protected mirror metadata value is missing")?;
        ensure!(
            values.insert(kv.key.as_str(), value).is_none(),
            "duplicate protected mirror metadata key"
        );
    }
    let raw = values
        .get(ENVELOPE_KEY)
        .context("legacy cursor cannot substitute for protected mirror")?;
    let envelope: Envelope = serde_json::from_str(raw)
        .map_err(|_| anyhow::anyhow!("invalid protected mirror envelope"))?;
    envelope.validate(descriptor)?;
    let expected = expected_state(&envelope, descriptor)?;
    let expected_values: BTreeMap<_, _> = expected
        .file_metadata
        .entries
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    for (key, value) in &expected_values {
        ensure!(
            values.get(key) == Some(value),
            "protected mirror metadata disagrees with authoritative descriptor/checkpoint"
        );
    }
    ensure!(
        values.keys().all(|key| expected_values.contains_key(key)
            || *key == parquet::arrow::ARROW_SCHEMA_META_KEY),
        "unknown protected mirror metadata key"
    );
    // Validate physical page cardinality before Arrow dictionary/value decoders.
    // Footer row counts alone cannot bound a dishonest dictionary-page count.
    let pages = SerializedFileReader::new(bytes)
        .map_err(|_| anyhow::anyhow!("invalid protected mirror page metadata"))?;
    let row_group = pages
        .get_row_group(0)
        .map_err(|_| anyhow::anyhow!("invalid protected mirror page group"))?;
    for column in 0..row_group.num_columns() {
        let mut reader = row_group
            .get_column_page_reader(column)
            .map_err(|_| anyhow::anyhow!("invalid protected mirror column pages"))?;
        let (mut page_count, mut data_values) = (0, 0);
        while let Some(page) = reader
            .get_next_page()
            .map_err(|_| anyhow::anyhow!("invalid protected mirror page"))?
        {
            page_count += 1;
            ensure!(
                page_count <= 2
                    && page.num_values() <= 1
                    && page.buffer().len() <= MAX_ROW_BYTES as usize,
                "protected mirror page exceeds bounded row contract"
            );
            if page.is_data_page() {
                data_values += page.num_values();
            }
        }
        ensure!(
            data_values == 1,
            "protected mirror column does not contain one value"
        );
    }
    let mut reader = builder
        .with_batch_size(2)
        .build()
        .map_err(|_| anyhow::anyhow!("building protected mirror decoder failed"))?;
    let batch = reader
        .next()
        .transpose()
        .map_err(|_| anyhow::anyhow!("decoding protected mirror row failed"))?
        .context("protected mirror row is missing")?;
    ensure!(
        batch.num_rows() == 1 && reader.next().is_none(),
        "protected mirror has multiple rows"
    );
    for name in ["cursor", "last_block_num", "last_block_id", "updated_at"] {
        ensure!(
            batch
                .column_by_name(name)
                .context("protected mirror column missing")?
                .null_count()
                == 0,
            "protected mirror required value is null"
        );
    }
    let actual = CursorState::from_record_batch(&batch, Some(&kvs))
        .map_err(|_| anyhow::anyhow!("decoding protected cursor fields failed"))?;
    ensure!(
        actual.cursor == expected.cursor
            && actual.last_block_num == expected.last_block_num
            && actual.last_block_id == expected.last_block_id
            && actual.last_timestamp == expected.last_timestamp
            && actual.updated_at == expected.updated_at
            && actual.start_block == expected.start_block
            && actual.stop_block == expected.stop_block
            && actual.extended == expected.extended
            && actual.final_blocks_only == expected.final_blocks_only
            && actual.include_failed_transactions == expected.include_failed_transactions,
        "protected mirror row disagrees with its checkpoint"
    );
    Ok(envelope)
}

fn read_local(path: &Path) -> Result<Option<Bytes>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => bail!("reading protected mirror metadata failed"),
    };
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "protected mirror is not a regular file"
    );
    ensure!(
        metadata.len() <= MAX_MIRROR_BYTES as u64,
        "protected mirror exceeds byte limit"
    );
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|_| anyhow::anyhow!("opening protected mirror failed"))?
        .take((MAX_MIRROR_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| anyhow::anyhow!("reading protected mirror failed"))?;
    ensure!(
        bytes.len() <= MAX_MIRROR_BYTES && bytes.len() as u64 == metadata.len(),
        "protected mirror size changed during read"
    );
    Ok(Some(Bytes::from(bytes)))
}
fn write_local(path: &Path, bytes: &Bytes) -> Result<()> {
    let parent = path.parent().context("mirror has no parent")?;
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(parent)
        .map_err(|_| anyhow::anyhow!("creating private mirror directory failed"))?;
    sync_links(parent)?;
    let temporary = parent.join(format!(".fireparq-mirror-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .map_err(|_| anyhow::anyhow!("creating private mirror temporary failed"))?;
        checkpoint(Stage::Write)?;
        file.write_all(bytes)
            .map_err(|_| anyhow::anyhow!("writing protected mirror failed"))?;
        checkpoint(Stage::FileSync)?;
        file.sync_all()
            .map_err(|_| anyhow::anyhow!("syncing protected mirror failed"))?;
        checkpoint(Stage::Rename)?;
        fs::rename(&temporary, path)
            .map_err(|_| anyhow::anyhow!("publishing protected mirror failed"))?;
        checkpoint(Stage::DirectorySync)?;
        sync_links(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}
fn sync_private_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .map_err(|_| anyhow::anyhow!("setting private mirror permissions failed"))?;
    }
    checkpoint(Stage::ExistingFileSync)?;
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|_| anyhow::anyhow!("syncing existing mirror failed"))?;
    sync_links(path.parent().context("mirror has no parent")?)
}
fn sync_links(directory: &Path) -> Result<()> {
    let canonical = fs::canonicalize(directory)
        .map_err(|_| anyhow::anyhow!("resolving mirror directory failed"))?;
    for parent in canonical.ancestors().chain(directory.ancestors()) {
        #[cfg(test)]
        tests::directory_sync(parent)?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|_| anyhow::anyhow!("syncing mirror directory links failed"))?;
    }
    Ok(())
}
fn validate_remote_key(key: &str) -> Result<()> {
    ensure!(
        key.len() <= 4096
            && !key.is_empty()
            && !key.starts_with('/')
            && !key.contains(['\\', '?', '#', '\0'])
            && !key.contains("://")
            && key
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != "..")
            && !crate::artifacts::is_control_path(key),
        "invalid protected mirror object key"
    );
    Ok(())
}
async fn read_remote(owner: &S3Ownership, key: &str) -> Result<Option<(Bytes, UpdateVersion)>> {
    tokio::time::timeout(REQUEST_TIMEOUT, async {
        let response = match owner.object_store().get(&ObjectPath::from(key)).await {
            Ok(response) => response,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(_) => bail!("reading protected mirror failed"),
        };
        ensure!(
            response.meta.size <= MAX_MIRROR_BYTES as u64,
            "protected mirror exceeds byte limit"
        );
        let size = response.meta.size;
        let version = UpdateVersion {
            e_tag: response.meta.e_tag.clone(),
            version: response.meta.version.clone(),
        };
        ensure!(
            usable_version(&version),
            "protected mirror lacks a usable conditional version"
        );
        let mut bytes = Vec::new();
        let mut stream = response.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk =
                chunk.map_err(|_| anyhow::anyhow!("reading protected mirror body failed"))?;
            ensure!(
                chunk.len() <= MAX_MIRROR_BYTES.saturating_sub(bytes.len()),
                "protected mirror exceeds byte limit"
            );
            bytes.extend_from_slice(&chunk);
        }
        ensure!(
            bytes.len() as u64 == size,
            "protected mirror size changed during read"
        );
        Ok(Some((Bytes::from(bytes), version)))
    })
    .await
    .map_err(|_| anyhow::anyhow!("protected mirror read timed out"))?
}
fn record_success(metrics: Option<&PipelineMetrics>, authority: &AuthorityState) {
    if let Some(metrics) = metrics {
        metrics.cursor_saves_total.inc();
        metrics.cursor_last_block_num.set(
            authority
                .checkpoint
                .event
                .as_ref()
                .map_or(0, |event| event.block_num) as i64,
        );
        metrics
            .cursor_last_success_timestamp_seconds
            .set(time::OffsetDateTime::now_utc().unix_timestamp());
    }
}
fn record_failure(metrics: Option<&PipelineMetrics>) {
    if let Some(metrics) = metrics {
        metrics.cursor_save_failures_total.inc();
        metrics
            .errors_total
            .get_or_create(&ErrorLabels {
                kind: "cursor_save".into(),
            })
            .inc();
    }
}
struct RemoteAttempt<'a> {
    owner: &'a S3Ownership,
    failures: &'a FailureCount<'a>,
    resolved: bool,
}
impl Drop for RemoteAttempt<'_> {
    fn drop(&mut self) {
        if !self.resolved {
            self.owner.mark_mutation_uncertain();
            self.failures.record();
        }
    }
}

/// Counts one failed reconciliation unless a specific attempt already recorded
/// it. Drop also covers errors returned before any publication and a cancelled
/// future, so no failure path leaves the counters unchanged.
struct FailureCount<'a> {
    metrics: Option<&'a PipelineMetrics>,
    recorded: AtomicBool,
    resolved: AtomicBool,
}
impl<'a> FailureCount<'a> {
    fn new(metrics: Option<&'a PipelineMetrics>) -> Self {
        Self {
            metrics,
            recorded: AtomicBool::new(false),
            resolved: AtomicBool::new(false),
        }
    }
    fn record(&self) {
        record_failure(self.metrics);
        self.recorded.store(true, Ordering::SeqCst);
    }
    fn resolve(&self) {
        self.resolved.store(true, Ordering::SeqCst);
    }
}
impl Drop for FailureCount<'_> {
    fn drop(&mut self) {
        if !self.resolved.load(Ordering::SeqCst) && !self.recorded.load(Ordering::SeqCst) {
            record_failure(self.metrics);
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stage {
    Write,
    FileSync,
    Rename,
    DirectorySync,
    ExistingFileSync,
}
fn checkpoint(stage: Stage) -> Result<()> {
    #[cfg(test)]
    tests::checkpoint(stage)?;
    let _ = stage;
    Ok(())
}
#[cfg(test)]
mod tests;
