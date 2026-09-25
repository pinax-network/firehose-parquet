//! Strict transaction identity and durable protocol records.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const FORMAT_VERSION: u32 = 1;
pub const MAPPER_EPOCH: &str = "fireparq-mapping-v1";
const MAX_CURSOR_BYTES: usize = 64 * 1024;
const MAX_TABLES: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Digest(String);

impl Digest {
    pub fn parse(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            bail!("transaction digest must be 64 lowercase hexadecimal bytes");
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
    pub fn hash(domain: &str, value: &impl Serialize) -> Result<Self> {
        let value = serde_json::to_value(value).context("serializing transaction identity")?;
        let mut hasher = Sha256::new();
        hasher.update(b"fireparq-transaction-v1\0");
        hasher.update((domain.len() as u64).to_be_bytes());
        hasher.update(domain.as_bytes());
        hasher.update(canonical_json(&value)?);
        Ok(Self(hex::encode(hasher.finalize())))
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(value).map_err(|_| serde::de::Error::custom("invalid transaction digest"))
    }
}

/// Explicit recursive ordering does not depend on serde_json's map feature flags.
pub fn canonical_json(value: &serde_json::Value) -> Result<Vec<u8>> {
    fn append(value: &serde_json::Value, bytes: &mut Vec<u8>) -> Result<()> {
        match value {
            serde_json::Value::Object(map) => {
                bytes.push(b'{');
                let mut keys: Vec<_> = map.keys().collect();
                keys.sort();
                for (index, key) in keys.iter().enumerate() {
                    if index != 0 {
                        bytes.push(b',');
                    }
                    bytes.extend(serde_json::to_vec(key)?);
                    bytes.push(b':');
                    append(&map[*key], bytes)?;
                }
                bytes.push(b'}');
            }
            serde_json::Value::Array(values) => {
                bytes.push(b'[');
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        bytes.push(b',');
                    }
                    append(value, bytes)?;
                }
                bytes.push(b']');
            }
            _ => bytes.extend(serde_json::to_vec(value)?),
        }
        Ok(())
    }
    let mut bytes = Vec::new();
    append(value, &mut bytes)?;
    Ok(bytes)
}

/// Cursors may be serialized only into private control/mirror records.
#[derive(Clone, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct OpaqueCursor(String);
impl OpaqueCursor {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_CURSOR_BYTES {
            bail!("accepted event requires a nonempty bounded cursor");
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl std::fmt::Debug for OpaqueCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OpaqueCursor([redacted])")
    }
}
impl<'de> Deserialize<'de> for OpaqueCursor {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Self::new(String::deserialize(deserializer)?)
            .map_err(|_| serde::de::Error::custom("invalid accepted cursor"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BlockFamily {
    Evm,
    Bitcoin,
    Solana,
    Near,
    Antelope,
    Cosmos,
    Tron,
    Beacon,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PartitionPolicy {
    None,
    BlockRange { size: u64, anchor: u64 },
    Date,
    Hour,
    Minute,
    Second,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutingPolicy {
    DirectV1,
    GenesisLookaheadV1,
    SolanaLastKnownV1,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageIdentity {
    Local {
        canonical_root: String,
    },
    S3 {
        service: Digest,
        bucket: String,
        prefix: String,
    },
}
impl StorageIdentity {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Local { canonical_root } => validate_absolute_path(canonical_root),
            Self::S3 { bucket, prefix, .. } => {
                validate_identifier(bucket)?;
                validate_relative_path(prefix, true)
            }
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MirrorBinding {
    Disabled,
    Local {
        absolute_path: String,
    },
    S3 {
        service: Digest,
        bucket: String,
        key: String,
    },
}
impl MirrorBinding {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Disabled => Ok(()),
            Self::Local { absolute_path } => validate_absolute_path(absolute_path),
            Self::S3 { bucket, key, .. } => {
                validate_identifier(bucket)?;
                validate_relative_path(key, false)
            }
        }
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamDescriptor {
    pub format_version: u32,
    pub chain: String,
    pub family: BlockFamily,
    pub bytes_encoding: String,
    pub mapper_epoch: String,
    pub partition: PartitionPolicy,
    pub origin_start: u64,
    pub final_blocks_only: bool,
    pub extended: bool,
    pub with_votes: bool,
    pub include_failed_transactions: bool,
    pub routing_policy: RoutingPolicy,
    pub output: StorageIdentity,
    pub mirror: MirrorBinding,
    pub tables: BTreeMap<String, Digest>,
}
impl StreamDescriptor {
    pub fn validate(&self) -> Result<()> {
        if self.format_version != FORMAT_VERSION || self.mapper_epoch != MAPPER_EPOCH {
            bail!("unsupported ingestion format or semantic mapper epoch");
        }
        validate_identifier(&self.chain)?;
        if !matches!(
            self.bytes_encoding.as_str(),
            "binary" | "hex" | "hex_no_prefix" | "base58" | "tron_base58"
        ) {
            bail!("unsupported semantic byte encoding");
        }
        if let PartitionPolicy::BlockRange { size, anchor } = self.partition {
            if size == 0 || anchor != self.origin_start {
                bail!("invalid original block-range anchor or size");
            }
        }
        self.output.validate()?;
        self.mirror.validate()?;
        if self.tables.is_empty() || self.tables.len() > MAX_TABLES {
            bail!("invalid declared table inventory size");
        }
        for table in self.tables.keys() {
            validate_table(table)?;
        }
        Ok(())
    }
    pub fn id(&self) -> Result<Digest> {
        self.validate()?;
        Digest::hash("stream", self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventIdentity {
    pub cursor: OpaqueCursor,
    pub block_num: u64,
    pub block_id: String,
    pub fork_step: i32,
}
impl EventIdentity {
    pub fn validate(&self) -> Result<()> {
        if self.block_id.is_empty()
            || self.block_id.len() > 4096
            || !(0..=3).contains(&self.fork_step)
        {
            bail!("accepted event has an invalid block identity or fork step");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnchorProvenance {
    AcceptedPrefix,
    Lookahead,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimestampAnchor {
    pub source_ordinal: u64,
    pub source_block_num: u64,
    pub source_block_id: String,
    pub seconds: i64,
    pub provenance: AnchorProvenance,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingCheckpoint {
    pub policy: RoutingPolicy,
    pub anchor: Option<TimestampAnchor>,
}
impl RoutingCheckpoint {
    pub fn validate(&self, ordinal: u64) -> Result<()> {
        if let Some(anchor) = &self.anchor {
            crate::traits::checked_timestamp(anchor.seconds)?;
            if anchor.source_ordinal == 0
                || anchor.source_block_id.is_empty()
                || anchor.source_block_id.len() > 4096
            {
                bail!("invalid routing anchor provenance");
            }
            if self.policy == RoutingPolicy::DirectV1 {
                bail!("direct routing cannot carry a synthetic anchor");
            }
            if anchor.provenance == AnchorProvenance::AcceptedPrefix
                && anchor.source_ordinal > ordinal
            {
                bail!("routing anchor claims an event beyond the accepted prefix");
            }
        }
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub id: Digest,
    pub previous: Option<Digest>,
    pub ordinal: u64,
    pub event: Option<EventIdentity>,
    pub routing: RoutingCheckpoint,
    pub completed_stop: Option<u64>,
}
impl Checkpoint {
    fn identity(&self, stream: &Digest) -> Result<Digest> {
        Digest::hash(
            "checkpoint",
            &(
                stream,
                &self.previous,
                self.ordinal,
                &self.event,
                &self.routing,
                self.completed_stop,
            ),
        )
    }
    pub fn validate(&self, descriptor: &StreamDescriptor) -> Result<()> {
        if (self.ordinal == 0) != self.event.is_none() {
            bail!("checkpoint ordinal and accepted event disagree");
        }
        if (self.ordinal > 0 && self.previous.is_none())
            || (self.ordinal == 0 && self.routing.anchor.is_some())
        {
            bail!("checkpoint lacks a predecessor or gives an empty prefix a routing anchor");
        }
        if self.routing.policy != descriptor.routing_policy {
            bail!("checkpoint routing policy differs from its stream");
        }
        self.routing.validate(self.ordinal)?;
        if let Some(event) = &self.event {
            event.validate()?;
        }
        if self
            .completed_stop
            .is_some_and(|stop| stop <= descriptor.origin_start)
        {
            bail!("invalid completed request bound");
        }
        if self.id != self.identity(&descriptor.id()?)? {
            bail!("checkpoint identity mismatch");
        }
        Ok(())
    }
    fn initial(descriptor: &StreamDescriptor) -> Result<Self> {
        let stream = descriptor.id()?;
        let mut checkpoint = Self {
            id: stream.clone(),
            previous: None,
            ordinal: 0,
            event: None,
            routing: RoutingCheckpoint {
                policy: descriptor.routing_policy,
                anchor: None,
            },
            completed_stop: None,
        };
        checkpoint.id = checkpoint.identity(&stream)?;
        Ok(checkpoint)
    }
    fn advance(&self, descriptor: &StreamDescriptor, prefix: &AcceptedPrefix) -> Result<Self> {
        self.validate(descriptor)?;
        prefix.validate()?;
        if prefix.first_ordinal
            != self
                .ordinal
                .checked_add(1)
                .context("accepted ordinal exhausted")?
        {
            bail!("transaction does not begin immediately after authority");
        }
        let mut next = Self {
            id: self.id.clone(),
            previous: Some(self.id.clone()),
            ordinal: prefix.last_ordinal,
            event: Some(prefix.last_event.clone()),
            routing: prefix.routing.clone(),
            completed_stop: self.completed_stop,
        };
        next.id = next.identity(&descriptor.id()?)?;
        next.validate(descriptor)?;
        Ok(next)
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityState {
    pub descriptor: StreamDescriptor,
    pub checkpoint: Checkpoint,
}
impl AuthorityState {
    pub fn initial(descriptor: StreamDescriptor) -> Result<Self> {
        let checkpoint = Checkpoint::initial(&descriptor)?;
        Ok(Self {
            descriptor,
            checkpoint,
        })
    }
    pub fn validate(&self) -> Result<()> {
        self.descriptor.validate()?;
        self.checkpoint.validate(&self.descriptor)
    }
    pub fn install(&self, pending: &PendingTransaction) -> Result<Self> {
        self.validate()?;
        pending.validate(&self.descriptor)?;
        if pending.phase != TransactionPhase::Committed || pending.predecessor != self.checkpoint.id
        {
            bail!("authority cannot advance from this pending phase or predecessor");
        }
        if pending.prefix.first_ordinal
            != self
                .checkpoint
                .ordinal
                .checked_add(1)
                .context("accepted ordinal exhausted")?
            || pending.target.completed_stop != self.checkpoint.completed_stop
        {
            bail!("pending transaction skips accepted ordinals or changes request completion");
        }
        Ok(Self {
            descriptor: self.descriptor.clone(),
            checkpoint: pending.target.clone(),
        })
    }
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedPrefix {
    pub first_ordinal: u64,
    pub last_ordinal: u64,
    pub events_sha256: Digest,
    pub last_event: EventIdentity,
    pub routing: RoutingCheckpoint,
}
impl AcceptedPrefix {
    pub fn validate(&self) -> Result<()> {
        if self.first_ordinal == 0 || self.last_ordinal < self.first_ordinal {
            bail!("invalid contiguous accepted prefix");
        }
        self.last_event.validate()?;
        self.routing.validate(self.last_ordinal)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionPhase {
    Writing,
    Committed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePlan {
    pub table: String,
    pub rows: u64,
    pub schema_sha256: Digest,
    pub partition: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartReceipt {
    pub byte_size: u64,
    pub sha256: Digest,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlannedPart {
    pub table: String,
    pub final_relative_path: String,
    pub temporary_relative_path: String,
    pub schema_sha256: Digest,
    pub entry_index: u32,
    pub row_count: u64,
    pub receipt: Option<PartReceipt>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartCompression {
    None,
    Snappy,
    Gzip,
    Zstd,
}

#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingTransaction {
    pub format_version: u32,
    pub id: Digest,
    pub stream_id: Digest,
    pub predecessor: Digest,
    pub phase: TransactionPhase,
    pub prefix: AcceptedPrefix,
    pub target: Checkpoint,
    pub tables: Vec<TablePlan>,
    pub compression: PartCompression,
    pub parts: Vec<PlannedPart>,
}
impl PendingTransaction {
    pub fn prepare(
        authority: &AuthorityState,
        prefix: AcceptedPrefix,
        mut tables: Vec<TablePlan>,
        compression: PartCompression,
    ) -> Result<Self> {
        authority.validate()?;
        prefix.validate()?;
        tables.sort_by(|a, b| a.table.cmp(&b.table));
        let stream_id = authority.descriptor.id()?;
        let predecessor = authority.checkpoint.id.clone();
        let target = authority
            .checkpoint
            .advance(&authority.descriptor, &prefix)?;
        let id = Self::identity(
            &stream_id,
            &predecessor,
            &prefix,
            &target,
            &tables,
            compression,
        )?;
        let parts = Self::derive_parts(&id, &stream_id, &prefix, &tables)?;
        let pending = Self {
            format_version: FORMAT_VERSION,
            id,
            stream_id,
            predecessor,
            phase: TransactionPhase::Writing,
            prefix,
            target,
            tables,
            compression,
            parts,
        };
        pending.validate(&authority.descriptor)?;
        Ok(pending)
    }
    fn identity(
        stream: &Digest,
        predecessor: &Digest,
        prefix: &AcceptedPrefix,
        target: &Checkpoint,
        tables: &[TablePlan],
        compression: PartCompression,
    ) -> Result<Digest> {
        Digest::hash(
            "pending",
            &(stream, predecessor, prefix, &target.id, tables, compression),
        )
    }
    fn derive_parts(
        id: &Digest,
        stream: &Digest,
        prefix: &AcceptedPrefix,
        tables: &[TablePlan],
    ) -> Result<Vec<PlannedPart>> {
        let mut parts = Vec::new();
        for (index, table) in tables.iter().enumerate() {
            if table.rows == 0 {
                continue;
            }
            let directory = if table.partition.is_empty() {
                table.table.clone()
            } else {
                format!("{}/{}", table.table, table.partition)
            };
            let final_name = format!(
                "part-v1-{}-{}-{}-{}-{index}.parquet",
                stream.as_str(),
                prefix.first_ordinal,
                prefix.last_ordinal,
                id.as_str()
            );
            let temporary = format!(".fireparq-txn-{}-{index}.tmp", id.as_str());
            if final_name.len() > 255 {
                bail!("deterministic part filename exceeds filesystem component limit");
            }
            parts.push(PlannedPart {
                table: table.table.clone(),
                final_relative_path: format!("{directory}/{final_name}"),
                temporary_relative_path: format!("{directory}/{temporary}"),
                schema_sha256: table.schema_sha256.clone(),
                entry_index: u32::try_from(index)?,
                row_count: table.rows,
                receipt: None,
            });
        }
        Ok(parts)
    }
    pub fn validate(&self, descriptor: &StreamDescriptor) -> Result<()> {
        descriptor.validate()?;
        self.prefix.validate()?;
        self.target.validate(descriptor)?;
        if self.format_version != FORMAT_VERSION
            || self.stream_id != descriptor.id()?
            || self.target.previous.as_ref() != Some(&self.predecessor)
            || self.target.ordinal != self.prefix.last_ordinal
            || self.target.event.as_ref() != Some(&self.prefix.last_event)
            || self.target.routing != self.prefix.routing
        {
            bail!("pending transaction identity or target frontier is inconsistent");
        }
        if self.tables.len() != descriptor.tables.len() || self.tables.len() > MAX_TABLES {
            bail!("pending table inventory is incomplete");
        }
        let mut seen = BTreeSet::new();
        for (index, table) in self.tables.iter().enumerate() {
            validate_table(&table.table)?;
            validate_relative_path(&table.partition, true)?;
            if index > 0 && self.tables[index - 1].table >= table.table {
                bail!("pending table inventory is not uniquely sorted");
            }
            if descriptor.tables.get(&table.table) != Some(&table.schema_sha256)
                || !seen.insert(&table.table)
            {
                bail!("pending table schema differs from stream inventory");
            }
        }
        if self.id
            != Self::identity(
                &self.stream_id,
                &self.predecessor,
                &self.prefix,
                &self.target,
                &self.tables,
                self.compression,
            )?
        {
            bail!("pending transaction digest mismatch");
        }
        let expected = Self::derive_parts(&self.id, &self.stream_id, &self.prefix, &self.tables)?;
        if expected.len() != self.parts.len() {
            bail!("pending part inventory is incomplete");
        }
        for (expected, actual) in expected.iter().zip(&self.parts) {
            let mut without_receipt = actual.clone();
            without_receipt.receipt = None;
            if &without_receipt != expected {
                bail!("pending final or temporary path differs from its deterministic plan");
            }
            if actual
                .receipt
                .as_ref()
                .is_some_and(|receipt| receipt.byte_size == 0)
            {
                bail!("pending part receipt has an invalid byte size");
            }
            if self.phase == TransactionPhase::Committed && actual.receipt.is_none() {
                bail!("committed transaction lacks a complete part receipt");
            }
        }
        Ok(())
    }
    pub fn with_receipt(
        &self,
        entry_index: u32,
        receipt: PartReceipt,
        descriptor: &StreamDescriptor,
    ) -> Result<Self> {
        self.validate(descriptor)?;
        if self.phase != TransactionPhase::Writing {
            bail!("cannot change a committed transaction receipt");
        }
        let mut next = self.clone();
        let part = next
            .parts
            .iter_mut()
            .find(|part| part.entry_index == entry_index)
            .context("unknown planned part entry")?;
        if part
            .receipt
            .as_ref()
            .is_some_and(|previous| previous != &receipt)
        {
            bail!("cannot replace a frozen receipt with different bytes");
        }
        part.receipt = Some(receipt);
        next.validate(descriptor)?;
        Ok(next)
    }
    /// Controller calls only after every receipt was independently verified in storage.
    pub fn committed_after_verification(&self, descriptor: &StreamDescriptor) -> Result<Self> {
        self.validate(descriptor)?;
        let mut next = self.clone();
        next.phase = TransactionPhase::Committed;
        next.validate(descriptor)?;
        Ok(next)
    }
}

fn validate_identifier(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 255
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        || matches!(value, "." | "..")
    {
        bail!("invalid bounded storage or chain identifier");
    }
    Ok(())
}
fn validate_table(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        bail!("invalid table identifier");
    }
    Ok(())
}
fn validate_absolute_path(value: &str) -> Result<()> {
    let path = std::path::Path::new(value);
    if value.len() > 4096
        || !path.is_absolute()
        || path.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
        || crate::artifacts::is_control_path(value)
    {
        bail!("invalid absolute storage identity");
    }
    Ok(())
}
pub fn validate_relative_path(value: &str, empty_allowed: bool) -> Result<()> {
    if value.is_empty() && empty_allowed {
        return Ok(());
    }
    if value.is_empty()
        || value.len() > 4096
        || value.contains(['\\', '?', '#', '\0'])
        || value
            .split('/')
            .any(|part| part.is_empty() || matches!(part, "." | "..") || part.len() > 255)
        || crate::artifacts::is_control_path(value)
    {
        bail!("invalid relative transaction path");
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests;
