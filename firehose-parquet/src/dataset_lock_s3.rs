//! Persistent, non-expiring bucket ownership for cooperating dataset mutators.
//!
//! The supplied store must address the bucket root, without a prefix wrapper.
//! This primitive is deliberately not wired into ingestion yet. Never release
//! ownership while a data mutation is unresolved. Operator recovery additionally
//! requires provider-confirmed request quiescence; stopping a process is not
//! proof that already-sent remote PUTs cannot still complete.

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{Attribute, ObjectStore, PutMode, PutOptions, PutResult, UpdateVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

pub const OWNER_KEY: &str = ".fireparq-owner-v1.json";
pub const PROBE_PREFIX: &str = ".fireparq-owner-probes-v1";
const FORMAT_VERSION: u32 = 1;
const MAX_RECORD_BYTES: usize = 32 * 1024;
const MAX_SCOPES: usize = 32;
const MAX_SCOPE_BYTES: usize = 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

type Result<T> = std::result::Result<T, OwnershipError>;

/// Errors intentionally omit backend errors, payloads, paths and opaque versions.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum OwnershipError {
    #[error("invalid non-secret ownership operation or bucket-relative scopes")]
    InvalidRequest,
    #[error("bucket ownership is held; there is no age-based or automatic takeover")]
    Busy,
    #[error("bucket ownership record is malformed, oversized, or has an unknown version")]
    InvalidRecord,
    #[error("object store did not provide a usable conditional-write version")]
    MissingVersion,
    #[error("conditional-write capability could not be proven; ownership was not acquired")]
    ConditionalWritesUnproven,
    #[error("ownership read failed or timed out; state must be inspected before proceeding")]
    ReadFailed,
    #[error("ownership mutation could not be reconciled to the exact expected record and version")]
    MutationUncertain,
    #[error("ownership record or version changed unexpectedly")]
    StateChanged,
    #[error("ownership generation is exhausted")]
    GenerationExhausted,
    #[error("unresolved data mutation forbids ordinary ownership release")]
    DataMutationUncertain,
    #[error("operator recovery requires confirmed process cessation and provider-confirmed remote-request quiescence")]
    RecoveryEvidenceRequired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerState {
    Owned,
    Released,
}

/// Bounded public metadata only. There are no endpoint, credential or cursor fields.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerRecord {
    format_version: u32,
    generation: u64,
    owner_id: String,
    state: OwnerState,
    operation: String,
    scopes: Vec<String>,
    recovery: Option<RecoveryReceipt>,
}

impl fmt::Debug for OwnerRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnerRecord")
            .field("generation", &self.generation)
            .field("state", &self.state)
            .field("scope_count", &self.scopes.len())
            .finish_non_exhaustive()
    }
}

impl OwnerRecord {
    pub fn owner_id(&self) -> &str {
        &self.owner_id
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn state(&self) -> OwnerState {
        self.state
    }
    pub fn operation(&self) -> &str {
        &self.operation
    }
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }

    fn validate(&self) -> Result<()> {
        let valid_id = Uuid::parse_str(&self.owner_id)
            .is_ok_and(|id| id.get_version_num() == 4 && id.to_string() == self.owner_id);
        if self.format_version != FORMAT_VERSION
            || self.generation == 0
            || !valid_id
            || normalize_request(&self.operation, self.scopes.clone()).as_ref() != Ok(&self.scopes)
            || (self.state == OwnerState::Owned && self.recovery.is_some())
            || self
                .recovery
                .as_ref()
                .is_some_and(|receipt| !receipt.valid())
        {
            return Err(OwnershipError::InvalidRecord);
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryReceipt {
    stopped_writer_evidence_sha256: String,
    provider_quiescence_evidence_sha256: String,
}

impl RecoveryReceipt {
    fn valid(&self) -> bool {
        [
            &self.stopped_writer_evidence_sha256,
            &self.provider_quiescence_evidence_sha256,
        ]
        .iter()
        .all(|value| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        })
    }
}

/// An explicit operator attestation, not a conclusion the generic CLI can prove.
///
/// Construct only after the named provider confirms that every prior remote
/// mutation has completed or is permanently prevented from completing. Process
/// exit, cancellation, an empty listing, and elapsed time are insufficient.
/// References are hashed immediately and never retained, logged or serialized.
/// If the provider offers no such assurance, do not construct this capability;
/// recovery of a plain-glob dataset must remain blocked.
pub struct RecoveryAuthorization {
    expected_owner: String,
    expected_generation: u64,
    receipt: RecoveryReceipt,
}

impl fmt::Debug for RecoveryAuthorization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RecoveryAuthorization { evidence: <redacted> }")
    }
}

impl RecoveryAuthorization {
    /// This is an assertion by the operator/integration, not evidence verification.
    pub fn assert_provider_quiescence(
        expected: &OwnerRecord,
        confirmed_stopped_writer_reference: &str,
        provider_confirmed_no_pending_requests_reference: &str,
    ) -> Result<Self> {
        expected.validate()?;
        let valid_reference = |value: &str| !value.trim().is_empty() && value.len() <= 4096;
        if expected.state != OwnerState::Owned
            || !valid_reference(confirmed_stopped_writer_reference)
            || !valid_reference(provider_confirmed_no_pending_requests_reference)
        {
            return Err(OwnershipError::RecoveryEvidenceRequired);
        }
        Ok(Self {
            expected_owner: expected.owner_id.clone(),
            expected_generation: expected.generation,
            receipt: RecoveryReceipt {
                stopped_writer_evidence_sha256: hex::encode(Sha256::digest(
                    confirmed_stopped_writer_reference.as_bytes(),
                )),
                provider_quiescence_evidence_sha256: hex::encode(Sha256::digest(
                    provider_confirmed_no_pending_requests_reference.as_bytes(),
                )),
            },
        })
    }
}

/// An acquired bucket-wide guard. Dropping it never releases ownership.
pub struct S3Ownership {
    store: Arc<dyn ObjectStore>,
    owned: OwnerRecord,
    version: UpdateVersion,
    mutation_uncertain: AtomicBool,
    control_mutation: tokio::sync::Mutex<()>,
}

impl fmt::Debug for S3Ownership {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Ownership")
            .field("record", &self.owned)
            .field("mutation_uncertain", &self.is_mutation_uncertain())
            .finish_non_exhaustive()
    }
}

impl S3Ownership {
    /// Acquire once, without retry/takeover, using only Create or exact CAS.
    /// Scopes are diagnostic bucket-relative prefixes; all prefixes serialize.
    pub async fn acquire(
        store: Arc<dyn ObjectStore>,
        operation: &str,
        scopes: Vec<String>,
    ) -> Result<Self> {
        let scopes = normalize_request(operation, scopes)?;
        let current = read_record(&store).await?;
        if current
            .as_ref()
            .is_some_and(|(record, _)| record.state == OwnerState::Owned)
        {
            return Err(OwnershipError::Busy);
        }
        let generation = current
            .as_ref()
            .map_or(0, |(record, _)| record.generation)
            .checked_add(1)
            .ok_or(OwnershipError::GenerationExhausted)?;
        qualify_conditions(&store).await?;
        let owned = OwnerRecord {
            format_version: FORMAT_VERSION,
            generation,
            owner_id: Uuid::new_v4().to_string(),
            state: OwnerState::Owned,
            operation: operation.to_string(),
            scopes,
            recovery: None,
        };
        let mode = match current {
            None => PutMode::Create,
            Some((_, version)) => PutMode::Update(version),
        };
        let version = transition(&store, &owned, mode).await?;
        Ok(Self {
            store,
            owned,
            version,
            mutation_uncertain: AtomicBool::new(false),
            control_mutation: tokio::sync::Mutex::new(()),
        })
    }

    /// Read-only inspection. Never probes, renews, releases or creates an object.
    pub async fn status(store: &Arc<dyn ObjectStore>) -> Result<Option<OwnerRecord>> {
        Ok(read_record(store).await?.map(|(record, _)| record))
    }

    pub fn record(&self) -> &OwnerRecord {
        &self.owned
    }
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }
    pub fn is_mutation_uncertain(&self) -> bool {
        self.mutation_uncertain.load(Ordering::SeqCst)
    }

    /// Irreversible for this guard. Ordinary release must never hide unresolved PUTs.
    pub fn mark_mutation_uncertain(&self) {
        self.mutation_uncertain.store(true, Ordering::SeqCst);
    }

    /// Shared by every state store borrowing this guard. Fixed control slots
    /// still require conditional tombstones and unique record incarnations;
    /// a local mutex cannot stop a delayed remote DELETE retry.
    pub(crate) async fn lock_control_mutation(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.control_mutation.lock().await
    }

    /// Release only after all data mutations and their responses are resolved.
    pub async fn release(self) -> Result<()> {
        let _control = self.lock_control_mutation().await;
        if self.is_mutation_uncertain() {
            return Err(OwnershipError::DataMutationUncertain);
        }
        let Some((current, version)) = read_record(&self.store).await? else {
            return Err(OwnershipError::StateChanged);
        };
        if current != self.owned || version != self.version {
            return Err(OwnershipError::StateChanged);
        }
        let mut released = self.owned.clone();
        released.state = OwnerState::Released;
        transition(
            &self.store,
            &released,
            PutMode::Update(self.version.clone()),
        )
        .await?;
        Ok(())
    }

    /// Unlock a ceased, provider-quiescent owner for a new guarded recovery session.
    /// This does not repair/delete data or resolve an ingestion journal itself.
    pub async fn operator_release(
        store: Arc<dyn ObjectStore>,
        expected: &OwnerRecord,
        authorization: RecoveryAuthorization,
    ) -> Result<()> {
        expected.validate()?;
        if expected.state != OwnerState::Owned
            || authorization.expected_owner != expected.owner_id
            || authorization.expected_generation != expected.generation
        {
            return Err(OwnershipError::RecoveryEvidenceRequired);
        }
        let Some((current, version)) = read_record(&store).await? else {
            return Err(OwnershipError::StateChanged);
        };
        if current != *expected {
            return Err(OwnershipError::StateChanged);
        }
        qualify_conditions(&store).await?;
        let mut released = expected.clone();
        released.state = OwnerState::Released;
        released.recovery = Some(authorization.receipt);
        transition(&store, &released, PutMode::Update(version)).await?;
        Ok(())
    }
}

fn normalize_request(operation: &str, mut scopes: Vec<String>) -> Result<Vec<String>> {
    if operation.is_empty()
        || operation.len() > 64
        || !operation
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
        || scopes.is_empty()
        || scopes.len() > MAX_SCOPES
    {
        return Err(OwnershipError::InvalidRequest);
    }
    for scope in &mut scopes {
        if scope.len() > MAX_SCOPE_BYTES
            || scope.starts_with('/')
            || scope.ends_with("//")
            || !scope
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'/' | b'='))
        {
            return Err(OwnershipError::InvalidRequest);
        }
        *scope = scope.trim_end_matches('/').to_string();
        if !scope.is_empty()
            && scope
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
        {
            return Err(OwnershipError::InvalidRequest);
        }
    }
    if scopes.iter().map(String::len).sum::<usize>() > MAX_RECORD_BYTES / 2 {
        return Err(OwnershipError::InvalidRequest);
    }
    scopes.sort();
    scopes.dedup();
    Ok(scopes)
}

pub(crate) fn usable_version(version: &UpdateVersion) -> bool {
    let valid_etag = |value: &str| {
        !value.is_empty()
            && value.len() <= 1024
            && value
                .bytes()
                .all(|b| b.is_ascii_graphic() && b != b'*' && b != b',')
    };
    let valid_opaque = |value: &str| {
        !value.is_empty()
            && value.len() <= 4096
            && value
                .bytes()
                .all(|b| b.is_ascii_graphic() && b != b'*' && b != b',')
    };
    // A usable version ID must never mask an unsafe wildcard ETag: S3 uses
    // the ETag for CAS even when a version ID was also returned.
    if version
        .e_tag
        .as_deref()
        .is_some_and(|value| !valid_etag(value))
        || version
            .version
            .as_deref()
            .is_some_and(|value| !valid_opaque(value))
    {
        return false;
    }
    version.e_tag.is_some()
        || version
            .version
            .as_deref()
            .is_some_and(|value| value != "null")
}

async fn read_bytes(
    store: &Arc<dyn ObjectStore>,
    key: &Path,
) -> Result<Option<(Bytes, UpdateVersion)>> {
    tokio::time::timeout(REQUEST_TIMEOUT, async {
        let response = match store.get(key).await {
            Ok(response) => response,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(_) => return Err(OwnershipError::ReadFailed),
        };
        if response.meta.size > MAX_RECORD_BYTES as u64 {
            return Err(OwnershipError::InvalidRecord);
        }
        let version = UpdateVersion {
            e_tag: response.meta.e_tag.clone(),
            version: response.meta.version.clone(),
        };
        if !usable_version(&version) {
            return Err(OwnershipError::MissingVersion);
        }
        let mut content = Vec::new();
        let mut stream = response.into_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| OwnershipError::ReadFailed)?;
            if chunk.len() > MAX_RECORD_BYTES.saturating_sub(content.len()) {
                return Err(OwnershipError::InvalidRecord);
            }
            content.extend_from_slice(&chunk);
        }
        Ok(Some((Bytes::from(content), version)))
    })
    .await
    .map_err(|_| OwnershipError::ReadFailed)?
}

async fn read_record(store: &Arc<dyn ObjectStore>) -> Result<Option<(OwnerRecord, UpdateVersion)>> {
    let Some((bytes, version)) = read_bytes(store, &Path::from(OWNER_KEY)).await? else {
        return Ok(None);
    };
    let record: OwnerRecord =
        serde_json::from_slice(&bytes).map_err(|_| OwnershipError::InvalidRecord)?;
    record.validate()?;
    Ok(Some((record, version)))
}

fn options(mode: PutMode) -> PutOptions {
    let mut options = PutOptions {
        mode,
        ..Default::default()
    };
    options
        .attributes
        .insert(Attribute::ContentType, "application/json".into());
    options.attributes.insert(
        Attribute::CacheControl,
        "no-store, no-cache, max-age=0".into(),
    );
    options
}

/// PUT errors/timeouts are ambiguous: resolve only through the exact proposed
/// contents and a usable current version. Never try an unconditional fallback.
async fn write_and_verify(
    store: &Arc<dyn ObjectStore>,
    key: &Path,
    bytes: Bytes,
    mode: PutMode,
) -> Result<UpdateVersion> {
    let response = tokio::time::timeout(
        REQUEST_TIMEOUT,
        store.put_opts(key, bytes.clone().into(), options(mode)),
    )
    .await;
    let reported = match response {
        Ok(Ok(result)) => Some(result),
        _ => None,
    };
    let Some((current, version)) = read_bytes(store, key).await? else {
        return Err(OwnershipError::MutationUncertain);
    };
    if current != bytes
        || reported
            .as_ref()
            .is_some_and(|result| !reported_version_matches(result, &version))
    {
        return Err(OwnershipError::MutationUncertain);
    }
    Ok(version)
}

fn reported_version_matches(reported: &PutResult, current: &UpdateVersion) -> bool {
    reported
        .e_tag
        .as_ref()
        .is_none_or(|value| current.e_tag.as_ref() == Some(value))
        && reported
            .version
            .as_ref()
            .is_none_or(|value| current.version.as_ref() == Some(value))
}

async fn transition(
    store: &Arc<dyn ObjectStore>,
    next: &OwnerRecord,
    mode: PutMode,
) -> Result<UpdateVersion> {
    next.validate()?;
    let bytes = serde_json::to_vec(next).map_err(|_| OwnershipError::InvalidRecord)?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(OwnershipError::InvalidRecord);
    }
    write_and_verify(store, &Path::from(OWNER_KEY), bytes.into(), mode).await
}

/// Isolated negative probes must never target the real ownership record.
async fn qualify_conditions(store: &Arc<dyn ObjectStore>) -> Result<()> {
    let key = Path::from(format!("{PROBE_PREFIX}/{}.json", Uuid::new_v4()));
    let result = async {
        let first = Bytes::from_static(b"{\"probe\":1}");
        let second = Bytes::from_static(b"{\"probe\":2}");
        let version = write_and_verify(store, &key, first.clone(), PutMode::Create).await?;
        for mode in [
            PutMode::Create,
            PutMode::Update(UpdateVersion {
                e_tag: Some(format!("\"never-match-{}\"", Uuid::new_v4())),
                version: Some(format!("never-match-{}", Uuid::new_v4())),
            }),
        ] {
            let response = tokio::time::timeout(
                REQUEST_TIMEOUT,
                store.put_opts(&key, second.clone().into(), options(mode)),
            )
            .await;
            if !matches!(
                response,
                Ok(Err(object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. }))
            ) {
                return Err(OwnershipError::ConditionalWritesUnproven);
            }
            if read_bytes(store, &key).await? != Some((first.clone(), version.clone())) {
                return Err(OwnershipError::ConditionalWritesUnproven);
            }
        }
        let updated = write_and_verify(
            store,
            &key,
            second.clone(),
            PutMode::Update(version.clone()),
        )
        .await?;
        if updated == version {
            return Err(OwnershipError::ConditionalWritesUnproven);
        }
        // A previously valid version must stop matching after an update.
        let stale = tokio::time::timeout(
            REQUEST_TIMEOUT,
            store.put_opts(&key, first.into(), options(PutMode::Update(version))),
        )
        .await;
        if !matches!(stale, Ok(Err(object_store::Error::Precondition { .. })))
            || read_bytes(store, &key).await? != Some((second, updated))
        {
            return Err(OwnershipError::ConditionalWritesUnproven);
        }
        Ok(())
    }
    .await;
    // Only this random, private canary is deleted. The owner key is never deleted.
    let deleted = tokio::time::timeout(REQUEST_TIMEOUT, store.delete(&key)).await;
    let absent = matches!(read_bytes(store, &key).await, Ok(None));
    if result.is_err() || !matches!(deleted, Ok(Ok(()))) || !absent {
        return Err(OwnershipError::ConditionalWritesUnproven);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
