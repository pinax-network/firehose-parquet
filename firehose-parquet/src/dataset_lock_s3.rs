//! Persistent, non-expiring bucket ownership for cooperating dataset mutators.
//!
//! The supplied store must address the bucket root, without a prefix wrapper.
//! Mutating commands retain this owner through their complete operation. Never
//! release ownership while a data mutation is unresolved. A command may release
//! after a failure only when the latch proves every request was resolved; see
//! `DatasetOwnership::finish`. Operator recovery additionally requires
//! provider-confirmed request quiescence; stopping a process is not proof that
//! already-sent remote PUTs cannot still complete.

use bytes::Bytes;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{
    Attribute, GetOptions, ObjectStore, PutMode, PutOptions, PutResult, UpdateVersion,
};
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

/// How a store's `If-Match` preconditions carry an ETag (#678).
///
/// S3 returns ETags in the RFC 9110 quoted form (`"abc"`) and compares
/// `If-Match` against that form. Ceph RGW 19.2 compares `If-Match` literally
/// against its stored ETag *without* the quotes, so it refuses every correct
/// quoted compare-and-swap and accepts only the unquoted one. The
/// conditional-write canary chooses the form once, when ownership is
/// acquired, and every conditional request through that owner then uses it
/// ([`ETagForm::if_match`]). It is never mixed within a run, never chosen per
/// request, and never persisted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ETagForm {
    /// Exactly as the provider returned it: AWS S3, MinIO and every store
    /// that follows RFC 9110.
    AsReturned,
    /// Without its surrounding quotes: Ceph RGW 19.2.
    Unquoted,
}

impl ETagForm {
    /// `etag`, as the provider returned it, rendered for an `If-Match` header
    /// in this form. `None` when this form cannot carry it; callers treat that
    /// as a missing version and fail closed before sending anything.
    pub(crate) fn if_match(self, etag: &str) -> Option<String> {
        match self {
            ETagForm::AsReturned => Some(etag.to_string()),
            ETagForm::Unquoted => {
                let inner = etag
                    .strip_prefix('"')
                    .and_then(|rest| rest.strip_suffix('"'))
                    .unwrap_or(etag);
                (!inner.is_empty() && !inner.contains('"')).then(|| inner.to_string())
            }
        }
    }

    /// The precondition of a compare-and-swap PUT on `version`: its ETag
    /// rendered by [`ETagForm::if_match`], its version ID unchanged. Readbacks
    /// are compared with `version` itself, never with this.
    pub(crate) fn precondition(self, version: &UpdateVersion) -> Option<UpdateVersion> {
        let e_tag = match &version.e_tag {
            Some(etag) => Some(self.if_match(etag)?),
            None => None,
        };
        Some(UpdateVersion {
            e_tag,
            version: version.version.clone(),
        })
    }

    /// `PutMode::Update` on `version`, in this form.
    pub(crate) fn update(self, version: &UpdateVersion) -> Option<PutMode> {
        self.precondition(version).map(PutMode::Update)
    }
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
    native_upload: Option<crate::s3::upload::NativeS3Upload>,
    /// The Delta log store's client of this bucket (#643 L3): object_store
    /// 0.13, one attempt per write and a bounded few per idempotent read
    /// (#680). Only `build`'s output bucket has one.
    delta_log: Option<Arc<dyn object_store_delta::ObjectStore>>,
    owned: OwnerRecord,
    version: UpdateVersion,
    /// Chosen by this acquisition's canary; every conditional request
    /// through this owner uses it.
    etag_form: ETagForm,
    mutation_uncertain: AtomicBool,
    control_mutation: tokio::sync::Mutex<()>,
    transaction_session: crate::dataset_lock::session::SessionSlot,
}

impl fmt::Debug for S3Ownership {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Ownership")
            .field("record", &self.owned)
            .field("etag_form", &self.etag_form)
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
        let etag_form = qualify_conditions(&store).await?;
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
            Some((_, version)) => update(etag_form, &version)?,
        };
        let version = transition(&store, &owned, mode).await?;
        Ok(Self {
            store,
            native_upload: None,
            delta_log: None,
            owned,
            version,
            etag_form,
            mutation_uncertain: AtomicBool::new(false),
            control_mutation: tokio::sync::Mutex::new(()),
            transaction_session: Default::default(),
        })
    }

    pub(crate) async fn acquire_native(
        upload: crate::s3::upload::NativeS3Upload,
        operation: &str,
        scopes: Vec<String>,
    ) -> Result<Self> {
        let mut owner = Self::acquire(upload.object_store(), operation, scopes).await?;
        owner.native_upload = Some(upload);
        Ok(owner)
    }

    pub(crate) fn native_upload(&self) -> Option<&crate::s3::upload::NativeS3Upload> {
        self.native_upload.as_ref()
    }

    /// Attach the client the bucket's Delta log stores use.
    pub(crate) fn with_delta_log(
        mut self,
        client: Arc<dyn object_store_delta::ObjectStore>,
    ) -> Self {
        self.delta_log = Some(client);
        self
    }

    /// The client of this bucket's Delta log stores, when this owner is a
    /// `build` output owner.
    pub(crate) fn delta_log(&self) -> Option<&Arc<dyn object_store_delta::ObjectStore>> {
        self.delta_log.as_ref()
    }

    /// Read-only inspection. Never probes, renews, releases or creates an object.
    pub async fn status(store: &Arc<dyn ObjectStore>) -> Result<Option<OwnerRecord>> {
        Ok(read_record(store).await?.map(|(record, _)| record))
    }

    pub(crate) fn acquire_transaction_session(
        &self,
    ) -> anyhow::Result<crate::dataset_lock::session::SessionPermit<'_>> {
        self.transaction_session.acquire()
    }

    pub fn record(&self) -> &OwnerRecord {
        &self.owned
    }
    pub fn object_store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }
    /// The `If-Match` ETag form this owner's canary chose for its store.
    pub fn etag_form(&self) -> ETagForm {
        self.etag_form
    }

    /// `PutMode::Update` on `version` of an object in this owner's store, in
    /// its [`ETagForm`]. `None` (an ETag the form cannot carry) fails closed.
    pub(crate) fn update_mode(&self, version: &UpdateVersion) -> Option<PutMode> {
        self.etag_form.update(version)
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
            update(self.etag_form, &self.version)?,
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
        let etag_form = qualify_conditions(&store).await?;
        let mut released = expected.clone();
        released.state = OwnerState::Released;
        released.recovery = Some(authorization.receipt);
        transition(&store, &released, update(etag_form, &version)?).await?;
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

/// The provider refused a mutation with HTTP 401 or 403. S3 does not apply a
/// request it rejects, so the attempt is resolved: it cannot complete later and
/// does not set the uncertainty latch. Only these statuses qualify. Timeouts,
/// transport errors, lost or unreadable responses, 5xx, 409/412 and every
/// acknowledgement that fails readback stay uncertain.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("S3 refused the request with HTTP {status}; it did not take effect")]
pub(crate) struct ProviderRejected {
    pub(crate) status: u16,
}

/// Classify one object-store mutation error; see [`ProviderRejected`].
/// object_store maps only a received 401/403 response to these variants.
pub(crate) fn provider_rejection(error: &object_store::Error) -> Option<ProviderRejected> {
    match error {
        object_store::Error::PermissionDenied { .. } => Some(ProviderRejected { status: 403 }),
        object_store::Error::Unauthenticated { .. } => Some(ProviderRejected { status: 401 }),
        _ => None,
    }
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

/// `PutMode::Update` on `version` in `form`. An ETag that the form cannot
/// carry counts as a missing version.
fn update(form: ETagForm, version: &UpdateVersion) -> Result<PutMode> {
    form.update(version).ok_or(OwnershipError::MissingVersion)
}

/// Qualify the store's conditional writes and choose its [`ETagForm`] (#678).
///
/// The canary runs with [`ETagForm::AsReturned`] first. The whole canary runs
/// again on a fresh probe key with [`ETagForm::Unquoted`] only when all of
/// these hold: every earlier step passed, the provider then refused the
/// *correct* version with a precondition failure (to the conditional GET or
/// to the correct compare-and-swap), the probe was left unchanged, and the
/// refused ETag was quoted. That form is adopted only if every step passes,
/// including the wrong-version and stale-version refusals. Anything else
/// fails closed.
async fn qualify_conditions(store: &Arc<dyn ObjectStore>) -> Result<ETagForm> {
    match canary(store, ETagForm::AsReturned).await {
        Ok(()) => {
            tracing::debug!("s3 conditional writes: If-Match ETags sent as returned");
            Ok(ETagForm::AsReturned)
        }
        Err(CanaryFailure::CorrectVersionRefused { quoted: true }) => {
            canary(store, ETagForm::Unquoted)
                .await
                .map_err(|_| OwnershipError::ConditionalWritesUnproven)?;
            tracing::info!(
                "s3 conditional writes: If-Match ETags sent unquoted (provider compares them literally)"
            );
            Ok(ETagForm::Unquoted)
        }
        Err(_) => Err(OwnershipError::ConditionalWritesUnproven),
    }
}

/// Why one canary run did not qualify its form.
#[derive(Debug)]
enum CanaryFailure {
    /// Every earlier step passed, then the provider refused the correct
    /// version with a precondition failure and left the probe unchanged.
    /// `quoted`: the refused ETag was sent in the quoted form.
    CorrectVersionRefused { quoted: bool },
    /// Any other failure. It is never retried in another form.
    Unproven,
}

impl From<OwnershipError> for CanaryFailure {
    fn from(_: OwnershipError) -> Self {
        CanaryFailure::Unproven
    }
}

/// One canary run in `form`, on a fresh, never-reused probe key. Isolated
/// negative probes must never target the real ownership record.
async fn canary(
    store: &Arc<dyn ObjectStore>,
    form: ETagForm,
) -> std::result::Result<(), CanaryFailure> {
    let key = Path::from(format!("{PROBE_PREFIX}/{}.json", Uuid::new_v4()));
    let result = canary_steps(store, &key, form).await;
    // Only this random, private canary is deleted. The owner key is never deleted.
    let deleted = tokio::time::timeout(REQUEST_TIMEOUT, store.delete(&key)).await;
    let absent = matches!(read_bytes(store, &key).await, Ok(None));
    if !matches!(deleted, Ok(Ok(()))) || !absent {
        return Err(CanaryFailure::Unproven);
    }
    result
}

async fn canary_steps(
    store: &Arc<dyn ObjectStore>,
    key: &Path,
    form: ETagForm,
) -> std::result::Result<(), CanaryFailure> {
    let first = Bytes::from_static(b"{\"probe\":1}");
    let second = Bytes::from_static(b"{\"probe\":2}");
    let unchanged = |expected: (Bytes, UpdateVersion)| async move {
        match read_bytes(store, key).await {
            Ok(Some(current)) if current == expected => Ok(()),
            _ => Err(CanaryFailure::Unproven),
        }
    };
    // 1. Create-if-absent, read back exactly.
    let version = write_and_verify(store, key, first.clone(), PutMode::Create).await?;
    let refused = CanaryFailure::CorrectVersionRefused {
        quoted: version
            .e_tag
            .as_deref()
            .is_some_and(|etag| ETagForm::Unquoted.if_match(etag).as_deref() != Some(etag)),
    };
    // 2. A duplicate Create and 3. a wrong-version CAS in `form` are refused.
    let wrong = UpdateVersion {
        e_tag: Some(format!("\"never-match-{}\"", Uuid::new_v4())),
        version: Some(format!("never-match-{}", Uuid::new_v4())),
    };
    for mode in [PutMode::Create, update(form, &wrong)?] {
        let response = tokio::time::timeout(
            REQUEST_TIMEOUT,
            store.put_opts(key, second.clone().into(), options(mode)),
        )
        .await;
        if !matches!(
            response,
            Ok(Err(object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. }))
        ) {
            return Err(CanaryFailure::Unproven);
        }
        unchanged((first.clone(), version.clone())).await?;
    }
    // 4. A GET pinned to the correct version in `form` serves it, as a
    // protected part's verification pins its acknowledged upload.
    let if_match = match version.e_tag.as_deref() {
        Some(etag) => Some(form.if_match(etag).ok_or(CanaryFailure::Unproven)?),
        None => None,
    };
    let pinned = tokio::time::timeout(REQUEST_TIMEOUT, async {
        let response = store
            .get_opts(
                key,
                GetOptions {
                    if_match,
                    ..Default::default()
                },
            )
            .await?;
        let served = UpdateVersion {
            e_tag: response.meta.e_tag.clone(),
            version: response.meta.version.clone(),
        };
        if response.meta.size > MAX_RECORD_BYTES as u64 {
            return Ok(None);
        }
        Ok::<_, object_store::Error>(Some((response.bytes().await?, served)))
    })
    .await;
    match pinned {
        Ok(Ok(Some(served))) if served == (first.clone(), version.clone()) => {}
        Ok(Err(object_store::Error::Precondition { .. })) => {
            unchanged((first.clone(), version.clone())).await?;
            return Err(refused);
        }
        _ => return Err(CanaryFailure::Unproven),
    }
    // 5. The correct CAS in `form` succeeds, changes the version and is read
    // back exactly.
    let response = tokio::time::timeout(
        REQUEST_TIMEOUT,
        store.put_opts(key, second.clone().into(), options(update(form, &version)?)),
    )
    .await;
    let reported = match response {
        Ok(Ok(result)) => Some(result),
        Ok(Err(object_store::Error::Precondition { .. })) => {
            unchanged((first.clone(), version.clone())).await?;
            return Err(refused);
        }
        _ => None,
    };
    let Some((current, updated)) = read_bytes(store, key).await? else {
        return Err(CanaryFailure::Unproven);
    };
    if current != second
        || updated == version
        || reported
            .as_ref()
            .is_some_and(|result| !reported_version_matches(result, &updated))
    {
        return Err(CanaryFailure::Unproven);
    }
    // 6. The previous, now stale version no longer matches in `form`.
    let stale = tokio::time::timeout(
        REQUEST_TIMEOUT,
        store.put_opts(key, first.into(), options(update(form, &version)?)),
    )
    .await;
    if !matches!(stale, Ok(Err(object_store::Error::Precondition { .. }))) {
        return Err(CanaryFailure::Unproven);
    }
    unchanged((second, updated)).await
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod rgw19_store;
