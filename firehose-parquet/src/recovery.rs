//! Ownership inspection, provider-quiescent release and guarded transaction recovery.

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use object_store::ObjectStore;
use serde::Serialize;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::cli::{AwsArgs, AwsConfig};
use crate::dataset_lock::LocalOwnership;
use crate::dataset_lock_s3::{OwnerRecord, OwnerState, RecoveryAuthorization, S3Ownership};
use crate::durable_state::{
    ControlKey, ControlVersion, LocalStateStore, CONTROL_DIRECTORY, MAX_CONTROL_BYTES,
};

#[derive(Subcommand, Debug)]
pub enum RecoveryCommands {
    /// Read ownership and control-record summaries without changing S3 objects.
    Status(RecoveryStorageArgs),
    /// Recover protected ingestion/mirror and recognized merge journals under one owner.
    /// An S3 owner must first be released with provider-quiescence evidence.
    Recover(RecoveryStorageArgs),
    /// Release one exact S3 owner after provider-confirmed request quiescence.
    ///
    /// This changes only ownership. It does not repair data or journals. Process
    /// exit or elapsed time alone cannot make a delayed remote PUT/DELETE safe.
    /// If the provider cannot conclusively drain or revoke prior requests, this
    /// operation must not be used for a dataset read through ordinary file globs.
    Release(RecoveryReleaseArgs),
}

/// Endpoint variable `recovery` read before it shared `AWS_ENDPOINT_URL_S3` with every
/// other command. It is still accepted, but only when `AWS_ENDPOINT_URL_S3` is unset.
pub const LEGACY_ENDPOINT_ENV: &str = "AWS_ENDPOINT_URL";

#[derive(Args)]
#[command(mut_arg("aws_endpoint_url", |arg| arg.help(
    "AWS endpoint URL (for S3-compatible services); AWS_ENDPOINT_URL is read when \
     AWS_ENDPOINT_URL_S3 is unset"
)))]
pub struct RecoveryStorageArgs {
    /// Existing local dataset root or explicit s3://bucket/prefix URI.
    pub path: String,
    #[command(flatten)]
    pub aws: AwsArgs,
}

impl std::fmt::Debug for RecoveryStorageArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryStorageArgs")
            .finish_non_exhaustive()
    }
}

impl RecoveryStorageArgs {
    /// The S3 settings, with `--aws-endpoint-url` resolved like every other command
    /// (`AWS_ENDPOINT_URL_S3`), then from [`LEGACY_ENDPOINT_ENV`] when neither the
    /// flag nor `AWS_ENDPOINT_URL_S3` is set.
    pub fn aws(&self) -> AwsConfig {
        let mut aws = AwsConfig::from(&self.aws);
        if aws.aws_endpoint_url.is_none() && std::env::var_os("AWS_ENDPOINT_URL_S3").is_none() {
            aws.aws_endpoint_url = std::env::var(LEGACY_ENDPOINT_ENV).ok();
        }
        aws
    }
}

#[derive(Args)]
pub struct RecoveryReleaseArgs {
    #[command(flatten)]
    pub storage: RecoveryStorageArgs,
    /// Exact owner UUID reported by recovery status.
    #[arg(long)]
    pub expected_owner: String,
    /// Exact generation reported by recovery status.
    #[arg(long)]
    pub expected_generation: u64,
    /// Non-secret reference proving the prior writer can no longer issue requests.
    #[arg(long)]
    pub stopped_writer_evidence: String,
    /// Non-secret provider confirmation that prior requests completed or are permanently revoked.
    /// This is an operator assertion; the generic client cannot verify provider drain.
    #[arg(long)]
    pub provider_quiescence_evidence: String,
}

impl std::fmt::Debug for RecoveryReleaseArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryReleaseArgs")
            .finish_non_exhaustive()
    }
}

#[derive(Serialize)]
pub struct RecoveryStatus {
    backend: &'static str,
    ownership: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner: Option<OwnerSummary>,
    state: SlotSummary,
    pending: SlotSummary,
}

#[derive(Serialize)]
struct OwnerSummary {
    owner_id: String,
    generation: u64,
    operation: String,
}

#[derive(Default, Serialize)]
struct SlotSummary {
    present: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    incarnation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<u64>,
}

impl SlotSummary {
    fn active(version: ControlVersion) -> Self {
        Self {
            present: true,
            incarnation: Some(version.incarnation),
            revision: Some(version.revision),
        }
    }
}

pub async fn run_recovery(command: &RecoveryCommands) -> Result<()> {
    let path = match command {
        RecoveryCommands::Status(storage) | RecoveryCommands::Recover(storage) => &storage.path,
        RecoveryCommands::Release(args) => &args.storage.path,
    };
    refuse_artifact_directory(path)?;
    let result = match command {
        RecoveryCommands::Status(storage) => status(storage).await?,
        RecoveryCommands::Recover(storage) => {
            let aws = storage.aws();
            let prepared = crate::ingest::maintenance::acquire(
                "recovery",
                vec![crate::ingest::maintenance::MaintenanceTarget::input(
                    storage.path.clone(),
                )?],
                crate::ingest::maintenance::MaintenancePolicy::Recover,
                Some(&aws),
            )
            .await?;
            let roots = prepared.roots.len();
            let merges = prepared.recovered_merges;
            prepared.ownership.release().await?;
            println!(
                "{}",
                serde_json::json!({"recovered_protected_roots":roots,"recovered_merge_journals":merges})
            );
            return Ok(());
        }
        RecoveryCommands::Release(args) => {
            if !args.storage.path.starts_with("s3://") {
                bail!("local ownership is an OS lock and cannot be forcibly released; stop the owning process before recovery");
            }
            let (bucket, prefix) = remote_path(&args.storage.path)?;
            crate::cli::validate_s3_output_credentials(
                &args.storage.path,
                args.storage.aws.aws_access_key_id.as_deref(),
                args.storage.aws.aws_secret_access_key.as_deref(),
            )?;
            let store: Arc<dyn ObjectStore> =
                Arc::new(args.storage.aws().build_s3_client_for_mutation(&bucket)?);
            release_exact(store.clone(), args).await?;
            remote_status(store, &prefix).await?
        }
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn status(storage: &RecoveryStorageArgs) -> Result<RecoveryStatus> {
    if storage.path.starts_with("s3://") {
        let (bucket, prefix) = remote_path(&storage.path)?;
        let store: Arc<dyn ObjectStore> = Arc::new(storage.aws().build_read_client(&bucket)?);
        remote_status(store, &prefix).await
    } else {
        local_status(Path::new(&storage.path))
    }
}

fn local_status(root: &Path) -> Result<RecoveryStatus> {
    if !root.is_dir() {
        bail!("recovery status requires an existing local dataset directory");
    }
    // An existing root avoids creating directories. The OS guard makes the
    // two record reads a consistent snapshot; a busy owner is a precise error.
    let owner = LocalOwnership::acquire(&[root.to_path_buf()])
        .context("local ownership is busy or unavailable; status did not modify control records")?;
    let store = LocalStateStore::new(root, &owner)?;
    let load = |key| -> Result<SlotSummary> {
        Ok(store
            .load::<serde_json::Value>(key)?
            .map(|doc| SlotSummary::active(doc.version))
            .unwrap_or_default())
    };
    Ok(RecoveryStatus {
        backend: "local",
        ownership: "available",
        owner: None,
        state: load(ControlKey::State)?,
        pending: load(ControlKey::Pending)?,
    })
}

/// `_fireparq/` holds a dataset's artifacts, never its control records.
fn refuse_artifact_directory(path: &str) -> Result<()> {
    if crate::artifacts::is_in_artifacts_dir(path) {
        bail!(
            "recovery requires a dataset root, not the {}/ artifact directory inside it",
            crate::artifacts::ARTIFACTS_DIR
        );
    }
    Ok(())
}

fn remote_path(path: &str) -> Result<(String, String)> {
    let (bucket, prefix) = crate::writer::parse_s3_url(path)?;
    if bucket.is_empty()
        || bucket.contains(['?', '#'])
        || prefix.contains(['?', '#', '\\'])
        || (!prefix.is_empty()
            && prefix
                .split('/')
                .any(|part| part.is_empty() || matches!(part, "." | "..")))
        || crate::artifacts::is_control_path(&prefix)
    {
        bail!("recovery requires a dataset prefix, not an ambiguous path or control object");
    }
    Ok((bucket, prefix))
}

async fn remote_status(store: Arc<dyn ObjectStore>, prefix: &str) -> Result<RecoveryStatus> {
    let record = S3Ownership::status(&store).await?;
    let ownership = match record.as_ref().map(OwnerRecord::state) {
        Some(OwnerState::Owned) => "owned",
        Some(OwnerState::Released) => "released",
        None => "absent",
    };
    // GET-only: do not create ownership probes, acquire, or release anything.
    let state = remote_slot(&store, prefix, ControlKey::State).await?;
    let pending = remote_slot(&store, prefix, ControlKey::Pending).await?;
    // Status is not an ownership capability. Refuse a torn owner snapshot rather
    // than suggesting its generation can safely authorize a future mutation.
    if S3Ownership::status(&store).await? != record {
        bail!("ownership changed while reading status; retry the read-only status command");
    }
    Ok(RecoveryStatus {
        backend: "s3",
        ownership,
        owner: record.as_ref().map(|record| OwnerSummary {
            owner_id: record.owner_id().to_owned(),
            generation: record.generation(),
            operation: record.operation().to_owned(),
        }),
        state,
        pending,
    })
}

async fn remote_slot(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    key: ControlKey,
) -> Result<SlotSummary> {
    use futures::TryStreamExt;
    let key = object_store::path::Path::from(if prefix.is_empty() {
        format!("{CONTROL_DIRECTORY}/{}", key.filename())
    } else {
        format!("{prefix}/{CONTROL_DIRECTORY}/{}", key.filename())
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        let object = match store.get(&key).await {
            Ok(object) => object,
            Err(object_store::Error::NotFound { .. }) => return Ok(SlotSummary::default()),
            Err(_) => bail!("control status read failed"),
        };
        if object.meta.size > MAX_CONTROL_BYTES as u64 {
            bail!("control status record exceeds size limit");
        }
        let mut stream = object.into_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream
            .try_next()
            .await
            .map_err(|_| anyhow::anyhow!("control status body read failed"))?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_CONTROL_BYTES {
                bail!("control status record exceeds size limit");
            }
            bytes.extend_from_slice(&chunk);
        }
        let (version, payload) = crate::durable_state::decode_slot(&bytes)?;
        Ok(if payload.is_some() {
            SlotSummary::active(version)
        } else {
            SlotSummary::default()
        })
    })
    .await
    .map_err(|_| anyhow::anyhow!("control status read timed out"))?
}

async fn release_exact(store: Arc<dyn ObjectStore>, args: &RecoveryReleaseArgs) -> Result<()> {
    let expected = S3Ownership::status(&store)
        .await?
        .context("there is no S3 owner to release")?;
    if expected.owner_id() != args.expected_owner
        || expected.generation() != args.expected_generation
    {
        bail!("owner UUID or generation differs from the explicitly requested recovery target");
    }
    let authorization = RecoveryAuthorization::assert_provider_quiescence(
        &expected,
        &args.stopped_writer_evidence,
        &args.provider_quiescence_evidence,
    )?;
    S3Ownership::operator_release(store, &expected, authorization).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;

    #[test]
    fn local_status_does_not_create_controls_or_reveal_payload() {
        let temp = tempfile::tempdir().unwrap();
        let missing = temp.path().join("missing");
        assert!(local_status(&missing).is_err());
        assert!(!missing.exists());
        let status = local_status(temp.path()).unwrap();
        assert!(!status.state.present);
        assert!(!temp.path().join(CONTROL_DIRECTORY).exists());
        {
            let guard = LocalOwnership::acquire(&[temp.path().to_owned()]).unwrap();
            let store = LocalStateStore::new(temp.path(), &guard).unwrap();
            store
                .create(
                    ControlKey::State,
                    &serde_json::json!({"cursor":"opaque-private-cursor"}),
                )
                .unwrap();
            assert!(local_status(temp.path()).is_err());
        }
        let json = serde_json::to_string(&local_status(temp.path()).unwrap()).unwrap();
        assert!(json.contains("incarnation"));
        assert!(!json.contains("opaque-private-cursor"));
    }

    #[tokio::test]
    async fn the_artifact_directory_is_not_a_recovery_target() {
        let temp = tempfile::tempdir().unwrap();
        let artifacts = temp.path().join("mainnet/_fireparq");
        std::fs::create_dir_all(&artifacts).unwrap();
        for path in [
            artifacts.to_string_lossy().into_owned(),
            "s3://bucket/_fireparq".to_string(),
            "s3://bucket/mainnet/_fireparq/".to_string(),
        ] {
            for command in [
                RecoveryCommands::Status(RecoveryStorageArgs {
                    path: path.clone(),
                    aws: Default::default(),
                }),
                RecoveryCommands::Recover(RecoveryStorageArgs {
                    path: path.clone(),
                    aws: Default::default(),
                }),
            ] {
                let error = run_recovery(&command).await.unwrap_err().to_string();
                assert!(
                    error.contains("not the _fireparq/ artifact directory"),
                    "{path}: {error}"
                );
            }
        }
        assert_eq!(std::fs::read_dir(&artifacts).unwrap().count(), 0);
        assert!(!temp.path().join("mainnet").join(CONTROL_DIRECTORY).exists());
    }

    #[tokio::test]
    async fn remote_status_is_get_only_even_when_owner_absent() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        assert_eq!(
            remote_status(store.clone(), "dataset")
                .await
                .unwrap()
                .ownership,
            "absent"
        );
        let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert!(objects.is_empty());
    }

    /// `recovery status s3://bucket` reads a dataset written with
    /// `build --output s3://bucket` at the bucket root; its control records are
    /// not visible from a chain directory below it.
    #[tokio::test]
    async fn remote_status_reads_the_control_records_of_a_bucket_root_dataset() {
        for path in ["s3://bucket", "s3://bucket/"] {
            assert_eq!(
                remote_path(path).unwrap(),
                ("bucket".to_string(), String::new())
            );
        }
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let owner = S3Ownership::acquire(store.clone(), "build", vec![String::new()])
            .await
            .unwrap();
        crate::durable_state_s3::S3StateStore::new(&owner, "")
            .unwrap()
            .create(ControlKey::State, &serde_json::json!({ "fixture": true }))
            .await
            .unwrap();
        owner.release().await.unwrap();
        let status = remote_status(store.clone(), "").await.unwrap();
        assert!(status.state.present);
        assert!(!status.pending.present);
        assert_eq!(status.ownership, "released");
        assert!(!remote_status(store, "mainnet").await.unwrap().state.present);
    }

    #[test]
    fn release_cli_requires_both_evidence_references_and_exact_target() {
        use clap::Parser;
        #[derive(Parser)]
        struct Cli {
            #[command(subcommand)]
            command: RecoveryCommands,
        }
        let base = [
            "recovery",
            "release",
            "s3://fixture/data",
            "--expected-owner",
            "owner",
            "--expected-generation",
            "1",
            "--stopped-writer-evidence",
            "process-record",
        ];
        assert!(Cli::try_parse_from(base).is_err());
        let mut complete = base.to_vec();
        complete.extend(["--provider-quiescence-evidence", "provider-record"]);
        assert!(matches!(
            Cli::try_parse_from(complete).unwrap().command,
            RecoveryCommands::Release(_)
        ));
        assert!(Cli::try_parse_from([
            "recovery",
            "release",
            "s3://fixture/data",
            "--acknowledge-writer-stopped"
        ])
        .is_err());
    }

    fn args(owner: &OwnerRecord) -> RecoveryReleaseArgs {
        RecoveryReleaseArgs {
            storage: RecoveryStorageArgs {
                path: "s3://fixture/data".into(),
                aws: AwsArgs::default(),
            },
            expected_owner: owner.owner_id().to_owned(),
            expected_generation: owner.generation(),
            stopped_writer_evidence: "fixture task joined; no detached writer".into(),
            provider_quiescence_evidence: "in-memory fixture has no outstanding requests".into(),
        }
    }

    #[tokio::test]
    async fn explicit_release_requires_exact_identity_and_both_quiescence_assertions() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let owner = S3Ownership::acquire(store.clone(), "fixture", vec!["data".into()])
            .await
            .unwrap();
        let expected = owner.record().clone();
        drop(owner);
        let mut request = args(&expected);
        request.expected_generation += 1;
        assert!(release_exact(store.clone(), &request).await.is_err());
        request.expected_generation = expected.generation();
        request.provider_quiescence_evidence.clear();
        assert!(release_exact(store.clone(), &request).await.is_err());
        assert_eq!(
            S3Ownership::status(&store).await.unwrap(),
            Some(expected.clone())
        );
        request = args(&expected);
        release_exact(store.clone(), &request).await.unwrap();
        let released = S3Ownership::status(&store).await.unwrap().unwrap();
        assert_eq!(released.state(), OwnerState::Released);
        assert_eq!(released.generation(), expected.generation());
        assert!(release_exact(store, &request).await.is_err());
        let debug = format!("{request:?}");
        assert!(!debug.contains("fixture task"));
    }
}
