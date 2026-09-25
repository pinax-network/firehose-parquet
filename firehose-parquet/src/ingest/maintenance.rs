//! Common protected-root discovery and recovery before maintenance reads.
//! This module never adopts legacy data or infers authority from a mirror.

use anyhow::{bail, ensure, Context, Result};
use futures::StreamExt;
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::binding::{
    canonical_directory, mirror_service, output_path, resolve_output_identity,
    validate_runtime_bindings,
};
use super::controller::TransactionController;
use super::mirror::ProtectedMirror;
use super::parts::TransactionParts;
use super::state::{AuthorityState, MirrorBinding, StorageIdentity, StreamDescriptor};
use super::store::TransactionStateStore;
use crate::cli::AwsConfig;
use crate::dataset_lock::{DatasetOwnership, MutationScope};
use crate::durable_state::{ControlKey, LocalStateStore, CONTROL_DIRECTORY};
use crate::durable_state_s3::S3StateStore;

const MAX_ROOTS: usize = 256;
const MAX_EXPANSIONS: usize = 8;
const REMOTE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct MaintenanceTarget {
    path: String,
    file: bool,
    artifact_output: bool,
}
impl MaintenanceTarget {
    pub(crate) fn input(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        let file = !path.starts_with("s3://")
            && fs::metadata(&path)
                .context("maintenance input cannot be inspected")?
                .is_file();
        Ok(Self {
            path,
            file,
            artifact_output: false,
        })
    }
    pub(crate) fn directory(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            file: false,
            artifact_output: false,
        }
    }
    pub(crate) fn file(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            file: true,
            artifact_output: true,
        }
    }
    fn scope(&self) -> MutationScope {
        if self.file {
            MutationScope::file(self.path.clone())
        } else {
            MutationScope::directory(self.path.clone())
        }
    }
}

pub(crate) enum MaintenancePolicy {
    Merge,
    Recover,
    Artifacts,
    Truncate,
    Rollup {
        source: String,
        output: String,
        delete_source: bool,
    },
}

// No Debug: the descriptor may carry private storage bindings in future versions.
pub(crate) struct ProtectedRoot {
    pub identity: StorageIdentity,
    pub descriptor: StreamDescriptor,
}
pub(crate) struct PreparedMaintenance {
    pub ownership: DatasetOwnership,
    pub roots: Vec<ProtectedRoot>,
    pub recovered_merges: usize,
}

/// Acquire selected scopes, discover markers under that capability, then enlarge
/// the guard set before touching control/data files outside the original scope.
/// Every reacquisition starts discovery anew; no pre-lock listing is authority.
pub(crate) async fn acquire(
    operation: &str,
    targets: Vec<MaintenanceTarget>,
    policy: MaintenancePolicy,
    aws: Option<&AwsConfig>,
) -> Result<PreparedMaintenance> {
    let default_aws = empty_aws();
    let runtime_aws = aws.unwrap_or(&default_aws);
    let mut all_targets: BTreeSet<_> = targets.iter().cloned().collect();
    for _ in 0..MAX_EXPANSIONS {
        let ownership = DatasetOwnership::acquire(
            operation,
            all_targets.iter().map(MaintenanceTarget::scope).collect(),
            aws,
        )
        .await?;
        let planning = async {
            let markers = discover_markers(&ownership, &all_targets).await?;
            enforce_policy(&markers, &policy, runtime_aws)?;
            let mut expanded = all_targets.clone();
            expanded.extend(
                markers
                    .iter()
                    .map(|root| MaintenanceTarget::directory(root.clone())),
            );
            // An ancestor marker requires an exclusive root, not merely the
            // shared ancestor lock held by a selected table or partition.
            if expanded != all_targets {
                return Ok((expanded, None));
            }
            let roots = load_roots(&ownership, &markers, runtime_aws).await?;
            if matches!(policy, MaintenancePolicy::Artifacts) {
                validate_artifact_destinations(&targets, &roots, runtime_aws)?;
            }
            for root in &roots {
                match &root.descriptor.mirror {
                    MirrorBinding::Disabled => {}
                    MirrorBinding::Local { absolute_path } => {
                        expanded.insert(MaintenanceTarget::file(absolute_path.clone()));
                    }
                    MirrorBinding::S3 { bucket, key, .. } => {
                        expanded.insert(MaintenanceTarget::file(format!("s3://{bucket}/{key}")));
                    }
                }
            }
            Ok::<_, anyhow::Error>((expanded, Some(roots)))
        }
        .await;
        let (expanded, roots) = match planning {
            Ok(plan) => plan,
            Err(error) => {
                // Planning has not attempted data/control recovery. Resolved
                // acquisitions may be ordinarily released even on refusal.
                return match ownership.release().await {
                    Ok(()) => Err(error),
                    Err(_) => Err(error.context(
                        "planning also left unresolved ownership; inspect recovery status",
                    )),
                };
            }
        };
        if expanded != all_targets {
            ownership.release().await?;
            all_targets = expanded;
            continue;
        }
        let roots = roots.context("maintenance root plan did not stabilize")?;
        let recovered_merges = recover_roots(&ownership, &roots, runtime_aws).await?
            + if matches!(policy, MaintenancePolicy::Merge) {
                0
            } else {
                recover_selected_legacy_merges(&ownership, &targets, &roots, runtime_aws).await?
            };
        return Ok(PreparedMaintenance {
            ownership,
            roots,
            recovered_merges,
        });
    }
    bail!(
        "maintenance ownership scope did not stabilize; no recovery or data mutation was attempted"
    )
}

/// A synchronous command bridge for ordinary CLI and multi-thread runtimes.
/// Current-thread runtime callers use the async entry point.
pub(crate) fn acquire_blocking(
    operation: &str,
    targets: Vec<MaintenanceTarget>,
    policy: MaintenancePolicy,
    aws: Option<&AwsConfig>,
) -> Result<PreparedMaintenance> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(acquire(operation,targets,policy,aws))),
        Ok(_) => bail!("synchronous maintenance requires a multi-thread runtime; use the async maintenance API"),
        Err(_) => tokio::runtime::Builder::new_current_thread().enable_all().build().context("creating maintenance storage runtime")?.block_on(acquire(operation,targets,policy,aws)),
    }
}

async fn discover_markers(
    ownership: &DatasetOwnership,
    targets: &BTreeSet<MaintenanceTarget>,
) -> Result<BTreeSet<String>> {
    ownership.revalidate_local_paths()?;
    let mut roots = BTreeSet::new();
    for target in targets {
        ensure!(
            !crate::artifacts::is_control_path(&target.path),
            "control records cannot be maintenance targets"
        );
        if target.path.starts_with("s3://") {
            let (bucket, key) = crate::writer::parse_s3_url(&target.path)?;
            super::state::validate_relative_path(&key, true)?;
            let owner = ownership
                .remote(&bucket)
                .context("maintenance source bucket is not owned")?;
            let store = owner.object_store();
            let prefixes = remote_ancestors(&key);
            for ancestor in prefixes {
                let marker = if ancestor.is_empty() {
                    CONTROL_DIRECTORY.to_owned()
                } else {
                    format!("{ancestor}/{CONTROL_DIRECTORY}")
                };
                let marker_path = object_store::path::Path::from(marker.as_str());
                let exists = tokio::time::timeout(REMOTE_TIMEOUT, async {
                    let mut objects = store.list(Some(&marker_path));
                    while let Some(object) = objects.next().await {
                        let object = object.map_err(|_| {
                            anyhow::anyhow!("listing protected control markers failed")
                        })?;
                        if contains_remote(&marker, object.location.as_ref()) {
                            return Ok::<_, anyhow::Error>(true);
                        }
                    }
                    Ok(false)
                })
                .await
                .map_err(|_| anyhow::anyhow!("protected marker discovery timed out"))??;
                if exists {
                    insert_root(&mut roots, remote_url(&bucket, &ancestor))?;
                }
            }
            let prefix = (!key.is_empty()).then(|| object_store::path::Path::from(key.as_str()));
            tokio::time::timeout(REMOTE_TIMEOUT, async {
                let mut objects = store.list(prefix.as_ref());
                while let Some(object) = objects.next().await {
                    let object = object.map_err(|_| {
                        anyhow::anyhow!("listing protected descendant markers failed")
                    })?;
                    let location = object.location.as_ref();
                    if !contains_remote(&key, location) {
                        continue;
                    }
                    if let Some(root) = marker_parent(location) {
                        insert_root(&mut roots, remote_url(&bucket, &root))?;
                    }
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map_err(|_| anyhow::anyhow!("protected descendant discovery timed out"))??;
        } else {
            let lexical = absolute_path(Path::new(&target.path))?;
            let selected = if target.file {
                lexical.parent().context("maintenance file has no parent")?
            } else {
                lexical.as_path()
            };
            let canonical = canonical_directory(selected)?;
            for ancestor in lexical
                .ancestors()
                .skip(usize::from(target.file))
                .chain(canonical.ancestors())
            {
                if local_marker(ancestor)? {
                    insert_root(
                        &mut roots,
                        fs::canonicalize(ancestor)?.to_string_lossy().into_owned(),
                    )?;
                }
            }
            if !target.file {
                collect_local_markers(&canonical, &mut roots)?;
            }
        }
    }
    reject_nested(&roots)?;
    Ok(roots)
}

fn insert_root(roots: &mut BTreeSet<String>, root: String) -> Result<()> {
    roots.insert(root);
    ensure!(
        roots.len() <= MAX_ROOTS,
        "maintenance selection exceeds the protected-root limit"
    );
    Ok(())
}
fn local_marker(root: &Path) -> Result<bool> {
    match fs::symlink_metadata(root.join(CONTROL_DIRECTORY)) {
        Ok(meta) => {
            ensure!(
                meta.is_dir() && !meta.file_type().is_symlink(),
                "protected control marker is not a regular directory"
            );
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => bail!("inspecting protected control marker failed"),
    }
}
fn collect_local_markers(root: &Path, roots: &mut BTreeSet<String>) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    ensure!(root.is_dir(), "maintenance directory is not a directory");
    let mut pending = vec![root.to_owned()];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(&path).context("listing protected dataset descendants")? {
            let entry = entry.context("reading protected dataset entry")?;
            let kind = entry.file_type()?;
            ensure!(
                !kind.is_symlink(),
                "nested symlinks cannot be protected maintenance inputs"
            );
            if entry.file_name() == CONTROL_DIRECTORY {
                ensure!(
                    kind.is_dir(),
                    "protected control marker is not a regular directory"
                );
                insert_root(roots, path.to_string_lossy().into_owned())?;
            } else if kind.is_dir()
                && !crate::artifacts::is_control_path(&entry.path().to_string_lossy())
            {
                pending.push(entry.path());
            }
        }
    }
    Ok(())
}
fn absolute_path(path: &Path) -> Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    ensure!(
        !path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir)),
        "maintenance paths must not contain parent traversal"
    );
    Ok(path)
}
fn remote_url(bucket: &str, key: &str) -> String {
    if key.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{key}")
    }
}
fn contains_remote(root: &str, path: &str) -> bool {
    root.is_empty()
        || root == path
        || path
            .strip_prefix(root)
            .is_some_and(|tail| tail.starts_with('/'))
}
fn remote_ancestors(key: &str) -> Vec<String> {
    let mut values = vec![String::new()];
    let mut prefix = String::new();
    for piece in key.split('/').filter(|v| !v.is_empty()) {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(piece);
        values.push(prefix.clone());
    }
    values
}
fn marker_parent(key: &str) -> Option<String> {
    let parts: Vec<_> = key.split('/').collect();
    parts
        .iter()
        .position(|part| *part == CONTROL_DIRECTORY)
        .map(|index| parts[..index].join("/"))
}
fn path_contains(root: &str, path: &str) -> bool {
    if root.starts_with("s3://") || path.starts_with("s3://") {
        root.starts_with("s3://") && path.starts_with("s3://") && contains_remote(root, path)
    } else {
        Path::new(path).starts_with(root)
    }
}
fn reject_nested(roots: &BTreeSet<String>) -> Result<()> {
    for (index, root) in roots.iter().enumerate() {
        for other in roots.iter().skip(index + 1) {
            ensure!(
                !path_contains(root, other) && !path_contains(other, root),
                "nested protected datasets have conflicting authority; maintenance refused"
            );
        }
    }
    Ok(())
}
fn enforce_policy(
    roots: &BTreeSet<String>,
    policy: &MaintenancePolicy,
    aws: &AwsConfig,
) -> Result<()> {
    match policy {
        MaintenancePolicy::Truncate => ensure!(roots.is_empty(),"truncate is unsupported for protected datasets; it would invalidate the authoritative ingestion frontier"),
        MaintenancePolicy::Rollup {source,output,delete_source} => {
            let source=normalized_selection(source,aws)?; let output=normalized_selection(output,aws)?;
            for root in roots {
                ensure!(!path_contains(root,&output) && !path_contains(&output,root),"rollup output overlaps a protected dataset; choose a separate legacy export root");
                ensure!(!*delete_source || (!path_contains(root,&source) && !path_contains(&source,root)),"destructive rollup is unsupported for protected sources; copy to a separate export root without deleting sources");
            }
        },
        _ => {},
    }
    Ok(())
}
fn normalized_selection(path: &str, aws: &AwsConfig) -> Result<String> {
    if !path.starts_with("s3://") && fs::metadata(path).is_ok_and(|metadata| metadata.is_file()) {
        let path = absolute_path(Path::new(path))?;
        return Ok(
            canonical_directory(path.parent().context("selection has no parent")?)?
                .join(path.file_name().context("selection has no filename")?)
                .to_string_lossy()
                .into_owned(),
        );
    }
    Ok(output_path(&resolve_output_identity(path, aws)?))
}

async fn load_roots(
    ownership: &DatasetOwnership,
    markers: &BTreeSet<String>,
    aws: &AwsConfig,
) -> Result<Vec<ProtectedRoot>> {
    let mut roots = Vec::new();
    for path in markers {
        let identity = resolve_output_identity(path, aws)?;
        let authority: AuthorityState = match &identity {
            StorageIdentity::Local { canonical_root } => {
                LocalStateStore::new(
                    Path::new(canonical_root),
                    ownership
                        .local()
                        .context("protected local root is not owned")?,
                )?
                .load::<AuthorityState>(ControlKey::State)?
                .context("protected marker has no authoritative state; refusing legacy adoption")?
                .payload
            }
            StorageIdentity::S3 { bucket, prefix, .. } => {
                S3StateStore::new(
                    ownership
                        .remote(bucket)
                        .context("protected bucket is not owned")?,
                    prefix,
                )?
                .load::<AuthorityState>(ControlKey::State)
                .await?
                .context("protected marker has no authoritative state; refusing legacy adoption")?
                .payload
            }
        };
        authority.validate()?;
        validate_runtime_bindings(&authority.descriptor, path, aws)?;
        roots.push(ProtectedRoot {
            identity,
            descriptor: authority.descriptor,
        });
    }
    Ok(roots)
}

async fn recover_roots(
    ownership: &DatasetOwnership,
    roots: &[ProtectedRoot],
    aws: &AwsConfig,
) -> Result<usize> {
    // Validate every state/pending pair before recovering the first dataset.
    for root in roots {
        validate_ingestion_recovery_order(&root.identity, ownership).await?;
    }
    let mut recovered = 0;
    for root in roots {
        let service = mirror_service(&root.descriptor.mirror, aws)?;
        let mirror = ProtectedMirror::new(ownership, &root.descriptor.mirror, service.as_ref())?;
        let controller = TransactionController::open(
            state_store(&root.identity, ownership)?,
            part_store(&root.identity, ownership)?,
            &mirror,
            &root.descriptor,
        )
        .await?;
        drop(controller);
        recovered += crate::merge::recover_guarded_for_ingestion(
            &root.identity,
            ownership,
            Some(&root.descriptor.id()?),
        )
        .await?;
    }
    Ok(recovered)
}
fn state_store<'a>(
    identity: &StorageIdentity,
    ownership: &'a DatasetOwnership,
) -> Result<TransactionStateStore<'a>> {
    match identity {
        StorageIdentity::Local { canonical_root } => TransactionStateStore::local(
            Path::new(canonical_root),
            ownership.local().context("local dataset is not owned")?,
        ),
        StorageIdentity::S3 { bucket, prefix, .. } => TransactionStateStore::s3(
            prefix,
            ownership
                .remote(bucket)
                .context("dataset bucket is not owned")?,
        ),
    }
}
fn part_store<'a>(
    identity: &StorageIdentity,
    ownership: &'a DatasetOwnership,
) -> Result<TransactionParts<'a>> {
    match identity {
        StorageIdentity::Local { canonical_root } => TransactionParts::local(
            Path::new(canonical_root),
            ownership.local().context("local dataset is not owned")?,
        ),
        StorageIdentity::S3 { bucket, prefix, .. } => TransactionParts::s3(
            prefix,
            ownership
                .remote(bucket)
                .context("dataset bucket is not owned")?,
            "no-store, no-cache, max-age=0",
        ),
    }
}

/// Called before protected ingestion reads data or opens Blocks. This does not
/// acquire ownership again or take over an unrecognized legacy remote journal.
pub(crate) async fn prepare_ingestion(
    output: &StorageIdentity,
    ownership: &DatasetOwnership,
    aws: &AwsConfig,
) -> Result<()> {
    let path = output_path(output);
    let targets = BTreeSet::from([MaintenanceTarget::directory(path.clone())]);
    let markers = discover_markers(ownership, &targets).await?;
    ensure!(
        markers.iter().all(|root| *root == path),
        "selected ingestion output overlaps another protected root"
    );
    let roots = load_roots(ownership, &markers, aws).await?;
    let protected = roots.first().map(|root| root.descriptor.id()).transpose()?;
    crate::merge::recover_guarded_for_ingestion(output, ownership, protected.as_ref()).await?;
    Ok(())
}

#[cfg(test)]
mod tests;

fn empty_aws() -> AwsConfig {
    AwsConfig {
        aws_access_key_id: None,
        aws_secret_access_key: None,
        aws_session_token: None,
        aws_region: None,
        aws_endpoint_url: None,
    }
}

fn normalize_file(path: &str, aws: &AwsConfig) -> Result<String> {
    if path.starts_with("s3://") {
        return Ok(output_path(&resolve_output_identity(path, aws)?));
    }
    let path = absolute_path(Path::new(path))?;
    Ok(
        canonical_directory(path.parent().context("artifact has no parent")?)?
            .join(path.file_name().context("artifact has no filename")?)
            .to_string_lossy()
            .into_owned(),
    )
}
fn validate_artifact_destinations(
    targets: &[MaintenanceTarget],
    roots: &[ProtectedRoot],
    aws: &AwsConfig,
) -> Result<()> {
    for target in targets.iter().filter(|target| target.artifact_output) {
        let path = normalize_file(&target.path, aws)?;
        for root in roots {
            let mirror = match &root.descriptor.mirror {
                MirrorBinding::Disabled => None,
                MirrorBinding::Local { absolute_path } => Some(normalize_file(absolute_path, aws)?),
                MirrorBinding::S3 { bucket, key, .. } => Some(format!("s3://{bucket}/{key}")),
            };
            ensure!(
                mirror.as_ref() != Some(&path),
                "artifact destination would overwrite a protected cursor mirror"
            );
            let base = output_path(&root.identity);
            if path_contains(&base, &path) {
                let name = path.rsplit('/').next().unwrap_or("");
                ensure!(
                    name != "cursor.parquet"
                        && name != crate::merge_journal::JOURNAL_FILE
                        && !name.starts_with(".fireparq-"),
                    "artifact destination would overwrite protected recovery metadata"
                );
                ensure!(
                    !path.ends_with(".parquet")
                        || crate::artifacts::is_reserved_artifact_path(&path),
                    "artifact destination is an ordinary protected data part"
                );
            }
        }
    }
    Ok(())
}

async fn has_merge_journal(
    identity: &StorageIdentity,
    ownership: &DatasetOwnership,
) -> Result<bool> {
    match identity {
        StorageIdentity::Local { canonical_root } => {
            let mut pending = vec![PathBuf::from(canonical_root)];
            while let Some(path) = pending.pop() {
                for entry in fs::read_dir(path)? {
                    let entry = entry?;
                    let kind = entry.file_type()?;
                    ensure!(
                        !kind.is_symlink(),
                        "nested symlink discovered during recovery"
                    );
                    if entry.file_name() == crate::merge_journal::JOURNAL_FILE {
                        return Ok(true);
                    }
                    if kind.is_dir()
                        && !crate::artifacts::is_control_path(&entry.path().to_string_lossy())
                    {
                        pending.push(entry.path());
                    }
                }
            }
            Ok(false)
        }
        StorageIdentity::S3 { bucket, prefix, .. } => {
            let owner = ownership
                .remote(bucket)
                .context("merge discovery bucket is not owned")?;
            let key = (!prefix.is_empty()).then(|| object_store::path::Path::from(prefix.as_str()));
            tokio::time::timeout(REMOTE_TIMEOUT, async {
                let mut objects = owner.object_store().list(key.as_ref());
                while let Some(object) = objects.next().await {
                    let object =
                        object.map_err(|_| anyhow::anyhow!("merge journal discovery failed"))?;
                    if contains_remote(prefix, object.location.as_ref())
                        && object.location.filename() == Some(crate::merge_journal::JOURNAL_FILE)
                        && !crate::artifacts::is_control_path(object.location.as_ref())
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            })
            .await
            .map_err(|_| anyhow::anyhow!("merge journal discovery timed out"))?
        }
    }
}
async fn recover_selected_legacy_merges(
    ownership: &DatasetOwnership,
    targets: &[MaintenanceTarget],
    roots: &[ProtectedRoot],
    aws: &AwsConfig,
) -> Result<usize> {
    let mut paths = BTreeSet::new();
    for target in targets.iter().filter(|target| !target.artifact_output) {
        let source = if target.file {
            Path::new(&target.path)
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."))
                .to_string_lossy()
                .into_owned()
        } else {
            target.path.clone()
        };
        let identity = resolve_output_identity(&source, aws)?;
        let path = output_path(&identity);
        if roots
            .iter()
            .any(|root| path_contains(&output_path(&root.identity), &path))
        {
            continue;
        }
        paths.insert(path);
    }
    // Protected descendants were already fully recovered. Any journals left in
    // an enclosing legacy tree must be unbound legacy journals; foreign stream
    // bindings still fail closed instead of being treated as ordinary files.
    let mut recovered = 0;
    for path in paths {
        let identity = resolve_output_identity(&path, aws)?;
        recovered +=
            crate::merge::recover_guarded_for_ingestion(&identity, ownership, None).await?;
    }
    Ok(recovered)
}

/// Prove disjoint recovery protocols before either one changes public files.
/// Session calls this while its permit is held, before opening its controller.
pub(crate) async fn validate_ingestion_recovery_order(
    output: &StorageIdentity,
    ownership: &DatasetOwnership,
) -> Result<()> {
    let snapshot = state_store(output, ownership)?.load().await?;
    if snapshot.pending.is_some() {
        ensure!(
            !has_merge_journal(output, ownership).await?,
            "ingestion and merge journals coexist; refusing ambiguous recovery ordering"
        );
    }
    Ok(())
}

/// Before eligibility or initialization, reject overlapping protected roots using
/// markers only. A shared ancestor lock does not authorize reading its authority.
pub(crate) async fn validate_ingestion_target(
    output: &StorageIdentity,
    ownership: &DatasetOwnership,
) -> Result<()> {
    let path = output_path(output);
    let targets = BTreeSet::from([MaintenanceTarget::directory(path.clone())]);
    let markers = discover_markers(ownership, &targets).await?;
    ensure!(
        markers.iter().all(|root| root == &path),
        "selected ingestion output overlaps another protected root"
    );
    Ok(())
}
