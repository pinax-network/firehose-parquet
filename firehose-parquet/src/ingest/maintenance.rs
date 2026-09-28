//! Protected-root discovery and recovery for `recovery recover`, and the
//! overlapping-root check `build` runs at startup. This module never adopts
//! legacy data or infers authority from a mirror.

use anyhow::{bail, ensure, Context, Result};
use std::collections::BTreeSet;
use std::fs;
use std::ops::ControlFlow;
use std::path::{Path, PathBuf};
use std::time::Instant;

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
use crate::maintenance::discovery::{visit_objects, ListingStats, LIST_REQUEST_TIMEOUT};

/// A protected dataset encloses, or is nested in, the selected ingestion root.
/// Adding or dropping a `{chain}` segment in `--output` of an existing dataset
/// always lands here: `s3://b` and `s3://b/{chain}` overlap. No path is echoed.
const OVERLAPPING_INGESTION_ROOT: &str = "selected ingestion output overlaps another protected root: an enclosing or nested directory already holds a protected dataset. `build` writes to --output exactly as given, with {chain} expanded to the endpoint's chain_name; rerun with the --output the existing dataset was created with, or use a separate output root";

const MAX_ROOTS: usize = 256;
const MAX_EXPANSIONS: usize = 8;

/// How far marker discovery looks around a selected directory.
#[derive(Clone, Copy, Eq, PartialEq)]
enum MarkerScope {
    /// Ancestors, the directory itself and every descendant: `recovery
    /// recover`, and `build` creating a dataset.
    Tree,
    /// Strict ancestors only, O(depth) requests and no data listing: `build`
    /// resuming a dataset whose authority it already read (#655).
    Ancestors,
}

/// What `build` found at its output root before any other startup check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IngestionTarget {
    /// No authority yet: the dataset is being created and the whole tree is
    /// checked for enclosing and nested protected roots.
    Create,
    /// An existing authority: only its ancestors are checked (#655).
    Resume,
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct MaintenanceTarget {
    path: String,
    file: bool,
}
impl MaintenanceTarget {
    pub(crate) fn input(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        let file = !path.starts_with("s3://")
            && fs::metadata(&path)
                .context("maintenance input cannot be inspected")?
                .is_file();
        Ok(Self { path, file })
    }
    pub(crate) fn directory(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            file: false,
        }
    }
    /// A protected root's cursor mirror, owned beside the root.
    fn file(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            file: true,
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

// No Debug: the descriptor may carry private storage bindings in future versions.
pub(crate) struct ProtectedRoot {
    pub identity: StorageIdentity,
    pub descriptor: StreamDescriptor,
}
pub(crate) struct PreparedMaintenance {
    pub ownership: DatasetOwnership,
    pub roots: Vec<ProtectedRoot>,
}

/// Acquire selected scopes, discover markers under that capability, then enlarge
/// the guard set before touching control/data files outside the original scope.
/// Every reacquisition starts discovery anew; no pre-lock listing is authority.
pub(crate) async fn acquire(
    operation: &str,
    targets: Vec<MaintenanceTarget>,
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
            let markers = discover_markers(
                &ownership,
                &all_targets,
                MarkerScope::Tree,
                &ListingStats::default(),
            )
            .await?;
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
        recover_roots(&ownership, &roots, runtime_aws).await?;
        return Ok(PreparedMaintenance { ownership, roots });
    }
    bail!(
        "maintenance ownership scope did not stabilize; no recovery or data mutation was attempted"
    )
}

async fn discover_markers(
    ownership: &DatasetOwnership,
    targets: &BTreeSet<MaintenanceTarget>,
    scope: MarkerScope,
    stats: &ListingStats,
) -> Result<BTreeSet<String>> {
    ownership.revalidate_local_paths()?;
    let mut roots = BTreeSet::new();
    // With `Ancestors`, the selected directory itself is skipped: its own
    // authority was already read, and its descendants are not listed.
    let skip_self = usize::from(scope == MarkerScope::Ancestors);
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
            let mut prefixes = remote_ancestors(&key);
            prefixes.truncate(prefixes.len() - skip_self);
            for ancestor in prefixes {
                let marker = if ancestor.is_empty() {
                    CONTROL_DIRECTORY.to_owned()
                } else {
                    format!("{ancestor}/{CONTROL_DIRECTORY}")
                };
                // object_store prefixes match whole path segments, so the first
                // page of this control prefix answers the question: one
                // request per ancestor, never a data listing.
                let marker_path = object_store::path::Path::from(marker.as_str());
                let exists = visit_objects(
                    store.as_ref(),
                    Some(&marker_path),
                    "listing protected control markers",
                    LIST_REQUEST_TIMEOUT,
                    stats,
                    |object| {
                        Ok(if contains_remote(&marker, object.location.as_ref()) {
                            ControlFlow::Break(())
                        } else {
                            ControlFlow::Continue(())
                        })
                    },
                )
                .await?
                .is_some();
                if exists {
                    insert_root(&mut roots, remote_url(&bucket, &ancestor))?;
                }
            }
            if scope == MarkerScope::Ancestors {
                continue;
            }
            let prefix = (!key.is_empty()).then(|| object_store::path::Path::from(key.as_str()));
            visit_objects(
                store.as_ref(),
                prefix.as_ref(),
                "listing protected descendant markers",
                LIST_REQUEST_TIMEOUT,
                stats,
                |object| {
                    let location = object.location.as_ref();
                    // A Delta log holds commits and checkpoints, never control state.
                    if contains_remote(&key, location)
                        && !crate::artifacts::is_in_delta_log(location)
                    {
                        if let Some(root) = marker_parent(location) {
                            insert_root(&mut roots, remote_url(&bucket, &root))?;
                        }
                    }
                    Ok(ControlFlow::<()>::Continue(()))
                },
            )
            .await?;
        } else {
            let lexical = absolute_path(Path::new(&target.path))?;
            let selected = if target.file {
                lexical.parent().context("maintenance file has no parent")?
            } else {
                lexical.as_path()
            };
            let canonical = canonical_directory(selected)?;
            let skip = usize::from(target.file).max(skip_self);
            for ancestor in lexical
                .ancestors()
                .skip(skip)
                .chain(canonical.ancestors().skip(skip_self))
            {
                if local_marker(ancestor)? {
                    insert_root(
                        &mut roots,
                        fs::canonicalize(ancestor)?.to_string_lossy().into_owned(),
                    )?;
                }
            }
            if !target.file && scope == MarkerScope::Tree {
                let started = Instant::now();
                let mut counts = (0, 0);
                let walked = collect_local_markers(&canonical, &mut roots, &mut counts);
                stats.record(counts.0, counts.1, started.elapsed());
                walked?;
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
fn collect_local_markers(
    root: &Path,
    roots: &mut BTreeSet<String>,
    (reads, entries): &mut (u64, u64),
) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    ensure!(root.is_dir(), "maintenance directory is not a directory");
    let mut pending = vec![root.to_owned()];
    while let Some(path) = pending.pop() {
        *reads += 1;
        for entry in fs::read_dir(&path).context("listing protected dataset descendants")? {
            let entry = entry.context("reading protected dataset entry")?;
            *entries += 1;
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
                && entry.file_name() != crate::artifacts::DELTA_LOG_DIR
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
) -> Result<()> {
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
    }
    Ok(())
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

/// Before eligibility or initialization, reject overlapping protected roots using
/// markers only. A shared ancestor lock does not authorize reading its authority.
///
/// Creating a dataset checks its ancestors, itself and all its descendants; the
/// root is empty then, so that listing is small. Resuming checks only the
/// ancestors, O(depth) requests of control prefixes and no data listing: a
/// dataset nested in an existing one could only have been created after it,
/// and that creation's own ancestor check refuses it (#655).
pub(crate) async fn validate_ingestion_target(
    output: &StorageIdentity,
    ownership: &DatasetOwnership,
    target: IngestionTarget,
    stats: &ListingStats,
) -> Result<()> {
    let path = output_path(output);
    let targets = BTreeSet::from([MaintenanceTarget::directory(path.clone())]);
    let scope = match target {
        IngestionTarget::Create => MarkerScope::Tree,
        IngestionTarget::Resume => MarkerScope::Ancestors,
    };
    let markers = discover_markers(ownership, &targets, scope, stats).await?;
    ensure!(
        markers.iter().all(|root| root == &path),
        OVERLAPPING_INGESTION_ROOT
    );
    Ok(())
}
