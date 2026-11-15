//! `fireparq recovery rebind` (#635): bind a dataset that was copied or moved
//! to a new root to that root, so that it resumes there.
//!
//! A dataset is bound to the root it was created in. The root is part of its
//! stream descriptor (`output`, and `mirror` when the cursor mirror is inside
//! it), and the descriptor's SHA-256, the stream id, identifies the stream in
//! the head checkpoint of `state.json`, in the cursor mirror, in each Delta
//! table's `fireparq.descriptor` and in the `txn` application id of its
//! commits. A copy is therefore readable but refused by `build`, `recovery
//! recover`, `backfill seal` and `fireparq maintenance`'s recovery. A rebind
//! changes exactly those, at the copy only, in this order:
//!
//! 1. `state.json`, by one conditional replace (the commit point): the
//!    descriptor bound to the new root, and the head checkpoint identified
//!    under the new stream ([`AuthorityState::rebound`]);
//! 2. each Delta table, `blocks` last: one commit setting
//!    `fireparq.descriptor`, with `txn {fireparq:<new stream id>, authority's
//!    ordinal}` ([`DeltaRebind::commit`]);
//! 3. the cursor mirror, rewritten (conditionally) when it is the copied one
//!    ([`ProtectedMirror::reconcile_rebound`]).
//!
//! Before its first write it checks that the copy is complete and that both
//! roots are quiet: the root it was copied from is not owned (it is locked,
//! on local disk) and holds the same `state.json` (version and digest), no
//! pending journal and every table at the same version as the copy; the copy
//! has no pending journal, every Delta log is exactly at authority
//! ([`DeltaRebind::check`]), and its mirror is absent or the copied one.
//! Its ownership refuses a copy whose owner record was copied while held.
//!
//! A rerun after an interruption finds `state.json` rebound and completes
//! the tables and the mirror; between steps 1 and 2, `build` refuses the
//! copy ("belongs to another stream"). The root the copy was made from is
//! never written. Parts keep their names, which hold the previous stream id:
//! only a pending journal's parts are ever read by name, and a rebind
//! requires none. A sealed backfill's `backfill.json` and lane records are
//! not changed; `backfill status` reads them as before.
//!
//! This version relocates within one storage service: two local roots, or
//! two S3 roots on the same endpoint and region.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, bail, ensure, Context, Result};
use object_store::ObjectStore;
use serde::Serialize;
use serde_json::{json, Value};

use super::binding::{
    lexical_absolute, mirror_service, output_path, resolve_mirror_binding, resolve_output_identity,
};
use super::maintenance::{validate_ingestion_target, IngestionTarget};
use super::mirror::{MirrorOutcome, ProtectedMirror};
use super::state::{MirrorBinding, StorageIdentity, StreamDescriptor};
use crate::cli::AwsConfig;
use crate::dataset_lock::{DatasetOwnership, MutationScope};
use crate::dataset_lock_s3::{OwnerState, S3Ownership};
use crate::delta::commit::{latest_versions, DeltaRebind};
use crate::delta::store::DeltaStore;
use crate::delta::DeltaIdentity;
use crate::durable_state::{ControlKey, ControlVersion, CONTROL_DIRECTORY, MAX_CONTROL_BYTES};
use crate::maintenance::discovery::ListingStats;
use crate::recovery::{control_directory, remote_control_slot};

/// The `commitInfo` key of a rebind's Delta commits.
pub const AUDIT_KEY: &str = "fireparq.rebind";

/// What `fireparq recovery rebind` did, printed as JSON. It names no path:
/// the operator gave both.
#[derive(Debug, Serialize)]
pub struct Rebound {
    /// Whether this run bound `state.json` to the new root; `false` when an
    /// earlier run had, and this one completed or confirmed the rest.
    pub rebound_now: bool,
    /// The stream id (the descriptor's SHA-256) the dataset was bound to.
    pub previous_stream_id: String,
    /// Its stream id at the new root.
    pub stream_id: String,
    /// Authority's accepted ordinal, which a rebind does not change.
    pub ordinal: u64,
    /// The Delta tables this run committed the new descriptor to.
    pub tables_updated: Vec<String>,
    /// The cursor mirror: `disabled`, `unchanged` or `repaired`.
    pub mirror: &'static str,
}

/// A dataset root, as given and as resolved.
#[derive(Clone)]
pub(crate) struct Root {
    path: String,
    identity: StorageIdentity,
}

impl Root {
    pub(crate) fn resolve(path: &str, aws: &AwsConfig) -> Result<Self> {
        Ok(Self {
            path: path.to_owned(),
            identity: resolve_output_identity(path, aws)?,
        })
    }

    fn name(&self) -> String {
        output_path(&self.identity)
    }
}

/// The root a dataset was copied from, which a rebind only reads.
pub(crate) struct Source {
    pub(crate) root: Root,
    pub(crate) reader: SourceReader,
}

pub(crate) enum SourceReader {
    /// The files under the local root.
    Local,
    /// The root's bucket, read with GETs only, and its Delta tables.
    S3 {
        control: Arc<dyn ObjectStore>,
        delta: DeltaStore,
    },
}

/// `fireparq recovery rebind <path> --from <from>`.
pub async fn rebind(path: &str, from: &str, aws: &AwsConfig) -> Result<Rebound> {
    let target = Root::resolve(path, aws)?;
    let source = Root::resolve(from, aws)?;
    check_roots(&target, &source)?;
    let mut scopes = vec![MutationScope::directory(path.to_owned())];
    let reader = match &source.identity {
        StorageIdentity::Local { canonical_root } => {
            let StorageIdentity::Local {
                canonical_root: copy,
            } = &target.identity
            else {
                unreachable!("check_roots pairs local roots")
            };
            // Ownership would create a missing root.
            ensure!(
                Path::new(copy).is_dir(),
                "{} does not exist: copy the dataset at {} there first",
                target.name(),
                source.name()
            );
            // Its lock keeps a writer from starting at the source, and
            // creates nothing there; a source that is gone is not locked
            // (only a rerun after the rebind can continue without it).
            if Path::new(canonical_root).is_dir() {
                scopes.push(MutationScope::directory(from.to_owned()));
            }
            SourceReader::Local
        }
        StorageIdentity::S3 { bucket, prefix, .. } => SourceReader::S3 {
            control: Arc::new(aws.build_read_client(bucket)?),
            delta: DeltaStore::s3(
                bucket,
                prefix,
                crate::delta::store::s3_log_client(aws, bucket)?,
            )?,
        },
    };
    let ownership = DatasetOwnership::acquire("rebind", scopes, Some(aws)).await?;
    let source = Source {
        root: source,
        reader,
    };
    let result = rebind_owned(&target, &source, &ownership, aws).await;
    ownership.finish(result).await
}

/// Both roots on one kind of storage (and one S3 service), neither inside the
/// other.
fn check_roots(target: &Root, source: &Root) -> Result<()> {
    let (to, from) = (target.name(), source.name());
    let overlap = || format!("{to} and {from} overlap: a dataset is rebound to a separate root");
    match (&target.identity, &source.identity) {
        (
            StorageIdentity::Local {
                canonical_root: copy,
            },
            StorageIdentity::Local {
                canonical_root: original,
            },
        ) => {
            let (copy, original) = (Path::new(copy), Path::new(original));
            ensure!(
                !copy.starts_with(original) && !original.starts_with(copy),
                overlap()
            );
        }
        (
            StorageIdentity::S3 {
                service,
                bucket,
                prefix,
            },
            StorageIdentity::S3 {
                service: original_service,
                bucket: original_bucket,
                prefix: original_prefix,
            },
        ) => {
            ensure!(
                service == original_service,
                "{to} and {from} resolve to different S3 services: a rebind stays on one \
                 endpoint and region"
            );
            ensure!(
                bucket != original_bucket || !nested(prefix, original_prefix),
                overlap()
            );
        }
        _ => bail!(
            "{to} and {from} are on different kinds of storage: a dataset is rebound between \
             two local roots or two S3 roots"
        ),
    }
    Ok(())
}

/// Whether one of two prefixes of a bucket is the other or inside it.
fn nested(a: &str, b: &str) -> bool {
    a.is_empty()
        || b.is_empty()
        || a == b
        || a.starts_with(&format!("{b}/"))
        || b.starts_with(&format!("{a}/"))
}

/// The rebind of the dataset at `target`, copied from `source`, under the
/// ownership of `target` (and, on local disk, of `source`).
pub(crate) async fn rebind_owned(
    target: &Root,
    source: &Source,
    ownership: &DatasetOwnership,
    aws: &AwsConfig,
) -> Result<Rebound> {
    let (to, from) = (target.name(), source.root.name());
    validate_ingestion_target(
        &target.identity,
        ownership,
        IngestionTarget::Resume,
        &ListingStats::default(),
    )
    .await?;
    let states = super::maintenance::state_store(&target.identity, ownership)?;
    let snapshot = states.load().await?;
    let authority = snapshot.authority.with_context(|| {
        format!(
            "{to} holds no ingestion state: copy the dataset at {from} there first (a parallel \
             backfill is sealed, at {from}, before it is copied)"
        )
    })?;
    ensure!(
        snapshot.pending.is_none(),
        "{to} holds a pending ingestion transaction, copied from {from}: run `fireparq recovery \
         recover {from}`, then copy the dataset again"
    );
    let current = &authority.payload;
    let ordinal = current.checkpoint.ordinal;
    let rebound_now = current.descriptor.output != target.identity;
    let (previous, next) = if rebound_now {
        ensure!(
            current.descriptor.output == source.root.identity,
            "{to} holds a dataset bound to {}, not to {from}: pass the root it was copied from \
             as --from",
            output_path(&current.descriptor.output)
        );
        let next = relocate(&current.descriptor, &source.root, target, aws)?;
        (current.descriptor.clone(), next)
    } else {
        // An earlier run replaced `state.json`: the previous descriptor is
        // the one it was bound to at --from.
        let previous = relocate(&current.descriptor, target, &source.root, aws)?;
        ensure!(
            relocate(&previous, &source.root, target, aws)? == current.descriptor,
            "{to} is bound to it, but its cursor mirror is not where a rebind from {from} puts it"
        );
        (previous, current.descriptor.clone())
    };
    ensure!(
        ordinal > 0,
        "the dataset at {from} has accepted nothing, so there is nothing to relocate: build \
         into an empty {to} instead"
    );
    let (previous_identity, next_identity) =
        (DeltaIdentity::of(&previous)?, DeltaIdentity::of(&next)?);
    let mut tables = DeltaRebind::open(
        delta_store(&target.identity, ownership, aws)?,
        previous_identity.clone(),
        next_identity.clone(),
        next.tables.keys().map(String::as_str),
    )
    .await?;
    let mirror = ProtectedMirror::new(
        ownership,
        &next.mirror,
        mirror_service(&next.mirror, aws)?.as_ref(),
    )?;
    let not_rebound = tables.pending();
    let rebound = if rebound_now {
        ensure!(
            not_rebound.len() == next.tables.len(),
            "Delta tables at {to} already hold the descriptor of {to} while its `state.json` \
             does not: this is not a copy of {from} that a rebind can complete. Nothing was \
             changed"
        );
        tables.check(ordinal).await?;
        require_copy(source, target, ownership, &tables.versions()?).await?;
        let next_authority = current.rebound(next.clone())?;
        mirror.check_rebound(&next_authority, &previous).await?;
        states.rebind(&authority, next_authority).await?.payload
    } else {
        if !not_rebound.is_empty() {
            tables.check(ordinal).await?;
        }
        authority.payload
    };
    let audit = HashMap::from([(
        AUDIT_KEY.to_string(),
        json!({
            "previousDescriptor": previous_identity.descriptor,
            "ordinal": ordinal,
        }),
    )]);
    let committed = tables.commit(ordinal, &audit).await.with_context(|| {
        format!(
            "{to} is bound to its new root, but not every Delta table is yet; rerun `fireparq \
             recovery rebind` to complete it"
        )
    })?;
    let outcome = mirror
        .reconcile_rebound(&rebound, &previous)
        .await
        .with_context(|| {
            format!(
                "{to} is bound to its new root, but its cursor mirror is not yet; rerun \
                 `fireparq recovery rebind` to complete it"
            )
        })?;
    Ok(Rebound {
        rebound_now,
        previous_stream_id: previous_identity.descriptor,
        stream_id: next_identity.descriptor,
        ordinal: rebound.checkpoint.ordinal,
        tables_updated: committed.into_iter().map(|commit| commit.table).collect(),
        mirror: match outcome {
            MirrorOutcome::Disabled => "disabled",
            MirrorOutcome::Unchanged => "unchanged",
            MirrorOutcome::Repaired => "repaired",
        },
    })
}

/// `descriptor`, bound to `from`, bound to `to` instead: its output, and its
/// cursor mirror at the same path inside `to`, as `build --cursor <that
/// path>` at `to` resolves it. A mirror outside `from` is refused.
fn relocate(
    descriptor: &StreamDescriptor,
    from: &Root,
    to: &Root,
    aws: &AwsConfig,
) -> Result<StreamDescriptor> {
    ensure!(
        descriptor.output == from.identity,
        "the dataset is not bound to {}",
        from.name()
    );
    let outside = || {
        anyhow!(
            "the cursor mirror of the dataset at {} is outside that root: a rebind relocates a \
             dataset whose mirror is inside its root, or that has none",
            from.name()
        )
    };
    let relative = match (&descriptor.mirror, &from.identity) {
        (MirrorBinding::Disabled, _) => None,
        (
            MirrorBinding::S3 {
                service,
                bucket,
                key,
            },
            StorageIdentity::S3 {
                service: root_service,
                bucket: root_bucket,
                prefix,
            },
        ) => {
            if service != root_service || bucket != root_bucket {
                return Err(outside());
            }
            let relative = if prefix.is_empty() {
                key.as_str()
            } else {
                key.strip_prefix(&format!("{prefix}/"))
                    .ok_or_else(outside)?
            };
            Some(relative.to_owned())
        }
        (MirrorBinding::Local { absolute_path }, StorageIdentity::Local { canonical_root }) => {
            let path = Path::new(absolute_path);
            let roots = [
                lexical_absolute(Path::new(&from.path))?,
                PathBuf::from(canonical_root),
            ];
            let relative = roots
                .iter()
                .find_map(|root| path.strip_prefix(root).ok())
                .ok_or_else(outside)?;
            Some(
                relative
                    .to_str()
                    .context("cursor path must be UTF-8")?
                    .to_owned(),
            )
        }
        _ => return Err(outside()),
    };
    let mirror = match relative {
        None => MirrorBinding::Disabled,
        Some(relative) if relative.is_empty() => return Err(outside()),
        Some(relative) => resolve_mirror_binding(&to.path, Some(&relative), aws)?,
    };
    Ok(StreamDescriptor {
        output: to.identity.clone(),
        mirror,
        ..descriptor.clone()
    })
}

/// The Delta tables of the owned dataset at `identity`.
fn delta_store(
    identity: &StorageIdentity,
    ownership: &DatasetOwnership,
    aws: &AwsConfig,
) -> Result<DeltaStore> {
    match identity {
        StorageIdentity::Local { canonical_root } => DeltaStore::local(Path::new(canonical_root)),
        StorageIdentity::S3 { bucket, prefix, .. } => {
            let owner = ownership
                .remote(bucket)
                .context("dataset bucket is not owned")?;
            let client = match owner.delta_log() {
                Some(client) => Arc::clone(client),
                None => crate::delta::store::s3_log_client(aws, bucket)?,
            };
            DeltaStore::s3(bucket, prefix, client)
        }
    }
}

/// A control record as stored: its version, and its payload (`None` for a
/// tombstone), or `None` when it is absent.
type Slot = Option<(ControlVersion, Option<Value>)>;

/// The version of a live (not deleted) control record.
fn live(slot: &Slot) -> Option<&ControlVersion> {
    slot.as_ref()
        .filter(|(_, payload)| payload.is_some())
        .map(|(version, _)| version)
}

/// The copy at `target` is the dataset at `source` as it is now: the source
/// is not owned, holds the same `state.json` (version and digest, so the
/// same payload) and no pending journal, and each Delta table is at the
/// version `copied` holds. A source that changed after the copy (its writer
/// or its maintenance ran) may have been copied torn.
async fn require_copy(
    source: &Source,
    target: &Root,
    ownership: &DatasetOwnership,
    copied: &BTreeMap<String, u64>,
) -> Result<()> {
    let (to, from) = (target.name(), source.root.name());
    let names = copied.keys().map(String::as_str);
    let (state, pending, versions) = match (&source.reader, &source.root.identity) {
        (SourceReader::Local, StorageIdentity::Local { canonical_root }) => {
            let root = Path::new(canonical_root);
            let versions = latest_versions(&DeltaStore::local(root)?, names).await?;
            (
                local_slot(root, ControlKey::State)?,
                local_slot(root, ControlKey::Pending)?,
                versions,
            )
        }
        (SourceReader::S3 { control, delta }, StorageIdentity::S3 { bucket, prefix, .. }) => {
            // The copy's bucket owner, which this rebind holds, is the
            // source's when both are in one bucket.
            let shared = matches!(&target.identity, StorageIdentity::S3 { bucket: copy, .. } if copy == bucket);
            if !shared {
                if let Some(owner) = S3Ownership::status(control).await? {
                    ensure!(
                        owner.state() != OwnerState::Owned,
                        "{from} is owned (operation `{}`): stop its writer and its maintenance, \
                         wait until its owner is released, copy it again, then rerun the rebind",
                        owner.operation()
                    );
                }
            }
            let directory = control_directory(prefix);
            (
                remote_control_slot(control, &directory, ControlKey::State).await?,
                remote_control_slot(control, &directory, ControlKey::Pending).await?,
                latest_versions(delta, names).await?,
            )
        }
        _ => bail!("the reader of {from} is not for its kind of storage"),
    };
    let copy = target_slot(target, ownership, ControlKey::State).await?;
    ensure!(
        live(&state).is_some(),
        "{from} holds no ingestion state: pass the root {to} was copied from as --from"
    );
    let changed = || {
        format!(
            "{from} has changed since it was copied to {to} (its writer or its maintenance ran): \
             stop both, copy it again, then rerun the rebind. Nothing was changed"
        )
    };
    ensure!(live(&state) == live(&copy), changed());
    ensure!(
        live(&pending).is_none(),
        "{from} holds a pending ingestion transaction: run `fireparq recovery recover {from}`, \
         then copy the dataset again. Nothing was changed"
    );
    ensure!(versions == *copied, changed());
    Ok(())
}

/// The control record `key` of the owned dataset at `target`, as stored.
async fn target_slot(target: &Root, ownership: &DatasetOwnership, key: ControlKey) -> Result<Slot> {
    match &target.identity {
        StorageIdentity::Local { canonical_root } => local_slot(Path::new(canonical_root), key),
        StorageIdentity::S3 { bucket, prefix, .. } => {
            let owner = ownership
                .remote(bucket)
                .context("dataset bucket is not owned")?;
            remote_control_slot(owner.object_store(), &control_directory(prefix), key).await
        }
    }
}

/// The control record `key` of the local dataset at `root`, read without
/// ownership.
fn local_slot(root: &Path, key: ControlKey) -> Result<Slot> {
    let path = root.join(CONTROL_DIRECTORY).join(key.filename());
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("reading control record metadata"),
    };
    ensure!(
        metadata.file_type().is_file() && metadata.len() <= MAX_CONTROL_BYTES as u64,
        "control record {} is not a regular file within the size limit",
        key.filename()
    );
    let bytes = std::fs::read(&path).context("reading control record")?;
    Ok(Some(crate::durable_state::decode_slot(&bytes)?))
}

#[cfg(test)]
mod tests;
