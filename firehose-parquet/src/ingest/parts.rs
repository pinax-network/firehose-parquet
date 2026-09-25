//! Physical ownership checks and exact-path cleanup for a validated journal.
//! Only the controller decides whether rollback or roll-forward is legal.

use anyhow::{bail, Context, Result};
use object_store::path::Path as ObjectPath;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::state::{validate_relative_path, PendingTransaction, PlannedPart, TransactionPhase};
use crate::dataset_lock::LocalOwnership;
use crate::dataset_lock_s3::S3Ownership;
use crate::writer::protected::{self, EncodedPart, LocalPartStore, PartPresence, S3PartStore};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

pub enum TransactionParts<'a> {
    Local {
        root: PathBuf,
        owner: &'a LocalOwnership,
        store: LocalPartStore<'a>,
    },
    S3 {
        prefix: String,
        owner: &'a S3Ownership,
        store: S3PartStore<'a>,
    },
}

impl<'a> TransactionParts<'a> {
    pub fn local(root: &Path, owner: &'a LocalOwnership) -> Result<Self> {
        Ok(Self::Local {
            root: fs::canonicalize(root)?,
            owner,
            store: LocalPartStore::new(root, owner)?,
        })
    }
    pub fn s3(prefix: &str, owner: &'a S3Ownership, cache_control: &str) -> Result<Self> {
        validate_relative_path(prefix, true)?;
        Ok(Self::S3 {
            prefix: prefix.to_string(),
            owner,
            store: S3PartStore::new(owner, prefix)?.with_cache_control(cache_control),
        })
    }

    /// Called under exclusive ownership before persisting Writing. No intended
    /// name may predate this transaction. Thus a later partial private temp can
    /// be removed from its exact journal plan without guessing who created it.
    pub async fn require_unoccupied(&self, pending: &PendingTransaction) -> Result<()> {
        for part in &pending.parts {
            if self.final_exists(part).await? {
                bail!("planned final part already exists before Writing; refusing adoption or replacement");
            }
            if let Self::Local { root, owner, .. } = self {
                let temporary = local_path(root, owner, &part.temporary_relative_path)?;
                if exists_regular(&temporary)? {
                    bail!("planned temporary part already exists before Writing; refusing to claim it");
                }
            }
        }
        Ok(())
    }

    pub fn stage(&self, encoded: &EncodedPart) -> Result<()> {
        match self {
            Self::Local { store, .. } => store.stage(encoded),
            Self::S3 { .. } => Ok(()),
        }
    }
    pub async fn publish(&self, encoded: &EncodedPart) -> Result<()> {
        match self {
            Self::Local { store, .. } => store.publish(encoded.plan(), encoded.receipt()),
            Self::S3 { store, .. } => store.publish(encoded).await,
        }
    }

    pub async fn verify_all_finals(&self, pending: &PendingTransaction) -> Result<()> {
        for part in &pending.parts {
            if self.verify_final(pending, part).await? != PartPresence::Present {
                bail!("committed transaction is missing a required final part");
            }
        }
        Ok(())
    }

    pub async fn rollback_writing(&self, pending: &PendingTransaction) -> Result<()> {
        if pending.phase != TransactionPhase::Writing {
            bail!("cannot roll back parts after all-table commit");
        }
        // Verify every existing public file before deleting any file. A missing
        // frozen receipt is tolerable only while no public file exists.
        let mut present = Vec::new();
        for part in &pending.parts {
            if part.receipt.is_some() {
                if self.verify_final(pending, part).await? == PartPresence::Present {
                    present.push(part);
                }
            } else if self.final_exists(part).await? {
                bail!("Writing journal has a published part without a frozen receipt; refusing cleanup");
            }
        }
        for part in present {
            self.remove_final(part).await?;
        }
        self.cleanup_temporaries(pending)?;
        Ok(())
    }

    pub fn cleanup_temporaries(&self, pending: &PendingTransaction) -> Result<()> {
        if let Self::Local { root, owner, .. } = self {
            for part in &pending.parts {
                remove_local(root, owner, &part.temporary_relative_path)?;
            }
        }
        // S3 publishes one complete encoded object directly; it creates no
        // temporary object, so this protocol never deletes an S3 temp name.
        Ok(())
    }

    async fn verify_final(
        &self,
        pending: &PendingTransaction,
        part: &PlannedPart,
    ) -> Result<PartPresence> {
        let plan = writer_plan(pending, part);
        let receipt = writer_receipt(part)?;
        match self {
            Self::Local { store, .. } => store.verify(&plan, &receipt, false),
            Self::S3 { store, .. } => store.verify(&plan, &receipt).await,
        }
    }

    async fn final_exists(&self, part: &PlannedPart) -> Result<bool> {
        match self {
            Self::Local { root, owner, .. } => {
                exists_regular(&local_path(root, owner, &part.final_relative_path)?)
            }
            Self::S3 { prefix, owner, .. } => {
                let key = object_key(prefix, &part.final_relative_path)?;
                match tokio::time::timeout(REQUEST_TIMEOUT, owner.object_store().head(&key)).await {
                    Ok(Ok(_)) => Ok(true),
                    Ok(Err(object_store::Error::NotFound { .. })) => Ok(false),
                    _ => bail!("could not establish whether a planned remote part exists"),
                }
            }
        }
    }

    async fn remove_final(&self, part: &PlannedPart) -> Result<()> {
        match self {
            Self::Local { root, owner, .. } => remove_local(root, owner, &part.final_relative_path),
            Self::S3 { prefix, owner, .. } => {
                let _mutation = owner.lock_control_mutation().await;
                if owner.is_mutation_uncertain() {
                    bail!("remote rollback requires provider-quiescent ownership");
                }
                if S3Ownership::status(owner.object_store()).await?.as_ref() != Some(owner.record())
                {
                    bail!("remote ownership changed before journal rollback");
                }
                let key = object_key(prefix, &part.final_relative_path)?;
                let mut attempt = RemoteDeletion {
                    owner,
                    resolved: false,
                };
                match tokio::time::timeout(REQUEST_TIMEOUT,owner.object_store().delete(&key)).await {
                    Ok(Ok(()))|Ok(Err(object_store::Error::NotFound{..}))=>{attempt.resolved=true;Ok(())}
                    _=>bail!("remote rollback deletion was unresolved; retain ownership and establish provider quiescence before recovery"),
                }
            }
        }
    }
}

struct RemoteDeletion<'a> {
    owner: &'a S3Ownership,
    resolved: bool,
}
impl Drop for RemoteDeletion<'_> {
    fn drop(&mut self) {
        if !self.resolved {
            self.owner.mark_mutation_uncertain();
        }
    }
}

pub fn writer_plan(pending: &PendingTransaction, part: &PlannedPart) -> protected::PlannedPart {
    protected::PlannedPart {
        table: part.table.clone(),
        final_relative_path: part.final_relative_path.clone(),
        temporary_relative_path: part.temporary_relative_path.clone(),
        schema_sha256: part.schema_sha256.as_str().into(),
        stream_id: pending.stream_id.as_str().into(),
        transaction_id: pending.id.as_str().into(),
        first_ordinal: pending.prefix.first_ordinal,
        last_ordinal: pending.prefix.last_ordinal,
        entry_index: part.entry_index,
        row_count: part.row_count,
    }
}
fn writer_receipt(part: &PlannedPart) -> Result<protected::PartReceipt> {
    let receipt = part
        .receipt
        .as_ref()
        .context("journal lacks the frozen receipt required for physical verification")?;
    Ok(protected::PartReceipt {
        byte_size: receipt.byte_size,
        sha256: receipt.sha256.as_str().into(),
        row_count: part.row_count,
        schema_sha256: part.schema_sha256.as_str().into(),
    })
}

fn object_key(prefix: &str, relative: &str) -> Result<ObjectPath> {
    validate_relative_path(relative, false)?;
    Ok(ObjectPath::from(if prefix.is_empty() {
        relative.to_string()
    } else {
        format!("{prefix}/{relative}")
    }))
}

fn local_path(root: &Path, owner: &LocalOwnership, relative: &str) -> Result<PathBuf> {
    owner.revalidate()?;
    validate_relative_path(relative, false)?;
    let mut path = root.to_path_buf();
    for component in relative.split('/') {
        path.push(component);
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                bail!("nested symlink in an owned transaction path")
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => bail!("could not inspect an owned transaction path"),
        }
    }
    Ok(path)
}
fn exists_regular(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => bail!("an owned transaction file path is occupied by a non-regular entry"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => bail!("could not inspect an owned transaction file"),
    }
}
fn remove_local(root: &Path, owner: &LocalOwnership, relative: &str) -> Result<()> {
    let _mutation = owner.lock_control_mutation()?;
    let path = local_path(root, owner, relative)?;
    if exists_regular(&path)? {
        fs::remove_file(&path).context("removing an exact journal-owned file")?;
    }
    // Persist the absence before clearing the journal; include all directory
    // links in case a prior failed stage created them without completing sync.
    let parent = path.parent().context("journal-owned file has no parent")?;
    let existing = parent
        .ancestors()
        .find(|path| path.is_dir())
        .context("journal cleanup has no existing ancestor")?;
    // Re-sync even an already absent entry: an earlier attempt may have unlinked
    // it and failed before directory sync. Absence in this process is not proof
    // that it cannot return after a crash.
    for ancestor in existing.ancestors() {
        File::open(ancestor)
            .and_then(|file| file.sync_all())
            .context("syncing journal cleanup ancestry")?;
    }
    Ok(())
}
