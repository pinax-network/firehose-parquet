//! Typed two-slot state access. Physical part verification and rollback belong
//! to the controller; this layer enforces legal, versioned record transitions.

use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use std::path::Path;

use super::state::{AuthorityState, PartReceipt, PendingTransaction, TransactionPhase};
use crate::dataset_lock::LocalOwnership;
use crate::dataset_lock_s3::S3Ownership;
use crate::durable_state::{ControlKey, ControlVersion, LocalStateStore};
use crate::durable_state_s3::{S3ControlVersion, S3StateStore};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Version {
    Local(ControlVersion),
    S3(S3ControlVersion),
}

// Payload-bearing values deliberately have no Debug implementation.
pub struct Versioned<T> {
    version: Version,
    pub payload: T,
}

pub struct JournalSnapshot {
    pub authority: Option<Versioned<AuthorityState>>,
    pub pending: Option<Versioned<PendingTransaction>>,
}

pub enum TransactionStateStore<'a> {
    Local(LocalStateStore<'a>),
    S3(S3StateStore<'a>),
}

impl<'a> TransactionStateStore<'a> {
    pub fn local(root: &Path, owner: &'a LocalOwnership) -> Result<Self> {
        Ok(Self::Local(LocalStateStore::new(root, owner)?))
    }
    pub fn s3(prefix: &str, owner: &'a S3Ownership) -> Result<Self> {
        Ok(Self::S3(S3StateStore::new(owner, prefix)?))
    }

    pub async fn load(&self) -> Result<JournalSnapshot> {
        let authority = self.read::<AuthorityState>(ControlKey::State).await?;
        let pending = self.read::<PendingTransaction>(ControlKey::Pending).await?;
        if let Self::Local(store) = self {
            // Do not infer durable absence merely from a previous failed unlink
            // being visible in the current process. Re-establish both slots
            // before their recovery relationship authorizes any new mutation.
            fn local_version(version: &Version) -> &ControlVersion {
                match version {
                    Version::Local(version) => version,
                    Version::S3(_) => unreachable!("local store returns local version"),
                }
            }
            store.stabilize(
                ControlKey::State,
                authority
                    .as_ref()
                    .map(|document| local_version(&document.version)),
            )?;
            store.stabilize(
                ControlKey::Pending,
                pending
                    .as_ref()
                    .map(|document| local_version(&document.version)),
            )?;
        }
        if let Some(authority) = &authority {
            authority.payload.validate()?;
            if let Some(pending) = &pending {
                validate_pair(&authority.payload, &pending.payload)?;
            }
        } else if pending.is_some() {
            bail!("ingestion journal exists without an authoritative stream descriptor");
        }
        Ok(JournalSnapshot { authority, pending })
    }

    /// Caller must first prove the destination is eligible for initialization;
    /// absence of these slots alone is never permission to adopt legacy data.
    pub async fn initialize(&self, authority: AuthorityState) -> Result<Versioned<AuthorityState>> {
        authority.validate()?;
        if authority.checkpoint.ordinal != 0
            || authority.checkpoint.previous.is_some()
            || authority.checkpoint.completed_stop.is_some()
        {
            bail!("initial authority must be an empty, uncompleted checkpoint");
        }
        let observed = self.load().await?;
        if observed.authority.is_some() || observed.pending.is_some() {
            bail!("ingestion state already exists; initialization cannot replace it");
        }
        let version = self.create(ControlKey::State, &authority).await?;
        Ok(Versioned {
            version,
            payload: authority,
        })
    }

    pub async fn begin(
        &self,
        authority: &Versioned<AuthorityState>,
        pending: PendingTransaction,
    ) -> Result<Versioned<PendingTransaction>> {
        self.require(ControlKey::State, &authority.version).await?;
        authority.payload.validate_predecessor(&pending)?;
        if pending.phase != TransactionPhase::Writing
            || pending.parts.iter().any(|p| p.receipt.is_some())
        {
            bail!("new pending transaction must start Writing without receipts");
        }
        let version = self.create(ControlKey::Pending, &pending).await?;
        Ok(Versioned {
            version,
            payload: pending,
        })
    }

    pub async fn record_receipt(
        &self,
        authority: &Versioned<AuthorityState>,
        pending: &Versioned<PendingTransaction>,
        entry_index: u32,
        receipt: PartReceipt,
    ) -> Result<Versioned<PendingTransaction>> {
        self.require(ControlKey::State, &authority.version).await?;
        authority.payload.validate_predecessor(&pending.payload)?;
        let next =
            pending
                .payload
                .with_receipt(entry_index, receipt, &authority.payload.descriptor)?;
        let version = self
            .replace(ControlKey::Pending, &pending.version, &next)
            .await?;
        Ok(Versioned {
            version,
            payload: next,
        })
    }

    /// The controller must have verified every final part before calling this.
    pub async fn mark_committed(
        &self,
        authority: &Versioned<AuthorityState>,
        pending: &Versioned<PendingTransaction>,
    ) -> Result<Versioned<PendingTransaction>> {
        self.require(ControlKey::State, &authority.version).await?;
        authority.payload.validate_predecessor(&pending.payload)?;
        if pending.payload.phase != TransactionPhase::Writing {
            bail!("pending transaction was already marked committed");
        }
        let next = pending
            .payload
            .committed_after_verification(&authority.payload.descriptor)?;
        let version = self
            .replace(ControlKey::Pending, &pending.version, &next)
            .await?;
        Ok(Versioned {
            version,
            payload: next,
        })
    }

    pub async fn advance(
        &self,
        authority: &Versioned<AuthorityState>,
        pending: &Versioned<PendingTransaction>,
    ) -> Result<Versioned<AuthorityState>> {
        self.require(ControlKey::Pending, &pending.version).await?;
        let next = authority.payload.install(&pending.payload)?;
        let version = self
            .replace(ControlKey::State, &authority.version, &next)
            .await?;
        Ok(Versioned {
            version,
            payload: next,
        })
    }

    /// Only the uniquely permitted controller calls this after proving clean
    /// stream completion and an exact fully acknowledged boundary. There are
    /// no data mutations in this transition, so the one authority CAS is enough.
    pub async fn complete_request(
        &self,
        authority: &Versioned<AuthorityState>,
        stop: u64,
    ) -> Result<Versioned<AuthorityState>> {
        let observed = self.load().await?;
        if observed.pending.is_some() {
            bail!("cannot complete a request while an ingestion journal remains");
        }
        if observed.authority.as_ref().map(|state| &state.version) != Some(&authority.version) {
            bail!("authority changed before request completion");
        }
        let checkpoint = authority
            .payload
            .checkpoint
            .complete_request(&authority.payload.descriptor, stop)?;
        let next = AuthorityState {
            descriptor: authority.payload.descriptor.clone(),
            checkpoint,
        };
        let version = self
            .replace(ControlKey::State, &authority.version, &next)
            .await?;
        Ok(Versioned {
            version,
            payload: next,
        })
    }

    /// Only after verified rollback (Writing) or verified roll-forward + mirror
    /// reconciliation (Committed). Version checks cannot prove physical cleanup.
    pub async fn clear(
        &self,
        authority: &Versioned<AuthorityState>,
        pending: &Versioned<PendingTransaction>,
    ) -> Result<()> {
        self.require(ControlKey::State, &authority.version).await?;
        validate_pair(&authority.payload, &pending.payload)?;
        match pending.payload.phase {
            TransactionPhase::Writing => {
                authority.payload.validate_predecessor(&pending.payload)?
            }
            TransactionPhase::Committed
                if authority.payload.checkpoint != pending.payload.target =>
            {
                bail!("cannot clear a committed journal before authority roll-forward");
            }
            TransactionPhase::Committed => {}
        }
        match (self, &pending.version) {
            (Self::Local(store), Version::Local(version)) => {
                store.remove(ControlKey::Pending, version)
            }
            (Self::S3(store), Version::S3(version)) => {
                store.remove(ControlKey::Pending, version).await
            }
            _ => bail!("control version belongs to a different backend"),
        }
    }

    async fn read<T: DeserializeOwned>(&self, key: ControlKey) -> Result<Option<Versioned<T>>> {
        Ok(match self {
            Self::Local(store) => store.load(key)?.map(|d| Versioned {
                version: Version::Local(d.version),
                payload: d.payload,
            }),
            Self::S3(store) => store.load(key).await?.map(|d| Versioned {
                version: Version::S3(d.version),
                payload: d.payload,
            }),
        })
    }
    async fn require(&self, key: ControlKey, expected: &Version) -> Result<()> {
        let current = self
            .read::<serde_json::Value>(key)
            .await?
            .context("required ingestion control record is missing")?;
        if current.version != *expected {
            bail!("ingestion control record changed; refusing a stale transaction transition");
        }
        Ok(())
    }
    async fn create<T: Serialize>(&self, key: ControlKey, payload: &T) -> Result<Version> {
        Ok(match self {
            Self::Local(store) => Version::Local(store.create(key, payload)?),
            Self::S3(store) => Version::S3(store.create(key, payload).await?),
        })
    }
    async fn replace<T: Serialize>(
        &self,
        key: ControlKey,
        expected: &Version,
        payload: &T,
    ) -> Result<Version> {
        match (self, expected) {
            (Self::Local(store), Version::Local(version)) => {
                Ok(Version::Local(store.replace(key, version, payload)?))
            }
            (Self::S3(store), Version::S3(version)) => {
                Ok(Version::S3(store.replace(key, version, payload).await?))
            }
            _ => bail!("control version belongs to a different backend"),
        }
    }
}

fn validate_pair(authority: &AuthorityState, pending: &PendingTransaction) -> Result<()> {
    pending.validate(&authority.descriptor)?;
    if authority.checkpoint.id == pending.predecessor {
        authority.validate_predecessor(pending)?;
    } else if pending.phase != TransactionPhase::Committed || authority.checkpoint != pending.target
    {
        bail!("pending journal and authority do not form a recoverable transition");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
