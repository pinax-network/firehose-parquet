//! Conditional control slots under a persistent S3 ownership guard.
//!
//! Fixed keys are cleared with CAS tombstones. No unconditional DELETE may
//! race a later journal incarnation, including a delayed retry at the server.

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures::StreamExt;
use object_store::{path::Path, Attribute, PutMode, PutOptions, UpdateVersion};
use serde::{de::DeserializeOwned, Serialize};
use std::time::Duration;

use crate::dataset_lock_s3::{usable_version, S3Ownership};
use crate::durable_state::{
    decode_slot, encode, encode_tombstone, ControlKey, ControlVersion, CONTROL_DIRECTORY,
    MAX_CONTROL_BYTES,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The remote precondition is deliberately opaque and omitted from Debug.
#[derive(Clone, Eq, PartialEq)]
pub struct S3ControlVersion {
    pub record: ControlVersion,
    object: UpdateVersion,
}

impl std::fmt::Debug for S3ControlVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3ControlVersion")
            .field("record", &self.record)
            .finish_non_exhaustive()
    }
}

/// Intentionally has no Debug implementation because the payload may be private.
pub struct S3ControlDocument<T> {
    pub version: S3ControlVersion,
    pub payload: T,
}

struct Slot {
    bytes: Bytes,
    version: S3ControlVersion,
    payload: Option<serde_json::Value>,
}

pub struct S3StateStore<'a> {
    ownership: &'a S3Ownership,
    prefix: String,
}

impl<'a> S3StateStore<'a> {
    /// `dataset_prefix` is a normalized bucket-relative directory, never a URL.
    pub fn new(ownership: &'a S3Ownership, dataset_prefix: &str) -> Result<Self> {
        let prefix = dataset_prefix.trim_end_matches('/');
        if dataset_prefix.starts_with('/')
            || dataset_prefix.contains(['?', '#', '\\'])
            || dataset_prefix.contains("://")
            || (!prefix.is_empty()
                && prefix
                    .split('/')
                    .any(|component| component.is_empty() || component == "." || component == ".."))
            || crate::artifacts::is_control_path(prefix)
        {
            bail!("invalid bucket-relative control root");
        }
        if !ownership.record().scopes().iter().any(|scope| {
            scope.is_empty()
                || prefix == scope
                || prefix
                    .strip_prefix(scope)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        }) {
            bail!("control root is outside the declared ownership scopes");
        }
        Ok(Self {
            ownership,
            prefix: if prefix.is_empty() {
                CONTROL_DIRECTORY.into()
            } else {
                format!("{prefix}/{CONTROL_DIRECTORY}")
            },
        })
    }

    pub async fn load<T: DeserializeOwned>(
        &self,
        key: ControlKey,
    ) -> Result<Option<S3ControlDocument<T>>> {
        let Some(slot) = self.read_slot(key).await? else {
            return Ok(None);
        };
        let Some(payload) = slot.payload else {
            return Ok(None);
        };
        let payload = serde_json::from_value(payload)
            .map_err(|_| anyhow::anyhow!("invalid control record payload"))?;
        Ok(Some(S3ControlDocument {
            version: slot.version,
            payload,
        }))
    }

    pub async fn create<T: Serialize>(
        &self,
        key: ControlKey,
        payload: &T,
    ) -> Result<S3ControlVersion> {
        let _mutation = self.ownership.lock_control_mutation().await;
        self.require_resolved_owner()?;
        let mode = match self.read_slot(key).await? {
            None => PutMode::Create,
            Some(slot) if slot.payload.is_none() => PutMode::Update(slot.version.object),
            Some(_) => bail!("control record already exists"),
        };
        let (bytes, _) = encode(payload, &uuid::Uuid::new_v4().to_string(), 0)?;
        self.put_and_verify(key, Bytes::from(bytes), mode).await
    }

    pub async fn replace<T: Serialize>(
        &self,
        key: ControlKey,
        expected: &S3ControlVersion,
        payload: &T,
    ) -> Result<S3ControlVersion> {
        let _mutation = self.ownership.lock_control_mutation().await;
        self.require_resolved_owner()?;
        self.require_version(key, expected).await?;
        let revision = expected
            .record
            .revision
            .checked_add(1)
            .context("control revision exhausted")?;
        let (bytes, _) = encode(payload, &expected.record.incarnation, revision)?;
        self.put_and_verify(
            key,
            Bytes::from(bytes),
            PutMode::Update(expected.object.clone()),
        )
        .await
    }

    /// Logically clear a slot through CAS. The tombstone remains at the fixed key.
    pub async fn remove(&self, key: ControlKey, expected: &S3ControlVersion) -> Result<()> {
        let _mutation = self.ownership.lock_control_mutation().await;
        self.require_resolved_owner()?;
        self.require_version(key, expected).await?;
        let (bytes, _) = encode_tombstone(&expected.record)?;
        self.put_and_verify(
            key,
            Bytes::from(bytes),
            PutMode::Update(expected.object.clone()),
        )
        .await?;
        Ok(())
    }

    fn require_resolved_owner(&self) -> Result<()> {
        if self.ownership.is_mutation_uncertain() {
            bail!("unresolved remote mutation requires explicit quiescent recovery before further control writes");
        }
        Ok(())
    }

    fn key(&self, key: ControlKey) -> Path {
        Path::from(format!("{}/{}", self.prefix, key.filename()))
    }

    async fn require_version(&self, key: ControlKey, expected: &S3ControlVersion) -> Result<()> {
        let slot = self
            .read_slot(key)
            .await?
            .context("expected control record is missing")?;
        if slot.payload.is_none() || slot.version != *expected {
            bail!("control record changed; refusing stale replacement or removal");
        }
        Ok(())
    }

    async fn read_slot(&self, key: ControlKey) -> Result<Option<Slot>> {
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            let response = match self.ownership.object_store().get(&self.key(key)).await {
                Ok(response) => response,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(_) => bail!("control object read failed"),
            };
            if response.meta.size > MAX_CONTROL_BYTES as u64 {
                bail!("control object exceeds its byte limit");
            }
            let object = UpdateVersion {
                e_tag: response.meta.e_tag.clone(),
                version: response.meta.version.clone(),
            };
            if !usable_version(&object) {
                bail!("control object lacks a usable conditional version");
            }
            let mut bytes = Vec::new();
            let mut stream = response.into_stream();
            while let Some(chunk) = stream.next().await {
                let chunk =
                    chunk.map_err(|_| anyhow::anyhow!("control object body read failed"))?;
                if chunk.len() > MAX_CONTROL_BYTES.saturating_sub(bytes.len()) {
                    bail!("control object exceeds its byte limit");
                }
                bytes.extend_from_slice(&chunk);
            }
            let (record, payload) = decode_slot(&bytes)?;
            Ok(Some(Slot {
                bytes: Bytes::from(bytes),
                version: S3ControlVersion { record, object },
                payload,
            }))
        })
        .await
        .map_err(|_| anyhow::anyhow!("control object read timed out"))?
    }

    async fn put_and_verify(
        &self,
        key: ControlKey,
        bytes: Bytes,
        mode: PutMode,
    ) -> Result<S3ControlVersion> {
        // If this future is cancelled at any await, Drop marks the owner
        // uncertain before the caller can release it. A successful exact read
        // disarms the attempt; an error or partial response never does.
        let mut attempt = MutationAttempt {
            ownership: self.ownership,
            resolved: false,
        };
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
        let response = tokio::time::timeout(
            REQUEST_TIMEOUT,
            self.ownership
                .object_store()
                .put_opts(&self.key(key), bytes.clone().into(), options),
        )
        .await;
        let reported = match response {
            Ok(Ok(result)) => Some(result),
            _ => None,
        };
        let current = self
            .read_slot(key)
            .await?
            .context("control mutation could not be reconciled")?;
        if current.bytes != bytes
            || reported.as_ref().is_some_and(|result| {
                result
                    .e_tag
                    .as_ref()
                    .is_some_and(|etag| Some(etag) != current.version.object.e_tag.as_ref())
                    || result.version.as_ref().is_some_and(|version| {
                        Some(version) != current.version.object.version.as_ref()
                    })
            })
        {
            bail!("control mutation could not be reconciled to the exact expected record");
        }
        attempt.resolved = true;
        Ok(current.version)
    }
}

struct MutationAttempt<'a> {
    ownership: &'a S3Ownership,
    resolved: bool,
}

impl Drop for MutationAttempt<'_> {
    fn drop(&mut self) {
        if !self.resolved {
            self.ownership.mark_mutation_uncertain();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    use object_store::ObjectStore;
    use std::sync::Arc;

    #[tokio::test]
    async fn conditional_slots_recreate_without_deleting_or_reusing_versions() {
        let memory: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let owner = S3Ownership::acquire(memory.clone(), "test", vec!["data".into()])
            .await
            .unwrap();
        let store = S3StateStore::new(&owner, "data").unwrap();
        let old = store.create(ControlKey::Pending, &1).await.unwrap();
        store.remove(ControlKey::Pending, &old).await.unwrap();
        assert!(store
            .load::<u64>(ControlKey::Pending)
            .await
            .unwrap()
            .is_none());
        assert!(memory.head(&store.key(ControlKey::Pending)).await.is_ok());
        let recreated = store.create(ControlKey::Pending, &1).await.unwrap();
        assert_ne!(old.record.incarnation, recreated.record.incarnation);
        assert!(store.replace(ControlKey::Pending, &old, &2).await.is_err());
        assert!(store.remove(ControlKey::Pending, &old).await.is_err());
        assert_eq!(
            store
                .load::<u64>(ControlKey::Pending)
                .await
                .unwrap()
                .unwrap()
                .payload,
            1
        );
        assert!(!owner.is_mutation_uncertain());
        owner.release().await.unwrap();
    }

    #[tokio::test]
    async fn shared_owner_serializes_two_same_version_replacements() {
        let memory: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let owner = S3Ownership::acquire(memory, "test", vec!["data".into()])
            .await
            .unwrap();
        let one = S3StateStore::new(&owner, "data").unwrap();
        let two = S3StateStore::new(&owner, "data").unwrap();
        let old = one.create(ControlKey::State, &0).await.unwrap();
        let (first, second) = tokio::join!(
            one.replace(ControlKey::State, &old, &1),
            two.replace(ControlKey::State, &old, &2)
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert_eq!(
            one.load::<u64>(ControlKey::State)
                .await
                .unwrap()
                .unwrap()
                .version
                .record
                .revision,
            1
        );
        owner.release().await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_control_put_retains_owned_state() {
        let throttled = Arc::new(ThrottledStore::new(
            InMemory::new(),
            ThrottleConfig::default(),
        ));
        let object_store: Arc<dyn ObjectStore> = throttled.clone();
        let owner = S3Ownership::acquire(object_store.clone(), "test", vec!["data".into()])
            .await
            .unwrap();
        let store = S3StateStore::new(&owner, "data").unwrap();
        throttled.config_mut(|config| config.wait_put_per_call = Duration::from_secs(60));
        assert!(tokio::time::timeout(
            Duration::from_millis(20),
            store.create(ControlKey::State, &1)
        )
        .await
        .is_err());
        assert!(owner.is_mutation_uncertain());
        throttled.config_mut(|config| config.wait_put_per_call = Duration::ZERO);
        assert!(store.create(ControlKey::State, &2).await.is_err());
        assert!(owner.release().await.is_err());
        assert_eq!(
            S3Ownership::status(&object_store)
                .await
                .unwrap()
                .unwrap()
                .state(),
            crate::dataset_lock_s3::OwnerState::Owned
        );
    }

    #[tokio::test]
    async fn unsafe_roots_and_corrupt_private_records_fail_closed() {
        let memory: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let owner = S3Ownership::acquire(memory.clone(), "test", vec!["data".into()])
            .await
            .unwrap();
        for root in [
            "other",
            "data/../other",
            "/data",
            "data//table",
            "data/.fireparq-ingest",
            "s3://data",
        ] {
            assert!(S3StateStore::new(&owner, root).is_err());
        }
        let store = S3StateStore::new(&owner, "data").unwrap();
        memory
            .put(
                &store.key(ControlKey::State),
                Bytes::from_static(b"{private-cursor").into(),
            )
            .await
            .unwrap();
        let error = store.load::<u64>(ControlKey::State).await.err().unwrap();
        assert!(!format!("{error:#}").contains("private-cursor"));
        owner.release().await.unwrap();
    }
}
