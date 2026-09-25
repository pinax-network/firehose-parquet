//! Strict, bounded control records for the ingestion transaction protocol.
//!
//! This module alone does not make ingestion transactional. Callers must retain
//! exclusive ownership and implement the complete publication/recovery protocol.

use anyhow::{bail, Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::dataset_lock::LocalOwnership;

pub const CONTROL_DIRECTORY: &str = ".fireparq-ingest";
pub const CONTROL_FORMAT_VERSION: u32 = 1;
pub const MAX_CONTROL_BYTES: usize = 4 * 1024 * 1024;

/// Only known control keys can be read or mutated; journal contents cannot
/// supply a path escaping the control directory.
#[derive(Clone, Copy, Debug)]
pub enum ControlKey {
    State,
    Pending,
}

impl ControlKey {
    pub fn filename(self) -> &'static str {
        match self {
            Self::State => "state.json",
            Self::Pending => "pending.json",
        }
    }
}

/// Safe to print: this contains no cursor or configuration payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlVersion {
    /// New on create, preserved on replace; prevents remove/recreate ABA.
    pub incarnation: String,
    pub revision: u64,
    pub digest: String,
}

/// Intentionally has no Debug implementation: a payload may contain a cursor.
pub struct ControlDocument<T> {
    pub version: ControlVersion,
    pub payload: T,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    format_version: u32,
    incarnation: String,
    revision: u64,
    deleted: bool,
    payload: serde_json::Value,
    sha256: String,
}

fn payload_digest(
    version: u32,
    incarnation: &str,
    revision: u64,
    deleted: bool,
    payload: &serde_json::Value,
) -> Result<String> {
    // serde_json::Value's default map representation sorts object keys. The
    // tuple layout is versioned and excludes its own checksum.
    let bytes = serde_json::to_vec(&(version, incarnation, revision, deleted, payload))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

pub(crate) fn encode<T: Serialize>(
    payload: &T,
    incarnation: &str,
    revision: u64,
) -> Result<(Vec<u8>, ControlVersion)> {
    encode_record(payload, incarnation, revision, false)
}

/// Remote fixed slots are cleared through a conditional tombstone, never a
/// delayed unconditional DELETE that could erase a later journal incarnation.
pub(crate) fn encode_tombstone(expected: &ControlVersion) -> Result<(Vec<u8>, ControlVersion)> {
    let revision = expected
        .revision
        .checked_add(1)
        .context("control revision exhausted")?;
    encode_record(
        &serde_json::Value::Null,
        &expected.incarnation,
        revision,
        true,
    )
}

fn encode_record<T: Serialize>(
    payload: &T,
    incarnation: &str,
    revision: u64,
    deleted: bool,
) -> Result<(Vec<u8>, ControlVersion)> {
    let payload = serde_json::to_value(payload)
        .map_err(|_| anyhow::anyhow!("could not encode control payload"))?;
    let digest = payload_digest(
        CONTROL_FORMAT_VERSION,
        incarnation,
        revision,
        deleted,
        &payload,
    )?;
    let bytes = serde_json::to_vec(&Envelope {
        format_version: CONTROL_FORMAT_VERSION,
        incarnation: incarnation.to_owned(),
        revision,
        deleted,
        payload,
        sha256: digest.clone(),
    })?;
    if bytes.len() > MAX_CONTROL_BYTES {
        bail!("control record exceeds the {MAX_CONTROL_BYTES}-byte limit");
    }
    Ok((
        bytes,
        ControlVersion {
            incarnation: incarnation.to_owned(),
            revision,
            digest,
        },
    ))
}

pub(crate) fn decode_slot(bytes: &[u8]) -> Result<(ControlVersion, Option<serde_json::Value>)> {
    if bytes.len() > MAX_CONTROL_BYTES {
        bail!("control record exceeds the {MAX_CONTROL_BYTES}-byte limit");
    }
    // Do not include raw record contents in parse/validation errors.
    let record: Envelope = serde_json::from_slice(bytes).map_err(|error| {
        anyhow::anyhow!(
            "invalid control record JSON at line {}, column {}",
            error.line(),
            error.column()
        )
    })?;
    if record.format_version != CONTROL_FORMAT_VERSION {
        bail!(
            "unsupported control record version {}",
            record.format_version
        );
    }
    uuid::Uuid::parse_str(&record.incarnation)
        .map_err(|_| anyhow::anyhow!("invalid control record incarnation"))?;
    if record.sha256
        != payload_digest(
            record.format_version,
            &record.incarnation,
            record.revision,
            record.deleted,
            &record.payload,
        )?
    {
        bail!("control record checksum mismatch");
    }
    if record.deleted && !record.payload.is_null() {
        bail!("invalid control tombstone payload");
    }
    Ok((
        ControlVersion {
            incarnation: record.incarnation,
            revision: record.revision,
            digest: record.sha256,
        },
        (!record.deleted).then_some(record.payload),
    ))
}

pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<ControlDocument<T>> {
    let (version, payload) = decode_slot(bytes)?;
    let payload = payload.context("control record is a tombstone")?;
    let payload = serde_json::from_value(payload)
        .map_err(|_| anyhow::anyhow!("invalid control record payload"))?;
    Ok(ControlDocument { version, payload })
}

/// Durable local metadata access tied to the lifetime of an exclusive guard.
pub struct LocalStateStore<'a> {
    directory: PathBuf,
    ownership: &'a LocalOwnership,
}

impl<'a> LocalStateStore<'a> {
    pub fn new(dataset_root: &Path, ownership: &'a LocalOwnership) -> Result<Self> {
        let root = fs::canonicalize(dataset_root).context("resolving control record root")?;
        if !ownership
            .roots()
            .iter()
            .any(|scope| root.starts_with(scope))
        {
            bail!("control record root is outside the held dataset ownership scopes");
        }
        Ok(Self {
            directory: root.join(CONTROL_DIRECTORY),
            ownership,
        })
    }

    pub fn load<T: DeserializeOwned>(&self, key: ControlKey) -> Result<Option<ControlDocument<T>>> {
        self.validate_directory(false)?;
        let path = self.directory.join(key.filename());
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).context("reading control record metadata"),
        };
        if !metadata.file_type().is_file() {
            bail!("control record {} is not a regular file", key.filename());
        }
        let mut bytes = Vec::new();
        File::open(&path)?
            .take((MAX_CONTROL_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .context("reading control record")?;
        decode(&bytes)
            .with_context(|| format!("validating {}", key.filename()))
            .map(Some)
    }

    pub fn create<T: Serialize>(&self, key: ControlKey, payload: &T) -> Result<ControlVersion> {
        let _mutation = self.ownership.lock_control_mutation()?;
        let (bytes, version) = encode(payload, &uuid::Uuid::new_v4().to_string(), 0)?;
        self.write(key, &bytes, false)?;
        Ok(version)
    }

    pub fn replace<T: Serialize>(
        &self,
        key: ControlKey,
        expected: &ControlVersion,
        payload: &T,
    ) -> Result<ControlVersion> {
        let _mutation = self.ownership.lock_control_mutation()?;
        self.require_version(key, expected)?;
        let revision = expected
            .revision
            .checked_add(1)
            .context("control revision exhausted")?;
        let (bytes, version) = encode(payload, &expected.incarnation, revision)?;
        self.write(key, &bytes, true)?;
        Ok(version)
    }

    pub fn remove(&self, key: ControlKey, expected: &ControlVersion) -> Result<()> {
        let _mutation = self.ownership.lock_control_mutation()?;
        self.require_version(key, expected)?;
        fs::remove_file(self.directory.join(key.filename())).context("removing control record")?;
        self.sync_directory()
    }

    /// Establish durability of a previously observed record or absence during
    /// transaction recovery. A visible rename/unlink may be from an earlier
    /// attempt that failed before syncing its directory. Read-only status does
    /// not call this method and remains observational.
    pub(crate) fn stabilize(
        &self,
        key: ControlKey,
        expected: Option<&ControlVersion>,
    ) -> Result<()> {
        let _mutation = self.ownership.lock_control_mutation()?;
        self.ownership.revalidate()?;
        let observed = self.load::<serde_json::Value>(key)?;
        if observed.as_ref().map(|document| &document.version) != expected {
            bail!("control record changed while establishing recovery durability");
        }
        if observed.is_some() {
            #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
            tests::checkpoint(tests::Failure::FileSync)?;
            File::open(self.directory.join(key.filename()))?
                .sync_all()
                .context("syncing observed control record for recovery")?;
        }
        let existing = self
            .directory
            .ancestors()
            .find(|path| path.is_dir())
            .context("control recovery has no existing ancestor")?;
        for ancestor in existing.ancestors() {
            #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
            tests::checkpoint(tests::Failure::DirectorySync)?;
            File::open(ancestor)?
                .sync_all()
                .context("syncing observed control ancestry for recovery")?;
        }
        Ok(())
    }

    fn require_version(&self, key: ControlKey, expected: &ControlVersion) -> Result<()> {
        let current = self
            .load::<serde_json::Value>(key)?
            .context("expected control record is missing")?;
        if current.version != *expected {
            bail!("control record changed; refusing stale replacement or removal");
        }
        Ok(())
    }

    fn validate_directory(&self, create: bool) -> Result<()> {
        let exists = match fs::symlink_metadata(&self.directory) {
            Ok(metadata) if metadata.file_type().is_dir() => true,
            Ok(_) => bail!("control directory is not a normal directory"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !create => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
            Err(error) => return Err(error).context("reading control directory metadata"),
        };
        if !create {
            return Ok(());
        }
        if !exists {
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder
                .create(&self.directory)
                .context("creating private control directory")?;
        }
        // Guard acquisition has already durably created the dataset ancestry;
        // sync both ends even on retry, when an earlier failed attempt may have
        // created this link but failed to make it durable.
        File::open(&self.directory)?
            .sync_all()
            .context("syncing control directory before publication")?;
        #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
        tests::checkpoint(tests::Failure::ParentDirectorySync)?;
        File::open(
            self.directory
                .parent()
                .context("control directory has no parent")?,
        )?
        .sync_all()
        .context("syncing control directory parent")
    }

    fn sync_directory(&self) -> Result<()> {
        #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
        tests::checkpoint(tests::Failure::DirectorySync)?;
        File::open(&self.directory)?
            .sync_all()
            .context("syncing control directory")
    }

    fn write(&self, key: ControlKey, bytes: &[u8], replace: bool) -> Result<()> {
        self.validate_directory(true)?;
        let temporary =
            self.directory
                .join(format!(".{}.{}.tmp", key.filename(), uuid::Uuid::new_v4()));
        let target = self.directory.join(key.filename());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&temporary)
            .context("creating private control temporary file")?;
        let outcome = (|| {
            file.write_all(bytes).context("writing control record")?;
            #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
            tests::checkpoint(tests::Failure::FileSync)?;
            file.sync_all().context("syncing control record")?;
            #[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
            tests::checkpoint(tests::Failure::Publication)?;
            if replace {
                fs::rename(&temporary, &target).context("replacing control record")?;
            } else {
                fs::hard_link(&temporary, &target)
                    .context("creating control record without replacement")?;
                fs::remove_file(&temporary).context("removing control temporary name")?;
            }
            self.sync_directory()
        })();
        if temporary.exists() {
            if let Err(cleanup) = fs::remove_file(&temporary) {
                return match outcome {
                    Err(error) => Err(error.context(format!(
                        "also failed to clean control temporary file: {cleanup}"
                    ))),
                    Ok(()) => Err(cleanup).context("cleaning control temporary file"),
                };
            }
        }
        // A complete published record is deliberately retained if directory
        // sync failed. The caller must reload/recover, not assume rollback.
        outcome
    }
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::os::unix::fs::{symlink, PermissionsExt};

    #[derive(Clone, Copy, Eq, PartialEq)]
    pub(super) enum Failure {
        FileSync,
        Publication,
        DirectorySync,
        ParentDirectorySync,
    }

    thread_local! { static FAILURE: Cell<Option<Failure>> = const { Cell::new(None) }; }

    pub(super) fn checkpoint(stage: Failure) -> Result<()> {
        FAILURE.with(|failure| {
            if failure.get() == Some(stage) {
                failure.set(None);
                bail!("injected control persistence failure");
            }
            Ok(())
        })
    }

    #[test]
    fn recovery_stabilizes_visible_record_and_absence_after_failed_directory_sync() {
        let root = tempfile::tempdir().unwrap();
        let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
        let store = LocalStateStore::new(root.path(), &owner).unwrap();
        let version = store
            .create(ControlKey::Pending, &serde_json::json!({"event":1}))
            .unwrap();
        FAILURE.with(|failure| failure.set(Some(Failure::FileSync)));
        assert!(store
            .stabilize(ControlKey::Pending, Some(&version))
            .is_err());
        store
            .stabilize(ControlKey::Pending, Some(&version))
            .unwrap();
        FAILURE.with(|failure| failure.set(Some(Failure::DirectorySync)));
        assert!(store.remove(ControlKey::Pending, &version).is_err());
        assert!(store
            .load::<serde_json::Value>(ControlKey::Pending)
            .unwrap()
            .is_none());
        FAILURE.with(|failure| failure.set(Some(Failure::DirectorySync)));
        assert!(store.stabilize(ControlKey::Pending, None).is_err());
        store.stabilize(ControlKey::Pending, None).unwrap();
        let replacement = store
            .create(ControlKey::Pending, &serde_json::json!({"event":2}))
            .unwrap();
        assert!(store.stabilize(ControlKey::Pending, None).is_err());
        assert!(store
            .stabilize(ControlKey::Pending, Some(&version))
            .is_err());
        store
            .stabilize(ControlKey::Pending, Some(&replacement))
            .unwrap();
    }

    #[test]
    fn strict_records_reject_corruption_unknown_versions_and_oversize() {
        let payload = serde_json::json!({"cursor": "private", "ordinal": 7});
        let id = uuid::Uuid::new_v4().to_string();
        let (bytes, _) = encode(&payload, &id, 1).unwrap();
        let mut record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        record["payload"]["ordinal"] = 8.into();
        assert!(decode::<serde_json::Value>(&serde_json::to_vec(&record).unwrap()).is_err());
        record["format_version"] = 99.into();
        assert!(decode::<serde_json::Value>(&serde_json::to_vec(&record).unwrap()).is_err());
        assert!(decode::<serde_json::Value>(&vec![b' '; MAX_CONTROL_BYTES + 1]).is_err());
        assert!(encode(&"x".repeat(MAX_CONTROL_BYTES), &id, 0).is_err());
        assert!(decode::<serde_json::Value>(b"{partial").is_err());
        let error = decode::<u64>(&bytes).err().unwrap();
        assert!(!format!("{error:#}").contains("private"));
    }

    #[test]
    fn versions_enforce_create_replace_remove_and_private_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let guard = LocalOwnership::acquire(&[dir.path().to_owned()]).unwrap();
        let store = LocalStateStore::new(dir.path(), &guard).unwrap();
        let first = store.create(ControlKey::State, &vec![1, 2]).unwrap();
        assert!(store.create(ControlKey::State, &vec![9]).is_err());
        let next = store
            .replace(ControlKey::State, &first, &vec![1, 2, 3])
            .unwrap();
        assert!(store.replace(ControlKey::State, &first, &vec![9]).is_err());
        assert!(store.remove(ControlKey::State, &first).is_err());
        let read = store.load::<Vec<i32>>(ControlKey::State).unwrap().unwrap();
        assert_eq!(read.payload, vec![1, 2, 3]);
        assert_eq!(read.version, next);
        assert_eq!(
            fs::metadata(&store.directory).unwrap().permissions().mode() & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(store.directory.join("state.json"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        store.remove(ControlKey::State, &next).unwrap();
        assert!(store.load::<Vec<i32>>(ControlKey::State).unwrap().is_none());
    }

    #[test]
    fn failures_keep_old_or_complete_new_state_and_clean_normal_temps() {
        let dir = tempfile::tempdir().unwrap();
        let guard = LocalOwnership::acquire(&[dir.path().to_owned()]).unwrap();
        let store = LocalStateStore::new(dir.path(), &guard).unwrap();
        let old = store.create(ControlKey::State, &1).unwrap();
        for failure in [Failure::FileSync, Failure::Publication] {
            FAILURE.with(|slot| slot.set(Some(failure)));
            assert!(store.replace(ControlKey::State, &old, &2).is_err());
            assert_eq!(
                store
                    .load::<u64>(ControlKey::State)
                    .unwrap()
                    .unwrap()
                    .payload,
                1
            );
            assert_eq!(fs::read_dir(&store.directory).unwrap().count(), 1);
        }
        FAILURE.with(|slot| slot.set(Some(Failure::DirectorySync)));
        assert!(store.replace(ControlKey::State, &old, &2).is_err());
        assert_eq!(
            store
                .load::<u64>(ControlKey::State)
                .unwrap()
                .unwrap()
                .payload,
            2
        );
        assert_eq!(fs::read_dir(&store.directory).unwrap().count(), 1);
    }

    #[test]
    fn failed_control_parent_sync_remains_fatal_on_retry() {
        let dir = tempfile::tempdir().unwrap();
        let guard = LocalOwnership::acquire(&[dir.path().to_owned()]).unwrap();
        let store = LocalStateStore::new(dir.path(), &guard).unwrap();
        for _ in 0..2 {
            FAILURE.with(|slot| slot.set(Some(Failure::ParentDirectorySync)));
            assert!(store.create(ControlKey::State, &1).is_err());
            assert!(store.directory.is_dir());
            assert!(fs::read_dir(&store.directory).unwrap().next().is_none());
        }
        store.create(ControlKey::State, &1).unwrap();
    }

    #[test]
    fn out_of_scope_roots_and_symlink_controls_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let other = tempfile::tempdir().unwrap();
        let guard = LocalOwnership::acquire(&[dir.path().to_owned()]).unwrap();
        assert!(LocalStateStore::new(other.path(), &guard).is_err());
        symlink(other.path(), dir.path().join(CONTROL_DIRECTORY)).unwrap();
        let store = LocalStateStore::new(dir.path(), &guard).unwrap();
        assert!(store.create(ControlKey::State, &1).is_err());
        assert!(store.load::<u64>(ControlKey::State).is_err());
        assert!(fs::read_dir(other.path()).unwrap().next().is_none());
    }

    #[test]
    fn stores_sharing_one_guard_have_exactly_one_same_version_winner() {
        let dir = tempfile::tempdir().unwrap();
        let guard = LocalOwnership::acquire(&[dir.path().to_owned()]).unwrap();
        let first = LocalStateStore::new(dir.path(), &guard).unwrap();
        let second = LocalStateStore::new(dir.path(), &guard).unwrap();
        let initial = first.create(ControlKey::State, &0).unwrap();
        let barrier = std::sync::Barrier::new(2);
        let results = std::thread::scope(|threads| {
            let one = threads.spawn(|| {
                barrier.wait();
                first.replace(ControlKey::State, &initial, &1)
            });
            let two = threads.spawn(|| {
                barrier.wait();
                second.replace(ControlKey::State, &initial, &2)
            });
            [one.join().unwrap(), two.join().unwrap()]
        });
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let state = first.load::<u64>(ControlKey::State).unwrap().unwrap();
        assert_eq!(state.version.revision, 1);
        assert_eq!(state.version.incarnation, initial.incarnation);
        assert!([1, 2].contains(&state.payload));
    }

    #[test]
    fn remove_recreate_cannot_reuse_a_stale_control_version() {
        let dir = tempfile::tempdir().unwrap();
        let guard = LocalOwnership::acquire(&[dir.path().to_owned()]).unwrap();
        let store = LocalStateStore::new(dir.path(), &guard).unwrap();
        let first = store.create(ControlKey::Pending, &1).unwrap();
        store.remove(ControlKey::Pending, &first).unwrap();
        let recreated = store.create(ControlKey::Pending, &1).unwrap();
        assert_eq!(first.revision, recreated.revision);
        assert_ne!(first.incarnation, recreated.incarnation);
        assert!(store.replace(ControlKey::Pending, &first, &2).is_err());
        assert!(store.remove(ControlKey::Pending, &first).is_err());
        assert_eq!(
            store
                .load::<u64>(ControlKey::Pending)
                .unwrap()
                .unwrap()
                .payload,
            1
        );
    }
}
