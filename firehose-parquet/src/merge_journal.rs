//! Crash safety for `merge`.
//!
//! Merging a partition writes new part files and then deletes the parts it read. A crash, a
//! failed upload, or an overlapping run between those steps used to leave both, and the next
//! merge folded the duplicates into one file for good. Each partition merge is now recorded in a
//! journal, `_fireparq_merge.json`, in the partition directory:
//!
//! 1. The journal is created with state `writing` and the source part names before any output
//!    is written. Creating it is exclusive, so it also claims the partition.
//! 2. The outputs are written (locally to a temporary name, fsynced, then renamed).
//! 3. The journal is rewritten with state `committed` and the output names.
//! 4. The sources are deleted, then the journal.
//!
//! A merge that finds a journal left by a run that is no longer alive finishes it first
//! ([`recover`]): a `writing` journal is rolled back (its outputs are deleted; the sources were
//! never touched), and a `committed` one is rolled forward (the remaining sources are deleted).
//!
//! Each run holds a lock on the merge root: an OS file lock locally ([`LocalRunLock`]), released
//! by the OS when the process exits, and a conditionally created object on S3 ([`S3RunLock`]),
//! refreshed while the run is active and taken over once it is stale. A journal records its
//! run's lock, which is how a live run's journal is told apart from a crashed run's.

use anyhow::{Context, Result};
use object_store::ObjectStore;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cli::block_on_async;

/// Journal file name inside a partition directory.
pub(crate) const JOURNAL_FILE: &str = "_fireparq_merge.json";

/// Lock file (local) or object (S3) name at the merge root.
pub(crate) const LOCK_FILE: &str = ".fireparq-merge.lock";

/// Environment variable that aborts `merge` at a named step, to test crash recovery:
/// `after-outputs`, `after-commit`, or `after-first-delete`.
pub(crate) const CRASH_ENV: &str = "FIREPARQ_TEST_MERGE_CRASH_AT";

/// An S3 lock not refreshed for this long belongs to a run that is no longer alive.
const S3_LOCK_STALE_AFTER: Duration = Duration::from_secs(30 * 60);

/// Minimum time between refreshes of an S3 lock.
const S3_LOCK_HEARTBEAT_EVERY: Duration = Duration::from_secs(60);

const JOURNAL_VERSION: u32 = 1;

#[cfg(test)]
thread_local! {
    /// Step at which [`crash_point`] fails, for tests on this thread.
    pub(crate) static INJECTED_CRASH: std::cell::RefCell<Option<&'static str>> =
        const { std::cell::RefCell::new(None) };
}

/// Simulates a crash at `step` when [`CRASH_ENV`] names it: the process aborts without any
/// cleanup, like `kill -9`. Tests inject an error instead.
pub(crate) fn crash_point(step: &str) -> Result<()> {
    #[cfg(test)]
    if INJECTED_CRASH.with(|crash| *crash.borrow() == Some(step)) {
        anyhow::bail!("injected crash at {step}");
    }
    if std::env::var(CRASH_ENV).is_ok_and(|value| value == step) {
        eprintln!("{CRASH_ENV}={step}: aborting to simulate a crash");
        std::process::abort();
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JournalState {
    /// Outputs may be partially written; the sources are untouched.
    Writing,
    /// Every output is written; the sources may be partially deleted.
    Committed,
}

/// The record of one partition merge.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Journal {
    pub version: u32,
    pub run_id: String,
    /// The owning run's lock: a local path or an S3 key.
    pub lock: String,
    pub started_at: String,
    pub state: JournalState,
    /// Source part file names in the partition directory.
    pub sources: Vec<String>,
    /// Outputs are numbered from this part number up (`part-NNNNNN.parquet`).
    pub first_output_part: u32,
    /// Output file names, recorded at commit.
    #[serde(default)]
    pub outputs: Vec<String>,
}

impl Journal {
    pub(crate) fn new(run: &RunContext, sources: Vec<String>, first_output_part: u32) -> Journal {
        Journal {
            version: JOURNAL_VERSION,
            run_id: run.run_id.clone(),
            lock: run.lock.clone(),
            started_at: now_rfc3339(),
            state: JournalState::Writing,
            sources,
            first_output_part,
            outputs: Vec::new(),
        }
    }

    pub(crate) fn committed(&self, outputs: Vec<String>) -> Journal {
        Journal {
            state: JournalState::Committed,
            outputs,
            ..self.clone()
        }
    }

    fn encode(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec_pretty(self)?)
    }

    fn decode(data: &[u8], location: &str) -> Result<Journal> {
        let journal: Journal = serde_json::from_slice(data)
            .with_context(|| format!("parsing merge journal {location}"))?;
        if journal.version != JOURNAL_VERSION {
            anyhow::bail!(
                "merge journal {location} has version {}, but this build understands {JOURNAL_VERSION}",
                journal.version
            );
        }
        Ok(journal)
    }
}

/// The identity of the running merge, recorded in every journal it writes.
#[derive(Debug, Clone)]
pub(crate) struct RunContext {
    pub run_id: String,
    pub lock: String,
}

/// Temporary name of a local output while it is being written.
pub(crate) fn temp_output_name(name: &str, run_id: &str) -> String {
    format!(".{name}.{run_id}.tmp")
}

/// File operations on one partition directory, locally or on S3.
pub(crate) trait PartitionFiles {
    /// Human-readable location of the partition, for messages.
    fn label(&self) -> String;
    /// Names of every file directly in the partition directory.
    fn list_names(&self) -> Result<Vec<String>>;
    fn read_journal(&self) -> Result<Option<Journal>>;
    /// Creates the journal only if none exists. Returns false when one already does.
    fn create_journal(&self, journal: &Journal) -> Result<bool>;
    /// Replaces the journal atomically.
    fn replace_journal(&self, journal: &Journal) -> Result<()>;
    fn remove_journal(&self) -> Result<()>;
    /// Deletes a file; a file that is already gone is not an error.
    fn delete(&self, name: &str) -> Result<()>;
    /// Makes earlier creates, renames and deletes durable (fsyncs the directory locally).
    fn sync(&self) -> Result<()>;
}

/// What [`recover`] did to a partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Recovery {
    /// A `writing` journal: its outputs were deleted and the sources kept.
    RolledBack { deleted_outputs: usize },
    /// A `committed` journal: the remaining sources were deleted and the outputs kept.
    RolledForward { deleted_sources: usize },
}

impl std::fmt::Display for Recovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Recovery::RolledBack { deleted_outputs } => write!(
                f,
                "rolled back an interrupted merge (deleted {deleted_outputs} partial output(s))"
            ),
            Recovery::RolledForward { deleted_sources } => write!(
                f,
                "finished an interrupted merge (deleted {deleted_sources} remaining source(s))"
            ),
        }
    }
}

/// Finishes or undoes the merge recorded in `journal`, whose run is no longer alive.
pub(crate) fn recover(files: &dyn PartitionFiles, journal: &Journal) -> Result<Recovery> {
    let names = files.list_names()?;
    let recovery = match journal.state {
        JournalState::Writing => {
            let sources: HashSet<&str> = journal.sources.iter().map(String::as_str).collect();
            let temp_suffix = format!(".{}.tmp", journal.run_id);
            let mut deleted_outputs = 0;
            for name in &names {
                let partial_output = crate::merge::parse_part_number(name)
                    .is_some_and(|part| part >= journal.first_output_part)
                    && !sources.contains(name.as_str());
                let temp_file = name.starts_with('.') && name.ends_with(&temp_suffix);
                if partial_output || temp_file {
                    files.delete(name)?;
                    deleted_outputs += 1;
                }
            }
            Recovery::RolledBack { deleted_outputs }
        }
        JournalState::Committed => {
            let present: HashSet<&str> = names.iter().map(String::as_str).collect();
            if let Some(missing) = journal
                .outputs
                .iter()
                .find(|output| !present.contains(output.as_str()))
            {
                anyhow::bail!(
                    "{}: the interrupted merge recorded in {JOURNAL_FILE} committed output \
                     {missing}, which is missing. Its sources were kept; check the partition \
                     and remove {JOURNAL_FILE} once it holds each row exactly once",
                    files.label()
                );
            }
            let mut deleted_sources = 0;
            for source in &journal.sources {
                if present.contains(source.as_str()) {
                    files.delete(source)?;
                    deleted_sources += 1;
                }
            }
            Recovery::RolledForward { deleted_sources }
        }
    };
    files.sync()?;
    files.remove_journal()?;
    files.sync()?;
    Ok(recovery)
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default()
}

fn host_name() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

// ---------------------------------------------------------------------------
// Local filesystem
// ---------------------------------------------------------------------------

/// A local partition directory.
pub(crate) struct LocalPartition {
    dir: PathBuf,
}

impl LocalPartition {
    pub(crate) fn new(dir: &Path) -> Self {
        Self {
            dir: dir.to_path_buf(),
        }
    }

    fn journal_path(&self) -> PathBuf {
        self.dir.join(JOURNAL_FILE)
    }

    /// Writes `data` to a new temporary file next to `name` and fsyncs it.
    fn write_temp(&self, name: &str, data: &[u8]) -> Result<PathBuf> {
        let tmp = self
            .dir
            .join(format!(".{name}.{}.tmp", uuid::Uuid::new_v4().simple()));
        let mut file =
            File::create_new(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        file.write_all(data)
            .and_then(|()| file.sync_all())
            .with_context(|| format!("writing {}", tmp.display()))?;
        Ok(tmp)
    }
}

impl PartitionFiles for LocalPartition {
    fn label(&self) -> String {
        self.dir.display().to_string()
    }

    fn list_names(&self) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in
            std::fs::read_dir(&self.dir).with_context(|| format!("listing {}", self.label()))?
        {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                names.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
        names.sort();
        Ok(names)
    }

    fn read_journal(&self) -> Result<Option<Journal>> {
        let path = self.journal_path();
        match std::fs::read(&path) {
            Ok(data) => Journal::decode(&data, &path.display().to_string()).map(Some),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err).with_context(|| format!("reading {}", path.display())),
        }
    }

    fn create_journal(&self, journal: &Journal) -> Result<bool> {
        let path = self.journal_path();
        let tmp = self.write_temp(JOURNAL_FILE, &journal.encode()?)?;
        // A hard link fails when the target exists, so this claims the partition atomically
        // with complete contents.
        let linked = std::fs::hard_link(&tmp, &path);
        let _ = std::fs::remove_file(&tmp);
        match linked {
            Ok(()) => {
                self.sync()?;
                Ok(true)
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(err) => Err(err).with_context(|| format!("creating {}", path.display())),
        }
    }

    fn replace_journal(&self, journal: &Journal) -> Result<()> {
        let path = self.journal_path();
        let tmp = self.write_temp(JOURNAL_FILE, &journal.encode()?)?;
        if let Err(err) = std::fs::rename(&tmp, &path) {
            let _ = std::fs::remove_file(&tmp);
            return Err(err).with_context(|| format!("replacing {}", path.display()));
        }
        self.sync()
    }

    fn remove_journal(&self) -> Result<()> {
        self.delete(JOURNAL_FILE)
    }

    fn delete(&self, name: &str) -> Result<()> {
        let path = self.dir.join(name);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err).with_context(|| format!("deleting {}", path.display())),
        }
    }

    fn sync(&self) -> Result<()> {
        sync_dir(&self.dir)
    }
}

/// Writes a merge output atomically: to a temporary file, fsynced, then renamed.
pub(crate) fn write_local_output(
    dir: &Path,
    name: &str,
    data: &[u8],
    run_id: &str,
) -> Result<PathBuf> {
    let path = dir.join(name);
    let tmp = dir.join(temp_output_name(name, run_id));
    let write = || -> Result<()> {
        let mut file =
            File::create_new(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        file.write_all(data)
            .and_then(|()| file.sync_all())
            .with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("renaming {} to {}", tmp.display(), path.display()))
    };
    if let Err(err) = write() {
        let _ = std::fs::remove_file(&tmp);
        return Err(err);
    }
    Ok(path)
}

fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(dir)
        .and_then(|dir| dir.sync_all())
        .with_context(|| format!("syncing directory {}", dir.display()))?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// The lock a local merge run holds on its root directory.
///
/// It is an OS advisory lock on `<root>/.fireparq-merge.lock`, so the OS releases it when the
/// process exits, however it exits.
pub(crate) struct LocalRunLock {
    path: PathBuf,
    file: File,
}

impl LocalRunLock {
    pub(crate) fn acquire(root: &Path) -> Result<Self> {
        let root = root
            .canonicalize()
            .with_context(|| format!("resolving {}", root.display()))?;
        let path = root.join(LOCK_FILE);
        for _ in 0..3 {
            let file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)
                .with_context(|| format!("opening {}", path.display()))?;
            match file.try_lock() {
                Ok(()) => {}
                Err(std::fs::TryLockError::WouldBlock) => anyhow::bail!(
                    "another merge is running on {}: {} is locked by another process. Wait \
                     for it to finish; the lock is released when that process exits",
                    root.display(),
                    path.display()
                ),
                Err(std::fs::TryLockError::Error(err)) => {
                    return Err(err).with_context(|| format!("locking {}", path.display()))
                }
            }
            // A run that just finished may have removed the file between our open and lock.
            if same_file(&file, &path) {
                return Ok(Self { path, file });
            }
        }
        anyhow::bail!("could not lock {}: it keeps being replaced", path.display())
    }

    pub(crate) fn location(&self) -> String {
        self.path.to_string_lossy().into_owned()
    }

    /// Removes the lock file and releases the lock.
    pub(crate) fn release(self) -> Result<()> {
        match std::fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| format!("removing {}", self.path.display()))
            }
        }
        drop(self.file);
        Ok(())
    }

    /// Returns true when the run that wrote `journal` may still be running.
    pub(crate) fn owner_alive(&self, journal: &Journal) -> Result<bool> {
        if journal.lock == self.location() {
            // This run holds that lock now, so the journal's run has ended.
            return Ok(false);
        }
        let file = match File::open(&journal.lock) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(err).with_context(|| format!("opening {}", journal.lock)),
        };
        match file.try_lock() {
            // Unlocked when `file` is dropped.
            Ok(()) => Ok(false),
            Err(std::fs::TryLockError::WouldBlock) => Ok(true),
            Err(std::fs::TryLockError::Error(err)) => {
                Err(err).with_context(|| format!("checking lock {}", journal.lock))
            }
        }
    }
}

#[cfg(unix)]
fn same_file(file: &File, path: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    match (file.metadata(), std::fs::metadata(path)) {
        (Ok(a), Ok(b)) => a.dev() == b.dev() && a.ino() == b.ino(),
        _ => false,
    }
}

#[cfg(not(unix))]
fn same_file(_file: &File, path: &Path) -> bool {
    path.exists()
}

// ---------------------------------------------------------------------------
// S3
// ---------------------------------------------------------------------------

/// A partition directory on S3 (a key prefix).
pub(crate) struct S3Partition<'a> {
    pub client: &'a Arc<dyn ObjectStore>,
    pub bucket: &'a str,
    /// Key of the partition directory, without a trailing `/`.
    pub key: &'a str,
}

impl S3Partition<'_> {
    fn path(&self, name: &str) -> object_store::path::Path {
        if self.key.is_empty() {
            object_store::path::Path::from(name)
        } else {
            object_store::path::Path::from(format!("{}/{name}", self.key))
        }
    }
}

impl PartitionFiles for S3Partition<'_> {
    fn label(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.key)
    }

    fn list_names(&self) -> Result<Vec<String>> {
        let prefix = (!self.key.is_empty()).then(|| object_store::path::Path::from(self.key));
        let listing = block_on_async(self.client.list_with_delimiter(prefix.as_ref()))
            .with_context(|| format!("listing {}", self.label()))?;
        let mut names: Vec<String> = listing
            .objects
            .iter()
            .filter_map(|obj| obj.location.filename().map(str::to_string))
            .collect();
        names.sort();
        Ok(names)
    }

    fn read_journal(&self) -> Result<Option<Journal>> {
        let path = self.path(JOURNAL_FILE);
        match block_on_async(async { self.client.get(&path).await?.bytes().await }) {
            Ok(data) => Journal::decode(&data, &format!("s3://{}/{path}", self.bucket)).map(Some),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(err) => Err(err).with_context(|| format!("reading s3://{}/{path}", self.bucket)),
        }
    }

    fn create_journal(&self, journal: &Journal) -> Result<bool> {
        let path = self.path(JOURNAL_FILE);
        let payload = object_store::PutPayload::from(journal.encode()?);
        match block_on_async(self.client.put_opts(
            &path,
            payload.clone(),
            object_store::PutMode::Create.into(),
        )) {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => Ok(false),
            Err(object_store::Error::NotImplemented | object_store::Error::NotSupported { .. }) => {
                // No conditional writes: claim with a check, which the run lock backs up.
                if self.read_journal()?.is_some() {
                    return Ok(false);
                }
                block_on_async(self.client.put(&path, payload))
                    .with_context(|| format!("writing s3://{}/{path}", self.bucket))?;
                Ok(true)
            }
            Err(err) => Err(err).with_context(|| format!("writing s3://{}/{path}", self.bucket)),
        }
    }

    fn replace_journal(&self, journal: &Journal) -> Result<()> {
        let path = self.path(JOURNAL_FILE);
        block_on_async(
            self.client
                .put(&path, object_store::PutPayload::from(journal.encode()?)),
        )
        .with_context(|| format!("writing s3://{}/{path}", self.bucket))?;
        Ok(())
    }

    fn remove_journal(&self) -> Result<()> {
        self.delete(JOURNAL_FILE)
    }

    fn delete(&self, name: &str) -> Result<()> {
        let path = self.path(name);
        match block_on_async(self.client.delete(&path)) {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(err).with_context(|| format!("deleting s3://{}/{path}", self.bucket)),
        }
    }

    fn sync(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LockRecord {
    run_id: String,
    host: String,
    pid: u32,
    started_at: String,
}

/// The lock an S3 merge run holds on its root prefix: the object `<prefix>/.fireparq-merge.lock`,
/// created only if absent and refreshed while the run is active.
///
/// A lock not refreshed for 30 minutes belongs to a run that is no longer alive, and the next
/// run takes it over with a conditional update, so two runs cannot both take it.
pub(crate) struct S3RunLock {
    client: Arc<dyn ObjectStore>,
    location: object_store::path::Path,
    bucket: String,
    record: LockRecord,
    version: Option<object_store::UpdateVersion>,
    /// False when the store has no conditional writes, so the lock is best effort.
    enforced: bool,
    /// When the lock was last written; `None` forces the next [`Self::refresh`].
    last_refresh: Option<Instant>,
    stale_after: Duration,
}

impl S3RunLock {
    pub(crate) fn acquire(
        client: &Arc<dyn ObjectStore>,
        bucket: &str,
        prefix: &str,
        run_id: &str,
    ) -> Result<Self> {
        Self::acquire_with_stale_after(client, bucket, prefix, run_id, S3_LOCK_STALE_AFTER)
    }

    pub(crate) fn acquire_with_stale_after(
        client: &Arc<dyn ObjectStore>,
        bucket: &str,
        prefix: &str,
        run_id: &str,
        stale_after: Duration,
    ) -> Result<Self> {
        let key = if prefix.is_empty() {
            LOCK_FILE.to_string()
        } else {
            format!("{prefix}/{LOCK_FILE}")
        };
        let mut lock = Self {
            client: Arc::clone(client),
            location: object_store::path::Path::from(key),
            bucket: bucket.to_string(),
            record: LockRecord {
                run_id: run_id.to_string(),
                host: host_name(),
                pid: std::process::id(),
                started_at: now_rfc3339(),
            },
            version: None,
            enforced: true,
            last_refresh: Some(Instant::now()),
            stale_after,
        };
        let data = serde_json::to_vec_pretty(&lock.record)?;
        let uri = lock.uri();

        match block_on_async(lock.client.put_opts(
            &lock.location,
            object_store::PutPayload::from(data.clone()),
            object_store::PutMode::Create.into(),
        )) {
            Ok(result) => {
                lock.version = Some(update_version(result));
                return Ok(lock);
            }
            Err(
                object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. },
            ) => {}
            Err(object_store::Error::NotImplemented | object_store::Error::NotSupported { .. }) => {
                tracing::warn!(
                    lock = %uri,
                    "the S3 store does not support conditional writes; the merge lock is best effort, so do not run overlapping merges on this prefix"
                );
                block_on_async(
                    lock.client
                        .put(&lock.location, object_store::PutPayload::from(data)),
                )
                .with_context(|| format!("writing {uri}"))?;
                lock.enforced = false;
                return Ok(lock);
            }
            Err(err) => return Err(err).with_context(|| format!("creating {uri}")),
        }

        // The lock exists. Take it over only if its run stopped refreshing it.
        let existing = block_on_async(lock.client.get(&lock.location))
            .with_context(|| format!("reading {uri}"))?;
        let meta = existing.meta.clone();
        let holder = block_on_async(existing.bytes())
            .ok()
            .and_then(|data| serde_json::from_slice::<LockRecord>(&data).ok());
        let holder_text = holder
            .as_ref()
            .map(|h| {
                format!(
                    "run {} on host {} (pid {}), started {}",
                    h.run_id, h.host, h.pid, h.started_at
                )
            })
            .unwrap_or_else(|| "an unreadable lock".to_string());
        if !is_stale(meta.last_modified.timestamp(), lock.stale_after) {
            anyhow::bail!(
                "another merge is running on s3://{bucket}/{prefix}: {uri} is held by \
                 {holder_text}, last refreshed {}. Wait for it to finish; if no merge is \
                 running, delete that object or wait until it is 30 minutes old",
                meta.last_modified
            );
        }
        let takeover = object_store::PutMode::Update(object_store::UpdateVersion {
            e_tag: meta.e_tag.clone(),
            version: meta.version.clone(),
        });
        match block_on_async(lock.client.put_opts(
            &lock.location,
            object_store::PutPayload::from(data),
            takeover.into(),
        )) {
            Ok(result) => {
                tracing::warn!(
                    lock = %uri,
                    previous = %holder_text,
                    "took over a stale merge lock; its interrupted merges are recovered first"
                );
                lock.version = Some(update_version(result));
                Ok(lock)
            }
            Err(object_store::Error::Precondition { .. }) => {
                anyhow::bail!("another merge took the stale lock {uri} at the same time; try again")
            }
            Err(err) => Err(err).with_context(|| format!("taking over {uri}")),
        }
    }

    fn uri(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.location)
    }

    pub(crate) fn location(&self) -> String {
        self.location.as_ref().to_string()
    }

    /// Refreshes the lock (at most once a minute) and fails if another run took it over.
    pub(crate) fn refresh(&mut self) -> Result<()> {
        if self
            .last_refresh
            .is_some_and(|at| at.elapsed() < S3_LOCK_HEARTBEAT_EVERY)
        {
            return Ok(());
        }
        let data = object_store::PutPayload::from(serde_json::to_vec_pretty(&self.record)?);
        let mode = match (&self.version, self.enforced) {
            (Some(version), true) if version.e_tag.is_some() => {
                object_store::PutMode::Update(version.clone())
            }
            _ => object_store::PutMode::Overwrite,
        };
        match block_on_async(self.client.put_opts(&self.location, data, mode.into())) {
            Ok(result) => {
                self.version = Some(update_version(result));
                self.last_refresh = Some(Instant::now());
                Ok(())
            }
            Err(object_store::Error::Precondition { .. }) => anyhow::bail!(
                "lost the merge lock {}: another run took it over; stopping before deleting anything",
                self.uri()
            ),
            Err(err) => Err(err).with_context(|| format!("refreshing {}", self.uri())),
        }
    }

    /// Deletes the lock object.
    pub(crate) fn release(self) -> Result<()> {
        match block_on_async(self.client.delete(&self.location)) {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(err).with_context(|| format!("deleting {}", self.uri())),
        }
    }

    /// Returns true when the run that wrote `journal` may still be running.
    pub(crate) fn owner_alive(&self, journal: &Journal) -> Result<bool> {
        if journal.lock == self.location() {
            // This run holds that lock now, so the journal's run has ended.
            return Ok(false);
        }
        let location = object_store::path::Path::from(journal.lock.as_str());
        match block_on_async(self.client.head(&location)) {
            // Whoever holds that lock now is responsible for the partition.
            Ok(meta) => Ok(!is_stale(meta.last_modified.timestamp(), self.stale_after)),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => {
                Err(err).with_context(|| format!("checking lock s3://{}/{location}", self.bucket))
            }
        }
    }
}

fn update_version(result: object_store::PutResult) -> object_store::UpdateVersion {
    object_store::UpdateVersion {
        e_tag: result.e_tag,
        version: result.version,
    }
}

/// Returns true when an object last modified at `last_modified_unix` (seconds) is older than
/// `stale_after`.
fn is_stale(last_modified_unix: i64, stale_after: Duration) -> bool {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or(0);
    now.saturating_sub(last_modified_unix) >= stale_after.as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn run(lock: &str) -> RunContext {
        RunContext {
            run_id: "run1".to_string(),
            lock: lock.to_string(),
        }
    }

    fn touch(dir: &Path, name: &str) {
        std::fs::write(dir.join(name), name.as_bytes()).unwrap();
    }

    fn names(dir: &Path) -> Vec<String> {
        LocalPartition::new(dir).list_names().unwrap()
    }

    fn sources() -> Vec<String> {
        vec![
            "part-000001.parquet".to_string(),
            "part-abcd1234-000007.parquet".to_string(),
        ]
    }

    #[test]
    fn test_recover_rolls_back_a_writing_journal() {
        let dir = tempfile::tempdir().unwrap();
        let partition = LocalPartition::new(dir.path());
        for name in sources() {
            touch(dir.path(), &name);
        }
        let journal = Journal::new(&run("/lock"), sources(), 2);
        assert!(partition.create_journal(&journal).unwrap());
        // Partial outputs of that run, and files it must not touch.
        touch(dir.path(), "part-000002.parquet");
        touch(dir.path(), &temp_output_name("part-000003.parquet", "run1"));
        touch(
            dir.path(),
            &temp_output_name("part-000003.parquet", "other"),
        );
        touch(dir.path(), "part-feed0000-000009.parquet");

        let recovery = recover(&partition, &journal).unwrap();

        assert_eq!(recovery, Recovery::RolledBack { deleted_outputs: 2 });
        assert_eq!(
            names(dir.path()),
            vec![
                temp_output_name("part-000003.parquet", "other"),
                "part-000001.parquet".to_string(),
                "part-abcd1234-000007.parquet".to_string(),
                "part-feed0000-000009.parquet".to_string(),
            ]
        );
    }

    #[test]
    fn test_recover_rolls_forward_a_committed_journal() {
        let dir = tempfile::tempdir().unwrap();
        let partition = LocalPartition::new(dir.path());
        // One source was already deleted before the crash.
        touch(dir.path(), "part-abcd1234-000007.parquet");
        touch(dir.path(), "part-000002.parquet");
        let journal = Journal::new(&run("/lock"), sources(), 2)
            .committed(vec!["part-000002.parquet".to_string()]);
        partition.replace_journal(&journal).unwrap();

        let recovery = recover(&partition, &journal).unwrap();

        assert_eq!(recovery, Recovery::RolledForward { deleted_sources: 1 });
        assert_eq!(names(dir.path()), vec!["part-000002.parquet".to_string()]);
    }

    #[test]
    fn test_recover_keeps_sources_when_a_committed_output_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let partition = LocalPartition::new(dir.path());
        for name in sources() {
            touch(dir.path(), &name);
        }
        let journal = Journal::new(&run("/lock"), sources(), 2)
            .committed(vec!["part-000002.parquet".to_string()]);
        partition.replace_journal(&journal).unwrap();

        let err = recover(&partition, &journal).unwrap_err().to_string();

        assert!(
            err.contains("committed output part-000002.parquet"),
            "{err}"
        );
        assert!(err.contains("Its sources were kept"), "{err}");
        let mut expected = sources();
        expected.insert(0, JOURNAL_FILE.to_string());
        assert_eq!(names(dir.path()), expected);
    }

    #[test]
    fn test_create_journal_claims_the_partition_once() {
        let dir = tempfile::tempdir().unwrap();
        let partition = LocalPartition::new(dir.path());
        let journal = Journal::new(&run("/lock"), sources(), 2);

        assert!(partition.create_journal(&journal).unwrap());
        assert!(!partition
            .create_journal(&Journal::new(&run("/other"), vec![], 1))
            .unwrap());
        assert_eq!(partition.read_journal().unwrap(), Some(journal));
        // No temporary files are left behind.
        assert_eq!(names(dir.path()), vec![JOURNAL_FILE.to_string()]);
    }

    #[test]
    fn test_journal_with_an_unknown_version_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut journal = Journal::new(&run("/lock"), sources(), 2);
        journal.version = 99;
        std::fs::write(dir.path().join(JOURNAL_FILE), journal.encode().unwrap()).unwrap();

        let err = LocalPartition::new(dir.path())
            .read_journal()
            .unwrap_err()
            .to_string();
        assert!(err.contains("has version 99"), "{err}");
    }

    #[test]
    fn test_local_run_lock_is_exclusive_and_tells_live_owners_from_dead_ones() {
        let dir = tempfile::tempdir().unwrap();
        let lock = LocalRunLock::acquire(dir.path()).unwrap();
        let err = LocalRunLock::acquire(dir.path()).err().unwrap().to_string();
        assert!(err.contains("another merge is running"), "{err}");

        // A nested run's lock, held by a "live" run.
        let nested = dir.path().join("blocks");
        std::fs::create_dir_all(&nested).unwrap();
        let nested_lock = LocalRunLock::acquire(&nested).unwrap();
        let nested_journal = Journal::new(&run(&nested_lock.location()), vec![], 1);
        assert!(lock.owner_alive(&nested_journal).unwrap());

        // Once that run is gone, its journals belong to no one.
        drop(nested_lock);
        assert!(!lock.owner_alive(&nested_journal).unwrap());
        // A journal written under this run's own lock is from an earlier run.
        let own_journal = Journal::new(&run(&lock.location()), vec![], 1);
        assert!(!lock.owner_alive(&own_journal).unwrap());
        let missing = Journal::new(&run("/no/such/lock"), vec![], 1);
        assert!(!lock.owner_alive(&missing).unwrap());

        lock.release().unwrap();
        assert!(!dir.path().join(LOCK_FILE).exists());
        LocalRunLock::acquire(dir.path())
            .unwrap()
            .release()
            .unwrap();
    }

    #[test]
    fn test_s3_run_lock_refuses_a_fresh_lock_and_takes_over_a_stale_one() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut first = S3RunLock::acquire(&store, "bucket", "evm", "first").unwrap();
        assert_eq!(first.location(), format!("evm/{LOCK_FILE}"));

        let err = S3RunLock::acquire(&store, "bucket", "evm", "second")
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("another merge is running"), "{err}");
        assert!(err.contains("run first"), "{err}");

        // Treat any lock as stale: the next run takes it over.
        let taker =
            S3RunLock::acquire_with_stale_after(&store, "bucket", "evm", "third", Duration::ZERO)
                .unwrap();
        // The first run notices at its next refresh, before deleting anything.
        first.last_refresh = None;
        let err = first.refresh().unwrap_err().to_string();
        assert!(err.contains("lost the merge lock"), "{err}");

        // Journals written under the lock this run now holds are from an earlier run.
        let journal = Journal::new(&run(&taker.location()), vec![], 1);
        assert!(!taker.owner_alive(&journal).unwrap());
        taker.release().unwrap();

        // A fresh lock of a nested run protects its journals until it is released.
        let fourth = S3RunLock::acquire(&store, "bucket", "evm", "fourth").unwrap();
        let nested = S3RunLock::acquire(&store, "bucket", "evm/blocks", "nested").unwrap();
        let nested_journal = Journal::new(&run(&nested.location()), vec![], 1);
        assert!(fourth.owner_alive(&nested_journal).unwrap());
        nested.release().unwrap();
        assert!(!fourth.owner_alive(&nested_journal).unwrap());
        fourth.release().unwrap();
    }

    #[test]
    fn test_s3_partition_journal_roundtrip() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let partition = S3Partition {
            client: &store,
            bucket: "bucket",
            key: "evm/blocks/day=15",
        };
        let journal = Journal::new(&run("evm/.fireparq-merge.lock"), sources(), 2);

        assert!(partition.create_journal(&journal).unwrap());
        assert!(!partition.create_journal(&journal).unwrap());
        assert_eq!(partition.read_journal().unwrap(), Some(journal.clone()));
        partition
            .replace_journal(&journal.committed(vec!["part-000002.parquet".to_string()]))
            .unwrap();
        assert_eq!(
            partition.read_journal().unwrap().unwrap().state,
            JournalState::Committed
        );
        assert_eq!(
            partition.list_names().unwrap(),
            vec![JOURNAL_FILE.to_string()]
        );
        partition.remove_journal().unwrap();
        partition.remove_journal().unwrap();
        assert_eq!(partition.read_journal().unwrap(), None);
    }
}
