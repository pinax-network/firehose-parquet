//! Collect all mutation scopes before acquiring any operation capability.

use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::cli::AwsConfig;
use crate::dataset_lock_s3::{OwnershipError, S3Ownership};

use super::LocalOwnership;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MutationScope {
    path: String,
    file: bool,
}

impl MutationScope {
    pub fn directory(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            file: false,
        }
    }

    pub fn file(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            file: true,
        }
    }

    /// For existing input files versus directory roots. S3 input callers should
    /// pass their known shape; bucket-wide ownership is independent of it.
    pub fn input(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        if path.starts_with("s3://") {
            return Ok(Self::directory(path));
        }
        let metadata = std::fs::metadata(&path)
            .context("mutation input does not exist or cannot be inspected")?;
        Ok(Self {
            path,
            file: metadata.is_file(),
        })
    }
}

/// Owns all local scopes and remote buckets for one command. A caller ends it
/// with [`DatasetOwnership::finish`], or with `release` after success. Dropping
/// it without either (a panic, a cancelled future, or a command that keeps
/// ownership after any error) releases local OS locks but deliberately retains
/// remote Owned records, and logs the recovery commands for each one.
pub struct DatasetOwnership {
    local: Option<LocalOwnership>,
    remote: BTreeMap<String, S3Ownership>,
}

/// Why `finish` or a drop kept one bucket owner.
enum RetainReason {
    /// The latch is set: a request's outcome was never proven.
    UncertainMutation,
    /// Every request was resolved, but the owner record's own release failed.
    ReleaseFailed(OwnershipError),
    /// The guard was dropped without `finish`, so nothing was proven.
    Unfinished,
}

struct RetainedOwner {
    uri: String,
    owner_id: String,
    generation: u64,
    reason: RetainReason,
}

impl RetainedOwner {
    fn new(bucket: &str, owner: &S3Ownership, reason: RetainReason) -> Self {
        let record = owner.record();
        // The first sorted scope is the dataset root for build output (its
        // cursor key sorts after it); status and release accept any prefix.
        let uri = match record.scopes().first().filter(|scope| !scope.is_empty()) {
            Some(scope) => format!("s3://{bucket}/{scope}"),
            None => format!("s3://{bucket}"),
        };
        Self {
            uri,
            owner_id: record.owner_id().to_owned(),
            generation: record.generation(),
            reason,
        }
    }
}

/// Operator guidance for retained S3 owners. It names only public owner
/// metadata and bucket-relative scopes, never credentials or cursor values.
struct RetainedOwnership(Vec<RetainedOwner>);

impl std::fmt::Display for RetainedOwnership {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "S3 bucket ownership was retained; every other writing command on the bucket \
             fails with \"bucket ownership is held\" until it is released",
        )?;
        for owner in &self.0 {
            let RetainedOwner {
                uri,
                owner_id,
                generation,
                reason,
            } = owner;
            write!(
                f,
                "\n- {uri}: owner {owner_id}, generation {generation}, kept because "
            )?;
            match reason {
                RetainReason::UncertainMutation => f.write_str(
                    "a request to this bucket had an uncertain outcome (it timed out, lost its \
                     acknowledgement, was interrupted, or its result could not be verified) and \
                     may still take effect",
                )?,
                RetainReason::ReleaseFailed(error) => write!(
                    f,
                    "releasing the owner record failed ({error}); every data and control request \
                     had a definite outcome, so only the owner record is in doubt"
                )?,
                RetainReason::Unfinished => f.write_str(
                    "the command ended without proving that every request it sent had finished \
                     (a panic, an interrupted run, or a command that keeps ownership after any \
                     error)",
                )?,
            }
            write!(
                f,
                ".\n  Inspect: fireparq recovery status {uri}\n  Release: fireparq recovery \
                 release {uri} --expected-owner {owner_id} --expected-generation {generation} \
                 --stopped-writer-evidence <reference> --provider-quiescence-evidence <reference>"
            )?;
        }
        f.write_str(
            "\nRun the release only after confirming that this process has exited and that the \
             provider has completed or permanently revoked every request it sent; elapsed time \
             or process exit alone is not that evidence. Use the same AWS credential and \
             endpoint settings. The next build then recovers any pending transaction before it \
             streams.",
        )
    }
}

impl Drop for DatasetOwnership {
    fn drop(&mut self) {
        if self.remote.is_empty() {
            return;
        }
        // Dropping a guard sends nothing: each record stays Owned.
        let retained = std::mem::take(&mut self.remote)
            .iter()
            .rev()
            .map(|(bucket, owner)| RetainedOwner::new(bucket, owner, RetainReason::Unfinished))
            .collect();
        tracing::warn!("{}", RetainedOwnership(retained));
    }
}

impl DatasetOwnership {
    pub async fn acquire(
        operation: &str,
        scopes: Vec<MutationScope>,
        aws: Option<&AwsConfig>,
    ) -> Result<Self> {
        Self::acquire_inner(operation, scopes, aws, None, TreeCheck::Walk).await
    }

    /// Protected CLI ingestion may stream large authenticated S3 parts. The
    /// output owner and its signer are constructed from the same native client.
    /// Ordinary maintenance and external cursor buckets retain existing policy.
    ///
    /// Unlike [`DatasetOwnership::acquire`], this does not walk the local
    /// mutation tree for nested symlinks, so a local `build` start does not
    /// read every directory of its dataset (#655). `build` refuses a symlink on
    /// each path it touches instead, component by component: part staging and
    /// publication, transaction recovery, the control records and the mirror.
    /// A new root must be empty, and merge-journal recovery walks the tree
    /// with its own symlink check.
    pub async fn acquire_for_ingestion(
        scopes: Vec<MutationScope>,
        aws: Option<&AwsConfig>,
        output: &str,
    ) -> Result<Self> {
        let bucket = if output.starts_with("s3://") {
            Some(crate::writer::parse_s3_url(output)?.0)
        } else {
            None
        };
        Self::acquire_inner("build", scopes, aws, bucket.as_deref(), TreeCheck::Paths).await
    }

    /// The nested-symlink check [`DatasetOwnership::acquire`] runs, for a
    /// command that acquired without it and is about to walk its tree.
    /// Returns the number of directories read.
    pub(crate) fn validate_local_trees(&self) -> Result<u64> {
        match &self.local {
            Some(local) => validate_local_mutation_trees(local),
            None => Ok(0),
        }
    }

    async fn acquire_inner(
        operation: &str,
        scopes: Vec<MutationScope>,
        aws: Option<&AwsConfig>,
        native_bucket: Option<&str>,
        tree_check: TreeCheck,
    ) -> Result<Self> {
        let mut local = Vec::new();
        let mut remote: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for scope in scopes {
            if crate::artifacts::is_control_path(&scope.path) {
                bail!("recovery and ownership controls cannot be ordinary mutation targets");
            }
            if scope.path.starts_with("s3://") {
                let (bucket, key) = crate::writer::parse_s3_url(&scope.path)?;
                // All clients here come from the same validated AwsConfig, so
                // equal bucket names share one store/owner rather than acquiring
                // the same bucket twice for output and its external cursor.
                remote.entry(bucket).or_default().push(key);
            } else {
                let path = PathBuf::from(&scope.path);
                let directory = if scope.file {
                    path.parent()
                        .filter(|parent| !parent.as_os_str().is_empty())
                        .unwrap_or(Path::new("."))
                        .to_owned()
                } else {
                    path
                };
                if let Ok(canonical) = std::fs::canonicalize(&directory) {
                    if crate::artifacts::is_control_path(&canonical.to_string_lossy()) {
                        bail!(
                            "recovery and ownership controls cannot be ordinary mutation targets"
                        );
                    }
                }
                local.push(directory);
            }
        }
        // Build clients and validate credentials/endpoint routing before taking
        // any persistent owner. Local scope reduction happens in one call.
        let mut clients = Vec::new();
        anyhow::ensure!(
            native_bucket.is_none_or(|bucket| remote.contains_key(bucket)),
            "native ingestion output is absent from ownership scopes"
        );
        for (bucket, scopes) in remote {
            let aws = aws.context("AWS configuration is required for remote ownership")?;
            let native = if native_bucket == Some(bucket.as_str()) {
                Some(crate::s3::upload::NativeS3Upload::new(aws, &bucket)?)
            } else {
                None
            };
            let client = match &native {
                Some(native) => native.object_store(),
                None => Arc::new(aws.build_s3_client_for_mutation(&bucket)?)
                    as Arc<dyn object_store::ObjectStore>,
            };
            clients.push((bucket.clone(), client, native, scopes));
        }
        let local = if local.is_empty() {
            None
        } else {
            Some(LocalOwnership::acquire_without_creation(&local)?)
        };
        if let (Some(local), TreeCheck::Walk) = (&local, tree_check) {
            validate_local_mutation_trees(local)?;
        }
        let mut ownership = Self {
            local,
            remote: BTreeMap::new(),
        };
        for (bucket, client, native, scopes) in clients {
            let acquired = match native {
                Some(native) => S3Ownership::acquire_native(native, operation, scopes).await,
                None => S3Ownership::acquire(client, operation, scopes).await,
            };
            let guard = match acquired {
                Ok(guard) => guard,
                Err(error) => {
                    // Earlier bucket acquisitions are resolved and have not
                    // performed data writes. Release those exact owners; if
                    // release cannot be proven, its record remains Owned.
                    let release = ownership.release_remote().await;
                    return match release {
                        Ok(()) => Err(error).context("acquiring all dataset ownership scopes"),
                        Err(release_error) => Err(error).context(format!(
                            "acquisition also left an unresolved prior owner: {release_error:#}"
                        )),
                    };
                }
            };
            ownership.remote.insert(bucket, guard);
        }
        Ok(ownership)
    }

    pub fn acquire_blocking(
        operation: &str,
        scopes: Vec<MutationScope>,
        aws: Option<&AwsConfig>,
    ) -> Result<Self> {
        if scopes.iter().all(|scope| !scope.path.starts_with("s3://")) {
            // This future has no remote await points. Local-only APIs remain
            // usable inside a current-thread runtime without blocking Tokio.
            return futures::executor::block_on(Self::acquire(operation, scopes, aws));
        }
        block_storage(Self::acquire(operation, scopes, aws))
    }

    #[cfg(test)]
    pub(crate) fn from_remote_for_test(bucket: &str, owner: S3Ownership) -> Self {
        Self {
            local: None,
            remote: BTreeMap::from([(bucket.to_owned(), owner)]),
        }
    }

    pub fn revalidate_local_paths(&self) -> Result<()> {
        if let Some(local) = &self.local {
            local.revalidate()?;
        }
        Ok(())
    }

    pub fn local(&self) -> Option<&LocalOwnership> {
        self.local.as_ref()
    }

    pub fn remote(&self, bucket: &str) -> Option<&S3Ownership> {
        self.remote.get(bucket)
    }

    pub fn mark_remote_mutations_uncertain(&self) {
        for guard in self.remote.values() {
            guard.mark_mutation_uncertain();
        }
    }

    /// Call only after every mutation and response is resolved. Errors and
    /// cancelled operations keep remote owners for explicit quiescent recovery.
    pub async fn release(mut self) -> Result<()> {
        self.release_remote().await
    }

    /// End the command holding this ownership and return its result.
    ///
    /// Success releases every remote owner, as `release` does. After a failure
    /// each remote owner is released only when its uncertainty latch is clear.
    /// That is provable: every S3 mutation site arms a drop guard that sets the
    /// latch unless the request's outcome was proven (a verified readback, or a
    /// 401/403 refusal), mutation clients make one attempt with transport
    /// retries disabled, and this call consumes the guard, so no borrowed
    /// request can still be running. A clear latch therefore means no request
    /// this command sent can take effect later, the condition a successful
    /// release relies on. Pending transaction state stays for the next owner's
    /// mandatory startup recovery, which rolls Writing back or Committed
    /// forward exactly as after an operator release.
    ///
    /// An owner whose latch is set, or whose release request fails, is kept.
    /// The returned error then wraps the command's error with each kept owner,
    /// why it was kept, and the exact `fireparq recovery status`/`recovery
    /// release` commands. Local OS locks are released when this returns.
    pub async fn finish<T>(mut self, result: Result<T>) -> Result<T> {
        let failed = result.is_err();
        let mut retained = Vec::new();
        while let Some((bucket, owner)) = self.remote.pop_last() {
            let summary = RetainedOwner::new(&bucket, &owner, RetainReason::UncertainMutation);
            // `release` checks the latch first and then sends nothing.
            match owner.release().await {
                Ok(()) if failed => tracing::info!(
                    bucket = %bucket,
                    "no request to this bucket had an uncertain outcome; released S3 bucket ownership after the failure"
                ),
                Ok(()) => {}
                Err(OwnershipError::DataMutationUncertain) => retained.push(summary),
                Err(error) => retained.push(RetainedOwner {
                    reason: RetainReason::ReleaseFailed(error),
                    ..summary
                }),
            }
        }
        match (result, retained.is_empty()) {
            (result, true) => result,
            (Ok(_), false) => Err(anyhow::anyhow!("{}", RetainedOwnership(retained))),
            (Err(error), false) => Err(error.context(RetainedOwnership(retained).to_string())),
        }
    }

    pub fn release_blocking(self) -> Result<()> {
        if self.remote.is_empty() {
            return Ok(());
        }
        block_storage(self.release())
    }

    async fn release_remote(&mut self) -> Result<()> {
        while let Some((_, owner)) = self.remote.pop_last() {
            owner.release().await.context(
                "releasing dataset ownership; inspect retained remote ownership before recovery",
            )?;
        }
        Ok(())
    }
}

/// How acquisition checks local mutation trees for nested symlinks.
#[derive(Clone, Copy)]
enum TreeCheck {
    /// Walk every directory now: commands that read or delete across the tree.
    Walk,
    /// The command checks each path it touches instead (`build`, #655).
    Paths,
}

/// Explicit scope aliases have already been canonicalized and locked. Nested
/// aliases would escape that graph, so fail before any command data mutation.
/// This directory-only walk does not read data file contents.
/// Returns the number of directories read.
fn validate_local_mutation_trees(ownership: &LocalOwnership) -> Result<u64> {
    let mut pending = ownership.roots().to_vec();
    let mut reads = 0;
    while let Some(directory) = pending.pop() {
        if !directory.exists() {
            continue;
        }
        reads += 1;
        for entry in
            std::fs::read_dir(&directory).context("inspecting a guarded local mutation tree")?
        {
            let entry = entry.context("inspecting a guarded local mutation entry")?;
            let kind = entry
                .file_type()
                .context("inspecting a guarded entry type")?;
            if kind.is_symlink() {
                bail!("nested symlinks are unsupported inside a guarded mutation tree; use an explicit command-root alias or relocate the nested target");
            }
            if kind.is_dir() {
                pending.push(entry.path());
            }
        }
    }
    Ok(reads)
}

/// This sync bridge never panics in a current-thread runtime. Async callers can
/// use `acquire`/`release` directly instead of blocking their executor.
fn block_storage<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        Ok(_) => bail!("synchronous ownership operations require a multi-thread runtime; use the asynchronous ownership API"),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("creating ownership storage runtime")?
            .block_on(future),
    }
}

#[cfg(test)]
mod finish_tests;

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use super::*;

    #[test]
    fn combined_local_output_and_cursor_parents_share_one_guard() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output");
        let ownership = DatasetOwnership::acquire_blocking(
            "test",
            vec![
                MutationScope::directory(output.to_string_lossy()),
                MutationScope::file(output.join("state/cursor.parquet").to_string_lossy()),
            ],
            None,
        )
        .unwrap();
        assert_eq!(ownership.local().unwrap().roots().len(), 1);
        assert!(
            !output.exists(),
            "guard must not create output before validation"
        );
        assert!(LocalOwnership::acquire(&[output.clone()]).is_err());
        std::fs::create_dir_all(output.join("state")).unwrap();
        ownership.revalidate_local_paths().unwrap();
        assert!(LocalOwnership::acquire(&[output]).is_err());
        ownership.release_blocking().unwrap();
    }

    #[test]
    fn direct_control_targets_are_refused() {
        assert!(DatasetOwnership::acquire_blocking(
            "test",
            vec![MutationScope::directory("./.fireparq-ingest")],
            None
        )
        .is_err());
        assert!(DatasetOwnership::acquire_blocking(
            "test",
            vec![MutationScope::file("s3://bucket/.fireparq-owner-v1.json")],
            None
        )
        .is_err());
    }

    #[test]
    fn nested_symlinks_fail_before_mutation_but_explicit_root_aliases_work() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("output");
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&output).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let alias = dir.path().join("alias");
        symlink(&output, &alias).unwrap();
        let nested = output.join("table");
        symlink(&outside, &nested).unwrap();
        let error = DatasetOwnership::acquire_blocking(
            "test",
            vec![MutationScope::directory(alias.to_string_lossy())],
            None,
        )
        .err()
        .unwrap();
        assert!(error.to_string().contains("nested symlinks"));
        assert!(std::fs::read_dir(&outside).unwrap().next().is_none());
        std::fs::remove_file(nested).unwrap();
        let ownership = DatasetOwnership::acquire_blocking(
            "test",
            vec![MutationScope::directory(alias.to_string_lossy())],
            None,
        )
        .unwrap();
        ownership.release_blocking().unwrap();
    }

    #[test]
    fn deferred_scope_detects_alias_retargeting_before_publication() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("first");
        let second = dir.path().join("second");
        std::fs::create_dir(&first).unwrap();
        std::fs::create_dir(&second).unwrap();
        let alias = dir.path().join("alias");
        symlink(&first, &alias).unwrap();
        let owner = DatasetOwnership::acquire_blocking(
            "test",
            vec![MutationScope::directory(
                alias.join("new").to_string_lossy(),
            )],
            None,
        )
        .unwrap();
        assert!(!first.join("new").exists());
        // Simulate a non-cooperating external actor; cooperating commands are
        // excluded by the alias-parent/target locks.
        std::fs::remove_file(&alias).unwrap();
        symlink(&second, &alias).unwrap();
        assert!(owner.revalidate_local_paths().is_err());
        assert!(!second.join("new").exists());
    }

    #[tokio::test]
    async fn current_thread_sync_bridge_is_an_error_not_a_panic() {
        let result = DatasetOwnership::acquire_blocking(
            "test",
            vec![MutationScope::directory("s3://bucket/data")],
            None,
        );
        assert!(result.is_err());
    }
}
