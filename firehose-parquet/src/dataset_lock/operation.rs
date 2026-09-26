//! Collect all mutation scopes before acquiring any operation capability.

use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::cli::AwsConfig;
use crate::dataset_lock_s3::S3Ownership;

use super::LocalOwnership;

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

/// Owns all local scopes and remote buckets for one command. A successful caller
/// must explicitly release it. Dropping it after an error/cancellation releases
/// local OS locks but deliberately retains remote Owned records.
pub struct DatasetOwnership {
    local: Option<LocalOwnership>,
    remote: BTreeMap<String, S3Ownership>,
}

impl DatasetOwnership {
    pub async fn acquire(
        operation: &str,
        scopes: Vec<MutationScope>,
        aws: Option<&AwsConfig>,
    ) -> Result<Self> {
        Self::acquire_inner(operation, scopes, aws, None).await
    }

    /// Protected CLI ingestion may stream large authenticated S3 parts. The
    /// output owner and its signer are constructed from the same native client.
    /// Ordinary maintenance and external cursor buckets retain existing policy.
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
        Self::acquire_inner("build", scopes, aws, bucket.as_deref()).await
    }

    async fn acquire_inner(
        operation: &str,
        scopes: Vec<MutationScope>,
        aws: Option<&AwsConfig>,
        native_bucket: Option<&str>,
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
        if let Some(local) = &local {
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

/// Explicit scope aliases have already been canonicalized and locked. Nested
/// aliases would escape that graph, so fail before any command data mutation.
/// This directory-only walk does not read data file contents.
fn validate_local_mutation_trees(ownership: &LocalOwnership) -> Result<()> {
    let mut pending = ownership.roots().to_vec();
    while let Some(directory) = pending.pop() {
        if !directory.exists() {
            continue;
        }
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
    Ok(())
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
