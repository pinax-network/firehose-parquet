//! Where a dataset's Delta tables live, and the log store of each one
//! (#643 L3, `docs/design/delta-lake.md` §1.3, §1.6).
//!
//! Every table of a dataset is one Delta table at `<dataset root>/<table>/`,
//! with its log in `<table>/_delta_log/`. The log store is delta-rs's
//! `DefaultLogStore`, which commits each version with a conditional create
//! (`PutMode::Create`):
//!
//! - **Local disk:** a no-clobber hard link. fireparq then syncs the new
//!   commit file and its directories ([`DeltaStore::sync_commit`]), so a
//!   commit is durable before authority advances past it.
//! - **S3:** `If-None-Match: *` (`S3ConditionalPut::ETagMatch`), through an
//!   object_store 0.13 client of its own ([`s3_log_client`]), built from the
//!   same [`AwsConfig`] as fireparq's other clients, with **one attempt per
//!   request**: object_store resends even a conditional PUT on 5xx, so a
//!   commit whose response is lost could come back as a 412, be read as a
//!   lost race and be committed again at the next version. With one attempt
//!   the outcome surfaces as an error instead, and the `txn` action resolves
//!   it (design §3.5). Idempotent reads (`GET`, `HEAD`: log objects,
//!   checkpoints, listings) are the exception: [`read_retry`] sends them
//!   again, a bounded number of times, after a transient failure, below
//!   object_store, while every write still goes out once (#680).
//!
//! Without `deltalake-aws` (and its AWS SDK) delta-rs has no log store for
//! `s3://` URLs; [`DeltaStore::log_store`] registers the default one.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Once};
use std::time::Duration;

use anyhow::{ensure, Context, Result};
use deltalake_core::logstore::{
    default_logstore, logstore_factories, logstore_with, LogStore, LogStoreFactory, LogStoreRef,
    StorageConfig,
};
use object_store_delta::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store_delta::local::LocalFileSystem;
use object_store_delta::{ClientOptions, ObjectStore, RetryConfig};
use url::Url;

use crate::s3::AwsConfig;

mod read_retry;

pub use read_retry::READ_ATTEMPTS;

/// Per-request timeout of the log store's S3 client. A Delta commit or log
/// read is a small object; the same bound as fireparq's part requests.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// The storage of one dataset's Delta tables.
#[derive(Clone)]
pub enum DeltaStore {
    /// A local dataset root: an absolute, canonical directory.
    Local { root: PathBuf },
    /// A bucket (or any object store addressed from the bucket root) and the
    /// dataset prefix inside it; `url` is `s3://<bucket>/<prefix>/`.
    Remote {
        url: Url,
        prefix: String,
        store: Arc<dyn ObjectStore>,
    },
}

impl std::fmt::Debug for DeltaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local { .. } => f.write_str("DeltaStore::Local"),
            Self::Remote { url, .. } => write!(f, "DeltaStore::Remote({url})"),
        }
    }
}

impl DeltaStore {
    /// The tables of a local dataset root, which must be absolute.
    pub fn local(root: &Path) -> Result<Self> {
        ensure!(root.is_absolute(), "a Delta dataset root must be absolute");
        Ok(Self::Local {
            root: root.to_path_buf(),
        })
    }

    /// The tables of the dataset at `s3://<bucket>/<prefix>`, through `store`,
    /// a client of that bucket's root (usually [`s3_log_client`]).
    pub fn s3(bucket: &str, prefix: &str, store: Arc<dyn ObjectStore>) -> Result<Self> {
        let prefix = prefix.trim_matches('/');
        crate::ingest::state::validate_relative_path(prefix, true)?;
        let base = if prefix.is_empty() {
            format!("s3://{bucket}/")
        } else {
            format!("s3://{bucket}/{prefix}/")
        };
        Ok(Self::Remote {
            url: Url::parse(&base).context("building the dataset's s3:// URL")?,
            prefix: prefix.to_string(),
            store,
        })
    }

    /// The URL of `table`'s Delta table: `<root>/<table>/`.
    pub fn table_url(&self, table: &str) -> Result<Url> {
        match self {
            Self::Local { root } => Url::from_directory_path(root.join(table))
                .map_err(|()| anyhow::anyhow!("local Delta table path is not absolute")),
            Self::Remote { url, .. } => url
                .join(&format!("{table}/"))
                .context("building a Delta table URL"),
        }
    }

    /// The log store of `table`.
    pub fn log_store(&self, table: &str) -> Result<LogStoreRef> {
        let url = self.table_url(table)?;
        let root: Arc<dyn ObjectStore> = match self {
            Self::Local { .. } => Arc::new(LocalFileSystem::new()),
            Self::Remote { store, .. } => {
                register_s3_log_store();
                Arc::clone(store)
            }
        };
        logstore_with(root, &url, StorageConfig::default())
            .with_context(|| format!("opening the Delta log store of table `{table}`"))
    }

    /// Whether a local table has no `_delta_log/` directory at all, which
    /// delta-rs reports as an invalid location rather than as a missing
    /// table. Remote tables answer `false`: their absence is found by opening.
    /// Like every path `build` touches, the table directory and its log must
    /// not be symlinks.
    pub(crate) fn lacks_local_log(&self, table: &str) -> Result<bool> {
        let Self::Local { root } = self else {
            return Ok(false);
        };
        let table_dir = root.join(table);
        for (path, what) in [
            (&table_dir, "directory"),
            (&table_dir.join("_delta_log"), "`_delta_log`"),
        ] {
            match std::fs::symlink_metadata(path) {
                Ok(metadata) => ensure!(
                    metadata.is_dir(),
                    "table `{table}`'s {what} is not a directory (symlinks are refused)"
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("inspecting table `{table}`'s Delta log"))
                }
            }
        }
        Ok(false)
    }

    /// Makes a local commit durable: the commit file of `version`, its
    /// `_delta_log/` directory and, when `created`, the table directory and
    /// the dataset root that link them. Remote commits are durable once their
    /// PUT succeeded.
    pub(crate) fn sync_commit(&self, table: &str, version: u64, created: bool) -> Result<()> {
        let Self::Local { root } = self else {
            return Ok(());
        };
        let table_dir = root.join(table);
        let log = table_dir.join("_delta_log");
        sync(&log.join(format!("{version:020}.json")))?;
        sync(&log)?;
        if created {
            sync(&table_dir)?;
            sync(root)?;
        }
        Ok(())
    }

    /// Makes a local table's log tail durable: every commit file after the
    /// checkpoint `checkpoint` (all of them without one) and its `_delta_log/`
    /// directory. Recovery calls it for a table whose log already holds a
    /// pending transaction, which a process that died after the commit but
    /// before [`Self::sync_commit`] left possibly unsynced.
    pub(crate) fn sync_log_tail(&self, table: &str, checkpoint: Option<u64>) -> Result<()> {
        let Self::Local { root } = self else {
            return Ok(());
        };
        let log = root.join(table).join("_delta_log");
        for entry in std::fs::read_dir(&log).context("listing a local Delta log")? {
            let entry = entry.context("listing a local Delta log")?;
            let name = entry.file_name();
            let version = name
                .to_str()
                .and_then(|name| name.strip_suffix(".json"))
                .filter(|stem| stem.len() == 20)
                .and_then(|stem| stem.parse::<u64>().ok());
            if version.is_some_and(|version| checkpoint.is_none_or(|last| version > last)) {
                sync(&entry.path())?;
            }
        }
        sync(&log)
    }
}

fn sync(path: &Path) -> Result<()> {
    std::fs::File::open(path)
        .and_then(|file| file.sync_all())
        .context("syncing a local Delta commit")
}

/// delta-rs's `DefaultLogStore` (conditional-put commits) for `s3://` URLs.
struct ConditionalPutLogStoreFactory;

impl LogStoreFactory for ConditionalPutLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: Arc<dyn ObjectStore>,
        root_store: Arc<dyn ObjectStore>,
        location: &Url,
        options: &StorageConfig,
    ) -> deltalake_core::DeltaResult<Arc<dyn LogStore>> {
        Ok(default_logstore(
            prefixed_store,
            root_store,
            location,
            options,
        ))
    }
}

fn register_s3_log_store() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        logstore_factories().insert(
            Url::parse("s3://").expect("static scheme"),
            Arc::new(ConditionalPutLogStoreFactory),
        );
    });
}

/// The log store's S3 client for `bucket`: object_store 0.13 (delta-rs's
/// major), one attempt per write, up to [`READ_ATTEMPTS`] per idempotent
/// read, conditional creates with `If-None-Match: *`, and the credentials,
/// region and endpoint of `aws`. Without an access key it uses the AWS
/// provider chain, like fireparq's other ingestion clients.
pub fn s3_log_client(aws: &AwsConfig, bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    Ok(Arc::new(
        s3_log_builder(aws, bucket)?
            .build()
            .with_context(|| format!("building the Delta log client for bucket {bucket}"))?,
    ))
}

/// [`s3_builder`] with the log client's read retries
/// ([`read_retry::ReadRetryConnector`]).
pub(crate) fn s3_log_builder(aws: &AwsConfig, bucket: &str) -> Result<AmazonS3Builder> {
    Ok(s3_builder(aws, bucket)?.with_http_connector(read_retry::ReadRetryConnector))
}

/// An object_store 0.13 S3 builder for `bucket`, from `aws`, with conditional
/// creates and object_store's own retries off for every request (writes
/// included).
pub(crate) fn s3_builder(aws: &AwsConfig, bucket: &str) -> Result<AmazonS3Builder> {
    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .with_retry(RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        })
        .with_client_options(ClientOptions::new().with_timeout(REQUEST_TIMEOUT));
    if let Some(key) = &aws.aws_access_key_id {
        builder = builder.with_access_key_id(key);
    }
    if let Some(secret) = &aws.aws_secret_access_key {
        builder = builder.with_secret_access_key(secret);
    }
    if let Some(token) = &aws.aws_session_token {
        builder = builder.with_token(token);
    }
    if let Some(region) = &aws.aws_region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &aws.aws_endpoint_url {
        builder = builder
            .with_endpoint(endpoint)
            .with_virtual_hosted_style_request(crate::s3::endpoint_is_bucket_bound(
                endpoint, bucket,
            )?);
    }
    Ok(builder)
}

/// Test support: one in-memory Delta store per fireparq test bucket.
///
/// Tests that stand an in-memory (object_store 0.12) store in for a bucket
/// get, through [`crate::dataset_lock::DatasetOwnership::from_remote_for_test`],
/// the same in-memory Delta store for as long as that bucket store lives, so a
/// dataset reopened by a new owner finds its tables.
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Arc, Mutex, Weak};

    use object_store_delta::memory::InMemory;

    type Entry = (Weak<dyn object_store::ObjectStore>, Arc<InMemory>);
    static STORES: Mutex<Vec<Entry>> = Mutex::new(Vec::new());

    pub(crate) fn memory_for(bucket: &Arc<dyn object_store::ObjectStore>) -> Arc<InMemory> {
        let mut stores = STORES.lock().unwrap();
        stores.retain(|(weak, _)| weak.strong_count() > 0);
        // Compare the data pointers: the vtable part of a `dyn` pointer may
        // differ between codegen units.
        let key = Arc::as_ptr(bucket).cast::<()>();
        if let Some((_, store)) = stores.iter().find(|(weak, _)| {
            weak.upgrade()
                .is_some_and(|live| Arc::as_ptr(&live).cast::<()>() == key)
        }) {
            return Arc::clone(store);
        }
        let store = Arc::new(InMemory::new());
        stores.push((Arc::downgrade(bucket), Arc::clone(&store)));
        store
    }
}
