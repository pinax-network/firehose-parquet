//! Storage for the spike: local disk, an in-memory store, or loopback S3.
//!
//! Each [`Lake`] is one root (a local directory, an in-memory store or an S3
//! bucket) holding one Delta table per fireparq table at `<root>/<table>/`.
//! Parts are published with a conditional create, as fireparq does; the Delta
//! log store sits on the same object store.

use std::collections::HashMap;
use std::sync::{Arc, Once};
use std::time::Duration;

use deltalake_core::logstore::{
    default_logstore, logstore_factories, LogStore, LogStoreFactory, LogStoreRef, StorageConfig,
};
use deltalake_core::{DeltaResult, DeltaTableBuilder};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, RetryConfig};
use url::Url;

/// Delta's `DefaultLogStore` (conditional-put commits) for `s3://` URLs, without
/// the `deltalake-aws` crate and its AWS SDK dependency tree.
struct ConditionalPutLogStoreFactory;

impl LogStoreFactory for ConditionalPutLogStoreFactory {
    fn with_options(
        &self,
        prefixed_store: Arc<dyn ObjectStore>,
        root_store: Arc<dyn ObjectStore>,
        location: &Url,
        options: &StorageConfig,
    ) -> DeltaResult<Arc<dyn LogStore>> {
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
            Url::parse("s3://").expect("scheme"),
            Arc::new(ConditionalPutLogStoreFactory),
        );
    });
}

/// Loopback S3 settings. Only a `127.0.0.1`/`localhost` endpoint is accepted.
#[derive(Clone, Debug)]
pub struct S3Settings {
    pub endpoint: String,
    pub bucket: String,
}

impl S3Settings {
    /// Reads `DELTA_SPIKE_S3_ENDPOINT` and `DELTA_SPIKE_S3_BUCKET`.
    pub fn from_env() -> Option<Self> {
        let endpoint = std::env::var("DELTA_SPIKE_S3_ENDPOINT").ok()?;
        let bucket =
            std::env::var("DELTA_SPIKE_S3_BUCKET").unwrap_or_else(|_| "delta-spike".into());
        Some(Self { endpoint, bucket })
    }

    fn check_loopback(&self) {
        let url = Url::parse(&self.endpoint).expect("S3 endpoint URL");
        let host = url.host_str().unwrap_or_default();
        assert!(
            host == "127.0.0.1" || host == "localhost",
            "the spike only talks to a loopback S3 endpoint, not {host}"
        );
    }
}

/// Where a lake lives.
#[derive(Clone)]
pub enum Lake {
    Local(std::path::PathBuf),
    Memory(Arc<InMemory>),
    S3 {
        settings: S3Settings,
        prefix: String,
        store: Arc<dyn ObjectStore>,
    },
}

impl Lake {
    pub fn local(dir: &std::path::Path) -> Self {
        std::fs::create_dir_all(dir).expect("lake directory");
        Lake::Local(dir.canonicalize().expect("canonical lake directory"))
    }

    pub fn memory() -> Self {
        Lake::Memory(Arc::new(InMemory::new()))
    }

    /// A loopback S3 lake under `prefix` in the configured bucket.
    ///
    /// The client makes a single attempt per request (`max_retries: 0`), as
    /// fireparq's mutation clients do: a Delta commit whose response is lost must
    /// surface as an error, not be resent and misread as a lost race.
    pub fn s3(settings: S3Settings, prefix: &str) -> Self {
        settings.check_loopback();
        let store = AmazonS3Builder::new()
            .with_endpoint(&settings.endpoint)
            .with_bucket_name(&settings.bucket)
            .with_region("us-east-1")
            .with_access_key_id("spike")
            .with_secret_access_key("spike")
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .with_retry(RetryConfig {
                max_retries: 0,
                retry_timeout: Duration::from_secs(10),
                ..RetryConfig::default()
            })
            .build()
            .expect("loopback S3 client");
        Lake::S3 {
            settings,
            prefix: prefix.trim_matches('/').to_string(),
            store: Arc::new(store),
        }
    }

    /// The URL of one table.
    pub fn table_url(&self, table: &str) -> Url {
        match self {
            Lake::Local(dir) => Url::from_directory_path(dir.join(table)).expect("file URL"),
            Lake::Memory(_) => Url::parse(&format!("memory:///{table}/")).expect("memory URL"),
            Lake::S3 {
                settings, prefix, ..
            } => {
                Url::parse(&format!("s3://{}/{prefix}/{table}/", settings.bucket)).expect("s3 URL")
            }
        }
    }

    /// The object store addressing the root of the storage (bucket or filesystem).
    pub fn root_store(&self) -> Arc<dyn ObjectStore> {
        match self {
            Lake::Local(_) => Arc::new(LocalFileSystem::new()),
            Lake::Memory(store) => store.clone(),
            Lake::S3 { store, .. } => store.clone(),
        }
    }

    /// The object path of `relative` inside a table.
    pub fn object_path(&self, table: &str, relative: &str) -> Path {
        match self {
            Lake::Local(dir) => {
                Path::from_absolute_path(dir.join(table).join(relative)).expect("local path")
            }
            Lake::Memory(_) => Path::from(format!("{table}/{relative}")),
            Lake::S3 { prefix, .. } => Path::from(format!("{prefix}/{table}/{relative}")),
        }
    }

    /// The Delta log store of one table.
    pub fn log_store(&self, table: &str) -> LogStoreRef {
        let url = self.table_url(table);
        match self {
            Lake::Local(dir) => {
                std::fs::create_dir_all(dir.join(table)).expect("table directory");
                DeltaTableBuilder::from_url(url)
                    .expect("table URL")
                    .build_storage()
                    .expect("local log store")
            }
            Lake::Memory(_) | Lake::S3 { .. } => {
                register_s3_log_store();
                DeltaTableBuilder::from_url(url.clone())
                    .expect("table URL")
                    .with_storage_backend(self.root_store(), url)
                    .with_storage_options(HashMap::new())
                    .build_storage()
                    .expect("log store")
            }
        }
    }

    /// Publishes one complete part with a conditional create (never an overwrite).
    pub async fn publish_part(&self, table: &str, relative: &str, bytes: Vec<u8>) {
        self.try_publish_part(table, relative, bytes)
            .await
            .unwrap_or_else(|e| panic!("publish {table}/{relative}: {e}"));
    }

    /// Like [`Lake::publish_part`], returning the store error (`AlreadyExists`
    /// when the name is taken).
    pub async fn try_publish_part(
        &self,
        table: &str,
        relative: &str,
        bytes: Vec<u8>,
    ) -> object_store::Result<()> {
        let path = self.object_path(table, relative);
        if let Lake::Local(dir) = self {
            let parent = dir.join(table).join(relative);
            std::fs::create_dir_all(parent.parent().expect("parent")).expect("part directory");
        }
        self.root_store()
            .put_opts(
                &path,
                PutPayload::from(bytes),
                PutOptions {
                    mode: PutMode::Create,
                    ..PutOptions::default()
                },
            )
            .await
            .map(|_| ())
    }

    /// Reads back the exact bytes of a published object.
    pub async fn read(&self, table: &str, relative: &str) -> Vec<u8> {
        let path = self.object_path(table, relative);
        self.root_store()
            .get(&path)
            .await
            .unwrap_or_else(|e| panic!("get {path}: {e}"))
            .bytes()
            .await
            .expect("body")
            .to_vec()
    }

    /// Whether an object exists.
    pub async fn exists(&self, table: &str, relative: &str) -> bool {
        let path = self.object_path(table, relative);
        match self.root_store().head(&path).await {
            Ok(_) => true,
            Err(object_store::Error::NotFound { .. }) => false,
            Err(e) => panic!("head {path}: {e}"),
        }
    }
}
