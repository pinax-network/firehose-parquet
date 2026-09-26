//! Read-only observation of protected datasets, for `verify`, which never
//! mutates table data. Nothing here acquires ownership, recovers a pending
//! transaction or reconciles a cursor mirror: it reads the authoritative state
//! record as it is. Writers replace that record atomically (a local rename or
//! one S3 object write), so a read sees one complete revision.

use anyhow::{bail, ensure, Context, Result};
use futures::StreamExt;
use object_store::ObjectStore;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::time::Duration;

use super::state::AuthorityState;
use crate::cli::block_on_async;
use crate::durable_state::{ControlKey, CONTROL_DIRECTORY, MAX_CONTROL_BYTES};

const REMOTE_TIMEOUT: Duration = Duration::from_secs(60);

/// Whether the local directory `root` holds a protected dataset marker.
pub(crate) fn local_marker(root: &Path) -> Result<bool> {
    match fs::symlink_metadata(root.join(CONTROL_DIRECTORY)) {
        Ok(meta) => {
            ensure!(
                meta.is_dir() && !meta.file_type().is_symlink(),
                "protected control marker in {} is not a regular directory",
                root.display()
            );
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).with_context(|| {
            format!(
                "inspecting the protected control marker in {}",
                root.display()
            )
        }),
    }
}

/// Reads and validates the authoritative state of the local protected dataset
/// at `root`; `None` when it has none.
pub(crate) fn read_local_authority(root: &Path) -> Result<Option<AuthorityState>> {
    let path = root
        .join(CONTROL_DIRECTORY)
        .join(ControlKey::State.filename());
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("reading authoritative state metadata"),
    };
    ensure!(
        metadata.file_type().is_file(),
        "authoritative state {} is not a regular file",
        path.display()
    );
    let mut bytes = Vec::new();
    fs::File::open(&path)
        .and_then(|file| {
            file.take((MAX_CONTROL_BYTES + 1) as u64)
                .read_to_end(&mut bytes)
        })
        .with_context(|| format!("reading {}", path.display()))?;
    decode_authority(&bytes).map(Some)
}

fn remote_marker_key(prefix: &str) -> String {
    if prefix.is_empty() {
        CONTROL_DIRECTORY.to_owned()
    } else {
        format!("{prefix}/{CONTROL_DIRECTORY}")
    }
}

/// Whether the S3 prefix holds a protected dataset marker.
pub(crate) fn remote_marker(store: &dyn ObjectStore, prefix: &str) -> Result<bool> {
    let marker = remote_marker_key(prefix);
    let location = object_store::path::Path::from(marker.as_str());
    block_on_async(async {
        tokio::time::timeout(REMOTE_TIMEOUT, async {
            let mut objects = store.list(Some(&location));
            while let Some(object) = objects.next().await {
                let object = object
                    .map_err(|_| anyhow::anyhow!("listing protected control markers failed"))?;
                let key = object.location.as_ref();
                if key == marker || key.starts_with(&format!("{marker}/")) {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .await
        .map_err(|_| anyhow::anyhow!("protected marker discovery timed out"))?
    })
}

/// Reads and validates the authoritative state of the protected dataset at
/// an S3 prefix; `None` when it has none.
pub(crate) fn read_remote_authority(
    store: &dyn ObjectStore,
    prefix: &str,
) -> Result<Option<AuthorityState>> {
    let key = format!(
        "{}/{}",
        remote_marker_key(prefix),
        ControlKey::State.filename()
    );
    let location = object_store::path::Path::from(key.as_str());
    let bytes = block_on_async(async {
        tokio::time::timeout(REMOTE_TIMEOUT, async {
            let object = match store.get(&location).await {
                Ok(object) => object,
                Err(object_store::Error::NotFound { .. }) => return Ok(None),
                Err(_) => bail!("reading the authoritative state failed"),
            };
            ensure!(
                object.meta.size <= MAX_CONTROL_BYTES as u64,
                "authoritative state exceeds the size limit"
            );
            let mut stream = object.into_stream();
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk =
                    chunk.map_err(|_| anyhow::anyhow!("reading the authoritative state failed"))?;
                ensure!(
                    bytes.len().saturating_add(chunk.len()) <= MAX_CONTROL_BYTES,
                    "authoritative state exceeds the size limit"
                );
                bytes.extend_from_slice(&chunk);
            }
            Ok(Some(bytes))
        })
        .await
        .map_err(|_| anyhow::anyhow!("reading the authoritative state timed out"))?
    })?;
    bytes.map(|bytes| decode_authority(&bytes)).transpose()
}

fn decode_authority(bytes: &[u8]) -> Result<AuthorityState> {
    let state = crate::durable_state::decode::<AuthorityState>(bytes)
        .context("validating the authoritative state")?
        .payload;
    state.validate()?;
    Ok(state)
}

/// The nearest protected dataset root holding `path` (a local file path or
/// `s3://bucket/key`), searching its ancestor directories or prefixes.
pub(crate) fn protected_ancestor(
    path: &str,
    store: Option<&dyn ObjectStore>,
) -> Result<Option<String>> {
    if let Some(url) = path.strip_prefix("s3://") {
        let store = store.context("an S3 client is required to inspect S3 paths")?;
        let (bucket, key) = url.split_once('/').unwrap_or((url, ""));
        let mut ancestors: Vec<&str> = vec![""];
        let mut end = 0;
        while let Some(offset) = key[end..].find('/') {
            end += offset;
            ancestors.push(&key[..end]);
            end += 1;
        }
        for ancestor in ancestors.into_iter().rev() {
            if remote_marker(store, ancestor)? {
                return Ok(Some(if ancestor.is_empty() {
                    format!("s3://{bucket}")
                } else {
                    format!("s3://{bucket}/{ancestor}")
                }));
            }
        }
        return Ok(None);
    }
    let absolute = std::path::absolute(path)?;
    let Some(existing) = absolute.ancestors().skip(1).find(|dir| dir.is_dir()) else {
        return Ok(None);
    };
    for ancestor in fs::canonicalize(existing)?.ancestors() {
        if local_marker(ancestor)? {
            return Ok(Some(ancestor.to_string_lossy().into_owned()));
        }
    }
    Ok(None)
}
