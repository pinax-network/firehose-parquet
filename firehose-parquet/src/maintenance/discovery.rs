//! Shared read-only discovery mechanics with explicit existing command policies.
//!
//! Local traversal deliberately uses native paths/read_dir, not ObjectStore's
//! LocalFileSystem::list: the latter changes symlink, non-UTF8 and error behavior.
//! Sorting, direct-file selection, relative labels and reserved-artifact filtering
//! remain with callers. Nothing here creates clients, owns reservations or mutates.
use futures::TryStreamExt;
use object_store::{path::Path as ObjectPath, ObjectMeta, ObjectStore};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy)]
enum Selection<'a> {
    LowercaseParquet,
    AsciiInsensitiveParquet,
    Name(&'a str),
}

/// Policies correspond to actual differences in the existing native walkers.
#[derive(Clone, Copy)]
pub(crate) struct LocalPolicy<'a> {
    skip_non_directory: bool,
    prune_controls: bool,
    selection: Selection<'a>,
}

impl LocalPolicy<'_> {
    /// Rollup and CLI scan/validate: read_dir errors propagate, including bad roots.
    pub(crate) const PARQUET: Self = Self {
        skip_non_directory: false,
        prune_controls: false,
        selection: Selection::LowercaseParquet,
    };
    /// Merge/truncate: absent or non-directory roots are empty; control trees prune.
    pub(crate) const MUTATION_PARQUET: Self = Self {
        skip_non_directory: true,
        prune_controls: true,
        selection: Selection::LowercaseParquet,
    };
    /// Verify alone accepts ASCII case-insensitive local file extensions.
    pub(crate) const VERIFY_PARQUET: Self = Self {
        selection: Selection::AsciiInsensitiveParquet,
        ..Self::PARQUET
    };
}

impl<'a> LocalPolicy<'a> {
    /// Merge journal discovery: the mutation walk selecting one exact file name.
    pub(crate) fn merge_journals(name: &'a str) -> Self {
        Self {
            selection: Selection::Name(name),
            ..Self::MUTATION_PARQUET
        }
    }
}

/// Append native traversal results without sorting or path normalization.
/// Preserve the original is_dir behavior: follow directory symlinks and select
/// extension-matching non-directories (including broken links), not only files.
pub(crate) fn collect_local(
    directory: &Path,
    policy: LocalPolicy<'_>,
    out: &mut Vec<PathBuf>,
) -> std::io::Result<()> {
    if policy.skip_non_directory && !directory.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(directory)? {
        let path = entry?.path();
        if policy.prune_controls && crate::artifacts::is_control_path(&path.to_string_lossy()) {
            continue;
        }
        if path.is_dir() {
            collect_local(&path, policy, out)?;
        } else {
            let selected = match policy.selection {
                Selection::LowercaseParquet => path.extension().is_some_and(|ext| ext == "parquet"),
                Selection::AsciiInsensitiveParquet => path
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet")),
                Selection::Name(name) => path.file_name().is_some_and(|file| file == name),
            };
            if selected {
                out.push(path);
            }
        }
    }
    Ok(())
}

/// Raw backend listing, preserving returned order and errors. Prefix boundaries,
/// exact-object HEAD fallback, extension/reserved filtering and sorting are caller
/// policies, as are client credentials and retry configuration.
pub(crate) async fn list_objects(
    store: &dyn ObjectStore,
    prefix: &str,
) -> object_store::Result<Vec<ObjectMeta>> {
    let prefix = (!prefix.is_empty()).then(|| ObjectPath::from(prefix));
    store.list(prefix.as_ref()).try_collect().await
}

/// `key` relative to the listed `prefix`, without a leading `/`. A key outside the
/// prefix is returned unchanged.
pub(crate) fn relative_key<'a>(prefix: &str, key: &'a str) -> &'a str {
    key.strip_prefix(prefix)
        .map(|s| s.trim_start_matches('/'))
        .unwrap_or(key)
}

/// Existing unversioned whole-object read used by scan/inspect/validate only.
/// Do not use this for merge's reserved windows, rollup's pinned ranges, or
/// verify's ordered prefetch: those callers own materially different contracts.
pub(crate) async fn read_object_bytes(
    store: &dyn ObjectStore,
    location: &ObjectPath,
) -> object_store::Result<bytes::Bytes> {
    store.get(location).await?.bytes().await
}

#[cfg(test)]
mod tests;
