//! Equivalence of the shared walker with the six native walkers it replaced.
//! Each `legacy_*` function is frozen verbatim from `origin/main` 9372f99
//! (only its name and error type were adapted).
use super::*;
use std::collections::BTreeSet;

fn write(root: &Path, relative: &str) {
    let path = root.join(relative);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, relative).unwrap();
}

fn fixture(root: &Path) {
    for name in [
        "data.parquet",
        "UPPER.PARQUET",
        "mixed.Parquet",
        "notes.txt",
        "nested/part.parquet",
        "nested/_fireparq_merge.json",
        "nested/_fireparq_merge.json.parquet",
        "cursor.parquet",
        "partitions.parquet",
        "merkle_roots.parquet",
        "verify_runs/run/artifact.parquet",
        ".fireparq-ingest/hidden.parquet",
        ".fireparq-ingest/_fireparq_merge.json",
        ".fireparq-owner-probes-v1/probe.parquet",
        ".fireparq-owner-v1.json/record.parquet",
        ".fireparq-ingest-old/retained.parquet",
        "space %/part.parquet",
        "folder.parquet/child.parquet",
        "trailing.parquet.",
    ] {
        write(root, name);
    }
}

type Legacy = fn(&Path, &mut Vec<PathBuf>) -> std::io::Result<()>;

/// Every policy against its frozen walker(s): same error kind/message, same
/// append order and same selection (callers sort afterwards; order is native).
fn compare(root: &Path) {
    let cases: [(&str, LocalPolicy<'_>, Legacy); 5] = [
        ("merge", LocalPolicy::MUTATION_PARQUET, legacy_merge),
        ("truncate", LocalPolicy::MUTATION_PARQUET, legacy_truncate),
        ("rollup", LocalPolicy::PARQUET, legacy_rollup),
        ("cli", LocalPolicy::PARQUET, legacy_cli),
        ("verify", LocalPolicy::VERIFY_PARQUET, legacy_verify),
    ];
    for (name, policy, legacy) in cases {
        let mut expected = vec![PathBuf::from("already collected")];
        let mut actual = expected.clone();
        let before = legacy(root, &mut expected);
        let after = collect_local(root, policy, &mut actual);
        assert_eq!(
            before.as_ref().err().map(|e| (e.kind(), e.to_string())),
            after.as_ref().err().map(|e| (e.kind(), e.to_string())),
            "{name}"
        );
        assert_eq!(actual, expected, "{name}");
    }
    let mut expected = vec![];
    let mut actual = vec![];
    let before = legacy_named(root, "_fireparq_merge.json", &mut expected);
    let after = collect_local(
        root,
        LocalPolicy::merge_journals("_fireparq_merge.json"),
        &mut actual,
    );
    assert_eq!(
        before.err().map(|e| (e.kind(), e.to_string())),
        after.err().map(|e| (e.kind(), e.to_string()))
    );
    assert_eq!(actual, expected);
}

#[test]
fn native_policy_matches_frozen_walkers_and_explicit_inventory() {
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    compare(root.path());
    for policy in [
        LocalPolicy::PARQUET,
        LocalPolicy::MUTATION_PARQUET,
        LocalPolicy::VERIFY_PARQUET,
    ] {
        let mut paths = vec![];
        collect_local(root.path(), policy, &mut paths).unwrap();
        let selected: BTreeSet<_> = paths
            .iter()
            .map(|p| p.strip_prefix(root.path()).unwrap().to_str().unwrap())
            .collect();
        for reserved in [
            "cursor.parquet",
            "partitions.parquet",
            "merkle_roots.parquet",
            "verify_runs/run/artifact.parquet",
        ] {
            assert!(
                selected.contains(reserved),
                "reserved selection belongs to caller"
            );
        }
        assert!(selected.contains("folder.parquet/child.parquet"));
        assert!(!selected.contains("folder.parquet"));
        assert!(selected.contains("space %/part.parquet"));
        assert!(selected.contains(".fireparq-ingest-old/retained.parquet"));
        assert_eq!(
            selected.contains("UPPER.PARQUET"),
            matches!(policy.selection, Selection::AsciiInsensitiveParquet)
        );
        for control in [
            ".fireparq-ingest/hidden.parquet",
            ".fireparq-owner-probes-v1/probe.parquet",
            ".fireparq-owner-v1.json/record.parquet",
        ] {
            assert_eq!(selected.contains(control), !policy.prune_controls);
        }
        assert!(!selected.contains("notes.txt"));
    }
    let mut journals = vec![];
    collect_local(
        root.path(),
        LocalPolicy::merge_journals("_fireparq_merge.json"),
        &mut journals,
    )
    .unwrap();
    assert_eq!(
        journals,
        vec![root.path().join("nested/_fireparq_merge.json")]
    );
}

#[test]
fn missing_and_non_directory_roots_keep_skip_vs_read_dir_errors() {
    let root = tempfile::tempdir().unwrap();
    write(root.path(), "single.parquet");
    for path in [
        root.path().join("missing"),
        root.path().join("single.parquet"),
    ] {
        compare(&path);
        let mut files = vec![];
        collect_local(&path, LocalPolicy::MUTATION_PARQUET, &mut files).unwrap();
        assert!(files.is_empty(), "single-file selection belongs to caller");
        assert!(collect_local(&path, LocalPolicy::PARQUET, &mut files).is_err());
        assert!(collect_local(&path, LocalPolicy::VERIFY_PARQUET, &mut files).is_err());
    }
    // Control pruning uses the full path, including root ancestors.
    write(root.path(), ".fireparq-ingest/outer/table/part.parquet");
    compare(&root.path().join(".fireparq-ingest/outer"));
}

#[cfg(unix)]
#[test]
fn symlinks_and_non_utf8_paths_remain_native_and_unnormalized() {
    use std::ffi::OsString;
    use std::os::unix::{ffi::OsStringExt, fs::symlink};
    let root = tempfile::tempdir().unwrap();
    fixture(root.path());
    symlink(root.path().join("nested"), root.path().join("alias")).unwrap();
    symlink(
        root.path().join("data.parquet"),
        root.path().join("file-alias.parquet"),
    )
    .unwrap();
    symlink(
        root.path().join("absent"),
        root.path().join("broken.parquet"),
    )
    .unwrap();
    let non_utf8 = root
        .path()
        .join(OsString::from_vec(b"name-\xff.parquet".to_vec()));
    // Some filesystems (APFS) reject non-UTF-8 names; Linux CI exercises them.
    let non_utf8_supported = std::fs::write(&non_utf8, b"bytes").is_ok();
    if non_utf8_supported {
        std::fs::write(
            root.path()
                .join(OsString::from_vec(b"file.parquet\xff".to_vec())),
            b"ignored",
        )
        .unwrap();
    }
    compare(root.path());
    let mut paths = vec![];
    collect_local(root.path(), LocalPolicy::MUTATION_PARQUET, &mut paths).unwrap();
    assert_eq!(paths.contains(&non_utf8), non_utf8_supported);
    assert!(paths.contains(&root.path().join("alias/part.parquet")));
    assert!(paths.contains(&root.path().join("file-alias.parquet")));
    assert!(paths.contains(&root.path().join("broken.parquet")));
    assert!(paths
        .iter()
        .all(|p| !p.as_os_str().as_encoded_bytes().ends_with(b"parquet\xff")));
}

#[cfg(unix)]
#[test]
fn native_permission_errors_are_not_silently_dropped() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let denied = root.path().join("denied");
    std::fs::create_dir(&denied).unwrap();
    std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0)).unwrap();
    let native_error = std::fs::read_dir(&denied).err().map(|e| e.kind());
    let mut paths = vec![];
    let actual = collect_local(root.path(), LocalPolicy::PARQUET, &mut paths)
        .err()
        .map(|e| e.kind());
    compare(root.path());
    std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o700)).unwrap();
    // Privileged CI users may legitimately read mode 000; compare native behavior.
    assert_eq!(actual, native_error);
}

#[test]
fn relative_key_matches_each_inlined_s3_copy() {
    // Frozen from merge.rs relative_s3_key, rollup S3Root::relative, truncate_s3,
    // validate_parquet_s3 and scan_s3_display_key (non-exact branch).
    fn legacy(prefix: &str, key: &str) -> String {
        key.strip_prefix(prefix)
            .map(|s| s.trim_start_matches('/'))
            .unwrap_or(key)
            .to_string()
    }
    for (prefix, key) in [
        ("", "blocks/part-000001.parquet"),
        ("evm", "evm/blocks/part-000001.parquet"),
        ("evm", "evm"),
        ("evm", "evm2/blocks/part.parquet"),
        ("evm", "other/part.parquet"),
        ("evm/", "evm//x.parquet"),
        ("a/b", "a/b/c/d.parquet"),
    ] {
        assert_eq!(
            relative_key(prefix, key),
            legacy(prefix, key),
            "{prefix} {key}"
        );
    }
}

#[tokio::test]
async fn raw_object_listing_and_complete_reads_match_existing_contract() {
    use object_store::memory::InMemory;
    let store = InMemory::new();
    for key in [
        "root/z.parquet",
        "root/a.parquet",
        "root/UPPER.PARQUET",
        "root/cursor.parquet",
        "root/.fireparq-ingest/state.parquet",
        "root/space %/part.parquet",
        "root2/sibling.parquet",
    ] {
        store
            .put(
                &ObjectPath::from(key),
                bytes::Bytes::from(key.to_owned()).into(),
            )
            .await
            .unwrap();
    }
    for prefix in ["", "root", "root/a.parquet", "root/missing", "root/space %"] {
        // Frozen from the six former inline listings.
        let old_prefix = if prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(prefix))
        };
        let old: Vec<ObjectMeta> = store.list(old_prefix.as_ref()).try_collect().await.unwrap();
        let new = list_objects(&store, prefix).await.unwrap();
        assert_eq!(new, old, "raw order, metadata and prefix unchanged");
    }
    let raw = list_objects(&store, "root").await.unwrap();
    assert!(raw
        .iter()
        .any(|m| m.location.as_ref().ends_with("UPPER.PARQUET")));
    assert!(raw
        .iter()
        .any(|m| m.location.as_ref().ends_with("cursor.parquet")));
    assert!(raw
        .iter()
        .any(|m| m.location.as_ref().contains(".fireparq-ingest")));
    let key = ObjectPath::from("root/space %/part.parquet");
    assert_eq!(
        read_object_bytes(&store, &key).await.unwrap(),
        store.get(&key).await.unwrap().bytes().await.unwrap()
    );
    let missing = ObjectPath::from("root/missing");
    let error = read_object_bytes(&store, &missing).await.unwrap_err();
    assert!(matches!(error, object_store::Error::NotFound { .. }));
    assert_eq!(
        error.to_string(),
        store.get(&missing).await.unwrap_err().to_string()
    );
}

// --- Frozen walkers from origin/main 9372f99 -------------------------------

// merge.rs collect_parquet_files_recursive
fn legacy_merge(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if crate::artifacts::is_control_path(&path.to_string_lossy()) {
            continue;
        }
        if path.is_dir() {
            legacy_merge(&path, out)?;
        } else if path.extension().map_or(false, |ext| ext == "parquet") {
            out.push(path);
        }
    }
    Ok(())
}

// merge.rs collect_named_files_recursive
fn legacy_named(dir: &Path, name: &str, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if crate::artifacts::is_control_path(&path.to_string_lossy()) {
            continue;
        }
        if path.is_dir() {
            legacy_named(&path, name, out)?;
        } else if path.file_name().is_some_and(|file_name| file_name == name) {
            out.push(path);
        }
    }
    Ok(())
}

// truncate.rs collect_parquet_files_recursive (with its is_parquet_file inlined)
fn legacy_truncate(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if crate::artifacts::is_control_path(&path.to_string_lossy()) {
            continue;
        }
        if path.is_dir() {
            legacy_truncate(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            out.push(path);
        }
    }
    Ok(())
}

// rollup.rs collect_parquet_files_recursive
fn legacy_rollup(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            legacy_rollup(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            out.push(path);
        }
    }
    Ok(())
}

// cli/inspect.rs collect_parquet_files (also used by cli/validate.rs)
fn legacy_cli(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            legacy_cli(&path, out)?;
        } else if path.extension().map_or(false, |ext| ext == "parquet") {
            out.push(path);
        }
    }
    Ok(())
}

// verify.rs collect_parquet_files
fn legacy_verify(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            legacy_verify(&path, out)?;
        } else if path
            .extension()
            .map_or(false, |ext| ext.eq_ignore_ascii_case("parquet"))
        {
            out.push(path);
        }
    }
    Ok(())
}
