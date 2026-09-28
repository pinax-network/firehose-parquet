//! Durable local directories and no-replace publication, used by the protected
//! part store (`writer::protected`); this is not an ingestion transaction.

use anyhow::{Context, Result};
use std::collections::HashSet;
use std::fs::{self, File};
use std::path::Path;

/// Sync all directory links, including ancestors that another attempt may have
/// created before failing to sync them. Merely finding an existing directory on
/// retry does not establish that its link in its parent is durable.
pub(crate) fn create_dir_all_durable(dir: &Path) -> Result<()> {
    let absolute = if dir.is_absolute() {
        dir.to_owned()
    } else {
        std::env::current_dir()?.join(dir)
    };
    fs::create_dir_all(&absolute)
        .with_context(|| format!("creating output directory {}", dir.display()))?;
    let target = fs::canonicalize(&absolute)
        .with_context(|| format!("resolving output directory {}", dir.display()))?;
    // An alias can lead to newly created directories beneath a different
    // ancestry. Sync the target's links as well as the symlink/lexical chain.
    let mut synced = HashSet::new();
    for ancestor in target.ancestors().chain(absolute.ancestors()) {
        if !synced.insert(ancestor) {
            continue;
        }
        #[cfg(test)]
        tests::checkpoint(ancestor)?;
        sync_directory(ancestor)?;
    }
    Ok(())
}

pub(super) fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("syncing output directory {}", path.display()))
}

/// Publish an already complete inode without replacing an existing final name.
/// The caller retains responsibility for temporary cleanup and directory sync.
pub(super) fn publish_file_no_replace(temporary: &Path, final_path: &Path) -> Result<()> {
    fs::hard_link(temporary, final_path).with_context(|| {
        format!(
            "publishing Parquet file without replacement {}",
            final_path.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::path::PathBuf;
    use std::sync::{Arc, Barrier};

    /// Ancestor syncs observed on this thread, and one that fails.
    #[derive(Default)]
    struct Faults {
        fail_path: Option<PathBuf>,
        fail_all: bool,
        observed: Vec<PathBuf>,
    }

    thread_local! {
        static FAULTS: RefCell<Faults> = RefCell::new(Faults::default());
    }

    struct ResetFaults;
    impl Drop for ResetFaults {
        fn drop(&mut self) {
            FAULTS.with(|faults| *faults.borrow_mut() = Faults::default());
        }
    }

    fn inject(faults: Faults) -> ResetFaults {
        FAULTS.with(|state| *state.borrow_mut() = faults);
        ResetFaults
    }

    pub(super) fn checkpoint(path: &Path) -> Result<()> {
        FAULTS.with(|faults| {
            let mut faults = faults.borrow_mut();
            faults.observed.push(path.to_owned());
            if faults.fail_all || faults.fail_path.as_deref() == Some(path) {
                anyhow::bail!("injected ancestor sync failure");
            }
            Ok(())
        })
    }

    fn assert_both_ancestor_chains_synced(dir: &Path) {
        FAULTS.with(|state| {
            let state = state.borrow();
            let synced: Vec<_> = state.observed.iter().map(PathBuf::as_path).collect();
            let target = fs::canonicalize(dir).unwrap();
            for ancestor in target.ancestors().chain(dir.ancestors()) {
                assert!(
                    synced.contains(&ancestor),
                    "ancestor not synced: {}",
                    ancestor.display()
                );
            }
            assert_eq!(synced.len(), synced.iter().collect::<HashSet<_>>().len());
        });
    }

    #[test]
    fn retry_syncs_ancestors_left_by_failed_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new/nested");
        {
            let _reset = inject(Faults {
                fail_all: true,
                ..Default::default()
            });
            assert!(create_dir_all_durable(&path).is_err());
        }
        assert!(path.is_dir());
        let _reset = inject(Faults::default());
        create_dir_all_durable(&path).unwrap();
        assert_both_ancestor_chains_synced(&path);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_target_ancestors_are_synced_and_a_failure_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("separate/target");
        fs::create_dir_all(&target).unwrap();
        let alias = dir.path().join("alias");
        std::os::unix::fs::symlink(&target, &alias).unwrap();
        let path = alias.join("new/nested");
        let target_only_ancestor = fs::canonicalize(&target)
            .unwrap()
            .parent()
            .unwrap()
            .to_owned();
        assert!(!path
            .ancestors()
            .any(|ancestor| ancestor == target_only_ancestor));
        {
            let _reset = inject(Faults {
                fail_path: Some(target_only_ancestor.clone()),
                ..Default::default()
            });
            let error = create_dir_all_durable(&path).unwrap_err();
            assert!(format!("{error:#}").contains("injected ancestor sync failure"));
            FAULTS.with(|state| {
                assert!(state.borrow().observed.contains(&target_only_ancestor));
            });
        }
        let _reset = inject(Faults::default());
        create_dir_all_durable(&path).unwrap();
        assert_both_ancestor_chains_synced(&path);
    }

    #[test]
    fn existing_destination_is_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let temporary = dir.path().join("part.tmp");
        let path = dir.path().join("part.parquet");
        fs::write(&temporary, b"complete part").unwrap();
        fs::write(&path, b"unrelated existing contents").unwrap();
        let error = publish_file_no_replace(&temporary, &path).unwrap_err();
        assert!(format!("{error:#}").contains("without replacement"));
        assert_eq!(fs::read(&path).unwrap(), b"unrelated existing contents");
    }

    #[test]
    fn concurrent_publication_has_exactly_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part.parquet");
        let barrier = Arc::new(Barrier::new(2));
        let threads: Vec<_> = (0..2)
            .map(|index| {
                let temporary = dir.path().join(format!("part-{index}.tmp"));
                fs::write(&temporary, format!("writer {index}")).unwrap();
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    publish_file_no_replace(&temporary, &path)
                })
            })
            .collect();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let winner = fs::read_to_string(&path).unwrap();
        assert!(winner == "writer 0" || winner == "writer 1", "{winner}");
    }
}
