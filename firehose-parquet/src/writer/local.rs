//! Atomic publication of one local part; this is not an ingestion transaction.

use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashSet;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use uuid::Uuid;

/// Sync all directory links, including ancestors that another attempt may have
/// created before failing to sync them. Merely finding an existing directory on
/// retry does not establish that its link in its parent is durable.
pub(super) fn create_dir_all_durable(dir: &Path) -> Result<()> {
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
        tests::checkpoint(tests::Stage::AncestorSync, ancestor)?;
        sync_directory(ancestor)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .with_context(|| format!("syncing output directory {}", path.display()))
}

/// Success means both the complete file and its final directory entry are
/// synced. An error after publication can leave a complete final file; callers
/// must stop, not assume retrying this batch is safe under another random name.
pub(super) fn write_parquet(
    path: &Path,
    batch: &RecordBatch,
    props: WriterProperties,
) -> Result<usize> {
    let dir = path
        .parent()
        .context("Parquet output has no parent directory")?;
    create_dir_all_durable(dir)?;

    let temp_path = dir.join(format!(".fireparq-{}.tmp", Uuid::new_v4()));
    // Retain the same creation mode/umask as the previous File::create path.
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temp_path)
        .with_context(|| format!("creating temporary Parquet file {}", temp_path.display()))?;
    let mut temp = TemporaryFile {
        file,
        path: Some(temp_path),
    };

    let result = (|| {
        #[cfg(not(test))]
        let sink = &mut temp.file;
        #[cfg(test)]
        let sink = tests::FaultWriter(&mut temp.file);
        let mut writer = ArrowWriter::try_new(sink, batch.schema(), Some(props))
            .context("initializing temporary Parquet writer")?;
        writer
            .write(batch)
            .context("writing temporary Parquet data")?;
        #[cfg(test)]
        tests::checkpoint(tests::Stage::Closing, path)?;
        writer.close().context("closing temporary Parquet footer")?;

        #[cfg(test)]
        tests::checkpoint(tests::Stage::FileSync, path)?;
        temp.file
            .sync_all()
            .context("syncing completed temporary Parquet file")?;
        let compressed_bytes = temp.file.metadata()?.len() as usize;

        #[cfg(test)]
        tests::checkpoint(tests::Stage::Publish, path)?;
        // Linking in the same directory atomically creates a name for this
        // complete inode and fails if *anything* already occupies the final
        // name. A check followed by rename would race and could overwrite it.
        fs::hard_link(temp.path.as_ref().unwrap(), path).with_context(|| {
            format!(
                "publishing Parquet file without replacement {}",
                path.display()
            )
        })?;

        #[cfg(test)]
        tests::checkpoint(tests::Stage::TempRemoval, path)?;
        temp.remove()
            .context("removing published Parquet temporary name")?;
        #[cfg(test)]
        tests::checkpoint(tests::Stage::PublishedDirectorySync, path)?;
        sync_directory(dir)?;
        Ok(compressed_bytes)
    })();

    if result.is_err() {
        // Do not unlink a published final name: a later durability error is an
        // ambiguous commit, not proof that the complete output can be discarded.
        if let Err(cleanup) = temp.remove() {
            return result.context(format!(
                "also failed to remove temporary Parquet file: {cleanup}"
            ));
        }
    }
    result
}

struct TemporaryFile {
    file: File,
    path: Option<PathBuf>,
}

impl TemporaryFile {
    fn remove(&mut self) -> std::io::Result<()> {
        if let Some(path) = &self.path {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            self.path = None;
        }
        Ok(())
    }
}

impl Drop for TemporaryFile {
    fn drop(&mut self) {
        // Fallback for unwinding; normal errors report cleanup failures above.
        let _ = self.remove();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::cell::RefCell;
    use std::io::{self, Write};
    use std::sync::{Arc, Barrier};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(super) enum Stage {
        AncestorSync,
        Closing,
        FileSync,
        Publish,
        TempRemoval,
        PublishedDirectorySync,
    }

    #[derive(Default)]
    struct Faults {
        fail: Option<Stage>,
        fail_path: Option<PathBuf>,
        crash: Option<Stage>,
        fail_after_bytes: Option<usize>,
        bytes_written: usize,
        final_path: Option<PathBuf>,
        observed: Vec<(Stage, PathBuf)>,
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

    pub(super) fn checkpoint(stage: Stage, path: &Path) -> Result<()> {
        FAULTS.with(|faults| {
            let mut faults = faults.borrow_mut();
            faults.observed.push((stage, path.to_owned()));
            if faults.crash == Some(stage) {
                // Deliberately skip Drop to model abrupt process termination.
                std::process::exit(81);
            }
            if faults.fail == Some(stage)
                && faults
                    .fail_path
                    .as_deref()
                    .is_none_or(|expected| expected == path)
            {
                anyhow::bail!("injected {stage:?} failure");
            }
            Ok(())
        })
    }

    pub(super) struct FaultWriter<'a>(pub(super) &'a mut File);
    impl Write for FaultWriter<'_> {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            let allowed = FAULTS.with(|state| {
                let state = state.borrow();
                if let Some(path) = &state.final_path {
                    assert!(!path.exists(), "final file became visible during writing");
                }
                state.fail_after_bytes.map_or(data.len(), |limit| {
                    limit.saturating_sub(state.bytes_written).min(data.len())
                })
            });
            if allowed == 0 && !data.is_empty() {
                return Err(io::Error::other("injected partial write failure"));
            }
            let written = self.0.write(&data[..allowed])?;
            FAULTS.with(|state| state.borrow_mut().bytes_written += written);
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.0.flush()
        }
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "number",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from_iter_values(0..1024))],
        )
        .unwrap()
    }

    fn props() -> WriterProperties {
        WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .build()
    }

    fn assert_round_trip(path: &Path) {
        let actual = crate::writer::read_parquet(path).unwrap();
        let expected = batch();
        let joined = arrow::compute::concat_batches(&expected.schema(), &actual).unwrap();
        assert_eq!(joined, expected);
    }

    fn entries(dir: &Path) -> Vec<PathBuf> {
        fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    }

    fn assert_both_ancestor_chains_synced(dir: &Path, observed: &[(Stage, PathBuf)]) {
        let synced: Vec<_> = observed
            .iter()
            .filter(|entry| entry.0 == Stage::AncestorSync)
            .map(|entry| entry.1.as_path())
            .collect();
        let target = fs::canonicalize(dir).unwrap();
        for ancestor in target.ancestors().chain(dir.ancestors()) {
            assert!(
                synced.contains(&ancestor),
                "ancestor not synced: {}",
                ancestor.display()
            );
        }
        assert_eq!(synced.len(), synced.iter().collect::<HashSet<_>>().len());
    }

    #[test]
    fn success_is_complete_and_durable_before_return() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new/nested/part.parquet");
        let _reset = inject(Faults {
            final_path: Some(path.clone()),
            ..Default::default()
        });
        let size = write_parquet(&path, &batch(), props()).unwrap();
        assert_eq!(size as u64, fs::metadata(&path).unwrap().len());
        assert_round_trip(&path);
        assert_eq!(entries(path.parent().unwrap()), vec![path.clone()]);
        FAULTS.with(|state| {
            let state = state.borrow();
            let observed: Vec<_> = state.observed.iter().map(|entry| entry.0).collect();
            assert_eq!(
                &observed[observed.len() - 5..],
                &[
                    Stage::Closing,
                    Stage::FileSync,
                    Stage::Publish,
                    Stage::TempRemoval,
                    Stage::PublishedDirectorySync,
                ]
            );
            assert_both_ancestor_chains_synced(path.parent().unwrap(), &state.observed);
        });
    }

    #[test]
    fn partial_data_write_failure_never_exposes_final_name() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part.parquet");
        let _reset = inject(Faults {
            fail_after_bytes: Some(128),
            final_path: Some(path.clone()),
            ..Default::default()
        });
        let error = write_parquet(&path, &batch(), props()).unwrap_err();
        assert!(format!("{error:#}").contains("injected partial write"));
        FAULTS.with(|state| {
            let state = state.borrow();
            assert_eq!(state.bytes_written, 128);
            assert!(!state.observed.iter().any(|entry| entry.0 == Stage::Closing));
        });
        assert!(entries(dir.path()).is_empty());
    }

    #[test]
    fn partial_footer_failure_never_exposes_final_name() {
        let reference = tempfile::tempdir().unwrap();
        let size = write_parquet(
            &reference.path().join("reference.parquet"),
            &batch(),
            props(),
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part.parquet");
        let _reset = inject(Faults {
            fail_after_bytes: Some(size - 4),
            final_path: Some(path.clone()),
            ..Default::default()
        });
        let error = write_parquet(&path, &batch(), props()).unwrap_err();
        assert!(format!("{error:#}").contains("closing temporary Parquet footer"));
        FAULTS.with(|state| assert_eq!(state.borrow().bytes_written, size - 4));
        assert!(entries(dir.path()).is_empty());
    }

    #[test]
    fn failures_before_publication_clean_temporary_file() {
        for stage in [Stage::AncestorSync, Stage::FileSync, Stage::Publish] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("nested/part.parquet");
            let _reset = inject(Faults {
                fail: Some(stage),
                ..Default::default()
            });
            let error = write_parquet(&path, &batch(), props()).unwrap_err();
            assert!(format!("{error:#}").contains(&format!("{stage:?}")));
            assert!(entries(path.parent().unwrap()).is_empty());
        }
    }

    #[test]
    fn errors_after_publication_keep_complete_final_file() {
        for stage in [Stage::TempRemoval, Stage::PublishedDirectorySync] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("part.parquet");
            let _reset = inject(Faults {
                fail: Some(stage),
                ..Default::default()
            });
            assert!(write_parquet(&path, &batch(), props()).is_err());
            assert_round_trip(&path);
            assert_eq!(entries(dir.path()), vec![path]);
        }
    }

    #[test]
    fn retry_syncs_ancestors_left_by_failed_creation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new/nested/part.parquet");
        {
            let _reset = inject(Faults {
                fail: Some(Stage::AncestorSync),
                ..Default::default()
            });
            assert!(write_parquet(&path, &batch(), props()).is_err());
        }
        assert!(path.parent().unwrap().is_dir());
        let _reset = inject(Faults::default());
        write_parquet(&path, &batch(), props()).unwrap();
        FAULTS.with(|state| {
            let state = state.borrow();
            assert_both_ancestor_chains_synced(path.parent().unwrap(), &state.observed);
        });
    }

    #[cfg(unix)]
    #[test]
    fn symlink_target_ancestors_are_synced_and_failure_stops_publication() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("separate/target");
        fs::create_dir_all(&target).unwrap();
        let alias = dir.path().join("alias");
        std::os::unix::fs::symlink(&target, &alias).unwrap();
        let path = alias.join("new/nested/part.parquet");
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
                fail: Some(Stage::AncestorSync),
                fail_path: Some(target_only_ancestor.clone()),
                ..Default::default()
            });
            let error = write_parquet(&path, &batch(), props()).unwrap_err();
            assert!(format!("{error:#}").contains("AncestorSync"));
            assert!(entries(path.parent().unwrap()).is_empty());
            FAULTS.with(|state| {
                let state = state.borrow();
                assert!(state
                    .observed
                    .contains(&(Stage::AncestorSync, target_only_ancestor)));
                assert!(!state.observed.iter().any(|entry| entry.0 == Stage::Publish));
            });
        }
        let _reset = inject(Faults::default());
        write_parquet(&path, &batch(), props()).unwrap();
        assert_round_trip(&path);
        FAULTS.with(|state| {
            assert_both_ancestor_chains_synced(path.parent().unwrap(), &state.borrow().observed);
        });
    }

    #[test]
    fn existing_destination_is_never_overwritten() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part.parquet");
        fs::write(&path, b"unrelated existing contents").unwrap();
        let error = write_parquet(&path, &batch(), props()).unwrap_err();
        assert!(format!("{error:#}").contains("without replacement"));
        assert_eq!(fs::read(&path).unwrap(), b"unrelated existing contents");
        assert_eq!(entries(dir.path()), vec![path]);
    }

    #[test]
    fn concurrent_publication_has_exactly_one_winner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("part.parquet");
        let barrier = Arc::new(Barrier::new(2));
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    write_parquet(&path, &batch(), props())
                })
            })
            .collect();
        let results: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_round_trip(&path);
        assert_eq!(entries(dir.path()), vec![path]);
    }

    #[test]
    fn abrupt_process_exit_never_leaves_partial_final_parquet() {
        for stage in ["Closing", "PublishedDirectorySync"] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("part.parquet");
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "writer::local::tests::crash_child", "--ignored"])
                .env("FIREPARQ_ATOMIC_TEST_PATH", &path)
                .env("FIREPARQ_ATOMIC_TEST_STAGE", stage)
                .stdout(std::process::Stdio::null())
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(81));
            if stage == "Closing" {
                assert!(!path.exists());
                let remnants = entries(dir.path());
                assert_eq!(remnants.len(), 1);
                assert!(remnants[0]
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .starts_with(".fireparq-"));
                assert_eq!(remnants[0].extension().unwrap(), "tmp");
                assert!(fs::metadata(&remnants[0]).unwrap().len() > 0);
                assert!(crate::writer::read_parquet(&remnants[0]).is_err());
            } else {
                assert_round_trip(&path);
                assert_eq!(entries(dir.path()), vec![path]);
            }
        }
    }

    #[test]
    #[ignore = "subprocess entrypoint invoked by abrupt_process_exit_never_leaves_partial_final_parquet"]
    fn crash_child() {
        let path = PathBuf::from(std::env::var_os("FIREPARQ_ATOMIC_TEST_PATH").unwrap());
        let stage = match std::env::var("FIREPARQ_ATOMIC_TEST_STAGE")
            .unwrap()
            .as_str()
        {
            "Closing" => Stage::Closing,
            "PublishedDirectorySync" => Stage::PublishedDirectorySync,
            _ => panic!("unexpected crash stage"),
        };
        let _reset = inject(Faults {
            crash: Some(stage),
            ..Default::default()
        });
        write_parquet(&path, &batch(), props()).unwrap();
        panic!("child should have exited without running destructors");
    }
}
