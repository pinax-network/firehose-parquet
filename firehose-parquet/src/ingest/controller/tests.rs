use super::*;
use crate::dataset_lock::LocalOwnership;
use crate::dataset_lock_s3::S3Ownership;
use crate::ingest::frontier::AcceptedFrontier;
use crate::ingest::state::tests::{descriptor, event, routing};
use crate::ingest::state::{Checkpoint, RoutingPolicy, StorageIdentity};
use crate::writer::protected::schema_sha256;
use arrow::array::{Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use object_store::memory::InMemory;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::cell::{Cell, RefCell};
use std::fs;
use std::path::Path;
use std::sync::Arc;

mod remote_deletion;

thread_local! {static FAIL:Cell<Option<Stage>>=const{Cell::new(None)};}
pub(super) fn checkpoint(stage: Stage) -> Result<()> {
    if FAIL.with(|fail| fail.get()) == Some(stage) {
        bail!("injected transaction boundary failure");
    }
    if std::env::var("FIREPARQ_468_CRASH_STAGE").ok().as_deref()
        == Some(format!("{stage:?}").as_str())
    {
        let marker =
            std::env::var_os("FIREPARQ_468_CRASH_MARKER").context("missing crash marker")?;
        fs::write(marker, b"ready")?;
        loop {
            std::thread::park();
        }
    }
    Ok(())
}
struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        FAIL.with(|fail| fail.set(None));
    }
}
fn fail(stage: Stage) -> Reset {
    FAIL.with(|fail| fail.set(Some(stage)));
    Reset
}

#[derive(Default)]
struct Mirror {
    head: RefCell<Option<Checkpoint>>,
    fail: Cell<bool>,
}
impl MirrorAction for Mirror {
    async fn reconcile(&self, authority: &AuthorityState) -> Result<()> {
        if self.fail.get() {
            bail!("injected mirror persistence failure");
        }
        authority.validate()?;
        *self.head.borrow_mut() = Some(authority.checkpoint.clone());
        Ok(())
    }
}

fn data() -> HashMap<String, RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::UInt64, false),
        Field::new("value", DataType::UInt64, false),
    ]));
    [("blocks", vec![10, 11]), ("logs", vec![20, 21])]
        .into_iter()
        .map(|(table, values)| {
            (
                table.into(),
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from(vec![100, 101])),
                        Arc::new(UInt64Array::from(values)),
                    ],
                )
                .unwrap(),
            )
        })
        .collect()
}
fn actual_descriptor(root: &Path) -> StreamDescriptor {
    let mut descriptor = descriptor(RoutingPolicy::DirectV1);
    descriptor.output = StorageIdentity::Local {
        canonical_root: fs::canonicalize(root).unwrap().to_str().unwrap().into(),
    };
    descriptor.partition = PartitionPolicy::None;
    descriptor.tables = data()
        .iter()
        .map(|(table, batch)| {
            (
                table.clone(),
                Digest::parse(schema_sha256(batch.schema().as_ref()).unwrap()).unwrap(),
            )
        })
        .collect();
    descriptor
}
fn prefix(authority: &AuthorityState) -> AcceptedPrefix {
    let mut frontier = AcceptedFrontier::resume(&authority.checkpoint);
    for number in [100, 101] {
        let ordinal = frontier.receive(event(number, 1)).unwrap();
        frontier
            .accept(ordinal, routing(RoutingPolicy::DirectV1))
            .unwrap();
    }
    frontier.snapshot().unwrap().unwrap()
}
fn metadata() -> BlockMetadata {
    BlockMetadata {
        min_block_number: 100,
        max_block_number: 101,
        min_timestamp: None,
        max_timestamp: None,
    }
}
async fn initialize(root: &Path, owner: &LocalOwnership, descriptor: &StreamDescriptor) {
    TransactionStateStore::local(root, owner)
        .unwrap()
        .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
        .await
        .unwrap();
}
async fn open<'a>(
    root: &Path,
    owner: &'a LocalOwnership,
    mirror: &'a Mirror,
    descriptor: &StreamDescriptor,
) -> Result<TransactionController<'a, Mirror>> {
    TransactionController::open(
        TransactionStateStore::local(root, owner)?,
        TransactionParts::local(root, owner)?,
        mirror,
        descriptor,
    )
    .await
}
async fn commit(controller: &mut TransactionController<'_, Mirror>) -> Result<CommittedFlush> {
    controller
        .commit(
            prefix(controller.authority()),
            data(),
            metadata(),
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .await
}
fn data_files(root: &Path) -> Vec<PathBuf> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            if path.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}
fn assert_rows(root: &Path) {
    let files = data_files(root);
    assert_eq!(files.len(), 2);
    for path in files {
        let table = path
            .strip_prefix(root)
            .unwrap()
            .components()
            .next()
            .unwrap()
            .as_os_str()
            .to_str()
            .unwrap();
        let mut values = Vec::new();
        let mut numbers = Vec::new();
        for batch in ParquetRecordBatchReaderBuilder::try_new(fs::File::open(&path).unwrap())
            .unwrap()
            .build()
            .unwrap()
        {
            let batch = batch.unwrap();
            values.extend_from_slice(
                batch
                    .column_by_name("value")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values(),
            );
            numbers.extend_from_slice(
                batch
                    .column_by_name("block_num")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values(),
            );
        }
        assert_eq!(numbers, vec![100, 101]);
        assert_eq!(
            values,
            if table == "blocks" {
                vec![10, 11]
            } else {
                vec![20, 21]
            }
        );
    }
}

#[tokio::test]
async fn every_publication_boundary_recovers_to_one_complete_all_table_prefix() {
    for stage in [
        Stage::WritingPersisted,
        Stage::Staged(0),
        Stage::ReceiptPersisted(0),
        Stage::Published(0),
        Stage::Staged(1),
        Stage::ReceiptPersisted(1),
        Stage::Published(1),
        Stage::CommittedPersisted,
        Stage::AuthorityAdvanced,
        Stage::MirrorReconciled,
        Stage::PendingCleared,
    ] {
        let root = tempfile::tempdir().unwrap();
        let descriptor = actual_descriptor(root.path());
        let mirror = Mirror::default();
        {
            let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
            initialize(root.path(), &owner, &descriptor).await;
            let mut controller = open(root.path(), &owner, &mirror, &descriptor)
                .await
                .unwrap();
            let injected = fail(stage);
            assert!(commit(&mut controller).await.is_err(), "{stage:?}");
            drop(injected);
            assert!(
                commit(&mut controller).await.is_err(),
                "controller must stay poisoned after {stage:?}"
            );
        }
        let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
        let mut controller = open(root.path(), &owner, &mirror, &descriptor)
            .await
            .unwrap();
        let already_committed = matches!(
            stage,
            Stage::CommittedPersisted
                | Stage::AuthorityAdvanced
                | Stage::MirrorReconciled
                | Stage::PendingCleared
        );
        assert_eq!(
            controller.authority().checkpoint.ordinal,
            if already_committed { 2 } else { 0 },
            "{stage:?}"
        );
        if !already_committed {
            assert!(
                data_files(root.path()).is_empty(),
                "Writing files survived rollback at {stage:?}"
            );
            let committed = commit(&mut controller).await.unwrap();
            assert_eq!(
                (committed.rows, committed.files, committed.ordinal),
                (4, 2, 2)
            );
            assert!(committed.bytes > 0);
        }
        assert_rows(root.path());
        assert_eq!(
            mirror.head.borrow().as_ref().unwrap().id,
            controller.authority().checkpoint.id
        );
        assert!(TransactionStateStore::local(root.path(), &owner)
            .unwrap()
            .load()
            .await
            .unwrap()
            .pending
            .is_none());
    }
}

#[tokio::test]
async fn mirror_failure_retains_committed_journal_and_repairs_from_authority() {
    let root = tempfile::tempdir().unwrap();
    let descriptor = actual_descriptor(root.path());
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    initialize(root.path(), &owner, &descriptor).await;
    let mut controller = open(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap();
    mirror.fail.set(true);
    assert!(commit(&mut controller).await.is_err());
    let snapshot = TransactionStateStore::local(root.path(), &owner)
        .unwrap()
        .load()
        .await
        .unwrap();
    assert_eq!(snapshot.authority.unwrap().payload.checkpoint.ordinal, 2);
    assert_eq!(
        snapshot.pending.unwrap().payload.phase,
        TransactionPhase::Committed
    );
    mirror.fail.set(false);
    drop(controller);
    let controller = open(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap();
    assert_eq!(controller.authority().checkpoint.ordinal, 2);
    assert_rows(root.path());
}

#[tokio::test]
async fn corrupt_writing_part_stops_before_deleting_any_valid_final() {
    let root = tempfile::tempdir().unwrap();
    let descriptor = actual_descriptor(root.path());
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    initialize(root.path(), &owner, &descriptor).await;
    let mut controller = open(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap();
    let injected = fail(Stage::Published(1));
    assert!(commit(&mut controller).await.is_err());
    drop(injected);
    drop(controller);
    let files = data_files(root.path());
    let unchanged = fs::read(&files[0]).unwrap();
    fs::write(&files[1], b"corrupt").unwrap();
    assert!(open(root.path(), &owner, &mirror, &descriptor)
        .await
        .is_err());
    assert_eq!(fs::read(&files[0]).unwrap(), unchanged);
    assert_eq!(data_files(root.path()).len(), 2);
    assert!(TransactionStateStore::local(root.path(), &owner)
        .unwrap()
        .load()
        .await
        .unwrap()
        .pending
        .is_some());
}

#[tokio::test]
async fn committed_missing_or_corrupt_part_cannot_roll_forward_or_replay() {
    for missing in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let descriptor = actual_descriptor(root.path());
        let mirror = Mirror::default();
        let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
        initialize(root.path(), &owner, &descriptor).await;
        let mut controller = open(root.path(), &owner, &mirror, &descriptor)
            .await
            .unwrap();
        let injected = fail(Stage::CommittedPersisted);
        assert!(commit(&mut controller).await.is_err());
        drop(injected);
        drop(controller);
        let files = data_files(root.path());
        if missing {
            fs::remove_file(&files[1]).unwrap();
        } else {
            fs::write(&files[1], b"corrupt").unwrap();
        }
        assert!(open(root.path(), &owner, &mirror, &descriptor)
            .await
            .is_err());
        let snapshot = TransactionStateStore::local(root.path(), &owner)
            .unwrap()
            .load()
            .await
            .unwrap();
        assert_eq!(snapshot.authority.unwrap().payload.checkpoint.ordinal, 0);
        assert_eq!(
            snapshot.pending.unwrap().payload.phase,
            TransactionPhase::Committed
        );
        assert!(files[0].exists());
    }
}

#[tokio::test]
async fn invalid_table_and_preexisting_planned_name_fail_before_writing() {
    let root = tempfile::tempdir().unwrap();
    let descriptor = actual_descriptor(root.path());
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    initialize(root.path(), &owner, &descriptor).await;
    let mut controller = open(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap();
    let mut batches = data();
    batches.insert("foreign_table".into(), batches["blocks"].clone());
    assert!(controller
        .commit(
            prefix(controller.authority()),
            batches,
            metadata(),
            Compression::Zstd,
            ParquetFileMetadata::new()
        )
        .await
        .is_err());
    assert!(TransactionStateStore::local(root.path(), &owner)
        .unwrap()
        .load()
        .await
        .unwrap()
        .pending
        .is_none());
    assert!(data_files(root.path()).is_empty());
    drop(controller);
    let mut controller = open(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap();
    let tables = descriptor
        .tables
        .iter()
        .map(|(table, schema)| TablePlan {
            table: table.clone(),
            schema_sha256: schema.clone(),
            rows: 2,
            partition: String::new(),
        })
        .collect();
    let pending = PendingTransaction::prepare(
        controller.authority(),
        prefix(controller.authority()),
        tables,
        PartCompression::Zstd,
    )
    .unwrap();
    let collision = root.path().join(&pending.parts[0].temporary_relative_path);
    fs::create_dir_all(collision.parent().unwrap()).unwrap();
    fs::write(&collision, b"foreign").unwrap();
    assert!(commit(&mut controller).await.is_err());
    assert_eq!(fs::read(collision).unwrap(), b"foreign");
    assert!(TransactionStateStore::local(root.path(), &owner)
        .unwrap()
        .load()
        .await
        .unwrap()
        .pending
        .is_none());
}

#[tokio::test]
async fn zero_row_events_commit_and_reopen_without_creating_data_parts() {
    let root = tempfile::tempdir().unwrap();
    let descriptor = actual_descriptor(root.path());
    let mirror = Mirror::default();
    let owner = LocalOwnership::acquire(&[root.path().into()]).unwrap();
    initialize(root.path(), &owner, &descriptor).await;
    let mut controller = open(root.path(), &owner, &mirror, &descriptor)
        .await
        .unwrap();
    let result = controller
        .commit(
            prefix(controller.authority()),
            HashMap::new(),
            metadata(),
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .await
        .unwrap();
    assert_eq!((result.rows, result.files, result.ordinal), (0, 0, 2));
    assert!(data_files(root.path()).is_empty());
    drop(controller);
    assert_eq!(
        open(root.path(), &owner, &mirror, &descriptor)
            .await
            .unwrap()
            .authority()
            .checkpoint
            .ordinal,
        2
    );
}

#[tokio::test]
async fn remote_conditional_publication_and_rollback_obey_same_all_table_boundary() {
    for stage in [Stage::Published(0), Stage::CommittedPersisted] {
        let backend = Arc::new(InMemory::new());
        let owner = S3Ownership::acquire(backend, "test-ingest", vec!["dataset".into()])
            .await
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let mut descriptor = actual_descriptor(temp.path());
        descriptor.output = StorageIdentity::S3 {
            service: Digest::hash("service", &"fixture").unwrap(),
            bucket: "bucket".into(),
            prefix: "dataset".into(),
        };
        let states = TransactionStateStore::s3("dataset", &owner).unwrap();
        states
            .initialize(AuthorityState::initial(descriptor.clone()).unwrap())
            .await
            .unwrap();
        let mirror = Mirror::default();
        let mut controller = TransactionController::open(
            states,
            TransactionParts::s3("dataset", &owner, "").unwrap(),
            &mirror,
            &descriptor,
        )
        .await
        .unwrap();
        let injected = fail(stage);
        assert!(commit(&mut controller).await.is_err());
        drop(injected);
        drop(controller);
        let mut controller = TransactionController::open(
            TransactionStateStore::s3("dataset", &owner).unwrap(),
            TransactionParts::s3("dataset", &owner, "").unwrap(),
            &mirror,
            &descriptor,
        )
        .await
        .unwrap();
        if stage == Stage::Published(0) {
            assert_eq!(controller.authority().checkpoint.ordinal, 0);
            commit(&mut controller).await.unwrap();
        }
        assert_eq!(controller.authority().checkpoint.ordinal, 2);
        drop(controller);
        owner.release().await.unwrap();
    }
}

#[test]
#[ignore = "subprocess helper; exercised by abrupt_process_death_recovers_parts_and_checkpoint"]
fn crash_child_helper() {
    let root = std::env::var_os("FIREPARQ_468_CRASH_ROOT").expect("child root");
    let root = Path::new(&root);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let descriptor = actual_descriptor(root);
            let mirror = Mirror::default();
            let owner = LocalOwnership::acquire(&[root.into()]).unwrap();
            initialize(root, &owner, &descriptor).await;
            let mut controller = open(root, &owner, &mirror, &descriptor).await.unwrap();
            commit(&mut controller).await.unwrap();
        });
    panic!("crash helper did not reach its requested boundary");
}

#[test]
fn abrupt_process_death_recovers_parts_and_checkpoint() {
    for stage in [Stage::Published(0), Stage::CommittedPersisted] {
        let parent = tempfile::tempdir().unwrap();
        let root = parent.path().join("dataset");
        fs::create_dir(&root).unwrap();
        let marker = parent.path().join("ready");
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "ingest::controller::tests::crash_child_helper",
                "--nocapture",
            ])
            .env("FIREPARQ_468_CRASH_ROOT", &root)
            .env("FIREPARQ_468_CRASH_MARKER", &marker)
            .env("FIREPARQ_468_CRASH_STAGE", format!("{stage:?}"))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while !marker.exists() {
            assert!(
                child.try_wait().unwrap().is_none(),
                "crash helper exited before {stage:?}"
            );
            if std::time::Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("crash helper timed out");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        child.kill().unwrap();
        assert!(!child.wait().unwrap().success());
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let descriptor = actual_descriptor(&root);
                let mirror = Mirror::default();
                let owner = LocalOwnership::acquire(&[root.clone()]).unwrap();
                let mut controller = open(&root, &owner, &mirror, &descriptor).await.unwrap();
                if stage == Stage::Published(0) {
                    assert_eq!(controller.authority().checkpoint.ordinal, 0);
                    commit(&mut controller).await.unwrap();
                }
                assert_eq!(controller.authority().checkpoint.ordinal, 2);
                assert_rows(&root);
            });
    }
}
