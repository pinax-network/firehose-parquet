//! Exercise the public session boundary, not only the maintenance predicates.
use super::*;
use crate::dataset_lock::MutationScope;
use crate::durable_state::CONTROL_DIRECTORY;
use crate::ingest::state::tests::{event, routing};
use crate::merge_journal::{Journal, LocalPartition, PartitionFiles, RunContext, JOURNAL_FILE};
use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

fn mapper() -> MapperSemantics {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "block_num",
        DataType::UInt64,
        false,
    )]));
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(Vec::<u64>::new()))]).unwrap();
    MapperSemantics {
        chain: "mainnet".into(),
        family: BlockFamily::Evm,
        bytes_encoding: "binary".into(),
        extended: false,
        with_votes: false,
        include_failed_transactions: true,
        tables: declare_inventory(&[("blocks".into(), batch)].into(), &["blocks"]).unwrap(),
    }
}
fn config(output: &Path) -> Config {
    Config {
        output: output.into(),
        start_block: Some(100),
        partition: Partition::None,
        final_blocks_only: true,
        ..Default::default()
    }
}
async fn own(config: &Config) -> DatasetOwnership {
    DatasetOwnership::acquire(
        "session-order-test",
        vec![MutationScope::directory(config.output.to_string_lossy())],
        None,
    )
    .await
    .unwrap()
}
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(root, &path, result);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().into(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    walk(root, root, &mut result);
    result
}

#[tokio::test]
async fn session_refuses_ancestor_and_descendant_authority_before_initialization() {
    let temp = tempfile::tempdir().unwrap();
    let protected = temp.path().join("protected");
    let original = config(&protected);
    let owner = own(&original).await;
    drop(
        IngestionSession::open(&original, mapper(), &owner, None, None)
            .await
            .unwrap(),
    );
    owner.release().await.unwrap();
    let before = snapshot(&protected);

    for selected in [protected.join("missing-child"), temp.path().to_path_buf()] {
        let selected_config = config(&selected);
        let owner = own(&selected_config).await;
        let error = IngestionSession::open(&selected_config, mapper(), &owner, None, None)
            .await
            .err()
            .unwrap();
        assert!(
            error
                .to_string()
                .contains("overlaps another protected root"),
            "{error:#}"
        );
        assert_eq!(snapshot(&protected), before);
        assert!(!protected.join("missing-child").exists());
        assert!(!temp.path().join(CONTROL_DIRECTORY).exists());
        owner.release().await.unwrap();
    }
}

#[tokio::test]
async fn session_refuses_coexisting_journals_before_either_physical_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let config = config(&temp.path().join("dataset"));
    let owner = own(&config).await;
    let session = IngestionSession::open(&config, mapper(), &owner, None, None)
        .await
        .unwrap();
    let authority = session.authority().clone();
    drop(session);

    // This is a valid Writing journal with a staged temporary file. Ordinary
    // controller recovery would remove both that file and the pending record.
    let mut frontier = AcceptedFrontier::resume(&authority.checkpoint);
    let ordinal = frontier.receive(event(100, 1)).unwrap();
    frontier
        .accept(ordinal, routing(authority.descriptor.routing_policy))
        .unwrap();
    let pending = PendingTransaction::prepare(
        &authority,
        frontier.snapshot().unwrap().unwrap(),
        vec![TablePlan {
            table: "blocks".into(),
            rows: 1,
            schema_sha256: authority.descriptor.tables["blocks"].clone(),
            partition: String::new(),
        }],
        PartCompression::Zstd,
    )
    .unwrap();
    let staged = config
        .output
        .join(&pending.parts[0].temporary_relative_path);
    let states = TransactionStateStore::local(&config.output, owner.local().unwrap()).unwrap();
    let stored_authority = states.load().await.unwrap().authority.unwrap();
    states.begin(&stored_authority, pending).await.unwrap();
    fs::create_dir_all(staged.parent().unwrap()).unwrap();
    fs::write(&staged, b"interrupted private encoding").unwrap();

    // Separately create a recoverable legacy merge which would delete its new
    // duplicate. It is bound to this exact protected stream.
    let partition = config.output.join("blocks");
    fs::write(partition.join("part-000001.parquet"), b"original").unwrap();
    fs::write(
        partition.join("part-000002.parquet"),
        b"unfinished duplicate",
    )
    .unwrap();
    let journal = Journal::new(
        &RunContext {
            run_id: "interrupted-fixture".into(),
            lock: config
                .output
                .join("absent-lock")
                .to_string_lossy()
                .into_owned(),
        },
        vec!["part-000001.parquet".into()],
        2,
    )
    .with_protected_stream(Some(&authority.descriptor.id().unwrap()));
    LocalPartition::new(&partition)
        .create_journal(&journal)
        .unwrap();
    let before = snapshot(&config.output);

    let error = IngestionSession::open(&config, mapper(), &owner, None, None)
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("coexist"), "{error:#}");
    assert_eq!(
        snapshot(&config.output),
        before,
        "neither protocol may recover ahead of the coexistence check"
    );

    // Removing only the merge intent allows the same session API to roll back
    // Writing, demonstrating that the previous fixture really was actionable.
    fs::remove_file(partition.join(JOURNAL_FILE)).unwrap();
    drop(
        IngestionSession::open(&config, mapper(), &owner, None, None)
            .await
            .unwrap(),
    );
    assert!(!staged.exists());
    assert!(!config
        .output
        .join(CONTROL_DIRECTORY)
        .join("pending.json")
        .exists());
    assert_eq!(
        fs::read(partition.join("part-000002.parquet")).unwrap(),
        b"unfinished duplicate"
    );
    assert_eq!(
        states
            .load()
            .await
            .unwrap()
            .authority
            .unwrap()
            .payload
            .checkpoint
            .id,
        authority.checkpoint.id
    );
    drop(states);
    owner.release().await.unwrap();
}
