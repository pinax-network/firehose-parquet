use super::*;
use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

fn batches(numbers: &[u64]) -> HashMap<String, RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "block_num",
        DataType::UInt64,
        false,
    )]));
    ["blocks", "logs"]
        .into_iter()
        .map(|table| {
            (
                table.into(),
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(UInt64Array::from(numbers.to_vec()))],
                )
                .unwrap(),
            )
        })
        .collect()
}
fn mapper(family: BlockFamily) -> MapperSemantics {
    MapperSemantics {
        chain: "mainnet".into(),
        family,
        bytes_encoding: "binary".into(),
        extended: false,
        with_votes: false,
        include_failed_transactions: true,
        tables: declare_inventory(&batches(&[]), &["blocks", "logs"]).unwrap(),
    }
}
fn config(root: &Path) -> Config {
    Config {
        output: root.join("chain"),
        start_block: Some(100),
        partition: Partition::None,
        final_blocks_only: true,
        ..Default::default()
    }
}
async fn own(config: &Config) -> DatasetOwnership {
    DatasetOwnership::acquire(
        "test",
        vec![crate::dataset_lock::MutationScope::directory(
            config.output.to_string_lossy(),
        )],
        None,
    )
    .await
    .unwrap()
}
fn identity(number: u64, timestamp: i64) -> BlockIdentity {
    BlockIdentity {
        block_num: number,
        block_id: format!("block-{number}"),
        timestamp,
        ..Default::default()
    }
}
fn meta(first: u64, last: u64) -> BlockMetadata {
    BlockMetadata {
        min_block_number: first,
        max_block_number: last,
        min_timestamp: None,
        max_timestamp: None,
    }
}
async fn flush(session: &mut IngestionSession<'_>, numbers: &[u64]) -> CommittedFlush {
    session
        .flush(
            batches(numbers),
            meta(
                *numbers.first().unwrap_or(&0),
                *numbers.last().unwrap_or(&0),
            ),
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .await
        .unwrap()
        .unwrap()
}
fn receive(session: &mut IngestionSession<'_>, num: u64, time: i64, step: i32) -> u64 {
    let family = session.authority().descriptor.family;
    session
        .receive(
            format!("private-cursor-{num}-{step}"),
            &identity(num, time),
            step,
            family,
        )
        .unwrap()
}

#[tokio::test]
async fn new_session_commits_all_tables_repairs_deleted_mirror_and_completes_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.cursor_path = Some("cursor.parquet".into());
    let owner = own(&config).await;
    let (_, metrics) = crate::metrics::init();
    assert!(!config.output.exists());
    let mut session = IngestionSession::open(
        &config,
        mapper(BlockFamily::Evm),
        &owner,
        Some(&metrics),
        None,
    )
    .await
    .unwrap();
    assert!(session.resume_cursor().is_none());
    assert!(!config.output.join("cursor.parquet").exists());
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_000), None)
        .unwrap();
    let result = flush(&mut session, &[100]).await;
    assert_eq!((result.rows, result.files, result.ordinal), (2, 2, 1));
    for table in ["blocks", "logs"] {
        let labels = crate::metrics::TableLabels {
            table: table.into(),
        };
        assert_eq!(metrics.rows_written_total.get_or_create(&labels).get(), 1);
        assert_eq!(metrics.files_written_total.get_or_create(&labels).get(), 1);
        assert_eq!(metrics.buffer_rows.get_or_create(&labels).get(), 0);
    }
    assert_eq!(metrics.buffer_estimated_bytes.get(), 0);
    assert!(config.output.join("cursor.parquet").exists());
    assert!(session.complete_request(101, true).await.unwrap());
    assert!(session.request_already_complete(101).unwrap());
    let checkpoint = session.authority().checkpoint.id.clone();
    drop(session);
    std::fs::remove_file(config.output.join("cursor.parquet")).unwrap();
    let mut session = IngestionSession::open(
        &config,
        mapper(BlockFamily::Evm),
        &owner,
        Some(&metrics),
        None,
    )
    .await
    .unwrap();
    assert_eq!(checkpoint, session.authority().checkpoint.id);
    assert_eq!(session.resume_cursor(), Some("private-cursor-100-1"));
    assert!(config.output.join("cursor.parquet").exists());
    assert_eq!(metrics.cursor_last_block_num.get(), 100);
    let before = std::fs::read(config.output.join(".fireparq-ingest/state.json")).unwrap();
    assert!(!session.complete_request(101, true).await.unwrap());
    assert_eq!(
        before,
        std::fs::read(config.output.join(".fireparq-ingest/state.json")).unwrap()
    );
}

#[tokio::test]
async fn legacy_root_and_changed_semantics_never_initialize_or_rewind() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    let owner = own(&config).await;
    std::fs::create_dir_all(config.output.join("blocks")).unwrap();
    std::fs::write(config.output.join("blocks/random.parquet"), b"legacy").unwrap();
    assert!(
        IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
            .await
            .is_err()
    );
    assert!(!config.output.join(".fireparq-ingest").exists());
    std::fs::remove_file(config.output.join("blocks/random.parquet")).unwrap();
    let session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    drop(session);
    let mut changed = mapper(BlockFamily::Evm);
    changed.bytes_encoding = "hex".into();
    assert!(IngestionSession::open(&config, changed, &owner, None, None)
        .await
        .is_err());
    assert!(load_authoritative_resume(&config, &owner, true)
        .await
        .is_err());
    let mut changed = config.clone();
    changed.start_block = Some(101);
    assert!(load_authoritative_resume(&changed, &owner, false)
        .await
        .is_err());
    changed.start_block = None;
    let resume = load_authoritative_resume(&changed, &owner, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resume.start_block, Some(100));
    assert!(resume.cursor.is_empty());
}

#[tokio::test]
async fn restored_lookahead_routes_remaining_missing_prefix_before_source_is_reread() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    let owner = own(&config).await;
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    let first = receive(&mut session, 100, 0, 1);
    receive(&mut session, 101, 0, 1);
    let anchor = receive(&mut session, 102, 1_700_000_000, 1);
    session
        .accept_mapped(first, Some(1_700_000_000), Some(anchor))
        .unwrap();
    flush(&mut session, &[]).await;
    drop(session);
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert_eq!(session.routing_timestamp_hint(), Some(1_700_000_000));
    let next = receive(&mut session, 101, 0, 1);
    session
        .accept_mapped(next, Some(1_700_000_000), None)
        .unwrap();
    flush(&mut session, &[]).await;
    assert!(session
        .receive(
            "private-cursor-102-1".into(),
            &identity(102, 1_700_000_001),
            1,
            BlockFamily::Evm
        )
        .is_err());
    assert!(
        session
            .receive(
                "private-cursor-102-1".into(),
                &identity(102, 1_700_000_000),
                1,
                BlockFamily::Evm
            )
            .is_err(),
        "a mismatched received source poisons the session"
    );
}

#[tokio::test]
async fn filtered_future_inherits_actual_preceding_routing_and_zero_rows_commit_without_time() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.partition = Partition::Hour;
    let owner = own(&config).await;
    let mut session =
        IngestionSession::open(&config, mapper(BlockFamily::Solana), &owner, None, None)
            .await
            .unwrap();
    let first = receive(&mut session, 100, 1_700_000_000, 1);
    let filtered = receive(&mut session, 101, 0, 2);
    session.accept_filtered(filtered).unwrap();
    assert!(!session.has_accepted().unwrap());
    session
        .accept_mapped(first, Some(1_700_000_000), None)
        .unwrap();
    let outcome = flush(&mut session, &[]).await;
    assert_eq!((outcome.files, outcome.rows, outcome.ordinal), (0, 0, 2));
    let anchor = session
        .authority()
        .checkpoint
        .routing
        .anchor
        .as_ref()
        .unwrap();
    assert_eq!((anchor.source_ordinal, anchor.seconds), (1, 1_700_000_000));
    assert_eq!(
        session
            .authority()
            .checkpoint
            .event
            .as_ref()
            .unwrap()
            .source_timestamp,
        None
    );
    assert!(session.complete_request(102, true).await.unwrap());
}

#[tokio::test]
async fn solana_missing_source_keeps_explicit_seed_then_observed_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.partition = Partition::Hour;
    let owner = own(&config).await;
    let mut session =
        IngestionSession::open(&config, mapper(BlockFamily::Solana), &owner, None, None)
            .await
            .unwrap();
    let missing = receive(&mut session, 100, 0, 1);
    session
        .accept_mapped(missing, Some(SOLANA_GENESIS_ROUTING_SECONDS), None)
        .unwrap();
    flush(&mut session, &[]).await;
    let checkpoint = &session.authority().checkpoint;
    assert_eq!(checkpoint.event.as_ref().unwrap().source_timestamp, None);
    assert_eq!(
        checkpoint.routing.anchor.as_ref().unwrap().provenance,
        AnchorProvenance::SolanaGenesisFallback
    );
    let observed = receive(&mut session, 101, 1_700_000_000, 1);
    session
        .accept_mapped(observed, Some(1_700_000_000), None)
        .unwrap();
    let missing = receive(&mut session, 102, 0, 1);
    session
        .accept_mapped(missing, Some(1_700_000_000), None)
        .unwrap();
    flush(&mut session, &[]).await;
    assert_eq!(
        session
            .authority()
            .checkpoint
            .routing
            .anchor
            .as_ref()
            .unwrap()
            .source_ordinal,
        2
    );
}

#[tokio::test]
async fn mapper_family_order_time_and_inventory_mismatches_are_fatal_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    let owner = own(&config).await;
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert!(session
        .receive(
            "private".into(),
            &identity(100, 1_700_000_000),
            1,
            BlockFamily::Bitcoin
        )
        .is_err());
    drop(session);
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    assert!(session
        .accept_mapped(ordinal, Some(1_700_000_001), None)
        .is_err());
    assert!(!config.output.join("blocks").exists());
    assert!(declare_inventory(&batches(&[100]), &["blocks", "logs"]).is_err());
    assert!(declare_inventory(&batches(&[]), &["blocks"]).is_err());
}

#[tokio::test]
async fn remote_session_initializes_and_resumes_using_one_borrowed_owner() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let remote =
        crate::dataset_lock_s3::S3Ownership::acquire(store.clone(), "test", vec!["chain".into()])
            .await
            .unwrap();
    let owner = DatasetOwnership::from_remote_for_test("data", remote);
    let config = Config {
        output: "s3://data/chain".into(),
        start_block: Some(100),
        partition: Partition::None,
        cursor_path: None,
        ..Default::default()
    };
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_000), None)
        .unwrap();
    let first = flush(&mut session, &[100]).await;
    let checkpoint = session.authority().checkpoint.id.clone();
    drop(session);
    let session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert_eq!(session.authority().checkpoint.id, checkpoint);
    assert_eq!((first.files, first.rows), (2, 2));
    assert!(!owner.remote("data").unwrap().is_mutation_uncertain());
}
