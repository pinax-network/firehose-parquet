use super::*;
use arrow::array::UInt64Array;
use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// Rows of `numbers` in every table, all at the fixture time
/// (`date=2023-11-14`).
fn batches(numbers: &[u64]) -> HashMap<String, RecordBatch> {
    let (timestamp, times) = crate::ingest::state::tests::fixture_timestamp(numbers.len());
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::UInt64, false),
        timestamp,
    ]));
    ["blocks", "logs"]
        .into_iter()
        .map(|table| {
            (
                table.into(),
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(UInt64Array::from(numbers.to_vec())), times.clone()],
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
        tables: declare_inventory(&batches(&[]), &["blocks", "logs"], &DeltaTypes::default())
            .unwrap(),
        delta_types: DeltaTypes::default(),
    }
}
fn config(root: &Path) -> Config {
    Config {
        output: root.join("chain"),
        start_block: Some(100),
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
        min_timestamp: Some(crate::ingest::state::tests::FIXTURE_SECONDS),
        max_timestamp: Some(crate::ingest::state::tests::FIXTURE_SECONDS),
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

/// #652: a root in an older partition layout is refused before any stream is
/// opened. Without authority (v0.x `year=/month=/date=DD`, or pre-release
/// `year=/month=/day=` files) eligibility refuses the existing data; a
/// pre-release protected root carries semantic mapper epoch `v1`, whose
/// `{"kind":"date"}` meant `year=/month=/day=`, and is refused by name, so it
/// can never be resumed into a mixed layout.
#[tokio::test]
async fn roots_in_an_older_partition_layout_are_refused_before_any_stream() {
    use crate::durable_state::{ControlKey, LocalStateStore};
    for legacy in [
        "blocks/year=2024/month=01/date=15/part-000001.parquet",
        "blocks/year=2023/month=11/day=14/part-v1-a.parquet",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let owner = own(&config).await;
        let path = config.output.join(legacy);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"old layout").unwrap();
        let error = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("legacy data"), "{error:#}");
        assert!(!config.output.join(".fireparq-ingest").exists());
    }

    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    let owner = own(&config).await;
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_000), None)
        .unwrap();
    flush(&mut session, &[100]).await;
    drop(session);
    // Rewrite the authority as a pre-release build left it.
    let store = LocalStateStore::new(&config.output, owner.local().unwrap()).unwrap();
    let record = store
        .load::<AuthorityState>(ControlKey::State)
        .unwrap()
        .unwrap();
    let mut legacy = record.payload.clone();
    legacy.descriptor.mapper_epoch = "fireparq-mapping-v1".into();
    store
        .replace(ControlKey::State, &record.version, &legacy)
        .unwrap();
    let before = std::fs::read(config.output.join(".fireparq-ingest/state.json")).unwrap();
    let expected = |error: &anyhow::Error| {
        let error = format!("{error:#}");
        assert!(
            error.contains("semantic mapper epoch `fireparq-mapping-v1`")
                && error.contains("<table>/date=YYYY-MM-DD/")
                && error.contains("new, empty output root"),
            "{error}"
        );
    };
    expected(
        &load_authoritative_resume(&config, &owner, false)
            .await
            .err()
            .unwrap(),
    );
    expected(
        &IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
            .await
            .err()
            .unwrap(),
    );
    assert_eq!(
        std::fs::read(config.output.join(".fireparq-ingest/state.json")).unwrap(),
        before
    );
}

/// Ownership must guard exactly the mirror that authority binds, including an
/// independent cursor bucket and a literal brace in the path, and must not add
/// a mirror scope for `--cursor none`.
#[test]
fn mutation_scopes_follow_the_recorded_mirror_binding() {
    let local = tempfile::tempdir().unwrap();
    let local_output = local.path().join("chain");
    let external = local.path().join("state").join("worker.parquet");
    let braced = "mirrors/{worker}.parquet".to_string();
    let local_output = local_output.to_str().unwrap().to_string();
    let cases: Vec<(String, Option<String>, Option<MutationScope>)> = vec![
        (local_output.clone(), None, None),
        (
            local_output.clone(),
            Some("cursor.parquet".into()),
            Some(MutationScope::file(format!(
                "{local_output}/cursor.parquet"
            ))),
        ),
        (
            local_output.clone(),
            Some(crate::artifacts::DEFAULT_CURSOR_MIRROR.into()),
            Some(MutationScope::file(format!(
                "{local_output}/_fireparq/cursor.parquet"
            ))),
        ),
        (
            local_output.clone(),
            Some(braced.clone()),
            Some(MutationScope::file(format!("{local_output}/{braced}"))),
        ),
        (
            local_output.clone(),
            Some(external.to_str().unwrap().into()),
            Some(MutationScope::file(external.to_str().unwrap())),
        ),
        (
            local_output.clone(),
            Some("s3://state/external/cursor.parquet".into()),
            Some(MutationScope::file("s3://state/external/cursor.parquet")),
        ),
        ("s3://data/chain".into(), None, None),
        (
            "s3://data/chain".into(),
            Some("cursor.parquet".into()),
            Some(MutationScope::file("s3://data/chain/cursor.parquet")),
        ),
        (
            "s3://data/chain".into(),
            Some(braced.clone()),
            Some(MutationScope::file(format!("s3://data/chain/{braced}"))),
        ),
        (
            "s3://data".into(),
            Some("cursor.parquet".into()),
            Some(MutationScope::file("s3://data/cursor.parquet")),
        ),
        (
            "s3://data/chain".into(),
            Some(crate::artifacts::DEFAULT_CURSOR_MIRROR.into()),
            Some(MutationScope::file(
                "s3://data/chain/_fireparq/cursor.parquet",
            )),
        ),
        (
            "s3://data".into(),
            Some(crate::artifacts::DEFAULT_CURSOR_MIRROR.into()),
            Some(MutationScope::file("s3://data/_fireparq/cursor.parquet")),
        ),
        (
            "s3://data/chain".into(),
            Some("s3://state/external/cursor.parquet".into()),
            Some(MutationScope::file("s3://state/external/cursor.parquet")),
        ),
    ];
    for (output, cursor, expected_mirror) in cases {
        let config = Config {
            output: output.clone().into(),
            cursor_path: cursor.clone(),
            // A configured S3_BUCKET never redirects an explicit cursor URI.
            s3_bucket: Some("data".into()),
            ..Default::default()
        };
        let scopes = ingestion_mutation_scopes(&config).unwrap();
        assert_eq!(scopes[0], MutationScope::directory(output.clone()));
        assert_eq!(
            scopes.get(1),
            expected_mirror.as_ref(),
            "{output} {cursor:?}"
        );
        assert_eq!(scopes.len(), 1 + usize::from(expected_mirror.is_some()));
        // The same resolution is what authority records and ProtectedMirror uses.
        let binding =
            resolve_mirror_binding(&output, cursor.as_deref(), &aws_config(&config)).unwrap();
        let from_binding = match binding {
            MirrorBinding::Disabled => None,
            MirrorBinding::Local { absolute_path } => Some(MutationScope::file(absolute_path)),
            MirrorBinding::S3 { bucket, key, .. } => {
                Some(MutationScope::file(format!("s3://{bucket}/{key}")))
            }
        };
        assert_eq!(from_binding, expected_mirror, "{output} {cursor:?}");
    }
    // Paths the binding cannot record are refused before ownership is taken.
    for (output, cursor) in [
        ("s3://data/chain", "/absolute/cursor.parquet"),
        ("s3://data/chain", "../cursor.parquet"),
    ] {
        let config = Config {
            output: output.into(),
            cursor_path: Some(cursor.into()),
            ..Default::default()
        };
        assert!(ingestion_mutation_scopes(&config).is_err(), "{cursor}");
    }
}

/// A new dataset with the default `--cursor` binds the mirror inside the
/// dataset's `_fireparq/` directory, and a rerun with the same default resumes
/// without a binding drift. The root then holds only table directories,
/// `_fireparq/` and dot-prefixed control state.
#[tokio::test]
async fn default_mirror_is_bound_in_the_artifact_directory_and_resumes_without_drift() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.cursor_path = Some(crate::artifacts::DEFAULT_CURSOR_MIRROR.into());
    let owner = own(&config).await;
    let mirror = config.output.join("_fireparq/cursor.parquet");
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert!(
        matches!(
            &session.authority().descriptor.mirror,
            MirrorBinding::Local { absolute_path } if Path::new(absolute_path) == mirror
        ),
        "the mirror is bound at creation, in _fireparq/"
    );
    assert!(!mirror.exists());
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_000), None)
        .unwrap();
    flush(&mut session, &[100]).await;
    assert_eq!(
        crate::cursor::load_cursor_parquet(&mirror)
            .unwrap()
            .unwrap()
            .last_block_num,
        100
    );
    drop(session);

    let resume = load_authoritative_resume(&config, &owner, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resume.last_block_num, 100);
    let session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert_eq!(session.authority().checkpoint.ordinal, 1);
    drop(session);

    let mut entries: Vec<_> = std::fs::read_dir(&config.output)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    entries.sort();
    assert_eq!(entries, [".fireparq-ingest", "_fireparq", "blocks", "logs"]);

    // The pre-v1.0.0 spelling is another binding for this dataset.
    let mut legacy = config.clone();
    legacy.cursor_path = Some("cursor.parquet".into());
    let error = load_authoritative_resume(&legacy, &owner, false)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("configured cursor binding differs"),
        "{error}"
    );
}

/// A dataset created before v1.0.0 bound the then-default mirror
/// `<root>/cursor.parquet`. Rerunning it with the new default is refused before
/// anything is written, with the exact flag that resumes it; the mirror is not
/// moved.
#[tokio::test]
async fn pre_v1_default_mirror_is_named_when_resumed_with_the_new_default() {
    let dir = tempfile::tempdir().unwrap();
    let mut created = config(dir.path());
    created.cursor_path = Some(crate::cursor::CURSOR_PARQUET_FILENAME.into());
    let owner = own(&created).await;
    let mut session =
        IngestionSession::open(&created, mapper(BlockFamily::Evm), &owner, None, None)
            .await
            .unwrap();
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_000), None)
        .unwrap();
    flush(&mut session, &[100]).await;
    drop(session);
    let legacy_mirror = created.output.join("cursor.parquet");
    let before = std::fs::read(&legacy_mirror).unwrap();

    let mut current = created.clone();
    current.cursor_path = Some(crate::artifacts::DEFAULT_CURSOR_MIRROR.into());
    let error = load_authoritative_resume(&current, &owner, false)
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(error, PRE_V1_DEFAULT_MIRROR);
    assert!(error.contains("--cursor cursor.parquet"), "{error}");
    assert!(!error.contains(&dir.path().to_string_lossy().to_string()));
    assert!(!created.output.join("_fireparq").exists());
    assert_eq!(std::fs::read(&legacy_mirror).unwrap(), before);

    let resume = load_authoritative_resume(&created, &owner, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resume.last_block_num, 100);
}

#[tokio::test]
async fn cursor_override_is_refused_even_before_authority_exists() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    let owner = own(&config).await;
    let error = load_authoritative_resume(&config, &owner, true)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("only valid with --dry-run"), "{error}");
    assert!(load_authoritative_resume(&config, &owner, false)
        .await
        .unwrap()
        .is_none());
    assert!(!config.output.join(".fireparq-ingest").exists());
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
    let config = config(dir.path());
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
    let config = config(dir.path());
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
    assert!(declare_inventory(
        &batches(&[100]),
        &["blocks", "logs"],
        &DeltaTypes::default()
    )
    .is_err());
    assert!(declare_inventory(&batches(&[]), &["blocks"], &DeltaTypes::default()).is_err());
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

fn remote_config() -> Config {
    Config {
        output: "s3://data/chain".into(),
        start_block: Some(100),
        cursor_path: None,
        ..Default::default()
    }
}

async fn remote_owner(store: &Arc<object_store::memory::InMemory>) -> DatasetOwnership {
    let remote =
        crate::dataset_lock_s3::S3Ownership::acquire(store.clone(), "build", vec!["chain".into()])
            .await
            .unwrap();
    DatasetOwnership::from_remote_for_test("data", remote)
}

async fn remote_owner_record(
    store: &Arc<object_store::memory::InMemory>,
) -> crate::dataset_lock_s3::OwnerRecord {
    let store: Arc<dyn object_store::ObjectStore> = store.clone();
    crate::dataset_lock_s3::S3Ownership::status(&store)
        .await
        .unwrap()
        .unwrap()
}

/// `build` on a new S3 root: open initializes authority (a verified control
/// write), one block is buffered, then the Blocks stream is rejected before
/// the first flush. Ownership is released on exit and the next command
/// acquires the bucket immediately and resumes from the same authority.
#[tokio::test]
async fn remote_stream_failure_before_first_flush_releases_bucket_for_the_next_command() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let config = remote_config();
    let owner = remote_owner(&store).await;
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    let initial = session.authority().checkpoint.id.clone();
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_000), None)
        .unwrap();
    drop(session);
    let stream_error = anyhow::Error::new(tonic::Status::unauthenticated("rejected API key"))
        .context("Firehose Blocks stream failed");
    let error = owner.finish::<()>(Err(stream_error)).await.unwrap_err();
    assert!(error.chain().any(|cause| cause.is::<tonic::Status>()));
    assert!(!format!("{error:#}").contains("retained"), "{error:#}");
    let released = remote_owner_record(&store).await;
    assert_eq!(
        released.state(),
        crate::dataset_lock_s3::OwnerState::Released
    );

    let owner = remote_owner(&store).await;
    assert_eq!(
        owner.remote("data").unwrap().record().generation(),
        released.generation() + 1
    );
    let session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert_eq!(session.authority().checkpoint.id, initial);
    drop(session);
    owner.finish(Ok(())).await.unwrap();
}

/// A first signal ends the stream with `ShutdownRequested`; the runtime
/// discards the buffered window and returns success. Committed flushes stay,
/// ownership is released, and the next run resumes after the last commit.
#[tokio::test]
async fn remote_graceful_shutdown_keeps_commits_and_releases() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let config = remote_config();
    let owner = remote_owner(&store).await;
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_000), None)
        .unwrap();
    flush(&mut session, &[100]).await;
    let committed = session.authority().checkpoint.id.clone();
    let ordinal = receive(&mut session, 101, 1_700_000_001, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_001), None)
        .unwrap();
    drop(session);
    owner.finish(Ok(())).await.unwrap();
    assert_eq!(
        remote_owner_record(&store).await.state(),
        crate::dataset_lock_s3::OwnerState::Released
    );

    let owner = remote_owner(&store).await;
    let session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert_eq!(session.authority().checkpoint.id, committed);
    drop(session);
    owner.finish(Ok(())).await.unwrap();
}

async fn bucket_keys(store: &Arc<object_store::memory::InMemory>) -> Vec<String> {
    use futures::TryStreamExt;
    use object_store::ObjectStore;
    let mut keys: Vec<String> = store
        .list(None)
        .map_ok(|object| object.location.to_string())
        .try_collect()
        .await
        .unwrap();
    keys.sort();
    keys
}

/// The session configuration of `build --output <template>` against a
/// `mainnet` endpoint: the root comes from the one output resolver.
fn bucket_root_config(template: &str) -> Config {
    Config {
        output: crate::cli::resolve_output_root(template, "mainnet")
            .unwrap()
            .into(),
        start_block: Some(100),
        cursor_path: Some(crate::artifacts::DEFAULT_CURSOR_MIRROR.into()),
        ..Default::default()
    }
}

async fn bucket_owner(store: &Arc<object_store::memory::InMemory>) -> DatasetOwnership {
    let remote =
        crate::dataset_lock_s3::S3Ownership::acquire(store.clone(), "build", vec![String::new()])
            .await
            .unwrap();
    DatasetOwnership::from_remote_for_test("data", remote)
}

/// `build --output s3://data` keeps the whole dataset at the bucket root:
/// authority under `.fireparq-ingest/`, parts under the table prefixes and the
/// default mirror at `_fireparq/cursor.parquet`, beside the bucket-wide owner
/// record, which does not make the root ineligible. `s3://data/` resumes the
/// same root. `--output 's3://data/{chain}'` (`s3://data/mainnet`) is nested in
/// it and refused.
#[tokio::test]
async fn remote_session_at_the_bucket_root_resumes_and_refuses_a_chain_template() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let owner = bucket_owner(&store).await;
    let config = bucket_root_config("s3://data");
    assert_eq!(config.output, bucket_root_config("s3://data/").output);
    assert_eq!(
        ingestion_mutation_scopes(&config).unwrap(),
        [
            MutationScope::directory("s3://data"),
            MutationScope::file("s3://data/_fireparq/cursor.parquet"),
        ]
    );
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    match &session.authority().descriptor.output {
        StorageIdentity::S3 { bucket, prefix, .. } => {
            assert_eq!((bucket.as_str(), prefix.as_str()), ("data", ""))
        }
        StorageIdentity::Local { .. } => panic!("expected an S3 output identity"),
    }
    assert!(matches!(
        &session.authority().descriptor.mirror,
        MirrorBinding::S3 { bucket, key, .. } if bucket == "data" && key == "_fireparq/cursor.parquet"
    ));
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_000), None)
        .unwrap();
    let committed = flush(&mut session, &[100]).await;
    assert_eq!((committed.files, committed.rows), (2, 2));
    let checkpoint = session.authority().checkpoint.id.clone();
    drop(session);

    let keys = bucket_keys(&store).await;
    for expected in [
        crate::dataset_lock_s3::OWNER_KEY,
        ".fireparq-ingest/state.json",
        "_fireparq/cursor.parquet",
    ] {
        assert!(keys.iter().any(|key| key == expected), "{keys:?}");
    }
    for table in ["blocks", "logs"] {
        let partition = crate::ingest::state::tests::FIXTURE_DATE;
        assert!(
            keys.iter()
                .any(|key| key.starts_with(&format!("{table}/{partition}/part-v1-"))),
            "{keys:?}"
        );
    }
    assert!(
        keys.iter().all(|key| !key.starts_with("mainnet/")),
        "{keys:?}"
    );
    // The bucket root holds only table prefixes, `_fireparq/` and
    // dot-prefixed control state.
    for key in &keys {
        let top = key.split('/').next().unwrap();
        assert!(
            ["blocks", "logs", "_fireparq"].contains(&top) || top.starts_with('.'),
            "{key}"
        );
    }

    for resumed in ["s3://data", "s3://data/"] {
        let session = IngestionSession::open(
            &bucket_root_config(resumed),
            mapper(BlockFamily::Evm),
            &owner,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(session.authority().checkpoint.id, checkpoint);
        drop(session);
    }

    let nested = bucket_root_config("s3://data/{chain}");
    assert_eq!(nested.output, std::path::PathBuf::from("s3://data/mainnet"));
    let error = IngestionSession::open(&nested, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .err()
        .expect("a {chain} root inside the bucket-root dataset is refused");
    assert!(
        error
            .to_string()
            .contains("overlaps another protected root"),
        "{error:#}"
    );
    assert!(error.to_string().contains("{chain}"), "{error:#}");
    assert_eq!(bucket_keys(&store).await, keys);
    assert!(!owner.remote("data").unwrap().is_mutation_uncertain());
}

/// The reverse: a dataset created with `--output 's3://data/{chain}'` at
/// `s3://data/mainnet` resumes from the same template (or its expansion) and
/// cannot be shadowed by a new bucket-root dataset at `s3://data`.
#[tokio::test]
async fn remote_bucket_root_cannot_initialize_above_an_existing_chain_template_root() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let owner = bucket_owner(&store).await;
    let nested = bucket_root_config("s3://data/{chain}");
    let mut session = IngestionSession::open(&nested, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    let ordinal = receive(&mut session, 100, 1_700_000_000, 1);
    session
        .accept_mapped(ordinal, Some(1_700_000_000), None)
        .unwrap();
    flush(&mut session, &[100]).await;
    let checkpoint = session.authority().checkpoint.id.clone();
    drop(session);
    let keys = bucket_keys(&store).await;
    assert!(
        keys.iter()
            .any(|key| key == "mainnet/_fireparq/cursor.parquet"),
        "{keys:?}"
    );
    for resumed in ["s3://data/{chain}/", "s3://data/mainnet"] {
        let session = IngestionSession::open(
            &bucket_root_config(resumed),
            mapper(BlockFamily::Evm),
            &owner,
            None,
            None,
        )
        .await
        .unwrap();
        assert_eq!(session.authority().checkpoint.id, checkpoint);
        drop(session);
    }

    for above in ["s3://data", "s3://data/"] {
        let error = IngestionSession::open(
            &bucket_root_config(above),
            mapper(BlockFamily::Evm),
            &owner,
            None,
            None,
        )
        .await
        .err()
        .expect("a bucket-root dataset above an existing {chain} root is refused");
        assert!(
            error
                .to_string()
                .contains("overlaps another protected root"),
            "{error:#}"
        );
    }
    assert_eq!(bucket_keys(&store).await, keys);
}

// ---------------------------------------------------------------------------
// Non-final streams: `stream_ordinal` and lifecycle-expired parts
// ---------------------------------------------------------------------------

/// Non-final tables as the mappers write them: `fork_step` then `stream_ordinal`.
fn event_batches(rows: &[(u64, &str, u64)], timestamp: i64) -> HashMap<String, RecordBatch> {
    use arrow::array::{StringArray, TimestampMillisecondArray};
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::UInt64, false),
        Field::new(
            "timestamp",
            crate::traits::timestamp_millis_utc_type(),
            false,
        ),
        crate::traits::fork_step_field(),
        crate::traits::stream_ordinal_field(),
    ]));
    ["blocks", "logs"]
        .into_iter()
        .map(|table| {
            (
                table.into(),
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.0))),
                        Arc::new(
                            TimestampMillisecondArray::from_iter_values(
                                rows.iter().map(|_| timestamp * 1_000),
                            )
                            .with_timezone("UTC"),
                        ),
                        Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
                        Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.2))),
                    ],
                )
                .unwrap(),
            )
        })
        .collect()
}
fn non_final_mapper() -> MapperSemantics {
    MapperSemantics {
        tables: declare_inventory(
            &event_batches(&[], 0),
            &["blocks", "logs"],
            &DeltaTypes::default(),
        )
        .unwrap(),
        ..mapper(BlockFamily::Evm)
    }
}
async fn flush_events(
    session: &mut IngestionSession<'_>,
    rows: &[(u64, &str, u64)],
    timestamp: i64,
) -> Result<Option<CommittedFlush>> {
    session
        .flush(
            event_batches(rows, timestamp),
            BlockMetadata {
                min_block_number: rows.iter().map(|row| row.0).min().unwrap_or(0),
                max_block_number: rows.iter().map(|row| row.0).max().unwrap_or(0),
                min_timestamp: Some(timestamp),
                max_timestamp: Some(timestamp),
            },
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .await
}
/// Receive and map one event, returning the ordinal its rows must carry.
fn deliver(session: &mut IngestionSession<'_>, num: u64, time: i64, step: i32) -> u64 {
    let ordinal = receive(session, num, time, step);
    session.accept_mapped(ordinal, Some(time), None).unwrap();
    ordinal
}

/// The ordinal a non-final row carries is the session's accepted-event
/// ordinal: strictly increasing in delivery order (NEW(A), UNDO(A), NEW(A)
/// get three distinct ordinals), continued from the durable checkpoint by a
/// restarted session, and never reused once a row carrying it is committed.
/// A flush whose rows claim an ordinal outside its own accepted prefix, or
/// lack the column, is refused before anything is journaled.
#[tokio::test]
async fn non_final_rows_carry_durable_strictly_increasing_stream_ordinals() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        final_blocks_only: false,
        ..config(dir.path())
    };
    let owner = own(&config).await;
    let open = || IngestionSession::open(&config, non_final_mapper(), &owner, None, None);
    let t = 1_700_000_000;

    let mut session = open().await.unwrap();
    let ordinals = [
        deliver(&mut session, 100, t, 1),
        deliver(&mut session, 100, t, 2),
        deliver(&mut session, 100, t, 1),
    ];
    assert_eq!(ordinals, [1, 2, 3]);
    let committed = flush_events(
        &mut session,
        &[(100, "NEW", 1), (100, "UNDO", 2), (100, "NEW", 3)],
        t,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!((committed.ordinal, committed.rows), (3, 6));
    // Received and mapped but never committed: a restart reassigns it.
    assert_eq!(deliver(&mut session, 101, t + 1, 1), 4);
    drop(session);

    let mut session = open().await.unwrap();
    assert_eq!(session.authority().checkpoint.ordinal, 3);
    assert_eq!(deliver(&mut session, 101, t + 1, 1), 4);
    flush_events(&mut session, &[(101, "NEW", 4)], t + 1)
        .await
        .unwrap()
        .unwrap();
    drop(session);

    let mut session = open().await.unwrap();
    assert_eq!(deliver(&mut session, 102, t + 2, 1), 5);
    // Rows of an earlier, committed event cannot be committed again under a
    // later prefix.
    let error = flush_events(&mut session, &[(102, "NEW", 4)], t + 2)
        .await
        .err()
        .unwrap();
    assert!(
        format!("{error:#}").contains("outside the accepted prefix 5..=5"),
        "{error:#}"
    );
    drop(session);

    let mut session = open().await.unwrap();
    assert_eq!(session.authority().checkpoint.ordinal, 4);
    assert_eq!(deliver(&mut session, 102, t + 2, 1), 5);
    let error = session
        .flush(
            batches(&[102]),
            meta(102, 102),
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .await
        .err()
        .unwrap();
    assert!(
        format!("{error:#}").contains("lacks a UInt64 stream_ordinal"),
        "{error:#}"
    );
    assert!(
        TransactionStateStore::local(&config.output, owner.local().unwrap())
            .unwrap()
            .load()
            .await
            .unwrap()
            .pending
            .is_none()
    );
}

/// #643: the flush boundary maps every table onto its Delta data file types.
/// A value that does not fit its Delta type refuses the flush, with the table,
/// column and value named, before anything is journaled or written; a flush
/// that fits is written with `Int64` block numbers, the chain's
/// `decimal(20,0)` columns, microsecond timestamps and no `date` column.
#[tokio::test]
async fn flushes_become_delta_data_files_and_values_that_do_not_fit_are_refused() {
    use crate::delta::types::DecimalColumn;
    use arrow::array::{Date32Array, Decimal128Array, Int64Array, TimestampMicrosecondArray};
    const DECIMALS: &[DecimalColumn] = &[DecimalColumn {
        table: "logs",
        column: "amount",
        reason: "a currency amount",
    }];
    let types = DeltaTypes::new(DECIMALS);
    let seconds = crate::ingest::state::tests::FIXTURE_SECONDS;
    let day = crate::traits::date32_from_timestamp_seconds(seconds).unwrap();
    // One row per table: `gas` is a checked long everywhere, `amount` holds
    // u64::MAX and is decimal(20,0) in `logs` only.
    let rows = |tables: &[&str]| -> HashMap<String, RecordBatch> {
        let (timestamp, times) = crate::ingest::state::tests::fixture_timestamp(1);
        let schema = Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            timestamp,
            Field::new("date", DataType::Date32, false),
            Field::new("gas", DataType::UInt64, false),
            Field::new("amount", DataType::UInt64, false),
        ]));
        tables
            .iter()
            .map(|table| {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(UInt64Array::from(vec![100])),
                        times.clone(),
                        Arc::new(Date32Array::from(vec![day])),
                        Arc::new(UInt64Array::from(vec![21_000])),
                        Arc::new(UInt64Array::from(vec![u64::MAX])),
                    ],
                )
                .unwrap();
                (table.to_string(), batch)
            })
            .collect()
    };
    let empty: HashMap<_, _> = rows(&["blocks", "logs"])
        .into_iter()
        .map(|(table, batch)| (table, batch.slice(0, 0)))
        .collect();
    let semantics = || MapperSemantics {
        tables: declare_inventory(&empty, &["blocks", "logs"], &types).unwrap(),
        delta_types: types,
        ..mapper(BlockFamily::Evm)
    };
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    let owner = own(&config).await;
    let parts = |table: &str| -> Vec<std::path::PathBuf> {
        let directory = config.output.join(table).join("date=2023-11-14");
        std::fs::read_dir(&directory)
            .map(|entries| {
                entries
                    .map(|entry| entry.unwrap().path())
                    .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
                    .collect()
            })
            .unwrap_or_default()
    };

    // `blocks.amount` is not a decimal column, so u64::MAX does not fit its
    // checked `long`.
    let mut session = IngestionSession::open(&config, semantics(), &owner, None, None)
        .await
        .unwrap();
    let ordinal = receive(&mut session, 100, seconds, 1);
    session.accept_mapped(ordinal, Some(seconds), None).unwrap();
    let error = session
        .flush(
            rows(&["blocks", "logs"]),
            meta(100, 100),
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .await
        .err()
        .unwrap();
    let message = format!("{error:#}");
    assert!(
        message.contains("table `blocks` column `amount`: value 18446744073709551615")
            && message.contains("refused before anything was written"),
        "{message}"
    );
    let snapshot = TransactionStateStore::local(&config.output, owner.local().unwrap())
        .unwrap()
        .load()
        .await
        .unwrap();
    assert!(snapshot.pending.is_none());
    assert_eq!(snapshot.authority.unwrap().payload.checkpoint.ordinal, 0);
    assert!(parts("blocks").is_empty() && parts("logs").is_empty());
    drop(session);

    // Only `logs`, whose `amount` is decimal(20,0): every u64 fits.
    let mut session = IngestionSession::open(&config, semantics(), &owner, None, None)
        .await
        .unwrap();
    let ordinal = receive(&mut session, 100, seconds, 1);
    session.accept_mapped(ordinal, Some(seconds), None).unwrap();
    let committed = session
        .flush(
            rows(&["logs"]),
            meta(100, 100),
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!((committed.rows, committed.files), (1, 1));
    let [part] = parts("logs").try_into().unwrap();
    let read = crate::writer::read_parquet(&part).unwrap().remove(0);
    let names: Vec<String> = read
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, ["block_num", "timestamp", "gas", "amount"]);
    let column = |name: &str| read.column_by_name(name).unwrap();
    assert_eq!(
        column("block_num")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        100
    );
    assert_eq!(
        column("timestamp")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .value(0),
        seconds * 1_000_000
    );
    assert_eq!(
        column("amount")
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .value_as_string(0),
        u64::MAX.to_string()
    );
    assert_eq!(
        column("gas")
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        21_000
    );
    assert!(parts("blocks").is_empty());
}

/// A live bucket expires old committed parts with an S3 lifecycle rule that
/// fireparq does not own. Neither the running session (its next flush) nor a
/// restarted one (ownership, marker discovery, recovery, resume) reads a
/// committed part outside its own pending transaction, so
/// removing every earlier part changes nothing but the data. Control
/// records under `.fireparq-ingest/` and the bucket owner record must stay.
#[tokio::test]
async fn remote_live_session_is_unaffected_when_expired_committed_parts_disappear() {
    use futures::TryStreamExt;
    use object_store::ObjectStore as _;
    let store = Arc::new(object_store::memory::InMemory::new());
    let config = Config {
        final_blocks_only: false,
        ..remote_config()
    };
    let hour = 1_700_000_000 - 1_700_000_000 % 3_600;
    let owner = remote_owner(&store).await;
    let mut session = IngestionSession::open(&config, non_final_mapper(), &owner, None, None)
        .await
        .unwrap();
    for (num, time, step) in [(100, hour, 1), (100, hour + 1, 2), (101, hour + 2, 1)] {
        let ordinal = deliver(&mut session, num, time, step);
        let label = if step == 1 { "NEW" } else { "UNDO" };
        flush_events(&mut session, &[(num, label, ordinal)], time)
            .await
            .unwrap()
            .unwrap();
    }
    let ordinal = deliver(&mut session, 102, hour + 3_600, 1);
    flush_events(&mut session, &[(102, "NEW", ordinal)], hour + 3_600)
        .await
        .unwrap()
        .unwrap();

    let keys = |store: Arc<object_store::memory::InMemory>| async move {
        store
            .list(None)
            .map_ok(|object| object.location.to_string())
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    };
    let parts: Vec<String> = keys(store.clone())
        .await
        .into_iter()
        .filter(|key| key.contains("/part-v1-"))
        .collect();
    assert_eq!(parts.len(), 8, "{parts:?}");
    // The lifecycle rule: every part older than the current hour disappears.
    let (expired, current): (Vec<_>, Vec<_>) = parts
        .into_iter()
        .partition(|key| !key.contains(&format!("-{ordinal}-{ordinal}-")));
    assert_eq!((expired.len(), current.len()), (6, 2));
    for key in &expired {
        object_store::ObjectStore::delete(store.as_ref(), &key.as_str().into())
            .await
            .unwrap();
    }

    // Running: the next flush commits in the current hour.
    let ordinal = deliver(&mut session, 103, hour + 3_601, 1);
    assert_eq!(ordinal, 5);
    flush_events(&mut session, &[(103, "NEW", ordinal)], hour + 3_601)
        .await
        .unwrap()
        .unwrap();
    drop(session);
    owner.finish(Ok(())).await.unwrap();

    // Restarted: recovery, discovery and resume succeed, and ordinals continue.
    let owner = remote_owner(&store).await;
    let mut session = IngestionSession::open(&config, non_final_mapper(), &owner, None, None)
        .await
        .unwrap();
    assert_eq!(session.authority().checkpoint.ordinal, 5);
    assert_eq!(session.resume_cursor(), Some("private-cursor-103-1"));
    let ordinal = deliver(&mut session, 104, hour + 3_602, 1);
    assert_eq!(ordinal, 6);
    flush_events(&mut session, &[(104, "NEW", ordinal)], hour + 3_602)
        .await
        .unwrap()
        .unwrap();
    drop(session);
    owner.finish(Ok(())).await.unwrap();

    let remaining = keys(store.clone()).await;
    assert!(remaining.iter().all(|key| !expired.contains(key)));
    assert_eq!(
        remaining
            .iter()
            .filter(|key| key.contains("/part-v1-"))
            .count(),
        6
    );
    let owner = remote_owner(&store).await;
    let states = TransactionStateStore::s3("chain", owner.remote("data").unwrap()).unwrap();
    let snapshot = states.load().await.unwrap();
    assert!(snapshot.pending.is_none());
    assert_eq!(snapshot.authority.unwrap().payload.checkpoint.ordinal, 6);
    owner.finish(Ok(())).await.unwrap();
}

mod resume_cost;
