//! #643 L3 through the real session: every committed transaction reaches each
//! table's Delta log with `txn` = its last ordinal, a resume validates the
//! tables and continues, a stream with committed transactions refuses a table
//! without a log, and a Delta commit left without an answer leaves the S3
//! owner's latch clear and is resolved from its `txn` by the next owner
//! (#643 L4).
use super::*;
use crate::delta::commit::tests::loopback_s3;

async fn accept(session: &mut IngestionSession<'_>, numbers: &[u64]) {
    for number in numbers {
        let ordinal = receive(session, *number, 1_700_000_000, 1);
        session
            .accept_mapped(ordinal, Some(1_700_000_000), None)
            .unwrap();
    }
}

async fn txn(session: &IngestionSession<'_>, table: &str) -> Option<i64> {
    session
        .delta_tables()
        .unwrap()
        .txn_version(table)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn committed_transactions_reach_every_delta_table_and_a_resume_continues_their_txn() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    let owner = own(&config).await;
    let (registry, metrics) = crate::metrics::init();
    let mut session = IngestionSession::open(
        &config,
        mapper(BlockFamily::Evm),
        &owner,
        Some(&metrics),
        None,
    )
    .await
    .unwrap();
    // Every table exists, empty, before the first transaction.
    for table in ["blocks", "logs"] {
        assert_eq!(session.delta_tables().unwrap().version(table).unwrap(), 0);
        assert_eq!(txn(&session, table).await, None);
    }
    accept(&mut session, &[100]).await;
    let first = flush(&mut session, &[100]).await;
    assert_eq!(first.delta.len(), 2);
    assert_eq!(first.delta.last().unwrap().table, "blocks");
    accept(&mut session, &[101, 102]).await;
    let second = flush(&mut session, &[101, 102]).await;
    assert_eq!(second.ordinal, 3);
    for table in ["blocks", "logs"] {
        assert_eq!(session.delta_tables().unwrap().version(table).unwrap(), 2);
        assert_eq!(txn(&session, table).await, Some(3));
        let labels = crate::metrics::TableLabels {
            table: table.into(),
        };
        assert_eq!(
            metrics.delta_log_tail_commits.get_or_create(&labels).get(),
            3
        );
    }
    let mut exported = String::new();
    prometheus_client::encoding::text::encode(&mut exported, &registry).unwrap();
    for table in ["blocks", "logs"] {
        // One observed commit per transaction, and the tail of versions 0..=2.
        assert!(
            exported.contains(&format!(
                "firehose_parquet_delta_commit_seconds_count{{table=\"{table}\"}} 2"
            )),
            "{exported}"
        );
        assert!(exported.contains(&format!(
            "firehose_parquet_delta_log_tail_commits{{table=\"{table}\"}} 3"
        )));
        assert!(exported.contains(&format!(
            "firehose_parquet_delta_commit_retries_total{{table=\"{table}\"}} 0"
        )));
    }
    // The log adds the parts on disk, byte for byte in size.
    let log = std::fs::read_to_string(
        config
            .output
            .join("logs/_delta_log/00000000000000000002.json"),
    )
    .unwrap();
    let add = log
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .find_map(|action| action.get("add").cloned())
        .unwrap();
    let part = config
        .output
        .join("logs")
        .join(add["path"].as_str().unwrap());
    assert_eq!(
        std::fs::metadata(&part).unwrap().len(),
        add["size"].as_u64().unwrap()
    );
    drop(session);

    // A resume opens and validates the same tables and continues.
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert_eq!(txn(&session, "blocks").await, Some(3));
    accept(&mut session, &[103]).await;
    let third = flush(&mut session, &[103]).await;
    assert_eq!(third.ordinal, 4);
    for table in ["blocks", "logs"] {
        assert_eq!(session.delta_tables().unwrap().version(table).unwrap(), 3);
        assert_eq!(txn(&session, table).await, Some(4));
    }
    drop(session);

    // Once the stream has committed, a table without a Delta log is refused:
    // its committed rows would be unreachable.
    std::fs::remove_dir_all(config.output.join("logs/_delta_log")).unwrap();
    let refused = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .err()
        .unwrap();
    assert!(
        format!("{refused:#}").contains("has no Delta log"),
        "{refused:#}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_stream_commits_its_delta_tables_and_resumes_with_a_new_owner() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let config = remote_config();
    let owner = remote_owner(&store).await;
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    accept(&mut session, &[100]).await;
    flush(&mut session, &[100]).await;
    assert_eq!(txn(&session, "blocks").await, Some(1));
    drop(session);
    owner.finish(Ok(())).await.unwrap();
    let owner = remote_owner(&store).await;
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    accept(&mut session, &[101]).await;
    flush(&mut session, &[101]).await;
    for table in ["blocks", "logs"] {
        assert_eq!(txn(&session, table).await, Some(2));
        assert_eq!(session.delta_tables().unwrap().version(table).unwrap(), 2);
    }
    drop(session);
    owner.finish(Ok(())).await.unwrap();
}

/// A Delta commit whose response is lost (#643 L4, design §3.5): the flush
/// fails, but the S3 owner's uncertainty latch stays clear, because the log
/// resolves the outcome. `finish` releases the bucket, and the next owner's
/// start reads `txn`: the commit landed, so it is not made again, and
/// authority advances.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unanswered_delta_commit_releases_ownership_and_the_next_start_reads_its_txn() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let server = loopback_s3::Server::start().await;
    let aws = AwsConfig {
        aws_access_key_id: Some("loopback".into()),
        aws_secret_access_key: Some("loopback".into()),
        aws_session_token: None,
        aws_region: Some("us-east-1".into()),
        aws_endpoint_url: Some(server.endpoint.clone()),
    };
    let owner_with_log = || async {
        let client = crate::delta::store::s3_builder(&aws, loopback_s3::BUCKET)
            .unwrap()
            .with_allow_http(true)
            .build()
            .unwrap();
        let remote = crate::dataset_lock_s3::S3Ownership::acquire(
            store.clone(),
            "build",
            vec!["chain".into()],
        )
        .await
        .unwrap()
        .with_delta_log(Arc::new(client));
        DatasetOwnership::from_remote_for_test("data", remote)
    };
    let owner = owner_with_log().await;
    let config = remote_config();
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    // `blocks` commits last; its commit is stored but never answered.
    let blocks_commit = "chain/blocks/_delta_log/00000000000000000001.json";
    server.lose_put_responses(blocks_commit);
    accept(&mut session, &[100]).await;
    let failed = session
        .flush(
            batches(&[100]),
            meta(100, 100),
            Compression::Zstd,
            ParquetFileMetadata::new(),
        )
        .await
        .err()
        .unwrap();
    let message = format!("{failed:#}");
    assert!(
        message.contains("Delta log of table `blocks`")
            && message.contains("outcome is unknown")
            && message.contains("resolves it from the table's txn"),
        "{message}"
    );
    assert!(
        !owner.remote("data").unwrap().is_mutation_uncertain(),
        "a log commit leaves the latch clear"
    );
    assert!(server.object(blocks_commit).is_some(), "the commit landed");
    assert!(server
        .object("chain/logs/_delta_log/00000000000000000001.json")
        .is_some());
    assert_eq!(session.authority().checkpoint.ordinal, 0);
    drop(session);
    // The failed build releases the bucket.
    assert!(owner.finish(Err::<(), _>(failed)).await.is_err());
    assert_eq!(
        remote_owner_record(&store).await.state(),
        crate::dataset_lock_s3::OwnerState::Released
    );

    // The next owner's start finds `blocks` holding the transaction.
    let owner = owner_with_log().await;
    let mut session = IngestionSession::open(&config, mapper(BlockFamily::Evm), &owner, None, None)
        .await
        .unwrap();
    assert_eq!(session.authority().checkpoint.ordinal, 1);
    for table in ["blocks", "logs"] {
        assert_eq!(txn(&session, table).await, Some(1));
        assert_eq!(session.delta_tables().unwrap().version(table).unwrap(), 1);
    }
    accept(&mut session, &[101]).await;
    flush(&mut session, &[101]).await;
    for table in ["blocks", "logs"] {
        assert_eq!(txn(&session, table).await, Some(2));
        assert_eq!(session.delta_tables().unwrap().version(table).unwrap(), 2);
    }
    drop(session);
    owner.finish(Ok(())).await.unwrap();
}
