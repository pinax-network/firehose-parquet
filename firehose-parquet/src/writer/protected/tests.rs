use super::*;
use anyhow::bail;
use arrow::array::{ArrayRef, TimestampMillisecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field};
use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::{
    memory::InMemory, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use std::cell::Cell;
use std::sync::{Arc, Mutex};

thread_local! { static FAILURE: Cell<Option<Stage>> = const { Cell::new(None) }; }
pub(super) fn fails_writing() -> bool {
    FAILURE.with(|failure| failure.get()) == Some(Stage::Writing)
}
pub(super) fn checkpoint(stage: Stage) -> Result<()> {
    if FAILURE.with(|failure| failure.get()) == Some(stage) {
        bail!("injected {stage:?} failure");
    }
    Ok(())
}
struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        FAILURE.with(|failure| failure.set(None));
    }
}
fn fail(stage: Stage) -> Reset {
    FAILURE.with(|failure| failure.set(Some(stage)));
    Reset
}

fn batch(blocks: &[u64], timestamps: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new(
                "timestamp",
                crate::traits::timestamp_millis_utc_type(),
                true,
            ),
        ])),
        vec![
            Arc::new(UInt64Array::from(blocks.to_vec())) as ArrayRef,
            Arc::new(TimestampMillisecondArray::from(timestamps).with_timezone("UTC")),
        ],
    )
    .unwrap()
}
fn data() -> RecordBatch {
    batch(&[11, 10, 11], vec![Some(-1_000), None, Some(-2_000)])
}
fn metadata() -> BlockMetadata {
    BlockMetadata {
        min_block_number: 10,
        max_block_number: 11,
        min_timestamp: Some(-2),
        max_timestamp: Some(-1),
    }
}
fn plan(
    table: &str,
    index: u32,
    batch: &RecordBatch,
    partition: &Partition,
    metadata: &BlockMetadata,
) -> PlannedPart {
    let mut plan = PlannedPart {
        table: table.into(),
        final_relative_path: String::new(),
        temporary_relative_path: String::new(),
        schema_sha256: schema_sha256(batch.schema().as_ref()).unwrap(),
        stream_id: "a".repeat(64),
        transaction_id: "b".repeat(64),
        first_ordinal: 1,
        last_ordinal: 3,
        entry_index: index,
        row_count: batch.num_rows() as u64,
    };
    let writer = ParquetTableWriter::new(PathBuf::new(), partition.clone(), Compression::Zstd);
    let directory = writer.partition_suffix(table, metadata).unwrap();
    plan.final_relative_path = format!("{directory}/{}", final_name(&plan));
    plan.temporary_relative_path = format!("{directory}/{}", temporary_name(&plan));
    plan
}
fn prepare(data: RecordBatch, partition: Partition, metadata: BlockMetadata) -> PreparedFlush {
    let plan = plan("blocks", 0, &data, &partition, &metadata);
    PreparedFlush::new(
        HashMap::from([("blocks".into(), data)]),
        &BTreeMap::from([
            ("blocks".into(), plan.schema_sha256.clone()),
            ("zero_rows".into(), plan.schema_sha256.clone()),
        ]),
        vec![plan],
        partition,
        metadata,
        Compression::Zstd,
        ParquetFileMetadata::new(),
    )
    .unwrap()
}
fn encoded() -> EncodedPart {
    prepare(data(), Partition::Date, metadata())
        .encode(0)
        .unwrap()
}

#[test]
fn schema_digest_is_stable_for_nested_metadata_and_preserves_semantics() {
    fn schema(reverse: bool) -> Schema {
        let pairs = if reverse {
            vec![("b", "2"), ("a", "1")]
        } else {
            vec![("a", "1"), ("b", "2")]
        };
        let metadata: HashMap<String, String> = pairs
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        Schema::new_with_metadata(
            vec![Field::new(
                "items",
                DataType::List(Arc::new(
                    Field::new("item", DataType::Utf8, true).with_metadata(metadata.clone()),
                )),
                false,
            )],
            metadata,
        )
    }
    let first = schema(false);
    let digest = schema_sha256(&first).unwrap();
    assert_eq!(digest, schema_sha256(&schema(true)).unwrap());
    assert_eq!(digest.len(), 64);
    let changed = Schema::new(
        first
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    );
    assert_ne!(digest, schema_sha256(&changed).unwrap());
    let one = Schema::new(vec![
        Field::new("a", DataType::UInt64, false),
        Field::new("b", DataType::Utf8, true),
    ]);
    let two = Schema::new(vec![one.field(1).clone(), one.field(0).clone()]);
    assert_ne!(schema_sha256(&one).unwrap(), schema_sha256(&two).unwrap());
}

#[test]
fn preflight_rejects_invalid_later_table_and_inventory_or_identity_drift() {
    let valid = data();
    let invalid = batch(
        &[11, 10, 11],
        vec![Some(-1_000), Some(86_400_000), Some(-2_000)],
    );
    let parts = vec![
        plan("aaa", 0, &valid, &Partition::Date, &metadata()),
        plan("zzz", 1, &invalid, &Partition::Date, &metadata()),
    ];
    let inventory = BTreeMap::from([
        ("aaa".into(), parts[0].schema_sha256.clone()),
        ("zzz".into(), parts[1].schema_sha256.clone()),
    ]);
    let batches = HashMap::from([("aaa".into(), valid.clone()), ("zzz".into(), invalid)]);
    let rejected = PreparedFlush::new(
        batches,
        &inventory,
        parts.clone(),
        Partition::Date,
        metadata(),
        Compression::Zstd,
        ParquetFileMetadata::new(),
    );
    assert!(rejected
        .err()
        .unwrap()
        .to_string()
        .contains("spans partitions"));
    let mut good_batches = HashMap::from([("aaa".into(), valid.clone()), ("zzz".into(), valid)]);
    for kind in 0..8 {
        let mut plans = parts.clone();
        let mut inventory = inventory.clone();
        let mut batches = good_batches.clone();
        match kind {
            0 => {
                batches.remove("zzz");
            }
            1 => {
                batches.insert("undeclared".into(), data());
            }
            2 => {
                plans[1].row_count += 1;
            }
            3 => {
                plans[1].entry_index = 2;
            }
            4 => {
                plans[1].final_relative_path = format!("../{}", plans[1].final_relative_path);
            }
            5 => {
                plans[1].transaction_id = "c".repeat(64);
            }
            6 => {
                inventory.insert("zzz".into(), "c".repeat(64));
            }
            _ => {
                plans.pop();
            }
        }
        assert!(
            PreparedFlush::new(
                batches,
                &inventory,
                plans,
                Partition::Date,
                metadata(),
                Compression::Zstd,
                ParquetFileMetadata::new()
            )
            .is_err(),
            "case {kind}"
        );
    }
    // A zero-row declared table may be absent or carry an empty, correct schema.
    good_batches.insert("zzzz_zero".into(), data().slice(0, 0));
    let mut inventory = inventory;
    inventory.insert(
        "zzzz_zero".into(),
        schema_sha256(data().schema().as_ref()).unwrap(),
    );
    assert!(PreparedFlush::new(
        good_batches,
        &inventory,
        parts,
        Partition::Date,
        metadata(),
        Compression::Zstd,
        ParquetFileMetadata::new()
    )
    .is_ok());
}

#[test]
fn encoding_is_repeatable_retains_all_batches_and_roundtrips_negative_nullable_times() {
    let flush = prepare(data(), Partition::Date, metadata());
    let one = flush.encode(0).unwrap();
    let two = flush.encode(0).unwrap();
    assert_eq!(one.bytes, two.bytes);
    assert_eq!(one.receipt, two.receipt);
    assert_eq!(flush.batches["blocks"].num_rows(), 3);
    assert_eq!(flush.parts().len(), 1);
    verify_bytes(&one.plan, &one.receipt, one.bytes.clone()).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(one.bytes)
        .unwrap()
        .build()
        .unwrap();
    let actual: Vec<_> = reader
        .map(|batch| batch.unwrap().with_schema(data().schema()).unwrap())
        .collect();
    assert_eq!(
        arrow::compute::concat_batches(&data().schema(), &actual).unwrap(),
        data()
    );
    let mut metadata = metadata();
    metadata.min_timestamp = None;
    metadata.max_timestamp = None;
    let nullable = batch(&[11, 10], vec![None, None]);
    let flat = prepare(nullable.clone(), Partition::Date, metadata.clone());
    assert!(flat.parts()[0]
        .final_relative_path
        .starts_with("blocks/part-v1-"));
    let numeric = prepare(
        nullable,
        Partition::BlockRange {
            size: 10,
            start_block: Some(10),
        },
        metadata,
    );
    assert!(numeric.encode(0).is_ok());
}

#[test]
fn part_indices_include_declared_zero_row_tables_before_and_between_parts() {
    let data = data();
    let digest = schema_sha256(data.schema().as_ref()).unwrap();
    let inventory = ["aaa_empty", "blocks", "middle_empty", "transactions"]
        .into_iter()
        .map(|table| (table.to_owned(), digest.clone()))
        .collect();
    let parts = vec![
        plan("blocks", 1, &data, &Partition::Date, &metadata()),
        plan("transactions", 3, &data, &Partition::Date, &metadata()),
    ];
    let flush = PreparedFlush::new(
        HashMap::from([
            ("blocks".into(), data.clone()),
            ("transactions".into(), data),
        ]),
        &inventory,
        parts,
        Partition::Date,
        metadata(),
        Compression::Zstd,
        ParquetFileMetadata::new(),
    )
    .unwrap();
    assert!(flush.encode(0).is_err());
    assert!(flush.encode(2).is_err());
    for index in [1, 3] {
        let encoded = flush.encode(index).unwrap();
        assert_eq!(encoded.plan.entry_index, index);
        verify_bytes(&encoded.plan, &encoded.receipt, encoded.bytes).unwrap();
    }
    assert_eq!(flush.batches.len(), 2);
}

#[test]
fn reserved_footer_and_foreign_parquet_or_receipt_are_rejected() {
    let data = data();
    let plan = plan("blocks", 0, &data, &Partition::Date, &metadata());
    let mut footer = ParquetFileMetadata::new();
    footer.add("fireparq.ingest.stream_id", "injected");
    assert!(PreparedFlush::new(
        HashMap::from([("blocks".into(), data)]),
        &BTreeMap::from([("blocks".into(), plan.schema_sha256.clone())]),
        vec![plan],
        Partition::Date,
        metadata(),
        Compression::Zstd,
        footer
    )
    .is_err());
    let encoded = encoded();
    let mut wrong = encoded.receipt.clone();
    wrong.sha256 = "0".repeat(64);
    assert!(verify_bytes(&encoded.plan, &wrong, encoded.bytes.clone()).is_err());
    let mut wrong = encoded.plan.clone();
    wrong.transaction_id = "c".repeat(64);
    let dir = Path::new(&wrong.final_relative_path)
        .parent()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    wrong.final_relative_path = format!("{dir}/{}", final_name(&wrong));
    wrong.temporary_relative_path = format!("{dir}/{}", temporary_name(&wrong));
    assert!(verify_bytes(&wrong, &encoded.receipt, encoded.bytes.clone()).is_err());
}

#[test]
fn historical_rich_arrow_schema_roundtrips_with_exact_digest_and_values() {
    let fixture = Bytes::from_static(include_bytes!(
        "../../../tests/fixtures/parquet58/types.parquet"
    ));
    let reader = ParquetRecordBatchReaderBuilder::try_new(fixture)
        .unwrap()
        .build()
        .unwrap();
    let batches: Vec<_> = reader.map(|batch| batch.unwrap()).collect();
    let batch = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let flush = prepare(batch.clone(), Partition::None, metadata());
    let encoded = flush.encode(0).unwrap();
    verify_bytes(&encoded.plan, &encoded.receipt, encoded.bytes.clone()).unwrap();
    let actual: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(encoded.bytes)
        .unwrap()
        .build()
        .unwrap()
        .map(|row| row.unwrap().with_schema(batch.schema()).unwrap())
        .collect();
    assert_eq!(
        arrow::compute::concat_batches(&batch.schema(), &actual).unwrap(),
        batch
    );
}

#[test]
fn multiple_and_nested_dictionary_fields_survive_physical_schema_verification() {
    use arrow::array::{DictionaryArray, StructArray};
    use arrow::datatypes::Int32Type;
    let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::from_iter([
        "first", "second", "first",
    ]));
    let nested_field =
        Field::new("nested_enum", dictionary.data_type().clone(), false).with_metadata(
            HashMap::from([("dict_id".into(), "user-metadata-is-semantic".into())]),
        );
    let nested: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(nested_field),
        dictionary.clone(),
    )]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("block_num", DataType::UInt64, false),
            Field::new("type", dictionary.data_type().clone(), false),
            Field::new("status", dictionary.data_type().clone(), false),
            Field::new("nested", nested.data_type().clone(), false),
        ])),
        vec![
            Arc::new(UInt64Array::from(vec![10, 11, 11])),
            dictionary.clone(),
            dictionary,
            nested,
        ],
    )
    .unwrap();
    let encoded = prepare(batch.clone(), Partition::None, metadata())
        .encode(0)
        .unwrap();
    verify_bytes(&encoded.plan, &encoded.receipt, encoded.bytes.clone()).unwrap();
    let actual: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(encoded.bytes)
        .unwrap()
        .build()
        .unwrap()
        .map(|row| row.unwrap().with_schema(batch.schema()).unwrap())
        .collect();
    assert_eq!(
        arrow::compute::concat_batches(&batch.schema(), &actual).unwrap(),
        batch
    );
}

#[test]
#[allow(deprecated)] // Deliberately emulate IDs reassigned by the IPC transport.
fn schema_digest_normalizes_only_dictionary_transport_ids() {
    fn schema(id: i64, ordered: bool, metadata: &str) -> Schema {
        let dictionary = || {
            Field::new_dict(
                "enum",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
                id,
                ordered,
            )
            .with_metadata(HashMap::from([("dict_id".into(), metadata.into())]))
        };
        Schema::new_with_metadata(
            vec![
                dictionary(),
                Field::new("list", DataType::List(Arc::new(dictionary())), true),
                Field::new("struct", DataType::Struct(vec![dictionary()].into()), true),
                Field::new(
                    "map",
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(
                                vec![Field::new("key", DataType::Utf8, false), dictionary()].into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    false,
                ),
                Field::new(
                    "union",
                    DataType::Union(
                        [(0, Arc::new(dictionary()))].into_iter().collect(),
                        arrow::datatypes::UnionMode::Sparse,
                    ),
                    true,
                ),
            ],
            HashMap::from([("dict_id".into(), metadata.into())]),
        )
    }
    let expected = schema_sha256(&schema(0, false, "preserved")).unwrap();
    assert_eq!(
        expected,
        schema_sha256(&schema(29, false, "preserved")).unwrap()
    );
    assert_ne!(
        expected,
        schema_sha256(&schema(29, true, "preserved")).unwrap()
    );
    assert_ne!(
        expected,
        schema_sha256(&schema(29, false, "changed")).unwrap()
    );
}

#[test]
fn local_stage_and_publish_are_separate_durable_and_never_clobber() {
    let dir = tempfile::tempdir().unwrap();
    let owner = LocalOwnership::acquire(&[dir.path().to_owned()]).unwrap();
    let store = LocalPartStore::new(dir.path(), &owner).unwrap();
    let encoded = encoded();
    assert_eq!(
        store
            .verify(&encoded.plan, &encoded.receipt, false)
            .unwrap(),
        PartPresence::Missing
    );
    assert_eq!(
        store.verify(&encoded.plan, &encoded.receipt, true).unwrap(),
        PartPresence::Missing
    );
    store.stage(&encoded).unwrap();
    let final_path = dir.path().join(&encoded.plan.final_relative_path);
    let temporary = dir.path().join(&encoded.plan.temporary_relative_path);
    assert!(!final_path.exists());
    assert!(temporary.exists());
    assert_eq!(
        store.verify(&encoded.plan, &encoded.receipt, true).unwrap(),
        PartPresence::Present
    );
    assert!(store.stage(&encoded).is_err());
    fs::write(&final_path, b"foreign-complete-file").unwrap();
    assert!(store.publish(&encoded.plan, &encoded.receipt).is_err());
    assert_eq!(fs::read(&final_path).unwrap(), b"foreign-complete-file");
    fs::remove_file(&final_path).unwrap();
    store.publish(&encoded.plan, &encoded.receipt).unwrap();
    assert!(!temporary.exists());
    assert_eq!(
        store
            .verify(&encoded.plan, &encoded.receipt, false)
            .unwrap(),
        PartPresence::Present
    );
    assert!(store.publish(&encoded.plan, &encoded.receipt).is_err());
}

#[test]
fn local_failures_preserve_journal_owned_artifacts_and_postpublication_file() {
    for stage in [
        Stage::Writing,
        Stage::FileSync,
        Stage::StagedDirectorySync,
        Stage::Publish,
        Stage::TemporaryRemoval,
        Stage::PublishedDirectorySync,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let owner = LocalOwnership::acquire(&[dir.path().to_owned()]).unwrap();
        let store = LocalPartStore::new(dir.path(), &owner).unwrap();
        let encoded = encoded();
        let final_path = dir.path().join(&encoded.plan.final_relative_path);
        let temporary = dir.path().join(&encoded.plan.temporary_relative_path);
        let before_publish = matches!(
            stage,
            Stage::Writing | Stage::FileSync | Stage::StagedDirectorySync
        );
        if !before_publish {
            store.stage(&encoded).unwrap();
        }
        {
            let _reset = fail(stage);
            let result = if before_publish {
                store.stage(&encoded)
            } else {
                store.publish(&encoded.plan, &encoded.receipt)
            };
            assert!(result.is_err(), "{stage:?}");
        }
        if matches!(
            stage,
            Stage::TemporaryRemoval | Stage::PublishedDirectorySync
        ) {
            assert_eq!(
                store
                    .verify(&encoded.plan, &encoded.receipt, false)
                    .unwrap(),
                PartPresence::Present
            );
        } else {
            assert!(!final_path.exists(), "{stage:?}");
        }
        assert_eq!(
            temporary.exists(),
            stage != Stage::PublishedDirectorySync,
            "{stage:?}"
        );
        if stage == Stage::Writing {
            assert_eq!(fs::metadata(&temporary).unwrap().len(), 16);
        }
    }
}

#[test]
fn recovery_verification_requires_successful_file_and_directory_sync() {
    let dir = tempfile::tempdir().unwrap();
    let owner = LocalOwnership::acquire(&[dir.path().to_owned()]).unwrap();
    let store = LocalPartStore::new(dir.path(), &owner).unwrap();
    let encoded = encoded();
    store.stage(&encoded).unwrap();
    store.publish(&encoded.plan, &encoded.receipt).unwrap();
    for stage in [Stage::VerifiedFileSync, Stage::VerifiedDirectorySync] {
        let _reset = fail(stage);
        assert!(store
            .verify(&encoded.plan, &encoded.receipt, false)
            .is_err());
        assert!(dir.path().join(&encoded.plan.final_relative_path).is_file());
    }
    assert_eq!(
        store
            .verify(&encoded.plan, &encoded.receipt, false)
            .unwrap(),
        PartPresence::Present
    );
}

#[cfg(unix)]
#[test]
fn local_store_refuses_outside_root_and_nested_symlink_routes() {
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let owner = LocalOwnership::acquire(&[dir.path().to_owned()]).unwrap();
    assert!(LocalPartStore::new(outside.path(), &owner).is_err());
    let store = LocalPartStore::new(dir.path(), &owner).unwrap();
    std::os::unix::fs::symlink(outside.path(), dir.path().join("blocks")).unwrap();
    assert!(store.stage(&encoded()).is_err());
    assert!(fs::read_dir(outside.path()).unwrap().next().is_none());
}

#[derive(Clone, Copy, Default)]
enum RemoteFault {
    #[default]
    None,
    LostResponse,
    CancelledResponse,
    MissingVersion,
    BadReadVersion,
    Unsupported,
    ReadFailure,
}
#[derive(Default, Debug)]
struct Remote {
    inner: InMemory,
    fault: Mutex<RemoteFault>,
    puts: Mutex<Vec<PutMode>>,
}
impl std::fmt::Debug for RemoteFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fixture fault")
    }
}
impl std::fmt::Display for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("synthetic-remote")
    }
}
fn remote_error() -> object_store::Error {
    object_store::Error::Generic {
        store: "private-backend",
        source: "private-token-must-not-leak".into(),
    }
}
#[async_trait]
impl ObjectStore for Remote {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let data = path.as_ref().ends_with(".parquet");
        let fault = *self.fault.lock().unwrap();
        if data {
            self.puts.lock().unwrap().push(opts.mode.clone());
            if matches!(fault, RemoteFault::Unsupported) {
                return Err(object_store::Error::NotImplemented);
            }
        }
        let mut result = self.inner.put_opts(path, payload, opts).await?;
        if data {
            match fault {
                RemoteFault::LostResponse => return Err(remote_error()),
                RemoteFault::CancelledResponse => futures::future::pending::<()>().await,
                RemoteFault::MissingVersion => {
                    result.e_tag = None;
                    result.version = None;
                }
                _ => {}
            }
        }
        Ok(result)
    }
    async fn get_opts(
        &self,
        path: &ObjectPath,
        opts: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut result = self.inner.get_opts(path, opts).await?;
        if path.as_ref().ends_with(".parquet") {
            match *self.fault.lock().unwrap() {
                RemoteFault::ReadFailure => return Err(remote_error()),
                RemoteFault::BadReadVersion => result.meta.e_tag = Some("wrong-version".into()),
                _ => {}
            }
        }
        Ok(result)
    }
    async fn delete(&self, path: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(path).await
    }
    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(path, opts).await
    }
    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}
async fn remote_owner(remote: &Arc<Remote>) -> S3Ownership {
    S3Ownership::acquire(remote.clone(), "ingest", vec!["mainnet".into()])
        .await
        .unwrap()
}

#[tokio::test]
async fn remote_create_roundtrip_is_conditional_and_existing_foreign_file_is_preserved() {
    let remote = Arc::new(Remote::default());
    let owner = remote_owner(&remote).await;
    assert!(S3PartStore::new(&owner, "elsewhere").is_err());
    let store = S3PartStore::new(&owner, "mainnet").unwrap();
    let encoded = encoded();
    assert_eq!(
        store.verify(&encoded.plan, &encoded.receipt).await.unwrap(),
        PartPresence::Missing
    );
    store.publish(&encoded).await.unwrap();
    assert_eq!(
        store.verify(&encoded.plan, &encoded.receipt).await.unwrap(),
        PartPresence::Present
    );
    assert!(!owner.is_mutation_uncertain());
    assert!(matches!(
        remote.puts.lock().unwrap().as_slice(),
        [PutMode::Create]
    ));
    owner.release().await.unwrap();
    let owner = remote_owner(&remote).await;
    let store = S3PartStore::new(&owner, "mainnet").unwrap();
    assert!(store.publish(&encoded).await.is_err());
    assert_eq!(
        remote
            .inner
            .get(&store.key(&encoded.plan).unwrap())
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
        encoded.bytes
    );
    assert!(owner.is_mutation_uncertain());
}

#[tokio::test]
async fn remote_lost_response_or_uncertain_read_never_retries_or_releases_owner() {
    for fault in [
        RemoteFault::LostResponse,
        RemoteFault::MissingVersion,
        RemoteFault::BadReadVersion,
        RemoteFault::Unsupported,
        RemoteFault::ReadFailure,
    ] {
        let remote = Arc::new(Remote::default());
        let owner = remote_owner(&remote).await;
        *remote.fault.lock().unwrap() = fault;
        let store = S3PartStore::new(&owner, "mainnet").unwrap();
        let encoded = encoded();
        let error = store.publish(&encoded).await.unwrap_err();
        assert!(!format!("{error:#}").contains("private-token"));
        assert_eq!(remote.puts.lock().unwrap().len(), 1);
        assert!(owner.is_mutation_uncertain());
        assert!(store.publish(&encoded).await.is_err());
        assert_eq!(remote.puts.lock().unwrap().len(), 1);
        assert!(owner.release().await.is_err());
        let object_store: Arc<dyn ObjectStore> = remote;
        assert_eq!(
            S3Ownership::status(&object_store)
                .await
                .unwrap()
                .unwrap()
                .state(),
            crate::dataset_lock_s3::OwnerState::Owned
        );
    }
}

#[tokio::test]
async fn remote_cancelled_successful_put_latches_uncertainty_before_release() {
    let remote = Arc::new(Remote::default());
    let owner = remote_owner(&remote).await;
    *remote.fault.lock().unwrap() = RemoteFault::CancelledResponse;
    let store = S3PartStore::new(&owner, "mainnet").unwrap();
    let encoded = encoded();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), store.publish(&encoded))
            .await
            .is_err()
    );
    assert_eq!(remote.puts.lock().unwrap().len(), 1);
    assert!(remote
        .inner
        .get(&store.key(&encoded.plan).unwrap())
        .await
        .is_ok());
    assert!(owner.is_mutation_uncertain());
    assert!(owner.release().await.is_err());
}

#[test]
fn protected_and_legacy_parts_share_lookup_metadata_without_changing_rows() {
    use arrow::array::StringArray;
    use arrow::compute::concat_batches;
    for (heights, sorted) in [(vec![10, 10, 11], true), (vec![11, 10, 11], false)] {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("block_num", DataType::UInt64, false),
                Field::new("hash", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(UInt64Array::from(heights)),
                Arc::new(StringArray::from(vec!["first", "second", "third"])),
            ],
        )
        .unwrap();
        let protected = prepare(batch.clone(), Partition::None, metadata())
            .encode(0)
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let mut legacy =
            ParquetTableWriter::new(directory.path(), Partition::None, Compression::Zstd);
        let (path, _) = legacy.write_batch("blocks", &batch, &metadata()).unwrap();
        for (bytes, is_protected) in [
            (protected.bytes.clone(), true),
            (Bytes::from(std::fs::read(path).unwrap()), false),
        ] {
            let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
            let footer = reader.metadata().file_metadata();
            assert_eq!(
                footer
                    .key_value_metadata()
                    .unwrap()
                    .iter()
                    .any(|entry| entry.key.starts_with(FOOTER_PREFIX)),
                is_protected
            );
            assert_eq!(
                reader.metadata().row_group(0).sorting_columns().is_some(),
                sorted
            );
            let filter = reader
                .get_row_group_column_bloom_filter(0, 1)
                .unwrap()
                .unwrap();
            for hash in ["first", "second", "third"] {
                assert!(filter.check(hash));
            }
            let actual = concat_batches(
                &batch.schema(),
                &reader
                    .build()
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(actual, batch);
        }
    }
}

#[test]
fn native_spool_preserves_small_part_bytes_schema_rows_and_receipt() {
    let prepared = prepare(data(), Partition::Date, metadata());
    let memory = prepared.encode(0).unwrap();
    let native = prepared.encode_spooled(0).unwrap();
    assert!(native.bytes.is_empty());
    assert_eq!(memory.receipt, native.receipt);
    let mut file = native.spool.as_ref().unwrap().try_clone().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut actual = Vec::new();
    file.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, memory.bytes);
    verify_file(
        &native.plan,
        &native.receipt,
        native.spool.as_ref().unwrap(),
    )
    .unwrap();
    let batches: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        data().num_rows()
    );
}

#[test]
fn native_spool_rejects_bad_receipt_and_bounded_footer_before_parsing() {
    let encoded = prepare(data(), Partition::Date, metadata())
        .encode_spooled(0)
        .unwrap();
    let file = encoded.spool.as_ref().unwrap();
    let mut receipt = encoded.receipt.clone();
    receipt.sha256 = "f".repeat(64);
    assert!(verify_file(&encoded.plan, &receipt, file).is_err());
    receipt = encoded.receipt.clone();
    receipt.byte_size += 1;
    assert!(verify_file(&encoded.plan, &receipt, file).is_err());
    let mut changed = file.try_clone().unwrap();
    changed.seek(SeekFrom::End(-8)).unwrap();
    changed
        .write_all(&(u32::try_from(MAX_FOOTER_BYTES + 1).unwrap()).to_le_bytes())
        .unwrap();
    let error = verify_file(&encoded.plan, &encoded.receipt, file).unwrap_err();
    assert!(error.to_string().contains("32 MiB"));
}

#[test]
fn native_spool_size_limit_does_not_accept_partial_overflow_write() {
    let mut spool = SpoolWriter::new(8).unwrap();
    spool.write_all(b"1234").unwrap();
    assert!(spool.write_all(b"56789").is_err());
    assert_eq!(spool.size, 4);
    assert_eq!(spool.file.metadata().unwrap().len(), 4);
    assert_eq!(
        hex::encode(spool.hash.finalize()),
        hex::encode(Sha256::digest(b"1234"))
    );
}
