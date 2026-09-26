//! Reproducible local-only process measurements; driven by the audit script.
use super::*;
use arrow::array::BinaryArray;

fn fixture_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::UInt64, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}
fn fixture_batch(start: usize, count: usize) -> RecordBatch {
    let mut state = 0x41a3_f793_296a_832bu64.wrapping_add(start as u64);
    let mut bytes = vec![0u8; count * 1024];
    for chunk in bytes.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        chunk.copy_from_slice(&state.to_le_bytes());
    }
    RecordBatch::try_new(
        fixture_schema(),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                (start..start + count).map(|n| n as u64),
            )),
            Arc::new(BinaryArray::from_iter_values(bytes.chunks_exact(1024))),
        ],
    )
    .unwrap()
}
fn fixture_plan(rows: usize) -> PlannedPart {
    let empty = RecordBatch::new_empty(fixture_schema());
    let mut part = plan(
        "blocks",
        0,
        &empty,
        &Partition::None,
        &BlockMetadata {
            min_block_number: 0,
            max_block_number: rows as u64 - 1,
            min_timestamp: None,
            max_timestamp: None,
        },
    );
    part.row_count = rows as u64;
    part
}
#[test]
#[ignore = "local process qualification; run docs/audit/520-s3-spool-benchmark.py"]
fn native_spool_process_qualification() {
    let mode = std::env::var("FIREPARQ_520_MODE").unwrap();
    let rows: usize = std::env::var("FIREPARQ_520_ROWS").unwrap().parse().unwrap();
    assert!(rows > 0 && rows <= 1024 * 1024);
    let plan = fixture_plan(rows);
    let path = PathBuf::from(std::env::var_os("FIREPARQ_520_FILE").unwrap());
    match mode.as_str() {
        "prepare" => {
            let mut file = File::create(&path).unwrap();
            let mut writer =
                ParquetTableWriter::new(PathBuf::new(), Partition::None, Compression::Zstd);
            let mut meta = ParquetFileMetadata::new();
            meta.entries.extend(footer_identity(&plan));
            writer.set_file_metadata(meta);
            let batch = fixture_batch(0, 4096.min(rows));
            let mut parquet = ArrowWriter::try_new(
                &mut file,
                fixture_schema(),
                Some(writer.writer_properties(&batch).unwrap()),
            )
            .unwrap();
            for start in (0..rows).step_by(4096) {
                parquet
                    .write(&fixture_batch(start, 4096.min(rows - start)))
                    .unwrap();
                if parquet.memory_size() >= ROW_GROUP_MEMORY_BYTES {
                    parquet.flush().unwrap();
                }
            }
            parquet.close().unwrap();
            drop(file);
            let mut file = File::open(&path).unwrap();
            let mut hash = Sha256::new();
            let mut buffer = [0; 64 * 1024];
            loop {
                let n = file.read(&mut buffer).unwrap();
                if n == 0 {
                    break;
                }
                hash.update(&buffer[..n]);
            }
            let receipt = PartReceipt {
                byte_size: file.metadata().unwrap().len(),
                sha256: hex::encode(hash.finalize()),
                row_count: rows as u64,
                schema_sha256: plan.schema_sha256.clone(),
            };
            verify_file(&plan, &receipt, &file).unwrap();
            fs::write(
                path.with_extension("json"),
                serde_json::to_vec(&receipt).unwrap(),
            )
            .unwrap();
            println!(
                "QUALIFICATION {}",
                serde_json::json!({"mode":mode,"rows":rows,"encoded_bytes":receipt.byte_size})
            );
        }
        "encode-memory" | "encode-spool" => {
            let batch = fixture_batch(0, rows);
            let prepared = prepare(
                batch,
                Partition::None,
                BlockMetadata {
                    min_block_number: 0,
                    max_block_number: rows as u64 - 1,
                    min_timestamp: None,
                    max_timestamp: None,
                },
            );
            let encoded = if mode == "encode-memory" {
                prepared.encode(0).unwrap()
            } else {
                prepared.encode_spooled(0).unwrap()
            };
            println!(
                "QUALIFICATION {}",
                serde_json::json!({"mode":mode,"rows":rows,"encoded_bytes":encoded.receipt.byte_size,"encoded_memory_bytes":encoded.bytes.len(),"spool_bytes":encoded.spool.as_ref().map(|f|f.metadata().unwrap().len()).unwrap_or(0)})
            );
        }
        "transfer" => {
            let receipt: PartReceipt =
                serde_json::from_slice(&fs::read(path.with_extension("json")).unwrap()).unwrap();
            let encoded = EncodedPart {
                plan,
                receipt,
                bytes: Bytes::new(),
                spool: Some(File::open(path).unwrap()),
            };
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let endpoint = std::env::var("FIREPARQ_520_ENDPOINT").unwrap();
                    assert!(endpoint.starts_with("http://127.0.0.1:"));
                    let native =
                        crate::s3::upload::NativeS3Upload::fixture_client(endpoint).unwrap();
                    let owner =
                        S3Ownership::acquire_native(native, "fixture", vec!["dataset".into()])
                            .await
                            .unwrap();
                    let store = S3PartStore::new(&owner, "dataset").unwrap();
                    store.publish(&encoded).await.unwrap();
                    owner.release().await.unwrap();
                });
            println!(
                "QUALIFICATION {}",
                serde_json::json!({"mode":mode,"rows":rows,"encoded_bytes":encoded.receipt.byte_size,"encoded_memory_bytes":encoded.bytes.len(),"upload_spool_bytes":encoded.receipt.byte_size,"verification_spool_bytes":encoded.receipt.byte_size})
            );
        }
        _ => panic!("unknown fixture phase"),
    }
}

#[test]
#[ignore = "offline retained two-block EVM qualification; requires FIREPARQ_520_EVM_ROOT"]
fn retained_evm_tables_spool_without_schema_or_value_drift() {
    use arrow::compute::concat_batches;
    let root = PathBuf::from(std::env::var_os("FIREPARQ_520_EVM_ROOT").unwrap());
    let mut directories = vec![root];
    let mut count = 0;
    let mut rows = 0;
    let mut tables = std::collections::BTreeSet::new();
    while let Some(path) = directories.pop() {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                directories.push(path);
                continue;
            }
            if path.extension().is_none_or(|e| e != "parquet")
                || matches!(
                    entry.file_name().to_str(),
                    Some("cursor.parquet" | "partitions.parquet")
                )
            {
                continue;
            }
            let reader =
                ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap()).unwrap();
            let schema = reader.schema().clone();
            let input = concat_batches(
                &schema,
                &reader
                    .build()
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap(),
            )
            .unwrap();
            let numbers = input
                .column_by_name("block_num")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let metadata = BlockMetadata {
                min_block_number: numbers.values().iter().copied().min().unwrap(),
                max_block_number: numbers.values().iter().copied().max().unwrap(),
                min_timestamp: None,
                max_timestamp: None,
            };
            let prepared = prepare(input.clone(), Partition::None, metadata);
            let encoded = prepared.encode_spooled(0).unwrap();
            let reader = ParquetRecordBatchReaderBuilder::try_new(encoded.spool.unwrap()).unwrap();
            assert_eq!(reader.schema().fields(), schema.fields());
            let actual = concat_batches(
                &schema,
                &reader
                    .build()
                    .unwrap()
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(input, actual);
            count += 1;
            rows += input.num_rows();
            let base = PathBuf::from(std::env::var_os("FIREPARQ_520_EVM_ROOT").unwrap());
            tables.insert(
                path.strip_prefix(base)
                    .unwrap()
                    .components()
                    .next()
                    .unwrap()
                    .as_os_str()
                    .to_string_lossy()
                    .into_owned(),
            );
        }
    }
    assert_eq!(tables.len(), 14);
    assert_eq!(rows, 12_298);
    println!(
        "RETAINED_EVM tables={} files={} rows={} exact_schema_and_values=true",
        tables.len(),
        count,
        rows
    );
}
