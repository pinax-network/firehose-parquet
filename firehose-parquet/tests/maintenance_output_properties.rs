//! Merge and rollup outputs carry the shared #519 lookup writer properties:
//! explicit compression, bounded row groups, Bloom filters on scalar lookup
//! columns only, no streaming sort assertion, and preserved chain metadata.
//!
//! The inputs are written with Parquet defaults (no Bloom filters, no
//! compression, 1M-row groups), so every property checked here comes from the
//! maintenance writer rather than being copied from a source file.
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, BinaryArray, StringArray, UInt64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use firehose_parquet::config::Compression;
use firehose_parquet::merge::{run_merge, MergeConfig};
use firehose_parquet::rollup::{run_rollup, RollupConfig, RollupTarget};
use firehose_parquet::writer::properties::{for_schema, ROW_GROUP_ROWS};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;

/// Two parts of this size give a merged/rolled-up file that needs two row
/// groups under the shared cap, but one under Parquet's 1M-row default.
const PART_ROWS: usize = ROW_GROUP_ROWS / 2 + 1_000;
const LOOKUP_COLUMNS: [&str; 2] = ["tx_hash", "address"];
const PLAIN_COLUMNS: [&str; 2] = ["block_num", "data"];

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("block_num", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, true),
        Field::new("address", DataType::Binary, true),
        Field::new("data", DataType::Binary, true),
    ]))
}

fn part(first: usize) -> RecordBatch {
    let rows = first..first + PART_ROWS;
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(UInt64Array::from_iter_values(
                rows.clone().map(|i| i as u64),
            )),
            Arc::new(StringArray::from_iter(
                rows.clone()
                    .map(|i| (i % 97 != 0).then(|| format!("0x{i:064x}"))),
            )),
            Arc::new(BinaryArray::from_iter(rows.clone().map(|i| {
                (i % 89 != 0).then(|| (i as u64).to_be_bytes().to_vec())
            }))),
            Arc::new(BinaryArray::from_iter(
                rows.map(|i| Some((i as u32).to_le_bytes().to_vec())),
            )),
        ],
    )
    .unwrap()
}

fn source_metadata() -> Vec<KeyValue> {
    vec![
        KeyValue::new("firehose-parquet.block_type".into(), Some("evm".into())),
        KeyValue::new("firehose-parquet.chain_name".into(), Some("mainnet".into())),
        // Ingestion receipts describe one transaction, never a maintenance output.
        KeyValue::new(
            "fireparq.ingest.transaction".into(),
            Some("source-only".into()),
        ),
    ]
}

/// Parquet defaults on purpose: nothing below may be inherited from the input.
fn write_source(path: &Path, batch: &RecordBatch) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let props = WriterProperties::builder()
        .set_key_value_metadata(Some(source_metadata()))
        .build();
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(path).unwrap(),
        batch.schema(),
        Some(props),
    )
    .unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn parquet_files(dir: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|ext| ext == "parquet") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

fn assert_lookup_properties(path: &Path, expected: &RecordBatch) {
    let file = std::fs::File::open(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let metadata = builder.metadata().clone();
    let parquet_schema = metadata.file_metadata().schema_descr();
    let column_index = |name: &str| {
        parquet_schema
            .columns()
            .iter()
            .position(|column| column.path().parts() == [name])
            .unwrap()
    };

    // The same property source that ingestion uses selects the lookup columns.
    let shared = for_schema(Compression::Zstd, expected.schema().as_ref(), None);
    for name in LOOKUP_COLUMNS {
        assert!(shared
            .bloom_filter_properties(&ColumnPath::from(name))
            .is_some());
    }
    for name in PLAIN_COLUMNS {
        assert!(shared
            .bloom_filter_properties(&ColumnPath::from(name))
            .is_none());
    }

    assert_eq!(
        metadata.file_metadata().num_rows() as usize,
        expected.num_rows()
    );
    assert_eq!(metadata.num_row_groups(), 2, "{}", path.display());
    let rows = expected
        .column_by_name("block_num")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let hashes = expected
        .column_by_name("tx_hash")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let addresses = expected
        .column_by_name("address")
        .unwrap()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap();
    let mut offset = 0;
    for (group_index, group) in metadata.row_groups().iter().enumerate() {
        let group_rows = group.num_rows() as usize;
        assert!(group_rows <= ROW_GROUP_ROWS, "{group_rows} rows");
        // Streaming maintenance output makes no ordering assertion.
        assert!(group.sorting_columns().is_none());
        for column in group.columns() {
            // The footer records the codec, not the writer's zstd level.
            assert!(
                matches!(column.compression(), parquet::basic::Compression::ZSTD(_)),
                "{:?}",
                column.compression()
            );
        }
        for name in PLAIN_COLUMNS {
            assert!(builder
                .get_row_group_column_bloom_filter(group_index, column_index(name))
                .unwrap()
                .is_none());
        }
        let hash_filter = builder
            .get_row_group_column_bloom_filter(group_index, column_index("tx_hash"))
            .unwrap()
            .expect("tx_hash Bloom filter");
        let address_filter = builder
            .get_row_group_column_bloom_filter(group_index, column_index("address"))
            .unwrap()
            .expect("address Bloom filter");
        // Maintenance keeps source row order, so rows map by position.
        for row in offset..offset + group_rows {
            if hashes.is_valid(row) {
                assert!(
                    hash_filter.check(hashes.value(row)),
                    "row {}",
                    rows.value(row)
                );
            }
            if addresses.is_valid(row) {
                assert!(address_filter.check(addresses.value(row)));
            }
        }
        offset += group_rows;
    }

    let key_values = metadata.file_metadata().key_value_metadata().unwrap();
    let value = |key: &str| {
        key_values
            .iter()
            .find(|kv| kv.key == key)
            .and_then(|kv| kv.value.as_deref())
    };
    assert_eq!(value("firehose-parquet.block_type"), Some("evm"));
    assert_eq!(value("firehose-parquet.chain_name"), Some("mainnet"));
    assert_eq!(value("fireparq.ingest.transaction"), None);

    let batches = builder
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(&concat_batches(&schema(), &batches).unwrap(), expected);
}

fn expected_rows() -> RecordBatch {
    concat_batches(&schema(), &[part(0), part(PART_ROWS)]).unwrap()
}

#[test]
fn merge_output_carries_shared_lookup_properties() {
    let root = tempfile::tempdir().unwrap();
    let partition = root.path().join("blocks/year=2024/month=01/day=15");
    write_source(&partition.join("part-000001.parquet"), &part(0));
    write_source(&partition.join("part-000002.parquet"), &part(PART_ROWS));

    let result = run_merge(&MergeConfig {
        path: root.path().to_string_lossy().into_owned(),
        compression: Compression::Zstd,
        flush_rows: None,
        flush_bytes: 0,
        dry_run: false,
        verbose: false,
        aws: None,
        cache_control: String::new(),
    })
    .unwrap();
    assert_eq!(result.partitions_merged, 1);
    assert_eq!(result.files_written, 1);

    let outputs = parquet_files(&partition);
    assert_eq!(outputs.len(), 1, "{outputs:?}");
    assert_lookup_properties(&outputs[0], &expected_rows());
}

#[test]
fn rollup_output_carries_shared_lookup_properties() {
    let source = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let day = "blocks/year=2024/month=01/day=15";
    write_source(
        &source
            .path()
            .join(format!("{day}/hour=14/part-000001.parquet")),
        &part(0),
    );
    write_source(
        &source
            .path()
            .join(format!("{day}/hour=15/part-000001.parquet")),
        &part(PART_ROWS),
    );

    run_rollup(&RollupConfig {
        source: source.path().to_string_lossy().into_owned(),
        output: output.path().to_string_lossy().into_owned(),
        target: RollupTarget::Date,
        compression: Compression::Zstd,
        flush_bytes: 0,
        delete_source: false,
        aws: None,
        cache_control: String::new(),
    })
    .unwrap();

    let outputs = parquet_files(&output.path().join(day));
    assert_eq!(outputs.len(), 1, "{outputs:?}");
    assert_lookup_properties(&outputs[0], &expected_rows());
}
