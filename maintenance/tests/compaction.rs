//! The compaction keeps the writer's row order (`fireparq_maintenance::compact`):
//! a file an older compaction left out of order is sorted back into it, a
//! file whose order can't be recovered is left alone, and repairs are spread
//! over runs. The concatenation of real writer parts is checked beside a real
//! `fireparq build` in `blocks/tests/delta_maintenance.rs`.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use deltalake_core::arrow::array::{AsArray, Date32Array, Int64Array, RecordBatch, StringArray};
use deltalake_core::arrow::datatypes::{DataType as ArrowType, Field, Int64Type, Schema};
use deltalake_core::kernel::{DataType, PrimitiveType, StructField};
use deltalake_core::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use deltalake_core::parquet::file::reader::{FileReader, SerializedFileReader};
use deltalake_core::{DeltaTable, TableProperty};
use fireparq_maintenance::compact::{date_files, ROW_ORDER_KEY, WRITER_ORDER};
use fireparq_maintenance::Lake;
use serde_json::Value;

/// 2026-09-20 and 2026-09-21, as days since the epoch.
const DAYS: [(&str, i32); 2] = [("2026-09-20", 20_716), ("2026-09-21", 20_717)];

/// A `logs` row: its block, its `block_index`, and a payload naming both.
type Row = (i64, i64);

async fn create_logs(root: &Path) -> DeltaTable {
    create_table(root, "logs").await
}

/// A table with the columns of `logs`, under any name.
async fn create_table(root: &Path, name: &str) -> DeltaTable {
    let lake = Lake::local(root).unwrap();
    let table = DeltaTable::new(lake.log_store(name).unwrap());
    table
        .create()
        .with_columns([
            StructField::new("block_num", DataType::Primitive(PrimitiveType::Long), false),
            StructField::new(
                "block_index",
                DataType::Primitive(PrimitiveType::Long),
                false,
            ),
            StructField::new("data", DataType::Primitive(PrimitiveType::String), true),
            StructField::new("date", DataType::Primitive(PrimitiveType::Date), false),
        ])
        .with_partition_columns(["date"])
        .with_configuration_property(TableProperty::DataSkippingStatsColumns, Some("block_num"))
        .await
        .unwrap()
}

fn batch(rows: &[Row], day: i32) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_num", ArrowType::Int64, false),
        Field::new("block_index", ArrowType::Int64, false),
        Field::new("data", ArrowType::Utf8, true),
        Field::new("date", ArrowType::Date32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(
                rows.iter()
                    .map(|(block, index)| format!("log {index} of block {block}")),
            )),
            Arc::new(Date32Array::from(vec![day; rows.len()])),
        ],
    )
    .unwrap()
}

/// `blocks` blocks of `per_block` logs each, from `first`, in the writer's order.
fn writer_order(first: i64, blocks: i64, per_block: i64) -> Vec<Row> {
    (first..first + blocks)
        .flat_map(|block| (0..per_block).map(move |index| (block, index)))
        .collect()
}

/// As delta-rs's OPTIMIZE left a date: runs of blocks newest first, and one
/// block whose second half comes before its first.
fn scrambled(rows: &[Row], per_block: i64) -> Vec<Row> {
    let run = (per_block * 25) as usize;
    let mut out: Vec<Row> = rows.chunks(run).rev().flatten().copied().collect();
    let block = rows[rows.len() / 2].0;
    let start = out.iter().position(|row| row.0 == block).unwrap();
    let half = (per_block / 2) as usize;
    out[start..start + per_block as usize].rotate_left(half);
    out
}

async fn write(table: DeltaTable, rows: &[Row], day: i32) -> DeltaTable {
    table.write(vec![batch(rows, day)]).await.unwrap()
}

/// A local file's rows, in their order in the file.
fn file_rows(path: &Path) -> Vec<(Row, String)> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let block = batch
            .column_by_name("block_num")
            .unwrap()
            .as_primitive::<Int64Type>()
            .clone();
        let index = batch
            .column_by_name("block_index")
            .unwrap()
            .as_primitive::<Int64Type>()
            .clone();
        let data = batch
            .column_by_name("data")
            .unwrap()
            .as_string::<i32>()
            .clone();
        for i in 0..batch.num_rows() {
            rows.push(((block.value(i), index.value(i)), data.value(i).to_string()));
        }
    }
    rows
}

/// The date's rows, file by file in block order, each file in its own order.
async fn date_rows(root: &Path, date: &str) -> (Vec<(Row, String)>, usize, bool) {
    table_rows(root, "logs", date).await
}

async fn table_rows(root: &Path, name: &str, date: &str) -> (Vec<(Row, String)>, usize, bool) {
    let mut table = DeltaTable::new(Lake::local(root).unwrap().log_store(name).unwrap());
    table.load().await.unwrap();
    let mut files = date_files(&table, date).unwrap();
    files.sort_by_key(|file| file.first_block);
    let ordered = files.iter().all(|file| file.ordered);
    let mut rows = Vec::new();
    for file in &files {
        rows.extend(file_rows(&root.join(name).join(&file.path)));
    }
    (rows, files.len(), ordered)
}

fn footer(path: &Path) -> BTreeMap<String, String> {
    let reader = SerializedFileReader::new(std::fs::File::open(path).unwrap()).unwrap();
    reader
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .into_iter()
        .flatten()
        .filter_map(|pair| Some((pair.key.clone(), pair.value.clone()?)))
        .collect()
}

async fn job(root: &Path, settings: &[(&str, &str)]) -> (i32, Vec<Value>) {
    job_on(root, "logs", settings).await
}

async fn job_on(root: &Path, tables: &str, settings: &[(&str, &str)]) -> (i32, Vec<Value>) {
    let mut env: BTreeMap<&str, String> = BTreeMap::from([
        ("LAKE_ROOT", root.display().to_string()),
        ("LAKE_TABLES", tables.to_string()),
        ("OPTIMIZE_DATES", "all".to_string()),
        ("VACUUM_RETENTION_HOURS", "0".to_string()),
    ]);
    for (name, value) in settings {
        env.insert(name, value.to_string());
    }
    let lookup = |name: &str| env.get(name).cloned();
    let mut out = Vec::new();
    let status = fireparq_maintenance::run(&lookup, &mut out).await;
    let lines = String::from_utf8(out)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    (status, lines)
}

fn table_line(lines: &[Value]) -> &Value {
    lines.iter().find(|line| line["event"] == "table").unwrap()
}

fn expected(rows: &[Row]) -> Vec<(Row, String)> {
    rows.iter()
        .map(|&(block, index)| ((block, index), format!("log {index} of block {block}")))
        .collect()
}

#[tokio::test]
async fn a_file_out_of_order_is_sorted_back_into_the_writer_order() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    // 2000 blocks of 100 logs: several MiB, so a 1 MiB window sorts in pieces.
    let rows = writer_order(26_000_000, 2_000, 100);
    let table = create_logs(root).await;
    write(table, &scrambled(&rows, 100), DAYS[0].1).await;
    let (before, _, ordered) = date_rows(root, DAYS[0].0).await;
    assert!(!ordered, "a delta-rs compaction is not in writer order");
    assert_ne!(before, expected(&rows), "the file is out of order");

    // A target below the file's size: the bin is still one file, never split
    // inside a block (1.0.6 rolled it, and then repaired the pair every run).
    let small_target = [
        ("OPTIMIZE_REPAIR_WINDOW_BYTES", "1048576"),
        ("OPTIMIZE_TARGET_SIZE", "1048576"),
    ];
    let (status, lines) = job(root, &small_target).await;
    assert_eq!(status, 0, "{lines:?}");
    let line = table_line(&lines);
    assert_eq!(line["errors"], serde_json::json!([]), "{line}");
    assert_eq!(line["compacted"][0]["repaired_bins"], 1, "{line}");
    assert_eq!(line["compacted"][0]["files_added"], 1, "{line}");
    assert_eq!(line["compacted"][0]["rows"], rows.len(), "{line}");

    let (after, files, ordered) = date_rows(root, DAYS[0].0).await;
    assert!(ordered, "the repaired files are tagged in writer order");
    assert_eq!(
        after,
        expected(&rows),
        "every row back in the writer's order"
    );
    let lake = root.join("logs");
    let mut table = DeltaTable::new(Lake::local(root).unwrap().log_store("logs").unwrap());
    table.load().await.unwrap();
    for file in date_files(&table, DAYS[0].0).unwrap() {
        let keys = footer(&lake.join(&file.path));
        assert_eq!(
            keys.get(ROW_ORDER_KEY).map(String::as_str),
            Some(WRITER_ORDER),
            "{keys:?}"
        );
    }

    // A second run finds nothing to do.
    let (status, lines) = job(root, &small_target).await;
    assert_eq!(status, 0, "{lines:?}");
    assert_eq!(
        table_line(&lines)["dates_to_compact"],
        serde_json::json!([])
    );
    assert_eq!(date_rows(root, DAYS[0].0).await.1, files);
}

#[tokio::test]
async fn rows_that_tie_on_the_key_are_left_as_they_are() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    // The older date repairs; the newer one has two logs of block 160 with
    // the same block_index: no order to restore.
    let older = writer_order(100, 40, 4);
    let mut newer = scrambled(&writer_order(140, 40, 4), 4);
    let duplicate = newer.iter().position(|row| row.0 == 160).unwrap();
    newer.insert(duplicate, newer[duplicate]);
    let table = create_logs(root).await;
    let table = write(table, &scrambled(&older, 4), DAYS[0].1).await;
    write(table, &newer, DAYS[1].1).await;

    // One repair a run: the failing date holds back no other.
    let (status, lines) = job(root, &[("OPTIMIZE_REPAIR_DATES", "1")]).await;
    assert_eq!(status, 1, "a refused repair fails the table: {lines:?}");
    let line = table_line(&lines);
    let errors = line["errors"].to_string();
    assert!(errors.contains("tie on the row-order key"), "{line}");
    assert_eq!(line["compacted"][0]["date"], DAYS[0].0, "{line}");
    assert_eq!(line["compacted"].as_array().unwrap().len(), 1, "{line}");
    assert_eq!(date_rows(root, DAYS[0].0).await.0, expected(&older));
    let (after, files, ordered) = date_rows(root, DAYS[1].0).await;
    assert!(!ordered, "the date with the tie is left as it is");
    assert_eq!((after.len(), files), (newer.len(), 1));
}

#[tokio::test]
async fn a_table_without_a_known_order_is_left_as_it_is() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let rows = scrambled(&writer_order(100, 40, 4), 4);
    let table = create_table(root, "accounts").await;
    write(table, &rows, DAYS[0].1).await;

    let (status, lines) = job_on(root, "accounts", &[]).await;
    assert_eq!(status, 0, "nothing to fail for: {lines:?}");
    let line = table_line(&lines);
    assert_eq!(line["repairs_unsupported"], 1, "{line}");
    assert_eq!(line["compacted"], serde_json::json!([]), "{line}");
    let (after, _, ordered) = table_rows(root, "accounts", DAYS[0].0).await;
    assert!(!ordered);
    assert_eq!(after.len(), rows.len());
}

#[tokio::test]
async fn repairs_are_spread_over_runs_newest_date_first() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let first = writer_order(100, 40, 4);
    let second = writer_order(140, 40, 4);
    let table = create_logs(root).await;
    let table = write(table, &scrambled(&first, 4), DAYS[0].1).await;
    write(table, &scrambled(&second, 4), DAYS[1].1).await;

    let (status, lines) = job(root, &[("OPTIMIZE_REPAIR_DATES", "1")]).await;
    assert_eq!(status, 0, "{lines:?}");
    let line = table_line(&lines);
    assert_eq!(
        line["dates_to_compact"],
        serde_json::json!([DAYS[1].0]),
        "{line}"
    );
    assert_eq!(line["repairs_deferred"], 1, "{line}");
    assert_eq!(date_rows(root, DAYS[1].0).await.0, expected(&second));
    assert!(!date_rows(root, DAYS[0].0).await.2, "not repaired yet");

    let (status, lines) = job(root, &[("OPTIMIZE_REPAIR_DATES", "1")]).await;
    assert_eq!(status, 0, "{lines:?}");
    assert_eq!(
        table_line(&lines)["dates_to_compact"],
        serde_json::json!([DAYS[0].0])
    );
    assert_eq!(date_rows(root, DAYS[0].0).await.0, expected(&first));

    // OPTIMIZE_REPAIR_DATES=0 repairs nothing.
    let dir = tempfile::tempdir().unwrap();
    let table = create_logs(dir.path()).await;
    write(table, &scrambled(&first, 4), DAYS[0].1).await;
    let (status, lines) = job(dir.path(), &[("OPTIMIZE_REPAIR_DATES", "0")]).await;
    assert_eq!(status, 0, "{lines:?}");
    assert_eq!(table_line(&lines)["repairs_deferred"], 1);
    assert!(!date_rows(dir.path(), DAYS[0].0).await.2);
}
