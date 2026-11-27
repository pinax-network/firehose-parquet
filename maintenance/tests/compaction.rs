//! The compaction keeps the writer's row order (`fireparq_maintenance::compact`):
//! a file an older compaction left out of order is sorted back into it, a
//! file whose order can't be recovered is left alone, and repairs are spread
//! over runs. Several dates at once (`OPTIMIZE_CONCURRENT_DATES`) compact each
//! date once, as one at a time does. Two compactions of one date commit once
//! (#717), and one commits on top of the writer's appends. The concatenation of
//! real writer parts is checked beside a real `fireparq build` in
//! `blocks/tests/delta_maintenance.rs`.

use std::collections::{BTreeMap, HashMap};
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::Arc;

use deltalake_core::arrow::array::{AsArray, Date32Array, Int64Array, RecordBatch, StringArray};
use deltalake_core::arrow::datatypes::{DataType as ArrowType, Field, Int64Type, Schema};
use deltalake_core::datafile::writer::{PartitionWriter, PartitionWriterConfig};
use deltalake_core::kernel::transaction::{
    CommitBuilder, CommitConflictError, CommitProperties, TransactionError,
};
use deltalake_core::kernel::{Action, DataType, PrimitiveType, StructField};
use deltalake_core::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use deltalake_core::parquet::file::properties::WriterProperties;
use deltalake_core::parquet::file::reader::{FileReader, SerializedFileReader};
use deltalake_core::protocol::{DeltaOperation, SaveMode};
use deltalake_core::table::config::TablePropertiesExt as _;
use deltalake_core::{DeltaTable, DeltaTableError, Path as ObjectPath, TableProperty};
use fireparq_maintenance::compact::{
    date_files, plan, Compaction, ROW_ORDER_KEY, ROW_ORDER_TAG, WRITER_ORDER,
};
use fireparq_maintenance::{is_conflict, Lake};
use serde_json::Value;

/// 2026-09-20 to 2026-09-25, as days since the epoch.
const DAYS: [(&str, i32); 6] = [
    ("2026-09-20", 20_716),
    ("2026-09-21", 20_717),
    ("2026-09-22", 20_718),
    ("2026-09-23", 20_719),
    ("2026-09-24", 20_720),
    ("2026-09-25", 20_721),
];

/// A `logs` row: its block, its `block_index`, and a payload naming both.
type Row = (i64, i64);

async fn create_logs(root: &Path) -> DeltaTable {
    create_table(root, "logs").await
}

/// A table with the columns of `logs`, under any name.
async fn create_table(root: &Path, name: &str) -> DeltaTable {
    create_table_with(root, name, Vec::new()).await
}

/// The same, with more table properties.
async fn create_table_with(
    root: &Path,
    name: &str,
    properties: Vec<(TableProperty, &str)>,
) -> DeltaTable {
    let lake = Lake::local(root).unwrap();
    let table = DeltaTable::new(lake.log_store(name).unwrap());
    let mut create = table.create();
    for (property, value) in properties {
        create = create.with_configuration_property(property, Some(value));
    }
    create
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

/// Appends one data file of `rows`, in this order, to the day's partition, as
/// the compaction writes and commits files (delta-rs's write operation would
/// need DataFusion, which this crate doesn't link).
async fn write(table: DeltaTable, rows: &[Row], day: i32) -> DeltaTable {
    write_file(table, rows, day, false).await
}

/// The same; `ordered` tags the file as in writer order, as a writer part is.
async fn write_file(mut table: DeltaTable, rows: &[Row], day: i32, ordered: bool) -> DeltaTable {
    let date = DAYS.iter().find(|(_, days)| *days == day).unwrap().0;
    // The data file holds no partition column.
    let data = batch(rows, day).project(&[0, 1, 2]).unwrap();
    let snapshot = table.snapshot().unwrap();
    let config = snapshot.table_config();
    let stats_columns = config
        .data_skipping_stats_columns
        .as_ref()
        .map(|columns| columns.iter().map(ToString::to_string).collect());
    let writer_config = PartitionWriterConfig::try_new(
        data.schema(),
        Default::default(),
        None,
        None,
        None,
        None,
        Some(ObjectPath::parse(format!("date={date}")).unwrap()),
    )
    .unwrap();
    let mut writer = PartitionWriter::try_with_config(
        table.object_store(),
        writer_config,
        config.num_indexed_cols(),
        stats_columns,
    )
    .unwrap();
    writer.write(&data).await.unwrap();
    let actions = writer
        .close()
        .await
        .unwrap()
        .into_iter()
        .map(|mut add| {
            add.partition_values = HashMap::from([("date".to_string(), Some(date.to_string()))]);
            if ordered {
                add.tags = Some(HashMap::from([(
                    ROW_ORDER_TAG.to_string(),
                    Some(WRITER_ORDER.to_string()),
                )]));
            }
            Action::Add(add)
        })
        .collect();
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    };
    CommitBuilder::default()
        .with_actions(actions)
        .build(Some(snapshot), table.log_store(), operation)
        .await
        .unwrap();
    table.load().await.unwrap();
    table
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

#[tokio::test]
async fn optimize_off_checkpoints_and_compacts_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let rows = writer_order(100, 80, 4);
    let table = create_logs(root).await;
    let table = write(table, &rows[..160], DAYS[0].1).await;
    let table = write(table, &rows[160..], DAYS[0].1).await;
    let version = table.version();

    // OPTIMIZE=0 with VACUUM=0: a checkpoint at the current version, and no commit.
    let (status, lines) = job(root, &[("OPTIMIZE", "0"), ("VACUUM", "0")]).await;
    assert_eq!(status, 0, "{lines:?}");
    assert_eq!(lines[0]["optimize"], false, "{}", lines[0]);
    let line = table_line(&lines);
    assert_eq!(line["dates_to_compact"], serde_json::json!([]), "{line}");
    assert_eq!(line["compacted"], serde_json::json!([]), "{line}");
    assert_eq!(line["version_before"], line["version_after"], "{line}");
    assert_eq!(
        line["checkpoint_version"],
        serde_json::json!(version),
        "{line}"
    );
    assert_eq!(line["open_date"], Value::Null, "{line}");
    assert!(root.join("logs/_delta_log/_last_checkpoint").exists());
    let (after, files, _) = date_rows(root, DAYS[0].0).await;
    assert_eq!(
        (after, files),
        (expected(&rows), 2),
        "both parts are still there"
    );

    // A second run at the same version (two runs at once) checkpoints it again.
    let (status, lines) = job(root, &[("OPTIMIZE", "0"), ("VACUUM", "0")]).await;
    assert_eq!(status, 0, "{lines:?}");
    assert_eq!(
        table_line(&lines)["checkpoint_version"],
        serde_json::json!(version)
    );

    // A slower concurrent run may leave `_last_checkpoint` at an older
    // checkpoint: readers still load the newest version.
    let hint = root.join("logs/_delta_log/_last_checkpoint");
    let older = std::fs::read(&hint).unwrap();
    let table = DeltaTable::new(Lake::local(root).unwrap().log_store("logs").unwrap());
    let mut table = table;
    table.load().await.unwrap();
    let more = writer_order(180, 20, 4);
    let table = write(table, &more, DAYS[1].1).await;
    let (status, lines) = job(root, &[("OPTIMIZE", "0"), ("VACUUM", "0")]).await;
    assert_eq!(status, 0, "{lines:?}");
    assert_eq!(
        table_line(&lines)["checkpoint_version"],
        serde_json::json!(table.version())
    );
    assert_ne!(std::fs::read(&hint).unwrap(), older);
    std::fs::write(&hint, &older).unwrap();
    let mut reader = DeltaTable::new(Lake::local(root).unwrap().log_store("logs").unwrap());
    reader.load().await.unwrap();
    assert_eq!(reader.version(), table.version());
    assert_eq!(date_rows(root, DAYS[1].0).await.0, expected(&more));

    // The same run without OPTIMIZE=0 compacts the two parts.
    let (status, lines) = job(root, &[("VACUUM", "0"), ("OPTIMIZE_REPAIR_DATES", "2")]).await;
    assert_eq!(status, 0, "{lines:?}");
    assert_eq!(
        table_line(&lines)["dates_to_compact"],
        serde_json::json!([DAYS[0].0, DAYS[1].0])
    );
    let (after, files, _) = date_rows(root, DAYS[0].0).await;
    assert_eq!((after, files), (expected(&rows), 1));
}

#[tokio::test]
async fn checkpoint_only_runs_keep_the_table_readable_after_log_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let rows = writer_order(100, 120, 4);
    // Every commit behind a checkpoint has expired at once.
    let expired = vec![(TableProperty::LogRetentionDuration, "interval 0 seconds")];
    let table = create_table_with(root, "logs", expired).await;
    let table = write(table, &rows[..160], DAYS[0].1).await;
    let table = write(table, &rows[160..320], DAYS[0].1).await;
    let version = table.version().unwrap();
    let commit = |version: u64| root.join(format!("logs/_delta_log/{version:020}.json"));
    assert!(commit(0).exists() && commit(version).exists());

    let (status, lines) = job(root, &[("OPTIMIZE", "0"), ("VACUUM", "0")]).await;
    assert_eq!(status, 0, "{lines:?}");
    assert!(
        (0..version).all(|version| !commit(version).exists()),
        "the log cleanup deletes the commits behind the checkpoint"
    );
    assert!(commit(version).exists(), "the checkpointed commit stays");
    let (after, files, _) = date_rows(root, DAYS[0].0).await;
    assert_eq!((after, files), (expected(&rows[..320]), 2));

    // A writer appends on top of the checkpoint, and both kinds of run follow.
    let mut table = DeltaTable::new(Lake::local(root).unwrap().log_store("logs").unwrap());
    table.load().await.unwrap();
    assert_eq!(table.version(), Some(version));
    write(table, &rows[320..], DAYS[0].1).await;
    let (status, lines) = job(root, &[("OPTIMIZE", "0"), ("VACUUM", "0")]).await;
    assert_eq!(status, 0, "{lines:?}");
    let (after, files, _) = date_rows(root, DAYS[0].0).await;
    assert_eq!((after, files), (expected(&rows), 3));
    let (status, lines) = job(root, &[("VACUUM", "0")]).await;
    assert_eq!(status, 0, "{lines:?}");
    let (after, files, _) = date_rows(root, DAYS[0].0).await;
    assert_eq!((after, files), (expected(&rows), 1));
}

/// The keys of the `table` line, as they were before dates ran at once.
const TABLE_KEYS: [&str; 14] = [
    "checkpoint_version",
    "compacted",
    "conflicts",
    "dates_to_compact",
    "errors",
    "event",
    "open_date",
    "repairs_deferred",
    "repairs_unsupported",
    "seconds",
    "table",
    "vacuum",
    "version_after",
    "version_before",
];

fn events<'a>(lines: &'a [Value], event: &str) -> Vec<&'a Value> {
    lines.iter().filter(|line| line["event"] == event).collect()
}

/// The table line's `compacted` entries, by date.
fn compacted_by_date(line: &Value) -> BTreeMap<String, Value> {
    line["compacted"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| (entry["date"].as_str().unwrap().to_string(), entry.clone()))
        .collect()
}

/// The dates of the table's OPTIMIZE commits, one per commit, from its log.
fn optimized_dates(root: &Path, name: &str) -> Vec<String> {
    let mut dates = Vec::new();
    for entry in std::fs::read_dir(root.join(name).join("_delta_log")).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_none_or(|extension| extension != "json") {
            continue;
        }
        for line in std::fs::read_to_string(&path).unwrap().lines() {
            let action: Value = serde_json::from_str(line).unwrap();
            let info = &action["commitInfo"];
            if info["operation"] != "OPTIMIZE" {
                continue;
            }
            let predicate = info["operationParameters"]["predicate"].as_str().unwrap();
            let predicate: Vec<String> = serde_json::from_str(predicate).unwrap();
            let date = predicate[0]
                .strip_prefix("date = '")
                .and_then(|date| date.strip_suffix('\''))
                .unwrap();
            dates.push(date.to_string());
        }
    }
    dates.sort();
    dates
}

/// Six days of `logs`, each in its writer order: three tagged writer parts a
/// day, but two days (the second and fifth) left out of order by an older
/// compaction. Returns each day's rows.
async fn six_days(root: &Path) -> Vec<Vec<Row>> {
    let mut table = create_logs(root).await;
    let mut days = Vec::new();
    for (index, (_, day)) in DAYS.iter().enumerate() {
        let rows = writer_order(100 + 40 * index as i64, 40, 4);
        if index == 1 || index == 4 {
            table = write(table, &scrambled(&rows, 4), *day).await;
        } else {
            for part in rows.chunks(rows.len() / 3 + 1) {
                table = write_file(table, part, *day, true).await;
            }
        }
        days.push(rows);
    }
    days
}

/// With four at once, the first four dates start from the same version, so
/// three of their commits are rebased over the others'.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dates_compacted_at_once_are_each_compacted_once_as_one_at_a_time() {
    let mut runs = Vec::new();
    for concurrent in ["1", "4"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let days = six_days(root).await;
        let settings = [
            ("OPTIMIZE_CONCURRENT_DATES", concurrent),
            ("OPTIMIZE_REPAIR_DATES", "6"),
        ];
        let (status, lines) = job(root, &settings).await;
        assert_eq!(status, 0, "{lines:?}");
        assert_eq!(
            lines[0]["optimize_concurrent_dates"],
            concurrent.parse::<u64>().unwrap()
        );
        let line = table_line(&lines).clone();
        assert_eq!(line["errors"], serde_json::json!([]), "{line}");
        assert_eq!(line["conflicts"], serde_json::json!([]), "{line}");
        let keys: Vec<&str> = line
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, TABLE_KEYS, "the table line is unchanged");

        // Each date once, in one commit, with its rows in the writer's order.
        let all: Vec<&str> = DAYS.iter().map(|(date, _)| *date).collect();
        assert_eq!(line["dates_to_compact"], serde_json::json!(all), "{line}");
        assert_eq!(optimized_dates(root, "logs"), all);
        for ((date, _), rows) in DAYS.iter().zip(&days) {
            let (after, files, ordered) = date_rows(root, date).await;
            assert_eq!(after, expected(rows), "{date}");
            assert_eq!(files, 1, "{date}");
            assert!(ordered, "{date}");
        }

        // A `date` line per date, before the `table` line: its `compacted`
        // entry, with the table and the seconds it took.
        let compacted = compacted_by_date(&line);
        assert_eq!(compacted.len(), DAYS.len(), "{line}");
        let table_at = lines.iter().position(|line| line["event"] == "table");
        let date_lines = events(&lines, "date");
        assert_eq!(date_lines.len(), DAYS.len(), "{lines:?}");
        for date_line in date_lines {
            let at = lines.iter().position(|line| line == date_line);
            assert!(at < table_at, "{lines:?}");
            let mut fields = date_line.as_object().unwrap().clone();
            assert_eq!(fields.remove("event").unwrap(), "date");
            assert_eq!(fields.remove("table").unwrap(), "logs");
            assert!(fields.remove("seconds").unwrap().as_f64().is_some());
            let date = fields["date"].as_str().unwrap();
            assert_eq!(Value::Object(fields.clone()), compacted[date]);
        }
        assert!(events(&lines, "date_error").is_empty(), "{lines:?}");

        // A second run finds nothing to do and reports no date.
        let (status, lines) = job(root, &settings).await;
        assert_eq!(status, 0, "{lines:?}");
        assert_eq!(
            table_line(&lines)["dates_to_compact"],
            serde_json::json!([])
        );
        assert!(events(&lines, "date").is_empty(), "{lines:?}");
        runs.push(compacted);
    }
    assert_eq!(runs[0], runs[1], "the same compaction one date at a time");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_date_stops_no_other_and_repairs_keep_their_budget() {
    let mut runs = Vec::new();
    for concurrent in ["1", "4"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        // Four days out of order; the newest has two logs that tie on the key.
        let days: Vec<Vec<Row>> = (0..4)
            .map(|index| writer_order(100 + 40 * index, 40, 4))
            .collect();
        let mut table = create_logs(root).await;
        for (index, rows) in days.iter().enumerate() {
            let mut rows = scrambled(rows, 4);
            if index == 3 {
                let tie = rows.iter().position(|row| row.0 == 230).unwrap();
                rows.insert(tie, rows[tie]);
            }
            table = write(table, &rows, DAYS[index].1).await;
        }
        let settings = [
            ("OPTIMIZE_CONCURRENT_DATES", concurrent),
            ("OPTIMIZE_REPAIR_DATES", "2"),
        ];

        // A dry run plans two repairs, the newest days, and compacts nothing.
        let mut dry_run = settings.to_vec();
        dry_run.push(("DRY_RUN", "1"));
        let (status, lines) = job(root, &dry_run).await;
        assert_eq!(status, 0, "{lines:?}");
        let line = table_line(&lines);
        assert_eq!(
            line["dates_to_compact"],
            serde_json::json!([DAYS[2].0, DAYS[3].0]),
            "{line}"
        );
        assert_eq!(line["repairs_deferred"], 2, "{line}");
        assert!(events(&lines, "date").is_empty(), "{lines:?}");

        // The newest day fails and gives its repair turn back: the next two
        // days are repaired, the oldest is left for a later run.
        let (status, lines) = job(root, &settings).await;
        assert_eq!(status, 1, "the tie fails the table: {lines:?}");
        let line = table_line(&lines).clone();
        let errors = line["errors"].as_array().unwrap();
        assert_eq!(errors.len(), 1, "{line}");
        assert!(errors[0]
            .as_str()
            .unwrap()
            .contains("tie on the row-order key"));
        assert_eq!(line["conflicts"], serde_json::json!([]), "{line}");
        assert_eq!(line["repairs_deferred"], 1, "{line}");
        assert_eq!(
            line["dates_to_compact"],
            serde_json::json!([DAYS[1].0, DAYS[2].0, DAYS[3].0]),
            "{line}"
        );
        let failed = events(&lines, "date_error");
        assert_eq!(failed.len(), 1, "{lines:?}");
        assert_eq!(failed[0]["date"], DAYS[3].0);
        assert_eq!(failed[0]["conflict"], false);
        assert_eq!(failed[0]["error"], errors[0]);
        let committed: Vec<&str> = events(&lines, "date")
            .iter()
            .map(|line| line["date"].as_str().unwrap())
            .collect();
        assert_eq!(committed.len(), 2, "{lines:?}");
        assert_eq!(optimized_dates(root, "logs"), [DAYS[1].0, DAYS[2].0]);
        for index in [1, 2] {
            let (after, files, ordered) = date_rows(root, DAYS[index].0).await;
            assert_eq!((after, files), (expected(&days[index]), 1));
            assert!(ordered);
        }
        for index in [0, 3] {
            assert!(!date_rows(root, DAYS[index].0).await.2, "left as it is");
        }
        let mut line = line;
        for key in ["seconds", "version_after", "checkpoint_version"] {
            line.as_object_mut().unwrap().remove(key);
        }
        let compacted = compacted_by_date(&line);
        line.as_object_mut().unwrap().remove("compacted");
        runs.push((line, compacted));
    }
    assert_eq!(runs[0], runs[1], "the same outcome one date at a time");
}

/// The Parquet files in a date's directory of `logs`, named in the log or not.
fn files_on_disk(root: &Path, date: &str) -> usize {
    std::fs::read_dir(root.join("logs").join(format!("date={date}")))
        .unwrap()
        .filter(|entry| {
            let path = entry.as_ref().unwrap().path();
            path.extension()
                .is_some_and(|extension| extension == "parquet")
        })
        .count()
}

async fn version(root: &Path) -> i64 {
    let mut table = DeltaTable::new(Lake::local(root).unwrap().log_store("logs").unwrap());
    table.load().await.unwrap();
    i64::try_from(table.version().unwrap()).unwrap()
}

/// A compaction of `date` of `logs`, from `table` as it is, as the job runs it.
fn compaction<'a>(table: &'a DeltaTable, date: &'a str) -> Compaction<'a> {
    Compaction {
        table,
        name: "logs",
        date,
        target: NonZeroU64::new(1 << 30).unwrap(),
        properties: WriterProperties::default(),
        window_bytes: 256 << 20,
        commit: CommitProperties::default(),
    }
}

/// Two jobs that compact the same date from the same snapshot (#717): delta-rs
/// would rebase the second over the first, whose `remove` actions it ignores
/// (`dataChange: false`), and commit the date's rows twice.
#[tokio::test]
async fn two_compactions_of_a_date_from_one_snapshot_commit_once() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let (date, day) = DAYS[0];
    let rows = writer_order(26_000_000, 90, 10);
    let mut table = create_logs(root).await;
    for part in rows.chunks(300) {
        table = write_file(table, part, day, true).await;
    }
    let before = version(root).await;
    let (first, second) = (table.clone(), table.clone());
    let bins = plan(date_files(&first, date).unwrap(), 1 << 30);
    assert_eq!(bins.len(), 1);
    assert_eq!(bins[0].files.len(), 3);

    let done = compaction(&first, date).run(&bins).await.unwrap();
    assert_eq!(
        (done.files_removed, done.files_added, done.rows),
        (3, 1, 900)
    );
    let lost = compaction(&second, date).run(&bins).await.unwrap_err();
    assert!(is_conflict(&lost), "reported as a conflict: {lost}");
    assert!(
        matches!(
            lost,
            DeltaTableError::Transaction {
                source: TransactionError::CommitConflict(
                    CommitConflictError::ConcurrentDeleteDelete
                )
            }
        ),
        "{lost:?}"
    );

    let (after, files, ordered) = date_rows(root, date).await;
    assert_eq!(
        after,
        expected(&rows),
        "each row once, in the writer's order"
    );
    assert_eq!((files, ordered), (1, true));
    assert_eq!(version(root).await, before + 1, "one commit");
    // The three parts and the first compaction's file: the second deleted its
    // own, which nothing names.
    assert_eq!(files_on_disk(root, date), 4);
}

/// The writer appends to the date, and to the next one, after the date was
/// planned: the compaction commits on top of both and keeps the late part.
#[tokio::test]
async fn a_compaction_commits_on_top_of_appends_to_its_date() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let (date, day) = DAYS[0];
    let rows = writer_order(26_000_000, 60, 10);
    let late = writer_order(26_000_060, 30, 10);
    let mut table = create_logs(root).await;
    for part in rows.chunks(300) {
        table = write_file(table, part, day, true).await;
    }
    let planned = table.clone();
    let bins = plan(date_files(&planned, date).unwrap(), 1 << 30);
    let table = write_file(table, &late, day, true).await;
    write_file(table, &writer_order(26_000_090, 10, 10), DAYS[1].1, true).await;
    let before = version(root).await;

    let done = compaction(&planned, date).run(&bins).await.unwrap();
    assert_eq!((done.files_removed, done.files_added), (2, 1));
    let (after, files, ordered) = date_rows(root, date).await;
    let all: Vec<Row> = rows.iter().chain(&late).copied().collect();
    assert_eq!(after, expected(&all));
    assert_eq!(
        (files, ordered),
        (2, true),
        "the compacted file and the late part"
    );
    assert_eq!(version(root).await, before + 1);
}
