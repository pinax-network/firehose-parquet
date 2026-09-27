//! Deterministic Parquet parts written with the workspace's Parquet 60, plus the
//! Delta `add.stats` JSON that fireparq has to compute itself (delta-rs keeps its
//! footer-to-stats helper crate-private).

use arrow::array::{Array, Int16Array, Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use serde_json::{json, Map, Value};

/// Encodes one complete Parquet part in memory.
pub fn encode_part(batch: &RecordBatch, footer: &[(&str, String)]) -> Vec<u8> {
    let metadata = footer
        .iter()
        .map(|(k, v)| KeyValue::new((*k).to_string(), Some(v.clone())))
        .collect();
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("valid level"),
        ))
        .set_key_value_metadata(Some(metadata))
        .build();
    let mut bytes = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut bytes, batch.schema(), Some(properties)).expect("writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close part");
    bytes
}

/// A deterministic part name in the style of fireparq's `part-v1-*` names.
pub fn part_name(stream: &str, first: u64, last: u64, transaction: &str, index: usize) -> String {
    format!("part-v1-{stream}-{first}-{last}-{transaction}-{index}.parquet")
}

/// Delta `add.stats` for the given columns: `numRecords`, `minValues`,
/// `maxValues` and `nullCount`.
///
/// Only the column types the spike collects statistics for are supported:
/// `Int64`, `Int16` and `Timestamp(Microsecond, UTC)`. Delta encodes timestamp
/// statistics as ISO-8601 strings with millisecond precision; fireparq's
/// timestamps are whole milliseconds, so the bounds stay exact.
pub fn stats_json(batch: &RecordBatch, columns: &[&str]) -> String {
    let mut min = Map::new();
    let mut max = Map::new();
    let mut nulls = Map::new();
    for name in columns {
        let column = batch.column_by_name(name).expect("stats column exists");
        nulls.insert((*name).into(), json!(column.null_count()));
        let (lo, hi) = match column.data_type() {
            DataType::Int64 => {
                let a = column.as_any().downcast_ref::<Int64Array>().expect("int64");
                (
                    arrow::compute::min(a).map(Value::from),
                    arrow::compute::max(a).map(Value::from),
                )
            }
            DataType::Int16 => {
                let a = column.as_any().downcast_ref::<Int16Array>().expect("int16");
                (
                    arrow::compute::min(a).map(Value::from),
                    arrow::compute::max(a).map(Value::from),
                )
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let a = column
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .expect("timestamp");
                (
                    arrow::compute::min(a).map(|v| Value::from(iso_millis(v))),
                    arrow::compute::max(a).map(|v| Value::from(iso_millis(v))),
                )
            }
            other => panic!("no spike statistics for {name}: {other}"),
        };
        if let Some(lo) = lo {
            min.insert((*name).into(), lo);
        }
        if let Some(hi) = hi {
            max.insert((*name).into(), hi);
        }
    }
    json!({
        "numRecords": batch.num_rows(),
        "minValues": min,
        "maxValues": max,
        "nullCount": nulls,
    })
    .to_string()
}

/// Microseconds since the epoch as `YYYY-MM-DDTHH:MM:SS.sssZ`.
pub fn iso_millis(micros: i64) -> String {
    let millis = micros.div_euclid(1_000);
    let days = millis.div_euclid(86_400_000) as i32;
    let ms_of_day = millis.rem_euclid(86_400_000);
    let (h, m, s, ms) = (
        ms_of_day / 3_600_000,
        ms_of_day / 60_000 % 60,
        ms_of_day / 1_000 % 60,
        ms_of_day % 1_000,
    );
    format!(
        "{}T{h:02}:{m:02}:{s:02}.{ms:03}Z",
        crate::mapping::date_string(days)
    )
}
