//! The `add.stats` of each part (#643 L3, `docs/design/delta-lake.md` §1.4, §2).
//!
//! delta-rs keeps its footer-to-statistics helper crate-private, so fireparq
//! computes the statistics itself, from the batch it encodes into the part.
//! They are journaled with the part's receipt, so a commit (or a later
//! roll-forward) builds the exact `add` without reading the file.
//!
//! The JSON is the Delta protocol's per-file statistics:
//!
//! ```json
//! {"numRecords":2,
//!  "minValues":{"block_num":100,"timestamp":"2023-11-14T22:13:20.000Z"},
//!  "maxValues":{"block_num":101,"timestamp":"2023-11-14T22:13:21.000Z"},
//!  "nullCount":{"block_num":0,"timestamp":0}}
//! ```
//!
//! Only [`STATS_COLUMNS`] get bounds, the columns `delta.dataSkippingStatsColumns`
//! names; partition columns never have statistics. Integer bounds are JSON
//! numbers. Timestamp bounds are ISO-8601 UTC strings with millisecond
//! precision, as the protocol writes them: the minimum is rounded down and the
//! maximum up, so the bounds hold even for a value that is not a whole
//! millisecond (fireparq's times always are, so they are exact). A column
//! whose values are all null gets its `nullCount` and no bounds.

use anyhow::{bail, ensure, Context, Result};
use arrow::array::{
    Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, TimestampMicrosecondArray,
    TimestampMillisecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};
use serde_json::{json, Map, Value};

/// The columns with file statistics, in the order
/// `delta.dataSkippingStatsColumns` lists them.
pub const STATS_COLUMNS: [&str; 2] = ["block_num", "timestamp"];

/// The `add.stats` JSON of a part holding exactly `batch`.
///
/// A statistics column that the table does not have is left out. One whose
/// type has no bounds here (anything but a signed integer or a UTC
/// timestamp) keeps only its `nullCount`: readers then cannot skip the file
/// on it, which is never wrong.
pub fn stats_json(batch: &RecordBatch) -> Result<String> {
    let mut min = Map::new();
    let mut max = Map::new();
    let mut nulls = Map::new();
    for name in STATS_COLUMNS {
        let Some(column) = batch.column_by_name(name) else {
            continue;
        };
        nulls.insert(name.into(), json!(column.null_count()));
        if let Some((lo, hi)) =
            bounds(column.as_ref()).with_context(|| format!("statistics of column `{name}`"))?
        {
            min.insert(name.into(), lo);
            max.insert(name.into(), hi);
        }
    }
    Ok(json!({
        "numRecords": batch.num_rows(),
        "minValues": min,
        "maxValues": max,
        "nullCount": nulls,
    })
    .to_string())
}

/// Checks journaled statistics: a JSON object whose `numRecords` is the
/// part's row count. The controller never commits a part whose statistics
/// disagree with its planned rows.
pub fn check_stats(stats: &str, rows: u64) -> Result<()> {
    let value: Value = serde_json::from_str(stats).context("part statistics are not JSON")?;
    let object = value
        .as_object()
        .context("part statistics are not a JSON object")?;
    ensure!(
        object.get("numRecords").and_then(Value::as_u64) == Some(rows),
        "part statistics do not record the part's row count"
    );
    for key in ["minValues", "maxValues", "nullCount"] {
        if let Some(entry) = object.get(key) {
            ensure!(
                entry.is_object(),
                "part statistics `{key}` is not an object"
            );
        }
    }
    Ok(())
}

fn bounds(column: &dyn Array) -> Result<Option<(Value, Value)>> {
    macro_rules! integers {
        ($array:ty) => {{
            let array = column
                .as_any()
                .downcast_ref::<$array>()
                .context("statistics column type")?;
            arrow::compute::min(array)
                .zip(arrow::compute::max(array))
                .map(|(lo, hi)| (json!(lo), json!(hi)))
        }};
    }
    Ok(match column.data_type() {
        DataType::Int64 => integers!(Int64Array),
        DataType::Int32 => integers!(Int32Array),
        DataType::Int16 => integers!(Int16Array),
        DataType::Int8 => integers!(Int8Array),
        DataType::Timestamp(unit, Some(zone)) if zone.as_ref() == "UTC" => {
            let (lo, hi) = match unit {
                TimeUnit::Microsecond => {
                    let array = column
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .context("statistics column type")?;
                    match arrow::compute::min(array).zip(arrow::compute::max(array)) {
                        Some((lo, hi)) => (lo.div_euclid(1_000), ceil_millis(hi)),
                        None => return Ok(None),
                    }
                }
                TimeUnit::Millisecond => {
                    let array = column
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .context("statistics column type")?;
                    match arrow::compute::min(array).zip(arrow::compute::max(array)) {
                        Some(bounds) => bounds,
                        None => return Ok(None),
                    }
                }
                _ => return Ok(None),
            };
            Some((json!(iso_millis(lo)?), json!(iso_millis(hi)?)))
        }
        _ => None,
    })
}

/// Microseconds rounded up to the next whole millisecond.
fn ceil_millis(micros: i64) -> i64 {
    micros.div_euclid(1_000) + i64::from(micros.rem_euclid(1_000) != 0)
}

/// Milliseconds since the epoch as `YYYY-MM-DDTHH:MM:SS.sssZ`.
pub(crate) fn iso_millis(millis: i64) -> Result<String> {
    let time = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)
        .context("timestamp statistic is outside the supported range")?;
    if !(0..=9999).contains(&time.year()) {
        bail!("timestamp statistic is outside years 0-9999");
    }
    Ok(format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        time.year(),
        u8::from(time.month()),
        time.day(),
        time.hour(),
        time.minute(),
        time.second(),
        time.millisecond()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch(block_num: ArrayRefLike, timestamp: Option<Vec<Option<i64>>>) -> RecordBatch {
        let mut fields = vec![Field::new("block_num", block_num.data_type(), false)];
        let mut columns = vec![block_num.array()];
        if let Some(times) = timestamp {
            fields.push(Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ));
            columns.push(Arc::new(
                TimestampMicrosecondArray::from(times).with_timezone("UTC"),
            ));
        }
        fields.push(Field::new("other", DataType::Utf8, false));
        columns.push(Arc::new(StringArray::from(vec!["x"; columns[0].len()])));
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    enum ArrayRefLike {
        Long(Vec<i64>),
        Unsigned(Vec<u64>),
    }
    impl ArrayRefLike {
        fn data_type(&self) -> DataType {
            match self {
                Self::Long(_) => DataType::Int64,
                Self::Unsigned(_) => DataType::UInt64,
            }
        }
        fn array(&self) -> arrow::array::ArrayRef {
            match self {
                Self::Long(values) => Arc::new(Int64Array::from(values.clone())),
                Self::Unsigned(values) => Arc::new(UInt64Array::from(values.clone())),
            }
        }
    }

    #[test]
    fn statistics_cover_the_stats_columns_with_exact_bounds() {
        // 2023-11-14T22:13:20Z and one second later, in microseconds.
        let t = 1_700_000_000_000_000;
        let stats = stats_json(&batch(
            ArrayRefLike::Long(vec![101, 100, i64::MAX]),
            Some(vec![Some(t + 1_000_000), None, Some(t)]),
        ))
        .unwrap();
        let value: Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(
            value,
            json!({
                "numRecords": 3,
                "minValues": {"block_num": 100, "timestamp": "2023-11-14T22:13:20.000Z"},
                "maxValues": {"block_num": i64::MAX, "timestamp": "2023-11-14T22:13:21.000Z"},
                "nullCount": {"block_num": 0, "timestamp": 1},
            })
        );
        check_stats(&stats, 3).unwrap();
        assert!(check_stats(&stats, 2).is_err());
        assert!(check_stats("[]", 3).is_err());
        assert!(check_stats("not json", 3).is_err());
    }

    #[test]
    fn sub_millisecond_bounds_widen_and_null_or_unbounded_columns_keep_only_null_counts() {
        let stats = stats_json(&batch(
            ArrayRefLike::Long(vec![5]),
            Some(vec![Some(1_700_000_000_000_001)]),
        ))
        .unwrap();
        let value: Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(
            value["minValues"]["timestamp"],
            json!("2023-11-14T22:13:20.000Z")
        );
        assert_eq!(
            value["maxValues"]["timestamp"],
            json!("2023-11-14T22:13:20.001Z")
        );

        let stats = stats_json(&batch(ArrayRefLike::Long(vec![5]), Some(vec![None]))).unwrap();
        let value: Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(value["minValues"], json!({"block_num": 5}));
        assert_eq!(value["nullCount"], json!({"block_num": 0, "timestamp": 1}));

        // A table without `timestamp`, and a type without bounds.
        let stats = stats_json(&batch(ArrayRefLike::Unsigned(vec![5, 6]), None)).unwrap();
        let value: Value = serde_json::from_str(&stats).unwrap();
        assert_eq!(
            value,
            json!({"numRecords": 2, "minValues": {}, "maxValues": {}, "nullCount": {"block_num": 0}})
        );
    }

    #[test]
    fn the_stats_columns_are_the_table_property() {
        let property = crate::delta::table_properties()
            .into_iter()
            .find(|(key, _)| *key == deltalake_core::TableProperty::DataSkippingStatsColumns)
            .unwrap()
            .1;
        assert_eq!(property, STATS_COLUMNS.join(","));
    }
}
