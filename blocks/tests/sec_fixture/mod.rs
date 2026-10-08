//! The SEC real-data fixture (`tests/fixtures/sec-v013/`): its blocks, the
//! Firehose metadata firesec writes for them, and the JSON form of the golden
//! expectations (`expected/<table>.json`).
//!
//! Shared by `tests/sec_golden.rs`, `tests/sec_docs_sql.rs`,
//! `tests/engine_compat.rs` and `examples/refresh_sec_golden.rs`.
#![allow(dead_code)]

use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{
    DataType, Date32Type, Decimal128Type, Int32Type, Int64Type, TimeUnit, TimestampMillisecondType,
    UInt32Type, UInt64Type,
};
use blocks::sec::mapper::SecBlockMapper;
use blocks::sec::proto::sec;
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{BlockIdentity, BlockMapper, StreamEvent};
use prost::Message;
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

/// `blocks/tests/fixtures/sec-v013`.
pub fn dir() -> PathBuf {
    PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/sec-v013"
    ))
}

/// `expected/<table>.json`.
pub fn expected_path(table: &str) -> PathBuf {
    dir().join("expected").join(format!("{table}.json"))
}

/// One fixture window: its payload and the metadata firesec writes for it.
pub struct FixtureBlock {
    pub identity: BlockIdentity,
    pub payload: Vec<u8>,
    pub filings: usize,
}

/// Every `<block>.pb` of the fixture in block order. The identity is
/// firesec's: `id = n`, `parent = lib = n - 1` (decimal text), time = the
/// window start `n × 600`; each is checked against the payload header.
pub fn blocks() -> Vec<FixtureBlock> {
    let mut files: Vec<(u64, PathBuf)> = std::fs::read_dir(dir())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "pb"))
        .map(|path| {
            let number = path.file_stem().unwrap().to_str().unwrap().parse().unwrap();
            (number, path)
        })
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|(number, path)| {
            let payload = std::fs::read(&path).unwrap();
            let block = sec::Block::decode(payload.as_slice()).unwrap();
            let header = block.header.as_ref().unwrap();
            let time = header.block_time.as_ref().unwrap();
            assert_eq!(header.block_number, number, "{}", path.display());
            assert_eq!(time.seconds, number as i64 * 600, "{}", path.display());
            assert_eq!(time.nanos, 0);
            assert_eq!(
                header.feed_date,
                iso_date(time.seconds.div_euclid(86_400) as i32)
            );
            for (position, filing) in block.filings.iter().enumerate() {
                assert_eq!(filing.ordinal, position as u64, "{}", path.display());
            }
            FixtureBlock {
                identity: BlockIdentity {
                    block_num: number,
                    block_id: number.to_string(),
                    parent_num: number - 1,
                    parent_id: (number - 1).to_string(),
                    lib_num: number - 1,
                    timestamp: time.seconds,
                    timestamp_nanos: 0,
                    fork_step: None,
                },
                filings: block.filings.len(),
                payload,
            }
        })
        .collect()
}

/// Map every fixture block, in order, through the production entry point
/// (`map_block_bytes`) and flush once.
pub fn map(include_fork_step: bool, encoding: EncodeBytes) -> HashMap<String, RecordBatch> {
    let mut mapper = SecBlockMapper::new(include_fork_step, encoding);
    for (ordinal, block) in blocks().into_iter().enumerate() {
        let event = StreamEvent::new(include_fork_step.then_some("FINAL"), ordinal as u64 + 1);
        let filings = mapper
            .map_block_bytes(block.payload.into(), &block.identity, event)
            .unwrap_or_else(|error| panic!("block {}: {error:#}", block.identity.block_num));
        assert_eq!(filings, block.filings as u64);
    }
    mapper.flush().unwrap()
}

/// `YYYY-MM-DD` of a `Date32` value.
pub fn iso_date(days: i32) -> String {
    let date = time::Date::from_julian_day(days + 2_440_588).unwrap();
    format!(
        "{:04}-{:02}-{:02}",
        date.year(),
        date.month() as u8,
        date.day()
    )
}

/// `YYYY-MM-DD HH:MM:SS` (UTC) of unix milliseconds, with `.mmm` only when
/// there are milliseconds.
pub fn utc_text(millis: i64) -> String {
    let time =
        time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000).unwrap();
    let text = format!(
        "{} {:02}:{:02}:{:02}",
        iso_date((millis.div_euclid(86_400_000)) as i32),
        time.hour(),
        time.minute(),
        time.second()
    );
    match time.millisecond() {
        0 => text,
        ms => format!("{text}.{ms:03}"),
    }
}

/// The decimal text of `mantissa` at `scale`, always with `scale` digits.
pub fn decimal_text(mantissa: i128, scale: i8) -> String {
    let scale = u32::try_from(scale).unwrap();
    let sign = if mantissa < 0 { "-" } else { "" };
    let magnitude = mantissa.unsigned_abs();
    if scale == 0 {
        return format!("{sign}{magnitude}");
    }
    let unit = 10u128.pow(scale);
    format!(
        "{sign}{}.{:0width$}",
        magnitude / unit,
        magnitude % unit,
        width = scale as usize
    )
}

/// One Arrow value as the expectation files write it: strings, numbers and
/// booleans as JSON scalars, dictionaries as their label, decimals as text
/// at the column scale, dates ISO, timestamps `YYYY-MM-DD HH:MM:SS` UTC,
/// binary as lowercase hex, lists as arrays and structs as objects.
pub fn json_value(array: &dyn Array, row: usize) -> Value {
    serde_json::from_str(&json_text(array, row)).unwrap()
}

/// [`json_value`] as compact JSON text, struct members in schema order.
pub fn json_text(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "null".to_string();
    }
    let string = |text: &str| serde_json::to_string(text).unwrap();
    match array.data_type() {
        DataType::Utf8 => string(array.as_string::<i32>().value(row)),
        DataType::Dictionary(..) => {
            let dictionary = array.as_dictionary::<Int32Type>();
            let key = dictionary.keys().value(row) as usize;
            string(dictionary.values().as_string::<i32>().value(key))
        }
        DataType::Boolean => array.as_boolean().value(row).to_string(),
        DataType::UInt32 => array.as_primitive::<UInt32Type>().value(row).to_string(),
        DataType::UInt64 => array.as_primitive::<UInt64Type>().value(row).to_string(),
        DataType::Int32 => array.as_primitive::<Int32Type>().value(row).to_string(),
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).to_string(),
        DataType::Date32 => string(&iso_date(array.as_primitive::<Date32Type>().value(row))),
        DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => string(&utc_text(
            array.as_primitive::<TimestampMillisecondType>().value(row),
        )),
        DataType::Decimal128(_, scale) => string(&decimal_text(
            array.as_primitive::<Decimal128Type>().value(row),
            *scale,
        )),
        DataType::Binary => string(
            &array
                .as_binary::<i32>()
                .value(row)
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
        ),
        DataType::List(_) => {
            let items = array.as_list::<i32>().value(row);
            let items: Vec<String> = (0..items.len())
                .map(|item| json_text(items.as_ref(), item))
                .collect();
            format!("[{}]", items.join(","))
        }
        DataType::Struct(fields) => {
            let members: Vec<String> = fields
                .iter()
                .zip(array.as_struct().columns())
                .map(|(field, column)| {
                    format!(
                        "{}:{}",
                        string(field.name()),
                        json_text(column.as_ref(), row)
                    )
                })
                .collect();
            format!("{{{}}}", members.join(","))
        }
        other => panic!("no JSON form for {other}"),
    }
}

/// One table's expectation file, as `refresh_sec_golden` writes it: the
/// table, its columns, and one row object per line, keys in column order.
pub fn table_json(table: &str, batch: &RecordBatch) -> String {
    let schema = batch.schema();
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| serde_json::to_string(field.name()).unwrap())
        .collect();
    let mut out = format!(
        "{{\n  \"table\": {},\n  \"columns\": [{}],\n",
        serde_json::to_string(table).unwrap(),
        columns.join(", ")
    );
    if batch.num_rows() == 0 {
        out.push_str("  \"rows\": []\n}\n");
        return out;
    }
    let rows: Vec<String> = (0..batch.num_rows())
        .map(|row| {
            let cells: Vec<String> = columns
                .iter()
                .zip(batch.columns())
                .map(|(name, column)| format!("{name}:{}", json_text(column.as_ref(), row)))
                .collect();
            format!("    {{{}}}", cells.join(","))
        })
        .collect();
    out.push_str("  \"rows\": [\n");
    out.push_str(&rows.join(",\n"));
    out.push_str("\n  ]\n}\n");
    out
}
