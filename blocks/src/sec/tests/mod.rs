//! Shared helpers of the SEC value tests, and the cross-table fixtures.
//!
//! Each group keeps its value tests in its own module below and contributes
//! filings to [`make_every_body_block`] through its `contract_filings()`.
//!
//! ```ignore
//! let batches = map(&[window_block(BLOCK_NUM, vec![filing("4", Body::Ownership(doc))])]);
//! assert_eq!(batches.rows("ownership_transactions"), 2);
//! assert_eq!(batches.cell("ownership_transactions", "shares", 0), "100.000000");
//! assert_eq!(batches.issues()[0].column, "transaction_date");
//! ```

use std::collections::HashMap;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use firehose_parquet::config::Compression;
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{BlockIdentity, BlockMapper, StreamEvent};
use firehose_parquet::writer::{decode_parquet, encode_parquet, ParquetFileMetadata};
use prost::Message;

pub(crate) use crate::sec::proto::sec;
pub(crate) use sec::filing::Body;

use crate::sec::mapper::SecBlockMapper;

mod envelope;
mod form13f_beneficial;
mod funds;
mod hub;
mod nport;
mod ownership;
mod smallforms;

/// A real 2026-08-28 window (`2026-08-28T12:30:00Z`).
pub(crate) const BLOCK_NUM: u64 = 2_979_867;

/// The Firehose metadata firesec writes for window `n`: decimal ids, parent and
/// LIB `n - 1`, time = window start (`n × 600` seconds).
pub(crate) fn identity(n: u64) -> BlockIdentity {
    BlockIdentity {
        block_num: n,
        block_id: n.to_string(),
        parent_num: n - 1,
        parent_id: (n - 1).to_string(),
        lib_num: n - 1,
        timestamp: window_seconds(n),
        timestamp_nanos: 0,
        fork_step: None,
    }
}

/// The start of window `n`, unix seconds.
pub(crate) fn window_seconds(n: u64) -> i64 {
    n as i64 * 600
}

/// A block for window `n` with header time `timestamp` (the identity's
/// `timestamp`), and `filings` renumbered so that `ordinal` = position.
pub(crate) fn block_at(n: u64, timestamp: i64, filings: Vec<sec::Filing>) -> sec::Block {
    let days = timestamp.div_euclid(86_400) as i32;
    sec::Block {
        header: Some(sec::BlockHeader {
            block_number: n,
            block_time: Some(prost_types::Timestamp {
                seconds: timestamp,
                nanos: 0,
            }),
            feed_date: crate::sec::parse::iso_date(days).unwrap_or_default(),
        }),
        filings: filings
            .into_iter()
            .enumerate()
            .map(|(position, filing)| sec::Filing {
                ordinal: position as u64,
                ..filing
            })
            .collect(),
    }
}

/// A block for window `n` at its real time (`identity(n)`).
pub(crate) fn window_block(n: u64, filings: Vec<sec::Filing>) -> sec::Block {
    block_at(n, window_seconds(n), filings)
}

/// A filing with the envelope fields every filing has, and `body`. The
/// accession is derived from `form_type`; override fields with struct update.
pub(crate) fn filing(form_type: &str, body: Body) -> sec::Filing {
    sec::Filing {
        ordinal: 0,
        accession_number: format!("0000000000-26-{:06}", form_type.len()),
        form_type: form_type.to_string(),
        is_amendment: form_type.ends_with("/A"),
        cik: "0000000001".to_string(),
        company_name: "TEST CO".to_string(),
        filing_date: "2026-08-27".to_string(),
        acceptance_datetime: Some(prost_types::Timestamp {
            seconds: window_seconds(BLOCK_NUM) + 60,
            nanos: 0,
        }),
        primary_document: "primary_doc.xml".to_string(),
        source_path: format!("20260828.gz!0000000000-26-{:06}", form_type.len()),
        body: Some(body),
        ..Default::default()
    }
}

/// An empty 10-minute window (no filings), for the contract fixture.
/// `timestamp` is the header time, which must equal the identity's.
pub(crate) fn make_test_block(n: u64, timestamp: i64) -> sec::Block {
    block_at(n, timestamp, Vec::new())
}

/// One block whose filings give every SEC table at least one row once every
/// group has landed: the hub fixture plus each group's `contract_filings()`.
/// `timestamp` is the header time, which must equal the identity's.
pub(crate) fn make_every_body_block(n: u64, timestamp: i64) -> sec::Block {
    let mut filings = hub::contract_filings();
    filings.extend(envelope::contract_filings());
    filings.extend(ownership::contract_filings());
    filings.extend(form13f_beneficial::contract_filings());
    filings.extend(smallforms::contract_filings());
    filings.extend(nport::contract_filings());
    filings.extend(funds::contract_filings());
    block_at(n, timestamp, filings)
}

/// Map `blocks` (each with `identity(header.block_number)`), final-only, `Hex`.
pub(crate) fn map(blocks: &[sec::Block]) -> Batches {
    map_with(blocks, false, EncodeBytes::Hex)
}

/// Map `blocks` with the given options, flush once, and check that every
/// non-empty batch survives a Parquet round trip unchanged.
pub(crate) fn map_with(
    blocks: &[sec::Block],
    include_fork_step: bool,
    encoding: EncodeBytes,
) -> Batches {
    let mut mapper = SecBlockMapper::new(include_fork_step, encoding);
    for block in blocks {
        let n = block.header.as_ref().map_or(0, |h| h.block_number);
        let mut id = identity(n);
        if let Some(time) = block.header.as_ref().and_then(|h| h.block_time.as_ref()) {
            id.timestamp = time.seconds;
        }
        mapper
            .map_block(&block.encode_to_vec(), &id, StreamEvent::default())
            .unwrap_or_else(|error| panic!("map block {n}: {error:#}"));
    }
    let batches = mapper.flush().expect("flush");
    for (table, batch) in &batches {
        if batch.num_rows() > 0 {
            round_trip(table, batch);
        }
    }
    Batches::new(batches)
}

fn round_trip(table: &str, batch: &RecordBatch) {
    let bytes = encode_parquet(batch, Compression::Zstd, &ParquetFileMetadata::new()).unwrap();
    let batches = decode_parquet(bytes).unwrap();
    let actual = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    assert_eq!(&actual, batch, "Parquet round trip: {table}");
}

/// RFC 3339 UTC text of unix milliseconds: `2026-08-14T00:00:00Z`, with
/// `.250` when there are milliseconds.
pub(crate) fn utc_millis_text(millis: i64) -> String {
    let time = time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(millis) * 1_000_000)
        .expect("valid timestamp");
    let (year, month, day) = (time.year(), time.month() as u8, time.day());
    let (hour, minute, second) = (time.hour(), time.minute(), time.second());
    let ms = time.millisecond();
    if ms == 0 {
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
    } else {
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{ms:03}Z")
    }
}

/// Lowercase hex of `bytes` (how `cell` prints a `Binary` value).
pub(crate) fn hex_of(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// One `parse_issues` row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IssueRow {
    pub block_num: u64,
    pub filing_index: Option<u32>,
    pub accession_number: Option<String>,
    pub table: String,
    pub column: String,
    pub index: [Option<u32>; 3],
    pub raw: String,
    pub issue: String,
}

/// Flushed batches by table, with display helpers.
pub(crate) struct Batches(pub HashMap<String, RecordBatch>);

impl Batches {
    pub(crate) fn new(batches: HashMap<String, RecordBatch>) -> Self {
        Self(batches)
    }

    pub(crate) fn table(&self, table: &str) -> &RecordBatch {
        self.0
            .get(table)
            .unwrap_or_else(|| panic!("no table `{table}`"))
    }

    pub(crate) fn rows(&self, table: &str) -> usize {
        self.table(table).num_rows()
    }

    /// One value as text: `NULL`, decimals at the column scale (`1.500000`),
    /// dates ISO (`2026-08-14`), timestamps RFC 3339 (`2026-08-14T00:00:00Z`),
    /// lists `[a, b]`, structs `{name: x, date_changed: 2001-01-02}`, binary as
    /// lowercase hex, dictionaries as their label.
    pub(crate) fn cell(&self, table: &str, column: &str, row: usize) -> String {
        let batch = self.table(table);
        let array = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("no column `{table}.{column}`"));
        assert!(row < array.len(), "{table}.{column}: no row {row}");
        if let arrow::datatypes::DataType::Timestamp(_, Some(_)) = array.data_type() {
            // ArrayFormatter needs chrono-tz for named zones: print UTC millis.
            if array.is_null(row) {
                return "NULL".to_string();
            }
            let millis = arrow::array::AsArray::as_primitive::<
                arrow::datatypes::TimestampMillisecondType,
            >(array.as_ref())
            .value(row);
            return utc_millis_text(millis);
        }
        let options = FormatOptions::default().with_null("NULL");
        ArrayFormatter::try_new(array.as_ref(), &options)
            .unwrap()
            .value(row)
            .to_string()
    }

    /// Every value of a column, as [`Batches::cell`] prints it.
    pub(crate) fn column(&self, table: &str, column: &str) -> Vec<String> {
        (0..self.rows(table))
            .map(|row| self.cell(table, column, row))
            .collect()
    }

    /// One row as `column=value` pairs (debugging aid).
    pub(crate) fn row(&self, table: &str, row: usize) -> Vec<(String, String)> {
        self.table(table)
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().clone(), self.cell(table, field.name(), row)))
            .collect()
    }

    /// Every `parse_issues` row, in row order.
    pub(crate) fn issues(&self) -> Vec<IssueRow> {
        let table = "parse_issues";
        let opt_u32 = |column: &str, row: usize| {
            let text = self.cell(table, column, row);
            (text != "NULL").then(|| text.parse().unwrap())
        };
        (0..self.rows(table))
            .map(|row| IssueRow {
                block_num: self.cell(table, "block_num", row).parse().unwrap(),
                filing_index: opt_u32("filing_index", row),
                accession_number: Some(self.cell(table, "accession_number", row))
                    .filter(|text| text != "NULL"),
                table: self.cell(table, "table_name", row),
                column: self.cell(table, "column_name", row),
                index: [
                    opt_u32("index_1", row),
                    opt_u32("index_2", row),
                    opt_u32("index_3", row),
                ],
                raw: self.cell(table, "raw_value", row),
                issue: self.cell(table, "issue", row),
            })
            .collect()
    }

    /// Assert the row count of each `(table, rows)`.
    pub(crate) fn assert_rows(&self, expected: &[(&str, usize)]) {
        for (table, rows) in expected {
            assert_eq!(self.rows(table), *rows, "rows of {table}");
        }
    }
}

/// Real blocks from the local 0.13.0 sample FIRE files
/// (`/tmp/sec-fireparq/fire-v013/<day>.fire`), for `#[ignore]`d local checks.
/// Not available in CI.
pub(crate) mod fire {
    use super::*;
    use std::io::BufRead;

    /// The sample files, when present on this machine.
    pub(crate) const DAYS: [&str; 4] = ["2014-08-11", "2026-03-16", "2026-08-14", "2026-08-28"];

    pub(crate) fn path(day: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(format!("/tmp/sec-fireparq/fire-v013/{day}.fire"))
    }

    /// Every `(identity, payload)` of a FIRE file: `FIRE BLOCK <num> <id>
    /// <parent_num> <parent_id> <lib> <time_nanos> <base64 payload>`.
    pub(crate) fn blocks(day: &str) -> impl Iterator<Item = (BlockIdentity, Vec<u8>)> {
        let file = std::fs::File::open(path(day)).unwrap_or_else(|e| panic!("{day}: {e}"));
        std::io::BufReader::with_capacity(1 << 20, file)
            .lines()
            .map(|line| line.expect("read FIRE line"))
            .filter(|line| line.starts_with("FIRE BLOCK "))
            .map(|line| {
                let parts: Vec<&str> = line.splitn(9, ' ').collect();
                let nanos: i64 = parts[7].parse().unwrap();
                let identity = BlockIdentity {
                    block_num: parts[2].parse().unwrap(),
                    block_id: parts[3].to_string(),
                    parent_num: parts[4].parse().unwrap(),
                    parent_id: parts[5].to_string(),
                    lib_num: parts[6].parse().unwrap(),
                    timestamp: nanos.div_euclid(1_000_000_000),
                    timestamp_nanos: nanos.rem_euclid(1_000_000_000) as i32,
                    fork_step: None,
                };
                (identity, base64_decode(parts[8]))
            })
    }

    /// One decoded block of a sample day.
    pub(crate) fn block(day: &str, block_num: u64) -> (BlockIdentity, sec::Block) {
        let (identity, payload) = blocks(day)
            .find(|(identity, _)| identity.block_num == block_num)
            .unwrap_or_else(|| panic!("{day}: no block {block_num}"));
        (identity, sec::Block::decode(payload.as_slice()).unwrap())
    }

    /// Standard base64 with padding (the FIRE payload encoding).
    pub(crate) fn base64_decode(text: &str) -> Vec<u8> {
        fn value(c: u8) -> u32 {
            match c {
                b'A'..=b'Z' => u32::from(c - b'A'),
                b'a'..=b'z' => u32::from(c - b'a') + 26,
                b'0'..=b'9' => u32::from(c - b'0') + 52,
                b'+' => 62,
                b'/' => 63,
                other => panic!("invalid base64 byte {other}"),
            }
        }
        let bytes = text.trim_end().trim_end_matches('=').as_bytes();
        let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
        for chunk in bytes.chunks(4) {
            let mut acc = 0u32;
            for (i, &c) in chunk.iter().enumerate() {
                acc |= value(c) << (18 - 6 * i);
            }
            let produced = chunk.len() * 6 / 8;
            for i in 0..produced {
                out.push((acc >> (16 - 8 * i)) as u8);
            }
        }
        out
    }

    #[test]
    fn base64_matches_known_vectors() {
        assert_eq!(base64_decode("TWFu"), b"Man");
        assert_eq!(base64_decode("TWE="), b"Ma");
        assert_eq!(base64_decode("TQ=="), b"M");
        assert_eq!(base64_decode(""), b"");
    }

    /// Maps every block of every sample day present locally and prints the row
    /// counts per table (compare with final-spec §2).
    /// `cargo test -p blocks --lib sec::tests::fire::sample_days -- --ignored --nocapture`
    #[test]
    #[ignore = "local: needs /tmp/sec-fireparq/fire-v013"]
    fn sample_days_map_without_error() {
        for day in DAYS {
            if !path(day).exists() {
                continue;
            }
            let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
            let mut counts: std::collections::BTreeMap<String, usize> = Default::default();
            for (identity, payload) in blocks(day) {
                mapper
                    .map_block_bytes(payload.into(), &identity, StreamEvent::default())
                    .unwrap_or_else(|e| panic!("{day} block {}: {e:#}", identity.block_num));
                if mapper.total_rows() > 2_000_000 {
                    for (table, batch) in mapper.flush().unwrap() {
                        *counts.entry(table).or_default() += batch.num_rows();
                    }
                }
            }
            for (table, batch) in mapper.flush().unwrap() {
                *counts.entry(table).or_default() += batch.num_rows();
            }
            println!("{day}: {counts:?}");
        }
    }
}

/// Row-by-row comparison of the Rust mapper with the prototype mapper's output
/// (`proto_map.py` NDJSON, one file per table and sample day), for `#[ignore]`d
/// local checks:
///
/// ```ignore
/// oracle::assert_matches(&["2026-03-16"], &["ownership_documents"], &[]);
/// ```
///
/// The oracle may predate the critic fixes C2–C9 of `decisions.md`: three
/// renamed columns are then compared under their old names
/// ([`oracle::RENAMED`]), the added `form_c_co_issuers.filer_cik` has no
/// oracle value (the prototype has both since the golden fixture), and C2 (13F sequence
/// tokens `1.0`), C3 (`month_end` guard) and C4 (aggregate rules) may differ
/// where the oracle was generated before them; pass such columns in `known`.
pub(crate) mod oracle {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::{
        DataType, Date32Type, Int32Type, Int64Type, TimestampMillisecondType, UInt32Type,
        UInt64Type,
    };
    use serde_json::Value;
    use std::collections::BTreeMap;
    use std::io::BufRead;

    /// `(table, column in the spec, column in the oracle)`.
    pub(crate) const RENAMED: [(&str, &str, &str); 3] = [
        ("nport_reports", "holdings_count", "holding_count"),
        ("npx_reports", "declared_series_count", "series_count"),
        ("npx_other_managers", "other_manager_name", "manager_name"),
    ];

    /// The NDJSON directory of a sample day: the regenerated oracle when present,
    /// else the design prototype's.
    pub(crate) fn dir(day: &str) -> std::path::PathBuf {
        let regenerated =
            std::path::PathBuf::from(format!("/tmp/sec-fireparq/impl/oracle/out-{day}"));
        if regenerated.join("_counts.json").exists() {
            regenerated
        } else {
            std::path::PathBuf::from(format!("/tmp/sec-fireparq/design/final/proto/out-{day}"))
        }
    }

    fn utc_seconds_text(millis: i64) -> String {
        utc_millis_text(millis)
            .trim_end_matches('Z')
            .replace('T', " ")
    }

    /// One Arrow value as the prototype writes it in JSON.
    pub(crate) fn json_value(array: &dyn Array, row: usize) -> Value {
        if array.is_null(row) {
            return Value::Null;
        }
        match array.data_type() {
            DataType::Utf8 => Value::from(array.as_string::<i32>().value(row)),
            DataType::Dictionary(..) => {
                let dict = array.as_dictionary::<Int32Type>();
                let key = dict.keys().value(row) as usize;
                Value::from(dict.values().as_string::<i32>().value(key))
            }
            DataType::Boolean => Value::from(array.as_boolean().value(row)),
            DataType::UInt32 => Value::from(array.as_primitive::<UInt32Type>().value(row)),
            DataType::UInt64 => Value::from(array.as_primitive::<UInt64Type>().value(row)),
            DataType::Int32 => Value::from(array.as_primitive::<Int32Type>().value(row)),
            DataType::Int64 => Value::from(array.as_primitive::<Int64Type>().value(row)),
            DataType::Date32 => Value::from(crate::sec::parse::iso_date(
                array.as_primitive::<Date32Type>().value(row),
            )),
            DataType::Timestamp(..) => Value::from(utc_seconds_text(
                array.as_primitive::<TimestampMillisecondType>().value(row),
            )),
            DataType::Decimal128(_, scale) => Value::from(crate::sec::parse::decimal_string(
                array
                    .as_primitive::<arrow::datatypes::Decimal128Type>()
                    .value(row),
                *scale as u8,
            )),
            DataType::List(_) => {
                let list = array.as_list::<i32>();
                let items = list.value(row);
                Value::Array(
                    (0..items.len())
                        .map(|i| json_value(items.as_ref(), i))
                        .collect(),
                )
            }
            DataType::Struct(fields) => {
                let structs = array.as_struct();
                Value::Object(
                    fields
                        .iter()
                        .zip(structs.columns())
                        .map(|(field, column)| {
                            (field.name().clone(), json_value(column.as_ref(), row))
                        })
                        .collect(),
                )
            }
            // Raw bytes are not in the oracle.
            DataType::Binary => Value::Null,
            other => panic!("oracle: no JSON form for {other}"),
        }
    }

    /// Per-table comparison result.
    #[derive(Debug, Default)]
    pub(crate) struct TableReport {
        pub rust_rows: usize,
        pub oracle_rows: usize,
        /// column → (mismatching rows, first examples `row: rust != oracle`).
        pub mismatches: BTreeMap<String, (usize, Vec<String>)>,
        /// Columns the oracle does not have.
        pub absent: Vec<String>,
    }

    struct Reader {
        lines: std::io::Lines<std::io::BufReader<std::fs::File>>,
        row: usize,
    }

    /// Map every block of `days` and compare `tables` with the oracle,
    /// streaming (the mapper is flushed every few hundred thousand rows).
    pub(crate) fn compare(days: &[&str], tables: &[&str]) -> BTreeMap<String, TableReport> {
        let mut reports: BTreeMap<String, TableReport> = BTreeMap::new();
        for day in days {
            if !fire::path(day).exists() || !dir(day).exists() {
                println!("oracle: skipping {day} (no local sample)");
                continue;
            }
            let mut readers: BTreeMap<&str, Reader> = tables
                .iter()
                .map(|table| {
                    let path = dir(day).join(format!("{table}.ndjson"));
                    let file = std::fs::File::open(&path)
                        .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
                    (
                        *table,
                        Reader {
                            lines: std::io::BufReader::new(file).lines(),
                            row: 0,
                        },
                    )
                })
                .collect();
            let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
            let mut blocks = fire::blocks(day).peekable();
            while let Some((identity, payload)) = blocks.next() {
                mapper
                    .map_block_bytes(payload.into(), &identity, StreamEvent::default())
                    .unwrap_or_else(|e| panic!("{day} block {}: {e:#}", identity.block_num));
                if mapper.total_rows() < 500_000 && blocks.peek().is_some() {
                    continue;
                }
                let batches = mapper.flush().unwrap();
                for table in tables {
                    let report = reports.entry(table.to_string()).or_default();
                    compare_batch(
                        table,
                        &batches[*table],
                        readers.get_mut(table).unwrap(),
                        report,
                    );
                }
            }
            for table in tables {
                let report = reports.get_mut(*table).unwrap();
                let reader = readers.get_mut(table).unwrap();
                report.oracle_rows += reader.lines.by_ref().count();
            }
        }
        reports
    }

    fn compare_batch(
        table: &str,
        batch: &RecordBatch,
        reader: &mut Reader,
        report: &mut TableReport,
    ) {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            report.rust_rows += 1;
            let Some(line) = reader.lines.next() else {
                continue;
            };
            report.oracle_rows += 1;
            let expected: serde_json::Map<String, Value> =
                serde_json::from_str(&line.unwrap()).expect("oracle JSON");
            for (index, field) in schema.fields().iter().enumerate() {
                let name = field.name().as_str();
                // A regenerated oracle has the spec names; an older one the
                // names of `RENAMED`.
                let old_name = RENAMED
                    .iter()
                    .find(|(t, column, _)| *t == table && *column == name)
                    .map(|(_, _, old)| *old);
                let Some(expected) = expected
                    .get(name)
                    .or_else(|| old_name.and_then(|old| expected.get(old)))
                else {
                    if !report.absent.iter().any(|c| c == name) {
                        report.absent.push(name.to_string());
                    }
                    continue;
                };
                if matches!(field.data_type(), DataType::Binary) {
                    continue;
                }
                let actual = json_value(batch.column(index).as_ref(), row);
                if &actual != expected {
                    let entry = report.mismatches.entry(name.to_string()).or_default();
                    entry.0 += 1;
                    if entry.1.len() < 3 {
                        entry.1.push(format!(
                            "row {}: rust {actual} != oracle {expected}",
                            reader.row
                        ));
                    }
                }
            }
            reader.row += 1;
        }
    }

    /// Print the reports and assert that every table matches the oracle row for
    /// row, except the `(table, column)` pairs in `known`.
    pub(crate) fn assert_matches(days: &[&str], tables: &[&str], known: &[(&str, &str)]) {
        let reports = compare(days, tables);
        let mut failures = Vec::new();
        for (table, report) in &reports {
            println!(
                "{table}: rust {} rows, oracle {} rows, absent from oracle {:?}",
                report.rust_rows, report.oracle_rows, report.absent
            );
            if report.rust_rows != report.oracle_rows {
                failures.push(format!("{table}: row counts differ"));
            }
            for (column, (count, examples)) in &report.mismatches {
                let is_known = known.contains(&(table.as_str(), column.as_str()));
                println!(
                    "  {}{column}: {count} rows differ; {examples:?}",
                    if is_known { "(known) " } else { "" }
                );
                if !is_known {
                    failures.push(format!("{table}.{column}"));
                }
            }
        }
        assert!(
            failures.is_empty(),
            "differences from the prototype: {failures:?}"
        );
    }
}
