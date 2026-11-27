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

/// Real firesec 0.13.0 windows from the golden fixture
/// (`blocks/tests/fixtures/sec-v013/<block>.pb`, see its README): each keeps
/// only some filings of its window, renumbered from 0, and some N-PORT lists
/// are trimmed. The unit tests check their mirrors against these filings.
pub(crate) mod fixture {
    use super::*;

    /// Window `block_num` of the fixture, with firesec's metadata.
    pub(crate) fn block(block_num: u64) -> (BlockIdentity, sec::Block) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tests/fixtures/sec-v013/{block_num}.pb"));
        let payload = std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
        let block = sec::Block::decode(payload.as_slice()).unwrap();
        assert_eq!(block.header.as_ref().unwrap().block_number, block_num);
        (identity(block_num), block)
    }
}
