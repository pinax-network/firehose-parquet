//! SEC golden test: real firesec 0.13.0 filings (`tests/fixtures/sec-v013/`,
//! see its README) mapped through `SecBlockMapper` and compared with
//! `expected/<table>.json`, **every column of every table**, `parse_issues`
//! included. The expectations were produced by the independent reference
//! prototype (`proto_map.py`) from the same blocks; after an intended mapper
//! change, `cargo run -p blocks --example refresh_sec_golden` rewrites them
//! from the Rust mapper, and the diff is reviewed.
use arrow::record_batch::RecordBatch;
use blocks::sec::mapper::SecBlockMapper;
use blocks::sec::schema::TABLE_NAMES;
use firehose_parquet::encode::EncodeBytes;
use firehose_parquet::traits::{BlockMapper, StreamEvent};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

mod sec_fixture;

/// Rows per table on the fixture (README.md): every table has rows except
/// `filing_raw_xml` (the samples were produced without `--include-raw`).
const COUNTS: [(&str, usize); 43] = [
    ("blocks", 29),
    ("filings", 32),
    ("filing_raw_xml", 0),
    ("filing_parties", 44),
    ("filing_documents", 53),
    ("filing_series", 17),
    ("filing_series_classes", 25),
    ("filing_signatures", 30),
    ("ownership_documents", 3),
    ("ownership_reporting_owners", 4),
    ("ownership_transactions", 4),
    ("ownership_holdings", 3),
    ("ownership_footnotes", 5),
    ("form13f_reports", 7),
    ("form13f_other_managers", 10),
    ("form13f_holdings", 22),
    ("beneficial_reports", 4),
    ("beneficial_reporting_persons", 2),
    ("form144_notices", 2),
    ("form144_securities_information", 4),
    ("form144_securities_to_be_sold", 2),
    ("form144_sales_past_3_months", 7),
    ("nport_reports", 6),
    ("nport_monthly_returns", 9),
    ("nport_monthly_activity", 18),
    ("nport_holdings", 12),
    ("nport_debt_reference_instruments", 1),
    ("nport_debt_conversion_currencies", 1),
    ("nport_derivatives", 9),
    ("nport_derivative_swap_legs", 10),
    ("nport_derivative_index_components", 3),
    ("form_d_notices", 2),
    ("form_d_co_issuers", 1),
    ("form_d_related_persons", 4),
    ("form_d_sales_recipients", 2),
    ("npx_reports", 3),
    ("npx_votes", 6),
    ("npx_vote_records", 14),
    ("npx_other_managers", 3),
    ("ncen_reports", 1),
    ("form_c_notices", 2),
    ("form_c_co_issuers", 1),
    ("parse_issues", 18),
];

struct Expected {
    columns: Vec<String>,
    rows: Vec<serde_json::Map<String, Value>>,
}

fn expected(table: &str) -> Expected {
    let path = sec_fixture::expected_path(table);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    let file: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(file["table"], table, "{}", path.display());
    Expected {
        columns: file["columns"]
            .as_array()
            .unwrap()
            .iter()
            .map(|name| name.as_str().unwrap().to_string())
            .collect(),
        rows: file["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row.as_object().unwrap().clone())
            .collect(),
    }
}

/// Every difference between `batch` and the expectation of `table`, at most
/// three examples per column.
fn differences(table: &str, batch: &RecordBatch) -> Vec<String> {
    let expected = expected(table);
    let schema = batch.schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let mut out = Vec::new();
    if columns != expected.columns {
        out.push(format!(
            "{table}: columns {columns:?} != expected {:?}",
            expected.columns
        ));
        return out;
    }
    if batch.num_rows() != expected.rows.len() {
        out.push(format!(
            "{table}: {} rows != expected {}",
            batch.num_rows(),
            expected.rows.len()
        ));
    }
    let mut per_column: BTreeMap<&str, usize> = BTreeMap::new();
    for (row, want) in expected.rows.iter().enumerate().take(batch.num_rows()) {
        let mut keys: Vec<&String> = want.keys().collect();
        let mut columns_sorted: Vec<&String> = expected.columns.iter().collect();
        keys.sort();
        columns_sorted.sort();
        assert_eq!(keys, columns_sorted, "{table} row {row}: keys");
        for (index, name) in columns.iter().enumerate() {
            let actual = sec_fixture::json_value(batch.column(index).as_ref(), row);
            if &actual != want.get(name).unwrap() {
                let seen = per_column.entry(name).or_default();
                *seen += 1;
                if *seen <= 3 {
                    out.push(format!(
                        "{table}.{name} row {row}: mapper {actual} != expected {}",
                        want[name]
                    ));
                }
            }
        }
    }
    out
}

fn assert_matches_expected(batches: &HashMap<String, RecordBatch>) {
    let mut tables: Vec<&String> = batches.keys().collect();
    tables.sort();
    let mut names: Vec<&str> = TABLE_NAMES.to_vec();
    names.sort();
    assert_eq!(tables, names, "one batch per SEC table");
    let mut failures = Vec::new();
    for (table, rows) in COUNTS {
        let batch = &batches[table];
        assert_eq!(batch.num_rows(), rows, "rows of {table}");
        failures.extend(differences(table, batch));
    }
    assert!(
        failures.is_empty(),
        "the mapper differs from tests/fixtures/sec-v013/expected:\n{}",
        failures.join("\n")
    );
}

#[test]
fn fixture_tables_match_the_reference_prototype() {
    assert_eq!(
        COUNTS.map(|(table, _)| table),
        TABLE_NAMES,
        "COUNTS lists the tables in TABLE_NAMES order"
    );
    assert_matches_expected(&sec_fixture::map(false, EncodeBytes::Hex));
}

/// The borrowed entry point (`map_block`) gives the same tables, and with
/// `fork_step`/`stream_ordinal` appended the other columns are unchanged.
#[test]
fn fixture_tables_do_not_depend_on_the_entry_point_or_fork_step() {
    let owned = sec_fixture::map(false, EncodeBytes::Hex);
    let mut borrowed = SecBlockMapper::new(false, EncodeBytes::Hex);
    for block in sec_fixture::blocks() {
        borrowed
            .map_block(&block.payload, &block.identity, StreamEvent::default())
            .unwrap();
    }
    let borrowed = borrowed.flush().unwrap();
    assert_eq!(owned.len(), borrowed.len());
    for (table, batch) in &owned {
        assert_eq!(batch, &borrowed[table], "{table}");
    }

    let forked = sec_fixture::map(true, EncodeBytes::Hex);
    for (table, batch) in &owned {
        let with_step = &forked[table];
        let width = batch.num_columns();
        assert_eq!(with_step.num_columns(), width + 2, "{table}");
        let schema = with_step.schema();
        assert_eq!(schema.field(width).name(), "fork_step");
        assert_eq!(schema.field(width + 1).name(), "stream_ordinal");
        assert_eq!(
            &with_step.project(&(0..width).collect::<Vec<_>>()).unwrap(),
            batch
        );
    }
}

/// Under `Binary`, the decimal window number is written as its UTF-8 bytes.
#[test]
fn fixture_text_ids_under_binary_are_the_decimal_bytes() {
    let batches = sec_fixture::map(false, EncodeBytes::Binary);
    let blocks = &batches["blocks"];
    let ids = blocks.column_by_name("block_id").unwrap();
    let parents = blocks.column_by_name("parent_id").unwrap();
    for (row, block) in sec_fixture::blocks().iter().enumerate() {
        assert_eq!(
            sec_fixture::json_value(ids.as_ref(), row),
            Value::from(hex(block.identity.block_id.as_bytes()))
        );
        assert_eq!(
            sec_fixture::json_value(parents.as_ref(), row),
            Value::from(hex(block.identity.parent_id.as_bytes()))
        );
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
