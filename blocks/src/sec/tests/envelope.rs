//! Value tests of `filing_raw_xml`, `filing_parties`, `filing_documents`, `filing_series`, `filing_series_classes`, `filing_signatures`.
//! Owned by the `envelope` group: see `/tmp/sec-fireparq/impl/contracts.md`.

use super::*;

/// This group's tables.
pub(crate) const TABLES: [&str; 6] = [
    "filing_raw_xml",
    "filing_parties",
    "filing_documents",
    "filing_series",
    "filing_series_classes",
    "filing_signatures",
];

/// This group's filings in the cross-table contract fixture
/// (`make_every_body_block`): together they must give every table of
/// [`TABLES`] at least one row (and contribute signatures where the body has
/// them). Ordinals are reassigned by position.
pub(crate) fn contract_filings() -> Vec<sec::Filing> {
    vec![sec::Filing {
        raw_xml: b"<edgarSubmission/>".to_vec().into(),
        parties: vec![sec::FilingParty {
            role: "FILER".to_string(),
            cik: "0000000001".to_string(),
            name: "TEST CO".to_string(),
            former_names: vec![sec::FormerName {
                name: "OLD TEST CO".to_string(),
                date_changed: "2001-01-02".to_string(),
            }],
            ..Default::default()
        }],
        documents: vec![sec::SubmissionDocument {
            sequence: "1".to_string(),
            r#type: "N-PX".to_string(),
            filename: "primary_doc.xml".to_string(),
            ..Default::default()
        }],
        series: vec![sec::FundSeries {
            series_id: "S000000001".to_string(),
            classes: vec![sec::FundClass {
                class_id: "C000000001".to_string(),
                ticker_symbol: "TSTX".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..filing(
            "N-PX",
            Body::Raw(sec::RawFiling {
                reason: "no_xml".to_string(),
                ..Default::default()
            }),
        )
    }]
}

#[test]
fn contract_filings_map_without_error() {
    let filings = contract_filings();
    let count = filings.len();
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    assert_eq!(batches.rows("filings"), count);
}

/// Enable when this group's tables are mapped.
#[test]
#[ignore = "group: enable once every table of this group is mapped"]
fn contract_filings_fill_every_table_of_this_group() {
    let batches = map(&[window_block(BLOCK_NUM, contract_filings())]);
    for table in TABLES {
        assert!(batches.rows(table) > 0, "{table} has no rows");
    }
}
