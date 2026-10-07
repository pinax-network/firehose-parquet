//! Value tests of `form144_notices`, `form144_securities_information`, `form144_securities_to_be_sold`, `form144_sales_past_3_months`, `form_d_notices`, `form_d_co_issuers`, `form_d_related_persons`, `form_d_sales_recipients`, `form_c_notices`, `form_c_co_issuers`.
//! Owned by the `smallforms` group: see `/tmp/sec-fireparq/impl/contracts.md`.

use super::*;

/// This group's tables.
pub(crate) const TABLES: [&str; 10] = [
    "form144_notices",
    "form144_securities_information",
    "form144_securities_to_be_sold",
    "form144_sales_past_3_months",
    "form_d_notices",
    "form_d_co_issuers",
    "form_d_related_persons",
    "form_d_sales_recipients",
    "form_c_notices",
    "form_c_co_issuers",
];

/// This group's filings in the cross-table contract fixture
/// (`make_every_body_block`): together they must give every table of
/// [`TABLES`] at least one row (and contribute signatures where the body has
/// them). Ordinals are reassigned by position.
pub(crate) fn contract_filings() -> Vec<sec::Filing> {
    vec![
        filing("144", Body::Form144(sec::Form144Notice::default())),
        filing("D", Body::FormD(sec::FormDNotice::default())),
        filing("C", Body::FormC(sec::FormCNotice::default())),
    ]
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
