//! Value tests of `form13f_reports`, `form13f_other_managers`, `form13f_holdings`, `beneficial_reports`, `beneficial_reporting_persons`.
//! Owned by the `form13f-beneficial` group: see `/tmp/sec-fireparq/impl/contracts.md`.

use super::*;

/// This group's tables.
pub(crate) const TABLES: [&str; 5] = [
    "form13f_reports",
    "form13f_other_managers",
    "form13f_holdings",
    "beneficial_reports",
    "beneficial_reporting_persons",
];

/// This group's filings in the cross-table contract fixture
/// (`make_every_body_block`): together they must give every table of
/// [`TABLES`] at least one row (and contribute signatures where the body has
/// them). Ordinals are reassigned by position.
pub(crate) fn contract_filings() -> Vec<sec::Filing> {
    vec![
        filing("13F-HR", Body::Form13f(sec::Form13fReport::default())),
        filing(
            "SCHEDULE 13G",
            Body::Beneficial(sec::BeneficialOwnershipReport::default()),
        ),
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
