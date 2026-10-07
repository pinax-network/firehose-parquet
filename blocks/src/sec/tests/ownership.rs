//! Value tests of `ownership_documents`, `ownership_reporting_owners`, `ownership_transactions`, `ownership_holdings`, `ownership_footnotes`.
//! Owned by the `ownership` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The fixtures mirror real 0.13.0 filings of the samples (§8.7) field for
//! field; the `#[ignore]`d `real_*` tests check the mirrors against the local
//! FIRE files, and `ownership_matches_the_prototype` compares every row of the
//! four sample days with the prototype.

use super::*;

/// This group's tables.
pub(crate) const TABLES: [&str; 5] = [
    "ownership_documents",
    "ownership_reporting_owners",
    "ownership_transactions",
    "ownership_holdings",
    "ownership_footnotes",
];

/// This group's filings in the cross-table contract fixture
/// (`make_every_body_block`): together they must give every table of
/// [`TABLES`] at least one row (and contribute signatures where the body has
/// them). Ordinals are reassigned by position.
pub(crate) fn contract_filings() -> Vec<sec::Filing> {
    vec![joint_form4()]
}

#[test]
fn contract_filings_map_without_error() {
    let filings = contract_filings();
    let count = filings.len();
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    assert_eq!(batches.rows("filings"), count);
}

#[test]
fn contract_filings_fill_every_table_of_this_group() {
    let batches = map(&[window_block(BLOCK_NUM, contract_filings())]);
    for table in TABLES {
        assert!(batches.rows(table) > 0, "{table} has no rows");
    }
}

// ---------------------------------------------------------------------------
// Fixtures: prost mirrors of real sample filings
// ---------------------------------------------------------------------------

fn s(text: &str) -> String {
    text.to_string()
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| item.to_string()).collect()
}

fn at(seconds: i64) -> Option<prost_types::Timestamp> {
    Some(prost_types::Timestamp { seconds, nanos: 0 })
}

fn ownership(filing: &sec::Filing) -> &sec::OwnershipDocument {
    match &filing.body {
        Some(Body::Ownership(doc)) => doc,
        other => panic!("not an ownership body: {other:?}"),
    }
}

/// Window of `0001339459-26-000007` (2026-08-14T21:50:00Z).
const JOINT_BLOCK: u64 = 2_977_907;
/// Window of `0001477932-14-004179` (2014-08-11T16:30:00Z).
const TZ_BLOCK: u64 = 2_346_291;
/// Window of `0001209191-04-032033` (2026-03-16T00:00:00Z, window 0).
const REDISSEMINATED_BLOCK: u64 = 2_956_032;

/// `0001339459-26-000007` (2026-08-14, block 2977907): a joint Form 4 of a
/// CEO (director, officer, 10% owner) and his fund (10% owner), with one row
/// in each of Table I/II transactions and holdings and footnote-only prices
/// (`value_usd` NULL). The body is the real one, verbatim.
fn joint_form4() -> sec::Filing {
    let address = sec::Address {
        street1: s("C/O THUNDER BRIDGE CAPITAL PARTNERS V"),
        street2: s("LTD., 9912 GEORGETOWN PIKE, SUITE D203"),
        city: s("GREAT FALLS"),
        state: s("VA"),
        zip_code: s("22066"),
        ..Default::default()
    };
    let warrant_underlying = sec::UnderlyingSecurity {
        title: s("Class A ordinary shares"),
        shares: s("149000"),
        ..Default::default()
    };
    let doc = sec::OwnershipDocument {
        schema_version: s("X0609"),
        document_type: s("4"),
        period_of_report: s("2026-08-12"),
        issuer: Some(sec::Issuer {
            cik: s("0002140030"),
            name: s("Thunder Bridge Capital Partners V, Ltd."),
            trading_symbol: s("TBCV"),
            ..Default::default()
        }),
        reporting_owners: vec![
            sec::ReportingOwner {
                cik: s("0001339459"),
                name: s("Simanson Gary A"),
                address: Some(address.clone()),
                relationship: Some(sec::Relationship {
                    is_director: true,
                    is_officer: true,
                    is_ten_percent_owner: true,
                    officer_title: s("Chief Executive Officer"),
                    ..Default::default()
                }),
            },
            sec::ReportingOwner {
                cik: s("0002142210"),
                name: s("TBCP V, LLC"),
                address: Some(address),
                relationship: Some(sec::Relationship {
                    is_ten_percent_owner: true,
                    ..Default::default()
                }),
            },
        ],
        non_derivative_transactions: vec![sec::Transaction {
            security_title: s("Class A ordinary shares"),
            transaction_date: s("2026-08-12"),
            transaction_form_type: s("4"),
            transaction_code: s("P"),
            shares: s("447000"),
            acquired_disposed_code: s("A"),
            shares_owned_following: s("447000"),
            direct_or_indirect: s("I"),
            nature_of_ownership: s("See Footnote"),
            footnote_ids: strings(&["F1", "F1", "F2"]),
            ..Default::default()
        }],
        non_derivative_holdings: vec![sec::Holding {
            security_title: s("Class A ordinary shares"),
            shares_owned: s("447000"),
            direct_or_indirect: s("D"),
            footnote_ids: strings(&["F1", "F2"]),
            ..Default::default()
        }],
        derivative_transactions: vec![sec::Transaction {
            security_title: s("Redeemable Warrants"),
            transaction_date: s("2026-08-12"),
            transaction_form_type: s("4"),
            transaction_code: s("P"),
            shares: s("149000"),
            acquired_disposed_code: s("A"),
            shares_owned_following: s("149000"),
            direct_or_indirect: s("I"),
            nature_of_ownership: s("See Footnote"),
            conversion_or_exercise_price: s("11.5"),
            underlying_security: Some(warrant_underlying.clone()),
            footnote_ids: strings(&["F1", "F3", "F3", "F2"]),
            ..Default::default()
        }],
        derivative_holdings: vec![sec::Holding {
            security_title: s("Redeemable Warrants"),
            shares_owned: s("149000"),
            direct_or_indirect: s("D"),
            conversion_or_exercise_price: s("11.5"),
            underlying_security: Some(warrant_underlying),
            footnote_ids: strings(&["F3", "F3", "F2"]),
            ..Default::default()
        }],
        footnotes: JOINT_FOOTNOTES
            .iter()
            .map(|(id, text)| sec::Footnote {
                id: s(id),
                text: s(text),
            })
            .collect(),
        owner_signatures: vec![
            sec::Signature {
                name: s("/s/ Nelson Mullins Riley & Scarborough LLP, Attorney-in-Fact for Gary A. Simanson"),
                date: s("2026-08-14"),
            },
            sec::Signature {
                name: s("/s/ Nelson Mullins Riley & Scarborough LLP, Attorney-in-Fact for TBCP V, LLC"),
                date: s("2026-08-14"),
            },
        ],
        aff_10b5_one: Some(false),
        ..Default::default()
    };
    sec::Filing {
        accession_number: s("0001339459-26-000007"),
        cik: s("0002140030"),
        company_name: s("Thunder Bridge Capital Partners V, Ltd."),
        filing_date: s("2026-08-14"),
        period_of_report: s("2026-08-12"),
        // 2026-08-14T21:58:34Z
        acceptance_datetime: at(1_786_744_714),
        source_path: s("20260814.gz!0001339459-26-000007"),
        ..filing("4", Body::Ownership(doc))
    }
}

/// The footnotes of `0001339459-26-000007`, verbatim.
const JOINT_FOOTNOTES: [(&str, &str); 3] = [
    (
        "F1",
        "In connection with the issuer's initial public offering, TBCP V, LLC (the \"Sponsor\") purchased 447,000 private placement units at $10.00 per unit, each consisting of one Class A ordinary share, par value $0.0001 per share, and one-third of one redeemable warrant.",
    ),
    (
        "F2",
        "The securities are owned directly by the Sponsor. Mr. Simanson has an interest in the securities reported herein through his membership interest in the Sponsor. The Sponsor is managed and controlled by Gary A. Simanson, Chief Executive Officer and director of the issuer. Mr. Simanson is the controlling member of the Sponsor and exercises voting and dispositive control over the securities held by the Sponsor. Mr. Simanson disclaims any beneficial ownership of the securities reported herein other than to the extent of any pecuniary interest he may have therein, directly or indirectly.",
    ),
    (
        "F3",
        "The warrants will become exercisable on the later of 30 days after the completion of the issuer's initial business combination and 12 months from the closing of the issuer's initial public offering. If the issuer is unable to complete its initial business combination within the completion window, the warrants may expire worthless.",
    ),
];

/// `0001477932-14-004179` (2014-08-11, block 2346291): a 2014 Form 4/A whose
/// dates carry a `-05:00` zone suffix (§4.6 `tz_dropped`: 5 of the 6 sample
/// cases; the sixth is its owner signature, on `filing_signatures`). The body
/// is the real one, verbatim.
fn tz_form4a() -> sec::Filing {
    let doc = sec::OwnershipDocument {
        schema_version: s("X0306"),
        document_type: s("4/A"),
        period_of_report: s("2014-06-30-05:00"),
        issuer: Some(sec::Issuer {
            cik: s("0001099132"),
            name: s("MANHATTAN SCIENTIFICS INC"),
            trading_symbol: s("MHTX"),
            ..Default::default()
        }),
        reporting_owners: vec![sec::ReportingOwner {
            cik: s("0001415910"),
            name: s("Friedman Leonard"),
            address: Some(sec::Address {
                street1: s("405 LEXINGTON AVENUE"),
                street2: s("26TH FLOOR"),
                city: s("NEW YORK"),
                state: s("NY"),
                zip_code: s("10174"),
                ..Default::default()
            }),
            relationship: Some(sec::Relationship {
                is_director: true,
                is_officer: true,
                officer_title: s("Secretary"),
                ..Default::default()
            }),
        }],
        non_derivative_holdings: vec![sec::Holding {
            security_title: s("Common Stock, $.001 par value"),
            shares_owned: s("9923641"),
            direct_or_indirect: s("D"),
            footnote_ids: strings(&["F1"]),
            ..Default::default()
        }],
        derivative_transactions: vec![sec::Transaction {
            security_title: s("Stock Options"),
            transaction_date: s("2014-06-30-05:00"),
            transaction_form_type: s("4"),
            transaction_code: s("A"),
            shares: s("500000"),
            price_per_share: s("0.13"),
            acquired_disposed_code: s("A"),
            shares_owned_following: s("500000"),
            direct_or_indirect: s("D"),
            conversion_or_exercise_price: s("0.13"),
            exercise_date: s("2014-06-30-05:00"),
            expiration_date: s("2024-06-30-05:00"),
            underlying_security: Some(sec::UnderlyingSecurity {
                title: s("Common Stock, $.001 par value"),
                shares: s("500000"),
                ..Default::default()
            }),
            footnote_ids: strings(&["F2"]),
            ..Default::default()
        }],
        footnotes: vec![
            sec::Footnote {
                id: s("F1"),
                text: s("Mr. Friedman exercised a warrant which converted into 723,641 shares of common stock of the Company."),
            },
            sec::Footnote {
                id: s("F2"),
                text: s("Mr. Friedman was granted the stock option for service as a director to the company."),
            },
        ],
        owner_signatures: vec![sec::Signature {
            name: s("/s/ Leonard Friedman"),
            date: s("2014-08-11-05:00"),
        }],
        date_of_original_submission: s("2014-07-22-05:00"),
        ..Default::default()
    };
    sec::Filing {
        accession_number: s("0001477932-14-004179"),
        cik: s("0001099132"),
        company_name: s("MANHATTAN SCIENTIFICS INC"),
        filing_date: s("2014-08-11"),
        period_of_report: s("2014-06-30"),
        // 2014-08-11T16:31:14Z
        acceptance_datetime: at(1_407_774_674),
        source_path: s("20140811.gz!0001477932-14-004179"),
        ..filing("4/A", Body::Ownership(doc))
    }
}

/// `0001209191-04-032033` (2026-03-16, block 2956032): a 2004 Form 4
/// re-disseminated in 2026 (an open-market purchase with a price, a fractional
/// holding after it). The body is the real one, verbatim.
fn redisseminated_form4() -> sec::Filing {
    let doc = sec::OwnershipDocument {
        schema_version: s("X0202"),
        document_type: s("4"),
        period_of_report: s("2004-06-18"),
        issuer: Some(sec::Issuer {
            cik: s("0000919864"),
            name: s("NORTHWEST INDIANA BANCORP"),
            trading_symbol: s("NWIB(OB)"),
            ..Default::default()
        }),
        reporting_owners: vec![sec::ReportingOwner {
            cik: s("0001212469"),
            name: s("DEGUILIO JON E"),
            address: Some(sec::Address {
                street1: s("XXXXX"),
                city: s("XXXXX"),
                state: s("XX"),
                zip_code: s("XXXXX"),
                ..Default::default()
            }),
            relationship: Some(sec::Relationship {
                is_officer: true,
                officer_title: s("Executive Vice-President"),
                ..Default::default()
            }),
        }],
        non_derivative_transactions: vec![sec::Transaction {
            security_title: s("Common Stock"),
            transaction_date: s("2004-06-18"),
            deemed_execution_date: s("2004-06-18"),
            transaction_form_type: s("4"),
            transaction_code: s("P"),
            shares: s("86"),
            price_per_share: s("31.08"),
            acquired_disposed_code: s("A"),
            shares_owned_following: s("1418.1223"),
            direct_or_indirect: s("D"),
            ..Default::default()
        }],
        owner_signatures: vec![sec::Signature {
            name: s("/s/ Jon E. DeGuilio"),
            date: s("2004-06-18"),
        }],
        ..Default::default()
    };
    sec::Filing {
        accession_number: s("0001209191-04-032033"),
        cik: s("0000919864"),
        company_name: s("NORTHWEST INDIANA BANCORP"),
        filing_date: s("2004-06-21"),
        period_of_report: s("2004-06-18"),
        // 2004-06-21T19:27:24Z
        acceptance_datetime: at(1_087_846_044),
        source_path: s("20260316.gz!0001209191-04-032033"),
        ..filing("4", Body::Ownership(doc))
    }
}

// ---------------------------------------------------------------------------
// Row assertions
// ---------------------------------------------------------------------------

/// The 7 canonical identity columns precede every table's own columns.
const CANONICAL: usize = 7;

/// The [FC] cells of a filing: `filing_index`, `accession_number`,
/// `form_type`, `filing_date`, `acceptance_datetime`.
type FcCells<'x> = [&'x str; 5];

const FC_NAMES: [&str; 5] = [
    "filing_index",
    "accession_number",
    "form_type",
    "filing_date",
    "acceptance_datetime",
];

const JOINT_FC: FcCells<'static> = [
    "0",
    "0001339459-26-000007",
    "4",
    "2026-08-14",
    "2026-08-14T21:58:34Z",
];

const TZ_FC: FcCells<'static> = [
    "0",
    "0001477932-14-004179",
    "4/A",
    "2014-08-11",
    "2014-08-11T16:31:14Z",
];

/// Assert **every** column of one row after the canonical ones, in schema
/// order: the [FC] cells, then `expected` (all remaining columns, by name).
fn assert_row(
    batches: &Batches,
    table: &str,
    row: usize,
    fc: FcCells<'_>,
    expected: &[(&str, &str)],
) {
    let actual = batches.row(table, row);
    let actual = &actual[CANONICAL..];
    let names: Vec<&str> = actual.iter().map(|(name, _)| name.as_str()).collect();
    let expected_names: Vec<&str> = FC_NAMES
        .iter()
        .copied()
        .chain(expected.iter().map(|(name, _)| *name))
        .collect();
    assert_eq!(names, expected_names, "{table}: columns");
    let expected_values = fc.iter().copied().chain(expected.iter().map(|(_, v)| *v));
    for ((name, value), expected) in actual.iter().zip(expected_values) {
        assert_eq!(value, expected, "{table}.{name} (row {row})");
    }
}

/// This group's issues, in `parse_issues` order (the envelope's signature
/// issues of the same filings are left out).
fn ownership_issues(batches: &Batches) -> Vec<IssueRow> {
    batches
        .issues()
        .into_iter()
        .filter(|issue| TABLES.contains(&issue.table.as_str()))
        .collect()
}

/// `(table, column, index, raw, issue)` of one issue.
type IssueKey<'x> = (&'x str, &'x str, [Option<u32>; 3], &'x str, &'x str);

fn issue_keys(issues: &[IssueRow]) -> Vec<IssueKey<'_>> {
    issues
        .iter()
        .map(|i| {
            (
                i.table.as_str(),
                i.column.as_str(),
                i.index,
                i.raw.as_str(),
                i.issue.as_str(),
            )
        })
        .collect()
}

/// `parent` followed by `own`.
fn cells(
    parent: &[(&'static str, &'static str)],
    own: &[(&'static str, &'static str)],
) -> Vec<(&'static str, &'static str)> {
    parent.iter().chain(own).copied().collect()
}

/// Expected values of the joint Form 4, shared by the prost test and the
/// real-block test.
fn assert_joint_form4_rows(batches: &Batches, fc: FcCells<'_>) {
    batches.assert_rows(&[
        ("ownership_documents", 1),
        ("ownership_reporting_owners", 2),
        ("ownership_transactions", 2),
        ("ownership_holdings", 2),
        ("ownership_footnotes", 3),
    ]);
    assert_row(
        batches,
        "ownership_documents",
        0,
        fc,
        &[
            ("issuer_cik", "0002140030"),
            ("issuer_name", "Thunder Bridge Capital Partners V, Ltd."),
            ("issuer_trading_symbol", "TBCV"),
            ("issuer_foreign_trading_symbol", "NULL"),
            ("schema_version", "X0609"),
            ("document_type", "4"),
            ("period_of_report", "2026-08-12"),
            ("not_subject_to_section16", "false"),
            ("aff_10b5_one", "false"),
            ("no_securities_owned", "NULL"),
            ("form3_holdings_reported", "NULL"),
            ("form4_transactions_reported", "NULL"),
            ("date_of_original_submission", "NULL"),
            ("remarks", "NULL"),
            ("reporting_owner_count", "2"),
            ("non_derivative_transaction_count", "1"),
            ("derivative_transaction_count", "1"),
            ("non_derivative_holding_count", "1"),
            ("derivative_holding_count", "1"),
            ("footnote_count", "3"),
            ("owner_signature_count", "2"),
            ("has_parse_issues", "false"),
        ],
    );

    let owner = |index: &'static str,
                 cik: &'static str,
                 name: &'static str,
                 roles: [&'static str; 4],
                 title: &'static str| {
        vec![
            ("issuer_cik", "0002140030"),
            ("issuer_trading_symbol", "TBCV"),
            ("owner_index", index),
            ("owner_cik", cik),
            ("owner_name", name),
            ("owner_street1", "C/O THUNDER BRIDGE CAPITAL PARTNERS V"),
            ("owner_street2", "LTD., 9912 GEORGETOWN PIKE, SUITE D203"),
            ("owner_city", "GREAT FALLS"),
            ("owner_state", "VA"),
            ("owner_zip_code", "22066"),
            ("owner_state_description", "NULL"),
            ("owner_country", "NULL"),
            ("owner_non_us_state_territory", "NULL"),
            ("is_director", roles[0]),
            ("is_officer", roles[1]),
            ("is_ten_percent_owner", roles[2]),
            ("is_other", roles[3]),
            ("officer_title", title),
            ("other_text", "NULL"),
        ]
    };
    assert_row(
        batches,
        "ownership_reporting_owners",
        0,
        fc,
        &owner(
            "0",
            "0001339459",
            "Simanson Gary A",
            ["true", "true", "true", "false"],
            "Chief Executive Officer",
        ),
    );
    assert_row(
        batches,
        "ownership_reporting_owners",
        1,
        fc,
        &owner(
            "1",
            "0002142210",
            "TBCP V, LLC",
            ["false", "false", "true", "false"],
            "NULL",
        ),
    );

    // The owner context: both owners listed, the ORs, and only the officer's
    // title (never the fund's).
    let parent = [
        ("issuer_cik", "0002140030"),
        ("issuer_name", "Thunder Bridge Capital Partners V, Ltd."),
        ("issuer_trading_symbol", "TBCV"),
        ("reporting_owner_count", "2"),
        ("owner_ciks", "[0001339459, 0002142210]"),
        ("owner_names", "[Simanson Gary A, TBCP V, LLC]"),
        ("any_owner_is_director", "true"),
        ("any_owner_is_officer", "true"),
        ("any_owner_is_ten_percent_owner", "true"),
        ("any_owner_is_other", "false"),
        ("officer_titles", "[Chief Executive Officer]"),
    ];
    let transaction_parent = cells(&parent, &[("aff_10b5_one", "false")]);
    assert_row(
        batches,
        "ownership_transactions",
        0,
        fc,
        &cells(
            &transaction_parent,
            &[
                ("transaction_index", "0"),
                ("is_derivative", "false"),
                ("security_title", "Class A ordinary shares"),
                ("transaction_date", "2026-08-12"),
                ("deemed_execution_date", "NULL"),
                ("transaction_form_type", "4"),
                ("transaction_code", "P"),
                ("equity_swap_involved", "false"),
                ("transaction_timeliness", "NULL"),
                ("shares", "447000.000000"),
                ("price_per_share", "NULL"),
                ("total_value", "NULL"),
                ("acquired_disposed_code", "A"),
                ("shares_owned_following", "447000.000000"),
                ("value_owned_following", "NULL"),
                ("direct_or_indirect", "I"),
                ("nature_of_ownership", "See Footnote"),
                ("conversion_or_exercise_price", "NULL"),
                ("exercise_date", "NULL"),
                ("expiration_date", "NULL"),
                ("underlying_security_title", "NULL"),
                ("underlying_security_shares", "NULL"),
                ("underlying_security_value", "NULL"),
                ("footnote_ids", "[F1, F1, F2]"),
                ("signed_shares", "447000.000000"),
                // Footnote-only price: NULL, never 0.
                ("value_usd", "NULL"),
                ("is_open_market", "true"),
                ("filing_lag_days", "2"),
                ("has_parse_issues", "false"),
            ],
        ),
    );
    assert_row(
        batches,
        "ownership_transactions",
        1,
        fc,
        &cells(
            &transaction_parent,
            &[
                // Table II rows follow Table I's.
                ("transaction_index", "1"),
                ("is_derivative", "true"),
                ("security_title", "Redeemable Warrants"),
                ("transaction_date", "2026-08-12"),
                ("deemed_execution_date", "NULL"),
                ("transaction_form_type", "4"),
                ("transaction_code", "P"),
                ("equity_swap_involved", "false"),
                ("transaction_timeliness", "NULL"),
                ("shares", "149000.000000"),
                ("price_per_share", "NULL"),
                ("total_value", "NULL"),
                ("acquired_disposed_code", "A"),
                ("shares_owned_following", "149000.000000"),
                ("value_owned_following", "NULL"),
                ("direct_or_indirect", "I"),
                ("nature_of_ownership", "See Footnote"),
                ("conversion_or_exercise_price", "11.500000"),
                ("exercise_date", "NULL"),
                ("expiration_date", "NULL"),
                ("underlying_security_title", "Class A ordinary shares"),
                ("underlying_security_shares", "149000.000000"),
                ("underlying_security_value", "NULL"),
                ("footnote_ids", "[F1, F3, F3, F2]"),
                ("signed_shares", "149000.000000"),
                ("value_usd", "NULL"),
                // A derivative purchase is never open-market.
                ("is_open_market", "false"),
                ("filing_lag_days", "2"),
                ("has_parse_issues", "false"),
            ],
        ),
    );

    assert_row(
        batches,
        "ownership_holdings",
        0,
        fc,
        &cells(
            &parent,
            &[
                ("holding_index", "0"),
                ("is_derivative", "false"),
                ("security_title", "Class A ordinary shares"),
                ("shares_owned", "447000.000000"),
                ("value_owned", "NULL"),
                ("direct_or_indirect", "D"),
                ("nature_of_ownership", "NULL"),
                ("conversion_or_exercise_price", "NULL"),
                ("exercise_date", "NULL"),
                ("expiration_date", "NULL"),
                ("underlying_security_title", "NULL"),
                ("underlying_security_shares", "NULL"),
                ("underlying_security_value", "NULL"),
                ("footnote_ids", "[F1, F2]"),
                ("has_parse_issues", "false"),
            ],
        ),
    );
    assert_row(
        batches,
        "ownership_holdings",
        1,
        fc,
        &cells(
            &parent,
            &[
                ("holding_index", "1"),
                ("is_derivative", "true"),
                ("security_title", "Redeemable Warrants"),
                ("shares_owned", "149000.000000"),
                ("value_owned", "NULL"),
                ("direct_or_indirect", "D"),
                ("nature_of_ownership", "NULL"),
                ("conversion_or_exercise_price", "11.500000"),
                ("exercise_date", "NULL"),
                ("expiration_date", "NULL"),
                ("underlying_security_title", "Class A ordinary shares"),
                ("underlying_security_shares", "149000.000000"),
                ("underlying_security_value", "NULL"),
                ("footnote_ids", "[F3, F3, F2]"),
                ("has_parse_issues", "false"),
            ],
        ),
    );

    for (row, (id, text)) in JOINT_FOOTNOTES.iter().enumerate() {
        let index = row.to_string();
        assert_row(
            batches,
            "ownership_footnotes",
            row,
            fc,
            &[
                ("issuer_cik", "0002140030"),
                ("footnote_index", &index),
                ("footnote_id", id),
                ("footnote_text", text),
            ],
        );
    }
}

/// Expected values of the 2014 Form 4/A with zone-suffixed dates.
fn assert_tz_form4a_rows(batches: &Batches, fc: FcCells<'_>) {
    batches.assert_rows(&[
        ("ownership_documents", 1),
        ("ownership_reporting_owners", 1),
        ("ownership_transactions", 1),
        ("ownership_holdings", 1),
        ("ownership_footnotes", 2),
    ]);
    assert_row(
        batches,
        "ownership_documents",
        0,
        fc,
        &[
            ("issuer_cik", "0001099132"),
            ("issuer_name", "MANHATTAN SCIENTIFICS INC"),
            ("issuer_trading_symbol", "MHTX"),
            ("issuer_foreign_trading_symbol", "NULL"),
            ("schema_version", "X0306"),
            ("document_type", "4/A"),
            // `tz_dropped`: the date is kept.
            ("period_of_report", "2014-06-30"),
            ("not_subject_to_section16", "false"),
            ("aff_10b5_one", "NULL"),
            ("no_securities_owned", "NULL"),
            ("form3_holdings_reported", "NULL"),
            ("form4_transactions_reported", "NULL"),
            ("date_of_original_submission", "2014-07-22"),
            ("remarks", "NULL"),
            ("reporting_owner_count", "1"),
            ("non_derivative_transaction_count", "0"),
            ("derivative_transaction_count", "1"),
            ("non_derivative_holding_count", "1"),
            ("derivative_holding_count", "0"),
            ("footnote_count", "2"),
            ("owner_signature_count", "1"),
            ("has_parse_issues", "true"),
        ],
    );
    assert_row(
        batches,
        "ownership_reporting_owners",
        0,
        fc,
        &[
            ("issuer_cik", "0001099132"),
            ("issuer_trading_symbol", "MHTX"),
            ("owner_index", "0"),
            ("owner_cik", "0001415910"),
            ("owner_name", "Friedman Leonard"),
            ("owner_street1", "405 LEXINGTON AVENUE"),
            ("owner_street2", "26TH FLOOR"),
            ("owner_city", "NEW YORK"),
            ("owner_state", "NY"),
            ("owner_zip_code", "10174"),
            ("owner_state_description", "NULL"),
            ("owner_country", "NULL"),
            ("owner_non_us_state_territory", "NULL"),
            ("is_director", "true"),
            ("is_officer", "true"),
            ("is_ten_percent_owner", "false"),
            ("is_other", "false"),
            ("officer_title", "Secretary"),
            ("other_text", "NULL"),
        ],
    );
    let parent = [
        ("issuer_cik", "0001099132"),
        ("issuer_name", "MANHATTAN SCIENTIFICS INC"),
        ("issuer_trading_symbol", "MHTX"),
        ("reporting_owner_count", "1"),
        ("owner_ciks", "[0001415910]"),
        ("owner_names", "[Friedman Leonard]"),
        ("any_owner_is_director", "true"),
        ("any_owner_is_officer", "true"),
        ("any_owner_is_ten_percent_owner", "false"),
        ("any_owner_is_other", "false"),
        ("officer_titles", "[Secretary]"),
    ];
    assert_row(
        batches,
        "ownership_transactions",
        0,
        fc,
        &cells(
            &parent,
            &[
                ("aff_10b5_one", "NULL"),
                // The only row, from Table II: index 0.
                ("transaction_index", "0"),
                ("is_derivative", "true"),
                ("security_title", "Stock Options"),
                ("transaction_date", "2014-06-30"),
                ("deemed_execution_date", "NULL"),
                ("transaction_form_type", "4"),
                ("transaction_code", "A"),
                ("equity_swap_involved", "false"),
                ("transaction_timeliness", "NULL"),
                ("shares", "500000.000000"),
                ("price_per_share", "0.130000"),
                ("total_value", "NULL"),
                ("acquired_disposed_code", "A"),
                ("shares_owned_following", "500000.000000"),
                ("value_owned_following", "NULL"),
                ("direct_or_indirect", "D"),
                ("nature_of_ownership", "NULL"),
                ("conversion_or_exercise_price", "0.130000"),
                ("exercise_date", "2014-06-30"),
                ("expiration_date", "2024-06-30"),
                ("underlying_security_title", "Common Stock, $.001 par value"),
                ("underlying_security_shares", "500000.000000"),
                ("underlying_security_value", "NULL"),
                ("footnote_ids", "[F2]"),
                ("signed_shares", "500000.000000"),
                // 500000 × 0.13
                ("value_usd", "65000.000000"),
                ("is_open_market", "false"),
                // 2014-08-11 − 2014-06-30.
                ("filing_lag_days", "42"),
                ("has_parse_issues", "true"),
            ],
        ),
    );
    assert_row(
        batches,
        "ownership_holdings",
        0,
        fc,
        &cells(
            &parent,
            &[
                ("holding_index", "0"),
                ("is_derivative", "false"),
                ("security_title", "Common Stock, $.001 par value"),
                ("shares_owned", "9923641.000000"),
                ("value_owned", "NULL"),
                ("direct_or_indirect", "D"),
                ("nature_of_ownership", "NULL"),
                ("conversion_or_exercise_price", "NULL"),
                ("exercise_date", "NULL"),
                ("expiration_date", "NULL"),
                ("underlying_security_title", "NULL"),
                ("underlying_security_shares", "NULL"),
                ("underlying_security_value", "NULL"),
                ("footnote_ids", "[F1]"),
                ("has_parse_issues", "false"),
            ],
        ),
    );
    assert_row(
        batches,
        "ownership_footnotes",
        1,
        fc,
        &[
            ("issuer_cik", "0001099132"),
            ("footnote_index", "1"),
            ("footnote_id", "F2"),
            (
                "footnote_text",
                "Mr. Friedman was granted the stock option for service as a director to the company.",
            ),
        ],
    );

    // §4.6: the five ownership `tz_dropped` issues of the samples, keyed and in
    // emit order (the document row, then the transaction in schema order).
    let issues = ownership_issues(batches);
    let accession = Some(fc[1]);
    let filing_index = fc[0].parse().ok();
    assert!(issues
        .iter()
        .all(|i| i.filing_index == filing_index && i.accession_number.as_deref() == accession));
    let none = [None, None, None];
    let tx0 = [Some(0), None, None];
    let d = "ownership_documents";
    let t = "ownership_transactions";
    assert_eq!(
        issue_keys(&issues),
        [
            (
                d,
                "period_of_report",
                none,
                "2014-06-30-05:00",
                "tz_dropped"
            ),
            (
                d,
                "date_of_original_submission",
                none,
                "2014-07-22-05:00",
                "tz_dropped"
            ),
            (t, "transaction_date", tx0, "2014-06-30-05:00", "tz_dropped"),
            (t, "exercise_date", tx0, "2014-06-30-05:00", "tz_dropped"),
            (t, "expiration_date", tx0, "2024-06-30-05:00", "tz_dropped"),
        ]
    );
}

// ---------------------------------------------------------------------------
// Value tests
// ---------------------------------------------------------------------------

/// §8.7: joint Form 4 (CEO + fund), footnote-only price.
#[test]
fn joint_form4_maps_every_column() {
    let batches = map(&[window_block(JOINT_BLOCK, vec![joint_form4()])]);
    assert_joint_form4_rows(&batches, JOINT_FC);
    assert!(ownership_issues(&batches).is_empty());
    // The canonical identity of a child row is its block's.
    for table in TABLES {
        assert_eq!(batches.cell(table, "block_num", 0), "2977907", "{table}");
        assert_eq!(batches.cell(table, "block_id", 0), "2977907", "{table}");
        assert_eq!(
            batches.cell(table, "timestamp", 0),
            "2026-08-14T21:50:00Z",
            "{table}"
        );
        assert_eq!(batches.cell(table, "date", 0), "2026-08-14", "{table}");
    }
}

/// §8.7 and §4.6: the 2014 `tz_dropped` dates keep their value and log one
/// issue each, with the row's key positions.
#[test]
fn tz_suffixed_dates_are_kept_and_logged() {
    let batches = map(&[window_block(TZ_BLOCK, vec![tz_form4a()])]);
    assert_tz_form4a_rows(&batches, TZ_FC);
}

/// §8.7: a 2004 Form 4 re-disseminated in 2026: `filing_lag_days` uses the
/// filing date, not the block; a priced open-market purchase has `value_usd`.
#[test]
fn redisseminated_form4_uses_its_own_filing_date() {
    let batches = map(&[window_block(
        REDISSEMINATED_BLOCK,
        vec![redisseminated_form4()],
    )]);
    let t = "ownership_transactions";
    for (column, value) in [
        ("filing_date", "2004-06-21"),
        ("date", "2026-03-16"),
        ("transaction_date", "2004-06-18"),
        ("deemed_execution_date", "2004-06-18"),
        ("filing_lag_days", "3"),
        ("shares", "86.000000"),
        ("price_per_share", "31.080000"),
        ("shares_owned_following", "1418.122300"),
        // 86 × 31.08
        ("value_usd", "2672.880000"),
        ("signed_shares", "86.000000"),
        ("is_open_market", "true"),
        ("officer_titles", "[Executive Vice-President]"),
        ("any_owner_is_director", "false"),
        ("any_owner_is_officer", "true"),
        ("issuer_trading_symbol", "NWIB(OB)"),
    ] {
        assert_eq!(batches.cell(t, column, 0), value, "{column}");
    }
    assert_eq!(
        batches.cell("ownership_reporting_owners", "owner_street1", 0),
        "XXXXX"
    );
    batches.assert_rows(&[
        ("ownership_documents", 1),
        ("ownership_reporting_owners", 1),
        ("ownership_transactions", 1),
        ("ownership_holdings", 0),
        ("ownership_footnotes", 0),
    ]);
}

/// An empty body still gives its document row: NULL strings and dates, false
/// proto bools, NULL optional bools, zero counts, and no child rows.
#[test]
fn an_empty_body_gives_one_document_row() {
    let batches = map(&[window_block(
        BLOCK_NUM,
        vec![filing(
            "4",
            Body::Ownership(sec::OwnershipDocument::default()),
        )],
    )]);
    batches.assert_rows(&[
        ("ownership_documents", 1),
        ("ownership_reporting_owners", 0),
        ("ownership_transactions", 0),
        ("ownership_holdings", 0),
        ("ownership_footnotes", 0),
    ]);
    assert_row(
        &batches,
        "ownership_documents",
        0,
        [
            "0",
            "0000000000-26-000001",
            "4",
            "2026-08-27",
            "2026-08-28T12:31:00Z",
        ],
        &[
            ("issuer_cik", "NULL"),
            ("issuer_name", "NULL"),
            ("issuer_trading_symbol", "NULL"),
            ("issuer_foreign_trading_symbol", "NULL"),
            ("schema_version", "NULL"),
            ("document_type", "NULL"),
            ("period_of_report", "NULL"),
            ("not_subject_to_section16", "false"),
            ("aff_10b5_one", "NULL"),
            ("no_securities_owned", "NULL"),
            ("form3_holdings_reported", "NULL"),
            ("form4_transactions_reported", "NULL"),
            ("date_of_original_submission", "NULL"),
            ("remarks", "NULL"),
            ("reporting_owner_count", "0"),
            ("non_derivative_transaction_count", "0"),
            ("derivative_transaction_count", "0"),
            ("non_derivative_holding_count", "0"),
            ("derivative_holding_count", "0"),
            ("footnote_count", "0"),
            ("owner_signature_count", "0"),
            ("has_parse_issues", "false"),
        ],
    );
    assert!(ownership_issues(&batches).is_empty());
}

/// §4.1 per bool kind: a proto `bool` of the body or of a list element is
/// non-null (false = false or absent); an `optional bool` is NULL only when
/// unset; a `Relationship` bool is NULL exactly when the relationship is
/// absent.
#[test]
fn bools_follow_their_presence_rules() {
    let doc = sec::OwnershipDocument {
        not_subject_to_section16: true,
        aff_10b5_one: Some(true),
        no_securities_owned: Some(false),
        form3_holdings_reported: Some(true),
        form4_transactions_reported: Some(false),
        issuer: Some(sec::Issuer {
            foreign_trading_symbol: s("TBCV.L"),
            ..Default::default()
        }),
        remarks: s("Joint filing."),
        reporting_owners: vec![
            // No relationship: NULL bools and texts; no address: NULL.
            sec::ReportingOwner {
                cik: s("0000000011"),
                name: s("NO ROLE"),
                ..Default::default()
            },
            // An empty relationship: false, not NULL.
            sec::ReportingOwner {
                cik: s("0000000012"),
                name: s("EMPTY ROLE"),
                address: Some(sec::Address {
                    state_description: s("ONTARIO, CANADA"),
                    country: s("CA"),
                    non_us_state_territory: s("ON"),
                    ..Default::default()
                }),
                relationship: Some(sec::Relationship::default()),
            },
            sec::ReportingOwner {
                cik: s("0000000013"),
                name: s("OTHER"),
                relationship: Some(sec::Relationship {
                    is_other: true,
                    other_text: s("Member of 10% group"),
                    // A title without the officer box is not an officer's.
                    officer_title: s("Trustee"),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
        non_derivative_transactions: vec![sec::Transaction {
            equity_swap_involved: true,
            transaction_timeliness: s("L"),
            ..Default::default()
        }],
        ..Default::default()
    };
    let batches = map(&[window_block(
        BLOCK_NUM,
        vec![filing("4", Body::Ownership(doc))],
    )]);
    let d = "ownership_documents";
    for (column, value) in [
        ("issuer_foreign_trading_symbol", "TBCV.L"),
        ("issuer_cik", "NULL"),
        ("remarks", "Joint filing."),
        ("not_subject_to_section16", "true"),
        ("aff_10b5_one", "true"),
        ("no_securities_owned", "false"),
        ("form3_holdings_reported", "true"),
        ("form4_transactions_reported", "false"),
    ] {
        assert_eq!(batches.cell(d, column, 0), value, "{column}");
    }

    let o = "ownership_reporting_owners";
    for column in [
        "is_director",
        "is_officer",
        "is_ten_percent_owner",
        "is_other",
    ] {
        let other = if column == "is_other" {
            "true"
        } else {
            "false"
        };
        assert_eq!(
            batches.column(o, column),
            ["NULL", "false", other],
            "{column}"
        );
    }
    assert_eq!(
        batches.column(o, "officer_title"),
        ["NULL", "NULL", "Trustee"]
    );
    assert_eq!(
        batches.column(o, "other_text"),
        ["NULL", "NULL", "Member of 10% group"]
    );
    assert_eq!(batches.column(o, "owner_street1"), ["NULL", "NULL", "NULL"]);
    assert_eq!(
        batches.column(o, "owner_state_description"),
        ["NULL", "ONTARIO, CANADA", "NULL"]
    );
    assert_eq!(batches.column(o, "owner_country"), ["NULL", "CA", "NULL"]);
    assert_eq!(
        batches.column(o, "owner_non_us_state_territory"),
        ["NULL", "ON", "NULL"]
    );
    assert_eq!(batches.column(o, "issuer_cik"), ["NULL", "NULL", "NULL"]);

    let t = "ownership_transactions";
    for (column, value) in [
        ("any_owner_is_director", "false"),
        ("any_owner_is_officer", "false"),
        ("any_owner_is_ten_percent_owner", "false"),
        ("any_owner_is_other", "true"),
        ("officer_titles", "[]"),
        ("aff_10b5_one", "true"),
        ("equity_swap_involved", "true"),
        ("transaction_timeliness", "L"),
        ("footnote_ids", "[]"),
        ("is_open_market", "false"),
        ("has_parse_issues", "false"),
        // An otherwise empty transaction: every value NULL, no issue.
        ("security_title", "NULL"),
        ("transaction_date", "NULL"),
        ("shares", "NULL"),
        ("price_per_share", "NULL"),
        ("signed_shares", "NULL"),
        ("value_usd", "NULL"),
        ("filing_lag_days", "NULL"),
        ("transaction_code", "NULL"),
    ] {
        assert_eq!(batches.cell(t, column, 0), value, "{column}");
    }
    assert!(ownership_issues(&batches).is_empty());
}

/// `owner_ciks`/`owner_names` keep every owner in order with `''` → NULL
/// items; `officer_titles` keeps the non-empty titles of officers only.
#[test]
fn owner_context_lists_every_owner() {
    let owner = |cik: &str, name: &str, officer: bool, title: &str| sec::ReportingOwner {
        cik: s(cik),
        name: s(name),
        relationship: Some(sec::Relationship {
            is_officer: officer,
            officer_title: s(title),
            ..Default::default()
        }),
        ..Default::default()
    };
    let doc = sec::OwnershipDocument {
        reporting_owners: vec![
            owner("0000000021", "CFO", true, "CFO"),
            owner("", "NO CIK", true, ""),
            owner("0000000023", "", false, "Former CEO"),
            owner("0000000024", "COO", true, "COO"),
        ],
        non_derivative_holdings: vec![sec::Holding::default()],
        ..Default::default()
    };
    let batches = map(&[window_block(
        BLOCK_NUM,
        vec![filing("4", Body::Ownership(doc))],
    )]);
    let h = "ownership_holdings";
    for (column, value) in [
        ("reporting_owner_count", "4"),
        ("owner_ciks", "[0000000021, NULL, 0000000023, 0000000024]"),
        ("owner_names", "[CFO, NO CIK, NULL, COO]"),
        ("officer_titles", "[CFO, COO]"),
        ("any_owner_is_officer", "true"),
        ("any_owner_is_director", "false"),
        ("footnote_ids", "[]"),
        ("shares_owned", "NULL"),
        ("has_parse_issues", "false"),
    ] {
        assert_eq!(batches.cell(h, column, 0), value, "{column}");
    }
    let o = "ownership_reporting_owners";
    assert_eq!(
        batches.column(o, "owner_cik"),
        ["0000000021", "NULL", "0000000023", "0000000024"]
    );
    assert_eq!(batches.column(o, "owner_index"), ["0", "1", "2", "3"]);
}

fn tx(code: &str, acquired_disposed: &str, shares: &str, price: &str) -> sec::Transaction {
    sec::Transaction {
        transaction_code: s(code),
        acquired_disposed_code: s(acquired_disposed),
        shares: s(shares),
        price_per_share: s(price),
        transaction_date: s("2026-08-25"),
        ..Default::default()
    }
}

/// `signed_shares`, `value_usd` (exact, half away from zero), `is_open_market`
/// and `filing_lag_days` (§3.11, §4.3, §4.4).
#[test]
fn derived_transaction_columns() {
    let doc = sec::OwnershipDocument {
        non_derivative_transactions: vec![
            tx("S", "D", "1000", "12.25"),
            tx("P", "A", "0.333333", "0.5"),
            tx("P", "A", "-0.333333", "0.5"),
            // Codes are compared verbatim.
            tx("p", "a", "10", ""),
            tx("J", "", "10", "2"),
            sec::Transaction {
                // After the filing date: a negative lag.
                transaction_date: s("2026-08-30"),
                ..tx("G", "D", "", "3")
            },
        ],
        derivative_transactions: vec![tx("S", "D", "5", "1.5")],
        ..Default::default()
    };
    let batches = map(&[window_block(
        BLOCK_NUM,
        vec![filing("4", Body::Ownership(doc))],
    )]);
    let t = "ownership_transactions";
    assert_eq!(
        batches.column(t, "transaction_index"),
        ["0", "1", "2", "3", "4", "5", "6"]
    );
    assert_eq!(
        batches.column(t, "is_derivative"),
        ["false", "false", "false", "false", "false", "false", "true"]
    );
    assert_eq!(
        batches.column(t, "signed_shares"),
        [
            "-1000.000000",
            "0.333333",
            "-0.333333",
            "NULL",
            "NULL",
            "NULL",
            "-5.000000"
        ]
    );
    // 0.333333 × 0.5 = 0.1666665 → 0.166667 (half away from zero, both signs).
    assert_eq!(
        batches.column(t, "value_usd"),
        [
            "12250.000000",
            "0.166667",
            "-0.166667",
            "NULL",
            "20.000000",
            "NULL",
            "7.500000"
        ]
    );
    assert_eq!(
        batches.column(t, "is_open_market"),
        ["true", "true", "true", "false", "false", "false", "false"]
    );
    // The filing date is 2026-08-27.
    assert_eq!(
        batches.column(t, "filing_lag_days"),
        ["2", "2", "2", "2", "2", "-3", "2"]
    );
    assert_eq!(batches.column(t, "has_parse_issues"), ["false"; 7]);
    assert!(ownership_issues(&batches).is_empty());

    // No filing date: no lag.
    let undated = sec::Filing {
        filing_date: String::new(),
        ..filing(
            "4",
            Body::Ownership(sec::OwnershipDocument {
                non_derivative_transactions: vec![tx("P", "A", "1", "1")],
                ..Default::default()
            }),
        )
    };
    let batches = map(&[window_block(BLOCK_NUM, vec![undated])]);
    assert_eq!(batches.cell(t, "filing_date", 0), "NULL");
    assert_eq!(batches.cell(t, "transaction_date", 0), "2026-08-25");
    assert_eq!(batches.cell(t, "filing_lag_days", 0), "NULL");
}

/// Every typed column logs its §4.6 issue with the row's position (Table II
/// positions follow Table I's) and flags only its own row; a `value_usd`
/// overflow logs its operands after the row's parsed columns.
#[test]
fn parse_issues_are_keyed_by_concatenated_positions() {
    // 32 integer digits: the largest Q6 integer part.
    let max_q6 = "99999999999999999999999999999999";
    let doc = sec::OwnershipDocument {
        period_of_report: s("2026-02-30"),
        date_of_original_submission: s("N/A"),
        non_derivative_transactions: vec![
            sec::Transaction::default(),
            sec::Transaction {
                transaction_date: s("20260801"),
                deemed_execution_date: s("NONE"),
                shares: s("1,000"),
                price_per_share: s("N/A"),
                total_value: s("1.2345675"),
                shares_owned_following: s("-1.2345675"),
                value_owned_following: s("999999999999999999999999999999999"),
                conversion_or_exercise_price: s("$5"),
                exercise_date: s("XXXX"),
                expiration_date: s("13/01/2030"),
                underlying_security: Some(sec::UnderlyingSecurity {
                    title: s("Common"),
                    shares: s("-"),
                    value: s(".5"),
                }),
                ..Default::default()
            },
        ],
        derivative_transactions: vec![sec::Transaction {
            transaction_code: s("P"),
            acquired_disposed_code: s("A"),
            shares: s(max_q6),
            price_per_share: s(max_q6),
            transaction_date: s("2026-08-27"),
            ..Default::default()
        }],
        non_derivative_holdings: vec![sec::Holding::default()],
        derivative_holdings: vec![
            sec::Holding::default(),
            sec::Holding {
                shares_owned: s("NA"),
                value_owned: s("0.0000005"),
                conversion_or_exercise_price: s("1e3"),
                exercise_date: s("2026-08-27Z"),
                expiration_date: s("NULL"),
                underlying_security: Some(sec::UnderlyingSecurity {
                    title: String::new(),
                    shares: s("+3.25"),
                    value: s("1.0000001"),
                }),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let batches = map(&[window_block(
        BLOCK_NUM,
        vec![filing("4", Body::Ownership(doc))],
    )]);

    let d = "ownership_documents";
    assert_eq!(batches.cell(d, "period_of_report", 0), "NULL");
    assert_eq!(batches.cell(d, "date_of_original_submission", 0), "NULL");
    assert_eq!(batches.cell(d, "has_parse_issues", 0), "true");

    let t = "ownership_transactions";
    assert_eq!(
        batches.column(t, "has_parse_issues"),
        ["false", "true", "true"]
    );
    assert_eq!(
        batches.column(t, "is_derivative"),
        ["false", "false", "true"]
    );
    for (column, value) in [
        ("transaction_date", "NULL"),
        ("deemed_execution_date", "NULL"),
        ("shares", "NULL"),
        ("price_per_share", "NULL"),
        ("total_value", "1.234568"),
        ("shares_owned_following", "-1.234568"),
        ("value_owned_following", "NULL"),
        ("conversion_or_exercise_price", "NULL"),
        ("exercise_date", "NULL"),
        ("expiration_date", "NULL"),
        ("underlying_security_title", "Common"),
        ("underlying_security_shares", "NULL"),
        ("underlying_security_value", "0.500000"),
        ("value_usd", "NULL"),
        ("filing_lag_days", "NULL"),
    ] {
        assert_eq!(batches.cell(t, column, 1), value, "{column}");
    }
    // The overflowing product: NULL, its operands kept.
    let max_value = format!("{max_q6}.000000");
    assert_eq!(batches.cell(t, "shares", 2), max_value);
    assert_eq!(batches.cell(t, "price_per_share", 2), max_value);
    assert_eq!(batches.cell(t, "signed_shares", 2), max_value);
    assert_eq!(batches.cell(t, "value_usd", 2), "NULL");
    assert_eq!(batches.cell(t, "filing_lag_days", 2), "0");

    let h = "ownership_holdings";
    assert_eq!(
        batches.column(h, "has_parse_issues"),
        ["false", "false", "true"]
    );
    assert_eq!(batches.column(h, "holding_index"), ["0", "1", "2"]);
    assert_eq!(
        batches.column(h, "is_derivative"),
        ["false", "true", "true"]
    );
    for (column, value) in [
        ("shares_owned", "NULL"),
        ("value_owned", "0.000001"),
        ("conversion_or_exercise_price", "NULL"),
        ("exercise_date", "2026-08-27"),
        ("expiration_date", "NULL"),
        ("underlying_security_title", "NULL"),
        ("underlying_security_shares", "3.250000"),
        ("underlying_security_value", "1.000000"),
    ] {
        assert_eq!(batches.cell(h, column, 2), value, "{column}");
    }

    let product = format!("{max_q6} * {max_q6}");
    let none = [None, None, None];
    let tx1 = [Some(1), None, None];
    let tx2 = [Some(2), None, None];
    let h2 = [Some(2), None, None];
    let issues = ownership_issues(&batches);
    assert!(issues.iter().all(|issue| issue.filing_index == Some(0)));
    assert_eq!(
        issue_keys(&issues),
        [
            (d, "period_of_report", none, "2026-02-30", "out_of_range"),
            (d, "date_of_original_submission", none, "N/A", "sentinel"),
            (t, "transaction_date", tx1, "20260801", "unparseable"),
            (t, "deemed_execution_date", tx1, "NONE", "sentinel"),
            (t, "shares", tx1, "1,000", "unparseable"),
            (t, "price_per_share", tx1, "N/A", "sentinel"),
            (t, "total_value", tx1, "1.2345675", "rounded"),
            (t, "shares_owned_following", tx1, "-1.2345675", "rounded"),
            (
                t,
                "value_owned_following",
                tx1,
                "999999999999999999999999999999999",
                "out_of_range"
            ),
            (t, "conversion_or_exercise_price", tx1, "$5", "unparseable"),
            (t, "exercise_date", tx1, "XXXX", "sentinel"),
            (t, "expiration_date", tx1, "13/01/2030", "out_of_range"),
            (t, "underlying_security_shares", tx1, "-", "sentinel"),
            (t, "value_usd", tx2, product.as_str(), "overflow"),
            (h, "shares_owned", h2, "NA", "sentinel"),
            (h, "value_owned", h2, "0.0000005", "rounded"),
            (h, "conversion_or_exercise_price", h2, "1e3", "unparseable"),
            (h, "exercise_date", h2, "2026-08-27Z", "tz_dropped"),
            (h, "expiration_date", h2, "NULL", "sentinel"),
            (h, "underlying_security_value", h2, "1.0000001", "rounded"),
        ]
    );
}

/// Footnote ids and texts are verbatim with `''` → NULL; `footnote_ids` items
/// are verbatim (an empty id stays an empty item).
#[test]
fn footnotes_are_verbatim() {
    let doc = sec::OwnershipDocument {
        issuer: Some(sec::Issuer {
            cik: s("0000000099"),
            ..Default::default()
        }),
        footnotes: vec![
            sec::Footnote {
                id: s("F1"),
                text: s("Weighted average price; range $10.00 to $10.50."),
            },
            sec::Footnote::default(),
        ],
        non_derivative_holdings: vec![sec::Holding {
            footnote_ids: strings(&["F1", ""]),
            ..Default::default()
        }],
        ..Default::default()
    };
    let batches = map(&[window_block(
        BLOCK_NUM,
        vec![filing("4", Body::Ownership(doc))],
    )]);
    let f = "ownership_footnotes";
    assert_eq!(batches.column(f, "footnote_index"), ["0", "1"]);
    assert_eq!(batches.column(f, "footnote_id"), ["F1", "NULL"]);
    assert_eq!(
        batches.column(f, "footnote_text"),
        ["Weighted average price; range $10.00 to $10.50.", "NULL"]
    );
    assert_eq!(
        batches.column(f, "issuer_cik"),
        ["0000000099", "0000000099"]
    );
    assert_eq!(
        batches.cell("ownership_holdings", "footnote_ids", 0),
        "[F1, ]"
    );
}

/// Several filings in one block: each child row carries its own filing's
/// context, and positions restart per filing.
#[test]
fn positions_restart_per_filing() {
    let batches = map(&[window_block(
        BLOCK_NUM,
        vec![tz_form4a(), joint_form4(), redisseminated_form4()],
    )]);
    let t = "ownership_transactions";
    assert_eq!(batches.column(t, "filing_index"), ["0", "1", "1", "2"]);
    assert_eq!(batches.column(t, "transaction_index"), ["0", "0", "1", "0"]);
    assert_eq!(
        batches.column(t, "accession_number"),
        [
            "0001477932-14-004179",
            "0001339459-26-000007",
            "0001339459-26-000007",
            "0001209191-04-032033"
        ]
    );
    assert_eq!(batches.column(t, "form_type"), ["4/A", "4", "4", "4"]);
    let f = "ownership_footnotes";
    assert_eq!(batches.column(f, "filing_index"), ["0", "0", "1", "1", "1"]);
    assert_eq!(
        batches.column(f, "footnote_index"),
        ["0", "1", "0", "1", "2"]
    );
    assert_eq!(
        batches.column("ownership_reporting_owners", "owner_index"),
        ["0", "0", "1", "0"]
    );
    // Only the 2014 filing has issues.
    assert_eq!(
        batches.column("ownership_documents", "has_parse_issues"),
        ["true", "false", "false"]
    );
    let issues = ownership_issues(&batches);
    assert_eq!(issues.len(), 5);
    assert!(issues.iter().all(|issue| issue.filing_index == Some(0)));
}

// ---------------------------------------------------------------------------
// Real sample blocks (local only)
// ---------------------------------------------------------------------------

/// The prost mirrors are the real bodies, field for field.
#[test]
#[ignore = "local: needs /tmp/sec-fireparq/fire-v013"]
fn real_bodies_equal_the_mirrors() {
    for (day, block_num, mirror) in [
        ("2026-08-14", JOINT_BLOCK, joint_form4()),
        ("2014-08-11", TZ_BLOCK, tz_form4a()),
        ("2026-03-16", REDISSEMINATED_BLOCK, redisseminated_form4()),
    ] {
        let (_, block) = fire::block(day, block_num);
        let real = block
            .filings
            .iter()
            .find(|filing| filing.accession_number == mirror.accession_number)
            .unwrap_or_else(|| panic!("{day}: no {}", mirror.accession_number));
        assert_eq!(ownership(real), ownership(&mirror), "{day}");
        for (real, mirror) in [
            (&real.form_type, &mirror.form_type),
            (&real.filing_date, &mirror.filing_date),
            (&real.company_name, &mirror.company_name),
            (&real.source_path, &mirror.source_path),
        ] {
            assert_eq!(real, mirror, "{day}");
        }
        assert_eq!(
            real.acceptance_datetime, mirror.acceptance_datetime,
            "{day}"
        );
    }
}

/// The §8.7 ownership filings mapped from their real blocks: the same rows as
/// the mirrors (at the filing's real position).
#[test]
#[ignore = "local: needs /tmp/sec-fireparq/fire-v013"]
fn real_fixture_filings_map_like_the_mirrors() {
    use arrow::array::{AsArray, BooleanArray};

    // Map the whole real block, then keep the rows of `accession` only.
    let only = |day: &str, block_num: u64, accession: &str| {
        let (identity, block) = fire::block(day, block_num);
        let position = block
            .filings
            .iter()
            .position(|filing| filing.accession_number == accession)
            .unwrap_or_else(|| panic!("{day}: no {accession}"));
        let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
        mapper
            .map_block(&block.encode_to_vec(), &identity, StreamEvent::default())
            .unwrap();
        let mut batches = mapper.flush().unwrap();
        for table in TABLES.iter().chain(&["parse_issues"]) {
            let batch = &batches[*table];
            let accessions = batch
                .column_by_name("accession_number")
                .unwrap()
                .as_string::<i32>();
            let mask: BooleanArray = accessions
                .iter()
                .map(|value| Some(value == Some(accession)))
                .collect();
            let kept = arrow::compute::filter_record_batch(batch, &mask).unwrap();
            batches.insert(table.to_string(), kept);
        }
        (position.to_string(), Batches::new(batches))
    };

    let (position, batches) = only("2026-08-14", JOINT_BLOCK, "0001339459-26-000007");
    let mut fc = JOINT_FC;
    fc[0] = &position;
    assert_joint_form4_rows(&batches, fc);

    let (position, batches) = only("2014-08-11", TZ_BLOCK, "0001477932-14-004179");
    let mut fc = TZ_FC;
    fc[0] = &position;
    assert_tz_form4a_rows(&batches, fc);
}

/// Every column of every row of this group's tables equals the prototype's
/// (`proto_map.py`) on every sample day present locally.
/// `cargo test -p blocks --lib sec::tests::ownership::ownership_matches -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn ownership_matches_the_prototype() {
    oracle::assert_matches(&fire::DAYS, &TABLES, &[]);
}

/// This group's `parse_issues` rows equal the prototype's, in order and in
/// every column, on every sample day present locally (the full-table check is
/// `hub::parse_issues_match_the_prototype`, once every group has landed).
/// `cargo test -p blocks --lib sec::tests::ownership::ownership_parse_issues -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn ownership_parse_issues_match_the_prototype() {
    use serde_json::Value;
    use std::io::BufRead;

    let ours = |row: &serde_json::Map<String, Value>| {
        row.get("table_name")
            .and_then(Value::as_str)
            .is_some_and(|table| TABLES.contains(&table))
    };
    for day in fire::DAYS {
        if !fire::path(day).exists() || !oracle::dir(day).exists() {
            println!("{day}: skipped (no local sample)");
            continue;
        }
        let file = std::fs::File::open(oracle::dir(day).join("parse_issues.ndjson")).unwrap();
        let expected: Vec<serde_json::Map<String, Value>> = std::io::BufReader::new(file)
            .lines()
            .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
            .filter(|row| ours(row))
            .collect();

        let mut actual = Vec::new();
        let mut drain = |mapper: &mut SecBlockMapper| {
            let batches = mapper.flush().unwrap();
            let batch = &batches["parse_issues"];
            let schema = batch.schema();
            for row in 0..batch.num_rows() {
                let object: serde_json::Map<String, Value> = schema
                    .fields()
                    .iter()
                    .zip(batch.columns())
                    .map(|(field, column)| {
                        (
                            field.name().clone(),
                            oracle::json_value(column.as_ref(), row),
                        )
                    })
                    .collect();
                if ours(&object) {
                    actual.push(object);
                }
            }
        };
        let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
        for (identity, payload) in fire::blocks(day) {
            mapper
                .map_block_bytes(payload.into(), &identity, StreamEvent::default())
                .unwrap_or_else(|e| panic!("{day} block {}: {e:#}", identity.block_num));
            if mapper.total_rows() > 500_000 {
                drain(&mut mapper);
            }
        }
        drain(&mut mapper);

        println!(
            "{day}: rust {} ownership issues, prototype {}",
            actual.len(),
            expected.len()
        );
        assert_eq!(actual.len(), expected.len(), "{day}: issue count");
        for (row, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
            for (column, value) in expected {
                assert_eq!(
                    actual.get(column),
                    Some(value),
                    "{day} issue {row}: {column}"
                );
            }
        }
    }
}
