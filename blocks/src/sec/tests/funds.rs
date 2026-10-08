//! Value tests of `npx_reports`, `npx_votes`, `npx_vote_records`, `npx_other_managers`, `ncen_reports`.
//! Owned by the `funds` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The fixtures mirror the §8.7 sample filings field for field (prost structs,
//! nothing read from disk); `real_fixtures_equal_their_mirrors` checks the
//! mirrors against the real blocks locally, and `funds_match_the_prototype`
//! compares every row of the five tables with the prototype on the four sample
//! days.

use super::*;

/// This group's tables.
pub(crate) const TABLES: [&str; 5] = [
    "npx_reports",
    "npx_votes",
    "npx_vote_records",
    "npx_other_managers",
    "ncen_reports",
];

/// This group's filings in the cross-table contract fixture
/// (`make_every_body_block`): together they must give every table of
/// [`TABLES`] at least one row (and contribute signatures where the body has
/// them). Ordinals are reassigned by position.
pub(crate) fn contract_filings() -> Vec<sec::Filing> {
    let cover = sec::NpxCoverPage {
        other_managers: vec![manager("1", "COVER MANAGER LLC", "028-00001")],
        summary_managers: vec![manager("1", "SUMMARY MANAGER LLC", "028-00002")],
        signatures: vec![sec::NpxSignature {
            reporting_person: "TEST FUND TRUST".to_string(),
            signature: "/s/ Jane Doe".to_string(),
            printed_signature: "Jane Doe".to_string(),
            title: "President".to_string(),
            date: "08/27/2026".to_string(),
        }],
        series_reports: vec![sec::NpxSeriesReport {
            series_id: "S000000001".to_string(),
            name: "TEST FUND".to_string(),
            lei: String::new(),
        }],
        ..npx_cover("RMIC", "2026")
    };
    let npx = sec::NpxReport {
        filer_cik: "0000000001".to_string(),
        cover_page: Some(cover),
        votes: vec![sec::ProxyVote {
            vote_other_managers: vec!["1".to_string()],
            vote_series: "S000000001".to_string(),
            ..vote(
                "ACME CORP",
                "000360206",
                "06/30/2026",
                "ELECT DIRECTORS",
                "100",
                &[("FOR", "60", "FOR"), ("AGAINST", "40", "FOR")],
            )
        }],
    };
    let ncen = sec::NcenReport {
        filer_cik: "0000000001".to_string(),
        investment_company_type: "N-1A".to_string(),
        report_ending_period: "2026-06-30".to_string(),
        registrant: Some(sec::NcenRegistrant {
            full_name: "TEST FUND TRUST".to_string(),
            cik: "0000000001".to_string(),
            ..Default::default()
        }),
        series_ids: vec!["S000000001".to_string()],
        ..Default::default()
    };
    vec![
        filing("N-PX", Body::Npx(npx)),
        filing("N-CEN", Body::Ncen(ncen)),
    ]
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
// Builders
// ---------------------------------------------------------------------------

fn s(text: &str) -> String {
    text.to_string()
}

fn manager(serial_number: &str, name: &str, form13f_file_number: &str) -> sec::NpxManager {
    sec::NpxManager {
        serial_number: s(serial_number),
        name: s(name),
        form13f_file_number: s(form13f_file_number),
        ..Default::default()
    }
}

fn address(street1: &str, street2: &str, city: &str, state: &str, zip: &str) -> sec::Address {
    sec::Address {
        street1: s(street1),
        street2: s(street2),
        city: s(city),
        state: s(state),
        zip_code: s(zip),
        ..Default::default()
    }
}

/// A cover page with only the registrant type and the report year set.
fn npx_cover(registrant_type: &str, report_calendar_year: &str) -> sec::NpxCoverPage {
    sec::NpxCoverPage {
        registrant_type: s(registrant_type),
        report_calendar_year: s(report_calendar_year),
        ..Default::default()
    }
}

/// A say-on-pay style vote: `ISSUER` source, one category, `shares_on_loan`
/// `0`, and `(how_voted, shares_voted, management_recommendation)` records.
fn vote(
    issuer_name: &str,
    cusip: &str,
    meeting_date: &str,
    vote_description: &str,
    shares_voted: &str,
    records: &[(&str, &str, &str)],
) -> sec::ProxyVote {
    sec::ProxyVote {
        issuer_name: s(issuer_name),
        cusip: s(cusip),
        meeting_date: s(meeting_date),
        vote_description: s(vote_description),
        vote_categories: vec![s("SECTION 14A SAY-ON-PAY VOTES")],
        vote_source: s("ISSUER"),
        shares_voted: s(shares_voted),
        shares_on_loan: s("0"),
        records: records
            .iter()
            .map(|(how, shares, recommendation)| sec::VoteRecord {
                how_voted: s(how),
                shares_voted: s(shares),
                management_recommendation: s(recommendation),
            })
            .collect(),
        ..Default::default()
    }
}

/// A filing as firesec writes it: `accepted` is seconds after the start of
/// window `block`.
fn sample_filing(
    block: u64,
    accession: &str,
    form_type: &str,
    filing_date: &str,
    accepted: i64,
    body: Body,
) -> sec::Filing {
    sec::Filing {
        accession_number: s(accession),
        filing_date: s(filing_date),
        acceptance_datetime: Some(prost_types::Timestamp {
            seconds: window_seconds(block) + accepted,
            nanos: 0,
        }),
        source_path: format!("{}.gz!{accession}", filing_date.replace('-', "")),
        ..filing(form_type, body)
    }
}

/// One §8.7 fixture: the sample day and block of the real filing, and its
/// prost mirror.
struct Fixture {
    day: &'static str,
    block: u64,
    filing: sec::Filing,
}

impl Fixture {
    fn map(&self) -> Batches {
        map(&[window_block(self.block, vec![self.filing.clone()])])
    }
}

/// §8.7 "N-PX with a split vote, summary managers and `vote_other_managers`
/// (4 votes)": `0001193125-26-372353`, block 2979867 (2026-08-28), filing 4.
fn split_vote_fixture() -> Fixture {
    let managers = ["1", "2", "3"].map(s).to_vec();
    let split =
        |issuer, cusip, meeting, description, shares: &str, how, recommendation| sec::ProxyVote {
            vote_other_managers: managers.clone(),
            ..vote(
                issuer,
                cusip,
                meeting,
                description,
                shares,
                &[(how, shares, recommendation); 3],
            )
        };
    let npx = sec::NpxReport {
        filer_cik: s("0001554913"),
        cover_page: Some(sec::NpxCoverPage {
            year_or_quarter: s("YEAR"),
            reporting_person_name: s("Pamplona Capital Management, LLC"),
            reporting_person_address: Some(address(
                "1330 AVENUE OF THE AMERICAS, 24TH FLOOR",
                "",
                "NEW YORK",
                "NY",
                "10019",
            )),
            report_type: s("INSTITUTIONAL MANAGER VOTING REPORT"),
            file_number: s("028-15480"),
            confidential_treatment: s("N"),
            explanatory_choice: s("N"),
            summary_managers: vec![
                manager("1", "Pamplona PE Investments Malta Ltd", "028-24956"),
                manager("2", "Halsted John C.", "028-24958"),
                manager("3", "Knaster Alexander M", "028-24957"),
            ],
            other_included_managers_count: s("3"),
            reporting_person_phone: s("212-207-6820"),
            signatures: vec![sec::NpxSignature {
                reporting_person: s("Pamplona Capital Management, LLC"),
                signature: s("Stephen Gauci"),
                printed_signature: s("Stephen Gauci"),
                title: s("Director of Managing Member"),
                date: s("08/28/2026"),
            }],
            ..npx_cover("IM", "2026")
        }),
        votes: vec![
            split(
                "AMAZON COM INC",
                "23135106",
                "05/20/2026",
                "ADVISORY VOTE TO APPROVE EXECUTIVE COMPENSATION",
                "0",
                "TAKE NO ACTION",
                "NONE",
            ),
            split(
                "BED BATH & BEYOND INC",
                "690370101",
                "05/14/2026",
                "The approval, on an advisory (non-binding) basis, of the compensation paid by the \
                 Company to its named executive officers",
                "330655",
                "FOR",
                "FOR",
            ),
            split(
                "ELASTIC N V",
                "N14506104",
                "09/30/2025",
                "Proposal to approve, on a non-binding advisory basis, the compensation of our named \
                 executive officers as described in the proxy statement",
                "210600",
                "FOR",
                "FOR",
            ),
            split(
                "MICROSOFT CORP",
                "594918104",
                "12/05/2025",
                "Advisory Vote to Approve Named Executive Officer Compensation",
                "0",
                "TAKE NO ACTION",
                "NONE",
            ),
        ],
    };
    Fixture {
        day: "2026-08-28",
        block: 2_979_867,
        filing: sample_filing(
            2_979_867,
            "0001193125-26-372353",
            "N-PX",
            "2026-08-28",
            95,
            Body::Npx(npx),
        ),
    }
}

/// §8.7 "N-PX with frequency (say-on-pay) votes (2 votes)":
/// `0002053459-26-000004`, block 2979918 (2026-08-28), filing 4.
fn frequency_vote_fixture() -> Fixture {
    let granite = |description, how| sec::ProxyVote {
        isin: s("US3874321074"),
        ..vote(
            "Granite Ridge Resources, Inc.",
            "387432107",
            "05/22/2026",
            description,
            "55265968",
            &[(how, "55265968", "FOR")],
        )
    };
    let office = || address("5217 MCKINNEY AVENUE", "SUITE 400", "DALLAS", "TX", "75205");
    let npx = sec::NpxReport {
        filer_cik: s("0002053459"),
        cover_page: Some(sec::NpxCoverPage {
            year_or_quarter: s("YEAR"),
            reporting_person_name: s("Grey Rock Energy Management, LLC"),
            reporting_person_address: Some(office()),
            report_type: s("INSTITUTIONAL MANAGER VOTING REPORT"),
            file_number: s("028-24880"),
            reporting_crd_number: s("17095"),
            reporting_sec_file_number: s("801-110553"),
            confidential_treatment: s("N"),
            explanatory_choice: s("N"),
            other_included_managers_count: s("0"),
            reporting_person_phone: s("214-797-5595"),
            agent_for_service_name: s("Emily Fuquay"),
            agent_for_service_address: Some(office()),
            signatures: vec![sec::NpxSignature {
                reporting_person: s("Grey Rock Energy Management, LLC"),
                signature: s("/s/ Emily Fuquay"),
                printed_signature: s("/s/ Emily Fuquay"),
                title: s("General Counsel and Chief Compliance Officer"),
                date: s("08/28/2026"),
            }],
            ..npx_cover("IM", "2026")
        }),
        votes: vec![
            granite("14A Executive Compensation", "FOR"),
            granite("14A Executive Compensation Vote\nFrequency", "1 YEAR"),
        ],
    };
    Fixture {
        day: "2026-08-28",
        block: 2_979_918,
        filing: sample_filing(
            2_979_918,
            "0002053459-26-000004",
            "N-PX",
            "2026-08-28",
            28,
            Body::Npx(npx),
        ),
    }
}

/// §8.7 "N-PX notice report (no votes)": `0000897423-26-000035`, block
/// 2956131 (2026-03-16), filing 19.
fn notice_report_fixture() -> Fixture {
    let office = || address("201 MAIN STREET", "SUITE 3100", "FORT WORTH", "TX", "76102");
    let npx = sec::NpxReport {
        filer_cik: s("0001056907"),
        cover_page: Some(sec::NpxCoverPage {
            year_or_quarter: s("YEAR"),
            reporting_person_name: s("GENRE PARTNERS L P"),
            reporting_person_address: Some(office()),
            report_type: s("INSTITUTIONAL MANAGER NOTICE REPORT"),
            file_number: s("028-07038"),
            confidential_treatment: s("N"),
            notice_explanation: s("REPORTING PERSON DID NOT EXERCISE VOTING"),
            explanatory_choice: s("Y"),
            explanatory_notes: s(NOTICE_EXPLANATORY_NOTES),
            reporting_person_phone: s("817-390-8400"),
            signatures: vec![sec::NpxSignature {
                reporting_person: s("GENRE PARTNERS L P"),
                signature: s("Thomas R. Hegi"),
                printed_signature: s("Thomas R. Hegi"),
                title: s("Attorney-in-Fact for Robert M. Bass, General Partner (1)"),
                date: s("03/16/2026"),
            }],
            ..npx_cover("IM", "2024")
        }),
        votes: Vec::new(),
    };
    Fixture {
        day: "2026-03-16",
        block: 2_956_131,
        filing: sample_filing(
            2_956_131,
            "0000897423-26-000035",
            "N-PX",
            "2026-03-16",
            561,
            Body::Npx(npx),
        ),
    }
}

const NOTICE_EXPLANATORY_NOTES: &str = "(1) A power of attorney authorizing Thomas R. Hegi to act \
     on behalf of Robert M. Bass, general partner of GenRe Partners, L.P., has been filed with the \
     Commission.";

/// §8.7 "N-CEN/A with `previous_accession_number`": `0000910472-26-004047`,
/// block 2956117 (2026-03-16), filing 0.
fn ncen_amendment_fixture() -> Fixture {
    let ncen = sec::NcenReport {
        filer_cik: s("0000912900"),
        investment_company_type: s("N-1A"),
        report_ending_period: s("2025-12-31"),
        registrant: Some(sec::NcenRegistrant {
            full_name: s("Praxis Funds"),
            file_number: s("811-08056"),
            cik: s("0000912900"),
            lei: s("5493000A2TZD2B7R2O62"),
            address: Some(sec::Address {
                country: s("US"),
                ..address("1110 N. Main Street", "", "Goshen", "US-IN", "46528-2638")
            }),
            phone: s("800-977-2947"),
        }),
        previous_accession_number: s("0000910472-26-004038"),
        ..Default::default()
    };
    Fixture {
        day: "2026-03-16",
        block: 2_956_117,
        filing: sample_filing(
            2_956_117,
            "0000910472-26-004047",
            "N-CEN/A",
            "2026-03-16",
            1,
            Body::Ncen(ncen),
        ),
    }
}

// ---------------------------------------------------------------------------
// Assertions
// ---------------------------------------------------------------------------

/// The 7 canonical and 5 [FC] cells of a row of `filing` in window `block`
/// (filing 0 of its block).
fn id_fc(block: u64, filing: &sec::Filing) -> Vec<(String, String)> {
    let window = window_seconds(block) * 1000;
    let acceptance = filing
        .acceptance_datetime
        .as_ref()
        .map_or_else(|| s("NULL"), |t| utc_millis_text(t.seconds * 1000));
    let day = crate::sec::parse::iso_date(window_seconds(block).div_euclid(86_400) as i32)
        .unwrap_or_default();
    [
        ("block_num", block.to_string()),
        ("block_id", block.to_string()),
        ("parent_num", (block - 1).to_string()),
        ("parent_id", (block - 1).to_string()),
        ("lib_num", (block - 1).to_string()),
        ("timestamp", utc_millis_text(window)),
        ("date", day),
        ("filing_index", s("0")),
        ("accession_number", filing.accession_number.clone()),
        ("form_type", filing.form_type.clone()),
        ("filing_date", filing.filing_date.clone()),
        ("acceptance_datetime", acceptance),
    ]
    .into_iter()
    .map(|(column, value)| (s(column), value))
    .collect()
}

/// Assert one whole row: the [ID] and [FC] cells of `fixture`, then `columns`
/// (every remaining column, in schema order).
fn assert_full_row(
    batches: &Batches,
    table: &str,
    row: usize,
    fixture: &Fixture,
    columns: &[(&str, &str)],
) {
    let mut expected = id_fc(fixture.block, &fixture.filing);
    expected.extend(columns.iter().map(|(c, v)| (s(c), s(v))));
    assert_eq!(batches.row(table, row), expected, "{table} row {row}");
}

/// Assert whole columns: `(column, every value in row order)`.
fn assert_columns(batches: &Batches, table: &str, columns: &[(&str, &[&str])]) {
    for (column, values) in columns {
        assert_eq!(&batches.column(table, column), values, "{table}.{column}");
    }
}

const CATEGORIES: &str = "[SECTION 14A SAY-ON-PAY VOTES]";

// ---------------------------------------------------------------------------
// N-PX
// ---------------------------------------------------------------------------

#[test]
fn split_vote_report_and_summary_managers() {
    let fixture = split_vote_fixture();
    let b = fixture.map();
    b.assert_rows(&[
        ("npx_reports", 1),
        ("npx_other_managers", 3),
        ("npx_votes", 4),
        ("npx_vote_records", 12),
        ("ncen_reports", 0),
    ]);
    assert_full_row(
        &b,
        "npx_reports",
        0,
        &fixture,
        &[
            ("filer_cik", "0001554913"),
            ("has_cover_page", "true"),
            ("registrant_type", "IM"),
            ("investment_company_type", "NULL"),
            ("year_or_quarter", "YEAR"),
            ("report_calendar_year", "2026"),
            ("report_type", "INSTITUTIONAL MANAGER VOTING REPORT"),
            ("reporting_person_name", "Pamplona Capital Management, LLC"),
            (
                "reporting_person_street1",
                "1330 AVENUE OF THE AMERICAS, 24TH FLOOR",
            ),
            ("reporting_person_street2", "NULL"),
            ("reporting_person_city", "NEW YORK"),
            ("reporting_person_state", "NY"),
            ("reporting_person_zip_code", "10019"),
            ("reporting_person_state_description", "NULL"),
            ("reporting_person_country", "NULL"),
            ("reporting_person_non_us_state_territory", "NULL"),
            ("reporting_person_phone", "212-207-6820"),
            ("file_number", "028-15480"),
            ("reporting_crd_number", "NULL"),
            ("reporting_sec_file_number", "NULL"),
            ("lei_number", "NULL"),
            ("confidential_treatment", "false"),
            ("notice_explanation", "NULL"),
            ("explanatory_choice", "false"),
            ("explanatory_notes", "NULL"),
            ("cover_is_amendment", "NULL"),
            ("amendment_number", "NULL"),
            ("amendment_type", "NULL"),
            ("conf_denied_expired", "NULL"),
            ("agent_for_service_name", "NULL"),
            ("agent_for_service_street1", "NULL"),
            ("agent_for_service_street2", "NULL"),
            ("agent_for_service_city", "NULL"),
            ("agent_for_service_state", "NULL"),
            ("agent_for_service_zip_code", "NULL"),
            ("agent_for_service_state_description", "NULL"),
            ("agent_for_service_country", "NULL"),
            ("agent_for_service_non_us_state_territory", "NULL"),
            ("series_reports", "[]"),
            ("other_included_managers_count", "3"),
            ("declared_series_count", "NULL"),
            ("vote_count", "4"),
            ("vote_record_count", "12"),
            ("other_manager_count", "3"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_full_row(
        &b,
        "npx_other_managers",
        1,
        &fixture,
        &[
            ("filer_cik", "0001554913"),
            ("other_manager_index", "1"),
            ("list_kind", "summary"),
            ("serial_number", "2"),
            ("other_manager_name", "Halsted John C."),
            ("form13f_file_number", "028-24958"),
            ("crd_number", "NULL"),
            ("sec_file_number", "NULL"),
            ("lei", "NULL"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_columns(
        &b,
        "npx_other_managers",
        &[
            ("other_manager_index", &["0", "1", "2"]),
            ("list_kind", &["summary"; 3]),
            ("serial_number", &["1", "2", "3"]),
            (
                "other_manager_name",
                &[
                    "Pamplona PE Investments Malta Ltd",
                    "Halsted John C.",
                    "Knaster Alexander M",
                ],
            ),
            (
                "form13f_file_number",
                &["028-24956", "028-24958", "028-24957"],
            ),
        ],
    );
    assert!(b.issues().is_empty());
}

#[test]
fn split_vote_votes_and_records() {
    let fixture = split_vote_fixture();
    let b = fixture.map();
    assert_full_row(
        &b,
        "npx_votes",
        1,
        &fixture,
        &[
            ("filer_cik", "0001554913"),
            ("registrant_type", "IM"),
            ("report_calendar_year", "2026"),
            ("vote_index", "1"),
            ("issuer_name", "BED BATH & BEYOND INC"),
            ("cusip", "690370101"),
            ("cusip_norm", "690370101"),
            ("isin", "NULL"),
            ("figi", "NULL"),
            ("meeting_date", "2026-05-14"),
            (
                "vote_description",
                "The approval, on an advisory (non-binding) basis, of the compensation paid by \
                 the Company to its named executive officers",
            ),
            ("other_vote_description", "NULL"),
            ("vote_categories", CATEGORIES),
            ("vote_source", "ISSUER"),
            ("vote_series", "NULL"),
            ("shares_voted", "330655.0000000000000000"),
            ("shares_on_loan", "0.0000000000000000"),
            ("vote_other_managers", "[1, 2, 3]"),
            ("vote_other_info", "NULL"),
            ("record_count", "3"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_columns(
        &b,
        "npx_votes",
        &[
            ("vote_index", &["0", "1", "2", "3"]),
            (
                "issuer_name",
                &[
                    "AMAZON COM INC",
                    "BED BATH & BEYOND INC",
                    "ELASTIC N V",
                    "MICROSOFT CORP",
                ],
            ),
            (
                "cusip",
                &["23135106", "690370101", "N14506104", "594918104"],
            ),
            // An 8-character CUSIP is not padded: NULL (§4.4).
            (
                "cusip_norm",
                &["NULL", "690370101", "N14506104", "594918104"],
            ),
            (
                "meeting_date",
                &["2026-05-20", "2026-05-14", "2025-09-30", "2025-12-05"],
            ),
            (
                "shares_voted",
                &[
                    "0.0000000000000000",
                    "330655.0000000000000000",
                    "210600.0000000000000000",
                    "0.0000000000000000",
                ],
            ),
            ("vote_other_managers", &["[1, 2, 3]"; 4]),
            ("record_count", &["3"; 4]),
            ("has_parse_issues", &["false"; 4]),
        ],
    );

    assert_full_row(
        &b,
        "npx_vote_records",
        5,
        &fixture,
        &[
            ("filer_cik", "0001554913"),
            ("registrant_type", "IM"),
            ("report_calendar_year", "2026"),
            ("vote_index", "1"),
            ("vote_series", "NULL"),
            ("meeting_date", "2026-05-14"),
            ("cusip_norm", "690370101"),
            ("vote_source", "ISSUER"),
            ("vote_categories", CATEGORIES),
            ("record_index", "2"),
            ("how_voted", "FOR"),
            ("shares_voted", "330655.0000000000000000"),
            ("management_recommendation", "FOR"),
            ("has_parse_issues", "false"),
        ],
    );
    let per_vote = |values: [&'static str; 4]| -> Vec<&'static str> {
        values.iter().flat_map(|v| [*v; 3]).collect()
    };
    let vote_index = per_vote(["0", "1", "2", "3"]);
    let cusip_norm = per_vote(["NULL", "690370101", "N14506104", "594918104"]);
    let meeting_date = per_vote(["2026-05-20", "2026-05-14", "2025-09-30", "2025-12-05"]);
    let how_voted = per_vote(["TAKE NO ACTION", "FOR", "FOR", "TAKE NO ACTION"]);
    let shares = per_vote([
        "0.0000000000000000",
        "330655.0000000000000000",
        "210600.0000000000000000",
        "0.0000000000000000",
    ]);
    let recommendation = per_vote(["NONE", "FOR", "FOR", "NONE"]);
    let record_index: Vec<&str> = (0..4).flat_map(|_| ["0", "1", "2"]).collect();
    assert_columns(
        &b,
        "npx_vote_records",
        &[
            ("vote_index", &vote_index),
            ("record_index", &record_index),
            ("cusip_norm", &cusip_norm),
            ("meeting_date", &meeting_date),
            ("how_voted", &how_voted),
            ("shares_voted", &shares),
            ("management_recommendation", &recommendation),
            ("has_parse_issues", &["false"; 12]),
        ],
    );
}

#[test]
fn frequency_votes_with_agent_for_service() {
    let fixture = frequency_vote_fixture();
    let b = fixture.map();
    b.assert_rows(&[
        ("npx_reports", 1),
        ("npx_other_managers", 0),
        ("npx_votes", 2),
        ("npx_vote_records", 2),
    ]);
    assert_full_row(
        &b,
        "npx_reports",
        0,
        &fixture,
        &[
            ("filer_cik", "0002053459"),
            ("has_cover_page", "true"),
            ("registrant_type", "IM"),
            ("investment_company_type", "NULL"),
            ("year_or_quarter", "YEAR"),
            ("report_calendar_year", "2026"),
            ("report_type", "INSTITUTIONAL MANAGER VOTING REPORT"),
            ("reporting_person_name", "Grey Rock Energy Management, LLC"),
            ("reporting_person_street1", "5217 MCKINNEY AVENUE"),
            ("reporting_person_street2", "SUITE 400"),
            ("reporting_person_city", "DALLAS"),
            ("reporting_person_state", "TX"),
            ("reporting_person_zip_code", "75205"),
            ("reporting_person_state_description", "NULL"),
            ("reporting_person_country", "NULL"),
            ("reporting_person_non_us_state_territory", "NULL"),
            ("reporting_person_phone", "214-797-5595"),
            ("file_number", "028-24880"),
            ("reporting_crd_number", "17095"),
            ("reporting_sec_file_number", "801-110553"),
            ("lei_number", "NULL"),
            ("confidential_treatment", "false"),
            ("notice_explanation", "NULL"),
            ("explanatory_choice", "false"),
            ("explanatory_notes", "NULL"),
            ("cover_is_amendment", "NULL"),
            ("amendment_number", "NULL"),
            ("amendment_type", "NULL"),
            ("conf_denied_expired", "NULL"),
            ("agent_for_service_name", "Emily Fuquay"),
            ("agent_for_service_street1", "5217 MCKINNEY AVENUE"),
            ("agent_for_service_street2", "SUITE 400"),
            ("agent_for_service_city", "DALLAS"),
            ("agent_for_service_state", "TX"),
            ("agent_for_service_zip_code", "75205"),
            ("agent_for_service_state_description", "NULL"),
            ("agent_for_service_country", "NULL"),
            ("agent_for_service_non_us_state_territory", "NULL"),
            ("series_reports", "[]"),
            ("other_included_managers_count", "0"),
            ("declared_series_count", "NULL"),
            ("vote_count", "2"),
            ("vote_record_count", "2"),
            ("other_manager_count", "0"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_full_row(
        &b,
        "npx_votes",
        1,
        &fixture,
        &[
            ("filer_cik", "0002053459"),
            ("registrant_type", "IM"),
            ("report_calendar_year", "2026"),
            ("vote_index", "1"),
            ("issuer_name", "Granite Ridge Resources, Inc."),
            ("cusip", "387432107"),
            ("cusip_norm", "387432107"),
            ("isin", "US3874321074"),
            ("figi", "NULL"),
            ("meeting_date", "2026-05-22"),
            (
                "vote_description",
                "14A Executive Compensation Vote\nFrequency",
            ),
            ("other_vote_description", "NULL"),
            ("vote_categories", CATEGORIES),
            ("vote_source", "ISSUER"),
            ("vote_series", "NULL"),
            ("shares_voted", "55265968.0000000000000000"),
            ("shares_on_loan", "0.0000000000000000"),
            ("vote_other_managers", "[]"),
            ("vote_other_info", "NULL"),
            ("record_count", "1"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_full_row(
        &b,
        "npx_vote_records",
        1,
        &fixture,
        &[
            ("filer_cik", "0002053459"),
            ("registrant_type", "IM"),
            ("report_calendar_year", "2026"),
            ("vote_index", "1"),
            ("vote_series", "NULL"),
            ("meeting_date", "2026-05-22"),
            ("cusip_norm", "387432107"),
            ("vote_source", "ISSUER"),
            ("vote_categories", CATEGORIES),
            ("record_index", "0"),
            ("how_voted", "1 YEAR"),
            ("shares_voted", "55265968.0000000000000000"),
            ("management_recommendation", "FOR"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_columns(&b, "npx_vote_records", &[("how_voted", &["FOR", "1 YEAR"])]);
}

#[test]
fn notice_report_has_no_votes() {
    let fixture = notice_report_fixture();
    let b = fixture.map();
    b.assert_rows(&[
        ("npx_reports", 1),
        ("npx_other_managers", 0),
        ("npx_votes", 0),
        ("npx_vote_records", 0),
    ]);
    assert_full_row(
        &b,
        "npx_reports",
        0,
        &fixture,
        &[
            ("filer_cik", "0001056907"),
            ("has_cover_page", "true"),
            ("registrant_type", "IM"),
            ("investment_company_type", "NULL"),
            ("year_or_quarter", "YEAR"),
            ("report_calendar_year", "2024"),
            ("report_type", "INSTITUTIONAL MANAGER NOTICE REPORT"),
            ("reporting_person_name", "GENRE PARTNERS L P"),
            ("reporting_person_street1", "201 MAIN STREET"),
            ("reporting_person_street2", "SUITE 3100"),
            ("reporting_person_city", "FORT WORTH"),
            ("reporting_person_state", "TX"),
            ("reporting_person_zip_code", "76102"),
            ("reporting_person_state_description", "NULL"),
            ("reporting_person_country", "NULL"),
            ("reporting_person_non_us_state_territory", "NULL"),
            ("reporting_person_phone", "817-390-8400"),
            ("file_number", "028-07038"),
            ("reporting_crd_number", "NULL"),
            ("reporting_sec_file_number", "NULL"),
            ("lei_number", "NULL"),
            ("confidential_treatment", "false"),
            (
                "notice_explanation",
                "REPORTING PERSON DID NOT EXERCISE VOTING",
            ),
            ("explanatory_choice", "true"),
            ("explanatory_notes", NOTICE_EXPLANATORY_NOTES),
            ("cover_is_amendment", "NULL"),
            ("amendment_number", "NULL"),
            ("amendment_type", "NULL"),
            ("conf_denied_expired", "NULL"),
            ("agent_for_service_name", "NULL"),
            ("agent_for_service_street1", "NULL"),
            ("agent_for_service_street2", "NULL"),
            ("agent_for_service_city", "NULL"),
            ("agent_for_service_state", "NULL"),
            ("agent_for_service_zip_code", "NULL"),
            ("agent_for_service_state_description", "NULL"),
            ("agent_for_service_country", "NULL"),
            ("agent_for_service_non_us_state_territory", "NULL"),
            ("series_reports", "[]"),
            ("other_included_managers_count", "NULL"),
            ("declared_series_count", "NULL"),
            ("vote_count", "0"),
            ("vote_record_count", "0"),
            ("other_manager_count", "0"),
            ("has_parse_issues", "false"),
        ],
    );
}

/// A fund report: every cover-page column filled, cover then summary managers
/// with every member, the series list with a NULL member, and the three bool
/// kinds of §4.1 (Y/N text, optional bool, presence).
#[test]
fn fund_report_cover_page_and_bool_kinds() {
    let cover = sec::NpxCoverPage {
        year_or_quarter: s("YEAR"),
        investment_company_type: s("N-1A"),
        reporting_person_name: s("VANGUARD INDEX FUNDS"),
        reporting_person_address: Some(sec::Address {
            state_description: s("PENNSYLVANIA"),
            country: s("US"),
            non_us_state_territory: s("NONE"),
            ..address("PO BOX 2600", "V26", "VALLEY FORGE", "PA", "19482")
        }),
        report_type: s("FUND VOTING REPORT"),
        file_number: s("811-02652"),
        reporting_crd_number: s("000105958"),
        reporting_sec_file_number: s("801-11953"),
        lei_number: s("549300ZBPVQ1FQ6TM470"),
        // Y/N text is trimmed and upper-cased (§4.1).
        confidential_treatment: s(" yes "),
        explanatory_choice: s(""),
        is_amendment: Some(false),
        amendment_number: s("1"),
        amendment_type: s("RESTATEMENT"),
        conf_denied_expired: Some(true),
        series_reports: vec![
            sec::NpxSeriesReport {
                series_id: s("S000002839"),
                name: s("Vanguard 500 Index Fund"),
                lei: s("549300D4ZM4G2BJ5UJ40"),
            },
            sec::NpxSeriesReport {
                series_id: s("S000002840"),
                name: s("Vanguard Extended Market Index Fund"),
                lei: String::new(),
            },
        ],
        other_managers: vec![sec::NpxManager {
            crd_number: s("000105958"),
            sec_file_number: s("801-11953"),
            lei: s("5493002789CX1XHSOM79"),
            ..manager("", "VANGUARD GROUP INC", "028-06408")
        }],
        summary_managers: vec![
            manager("1", "WELLINGTON MANAGEMENT CO LLP", "028-04557"),
            manager("2", "", ""),
        ],
        other_included_managers_count: s("2"),
        series_count: s("2"),
        reporting_person_phone: s("610-669-1000"),
        agent_for_service_name: s("ANNE E. ROBINSON"),
        agent_for_service_address: Some(address("100 VANGUARD BLVD", "", "MALVERN", "PA", "19355")),
        ..npx_cover("RMIC", "2026")
    };
    let report = sec::NpxReport {
        filer_cik: s("0000036405"),
        cover_page: Some(cover),
        votes: Vec::new(),
    };
    let fixture = Fixture {
        day: "",
        block: BLOCK_NUM,
        filing: filing("N-PX/A", Body::Npx(report)),
    };
    let b = fixture.map();
    assert_full_row(
        &b,
        "npx_reports",
        0,
        &fixture,
        &[
            ("filer_cik", "0000036405"),
            ("has_cover_page", "true"),
            ("registrant_type", "RMIC"),
            ("investment_company_type", "N-1A"),
            ("year_or_quarter", "YEAR"),
            ("report_calendar_year", "2026"),
            ("report_type", "FUND VOTING REPORT"),
            ("reporting_person_name", "VANGUARD INDEX FUNDS"),
            ("reporting_person_street1", "PO BOX 2600"),
            ("reporting_person_street2", "V26"),
            ("reporting_person_city", "VALLEY FORGE"),
            ("reporting_person_state", "PA"),
            ("reporting_person_zip_code", "19482"),
            ("reporting_person_state_description", "PENNSYLVANIA"),
            ("reporting_person_country", "US"),
            ("reporting_person_non_us_state_territory", "NONE"),
            ("reporting_person_phone", "610-669-1000"),
            ("file_number", "811-02652"),
            ("reporting_crd_number", "000105958"),
            ("reporting_sec_file_number", "801-11953"),
            ("lei_number", "549300ZBPVQ1FQ6TM470"),
            ("confidential_treatment", "true"),
            ("notice_explanation", "NULL"),
            ("explanatory_choice", "NULL"),
            ("explanatory_notes", "NULL"),
            ("cover_is_amendment", "false"),
            ("amendment_number", "1"),
            ("amendment_type", "RESTATEMENT"),
            ("conf_denied_expired", "true"),
            ("agent_for_service_name", "ANNE E. ROBINSON"),
            ("agent_for_service_street1", "100 VANGUARD BLVD"),
            ("agent_for_service_street2", "NULL"),
            ("agent_for_service_city", "MALVERN"),
            ("agent_for_service_state", "PA"),
            ("agent_for_service_zip_code", "19355"),
            ("agent_for_service_state_description", "NULL"),
            ("agent_for_service_country", "NULL"),
            ("agent_for_service_non_us_state_territory", "NULL"),
            (
                "series_reports",
                "[{series_id: S000002839, series_name: Vanguard 500 Index Fund, series_lei: \
                 549300D4ZM4G2BJ5UJ40}, {series_id: S000002840, series_name: Vanguard Extended \
                 Market Index Fund, series_lei: NULL}]",
            ),
            ("other_included_managers_count", "2"),
            ("declared_series_count", "2"),
            ("vote_count", "0"),
            ("vote_record_count", "0"),
            ("other_manager_count", "3"),
            ("has_parse_issues", "false"),
        ],
    );
    // Cover-page managers first, then summary-page managers (§8.2).
    assert_full_row(
        &b,
        "npx_other_managers",
        0,
        &fixture,
        &[
            ("filer_cik", "0000036405"),
            ("other_manager_index", "0"),
            ("list_kind", "cover"),
            ("serial_number", "NULL"),
            ("other_manager_name", "VANGUARD GROUP INC"),
            ("form13f_file_number", "028-06408"),
            ("crd_number", "000105958"),
            ("sec_file_number", "801-11953"),
            ("lei", "5493002789CX1XHSOM79"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_columns(
        &b,
        "npx_other_managers",
        &[
            ("other_manager_index", &["0", "1", "2"]),
            ("list_kind", &["cover", "summary", "summary"]),
            ("serial_number", &["NULL", "1", "2"]),
            (
                "other_manager_name",
                &["VANGUARD GROUP INC", "WELLINGTON MANAGEMENT CO LLP", "NULL"],
            ),
            ("form13f_file_number", &["028-06408", "028-04557", "NULL"]),
            ("crd_number", &["000105958", "NULL", "NULL"]),
            ("has_parse_issues", &["false"; 3]),
        ],
    );
    assert!(b.issues().is_empty());
}

/// Without a cover page every cover column is NULL, the series list is [],
/// there are no managers, and the votes copy NULL report keys.
#[test]
fn report_without_cover_page() {
    let report = sec::NpxReport {
        filer_cik: s("0000000002"),
        cover_page: None,
        votes: vec![vote(
            "ACME CORP",
            "000360206",
            "2026-06-30",
            "ELECT DIRECTORS",
            "",
            &[("FOR", "", "")],
        )],
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![filing("N-PX", Body::Npx(report))],
    )]);
    b.assert_rows(&[
        ("npx_reports", 1),
        ("npx_other_managers", 0),
        ("npx_votes", 1),
        ("npx_vote_records", 1),
    ]);
    let row = b.row("npx_reports", 0);
    for (column, value) in &row[12..] {
        let expected = match column.as_str() {
            "filer_cik" => "0000000002",
            "has_cover_page" | "has_parse_issues" => "false",
            "series_reports" => "[]",
            "vote_count" | "vote_record_count" => "1",
            "other_manager_count" => "0",
            _ => "NULL",
        };
        assert_eq!(value, expected, "npx_reports.{column}");
    }
    assert_eq!(row.len(), 12 + 45, "npx_reports has 45 own columns");
    for table in ["npx_votes", "npx_vote_records"] {
        assert_eq!(b.cell(table, "filer_cik", 0), "0000000002");
        assert_eq!(b.cell(table, "registrant_type", 0), "NULL");
        assert_eq!(b.cell(table, "report_calendar_year", 0), "NULL");
        assert_eq!(b.cell(table, "shares_voted", 0), "NULL");
    }
    assert_eq!(b.cell("npx_votes", "meeting_date", 0), "2026-06-30");
    assert_eq!(
        b.cell("npx_vote_records", "management_recommendation", 0),
        "NULL"
    );
}

/// The flat per-record and per-item preflight vectors stay aligned with
/// their votes when the record and item counts vary, including empty ones;
/// the records copy their own vote's keys.
#[test]
fn records_and_items_stay_with_their_vote() {
    let votes = vec![
        sec::ProxyVote {
            vote_other_managers: vec![s("1")],
            vote_series: s("S000000001"),
            ..vote(" n14506104 ", " n14506104 ", "1/2/2026", "A", "10", &[])
        },
        sec::ProxyVote {
            vote_source: s("SECURITY HOLDER"),
            vote_categories: vec![s("ENVIRONMENT OR CLIMATE"), s("OTHER SOCIAL ISSUES")],
            figi: s("BBG000BPH459"),
            other_vote_description: s("Report on climate lobbying"),
            vote_other_info: s("Shareholder proposal"),
            vote_series: s("S000000002"),
            ..vote(
                "MICROSOFT CORP",
                "594918104",
                "12/05/2025",
                "B",
                "387225.35800000001",
                &[
                    ("AGAINST", "387225.358", "AGAINST"),
                    ("FOR", "0.00000000000000001", "AGAINST"),
                ],
            )
        },
        sec::ProxyVote {
            vote_other_managers: vec![s("2"), s("03"), s("4.0")],
            vote_categories: Vec::new(),
            ..vote(
                "PLACEHOLDER INC",
                "000000000",
                "",
                "C",
                "-5",
                &[("ABSTAIN", "5", "NONE")],
            )
        },
    ];
    let report = sec::NpxReport {
        filer_cik: s("0000000003"),
        cover_page: Some(npx_cover("RMIC", "2026")),
        votes,
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![filing("N-PX", Body::Npx(report))],
    )]);
    assert_eq!(b.cell("npx_reports", "vote_count", 0), "3");
    assert_eq!(b.cell("npx_reports", "vote_record_count", 0), "3");
    assert_columns(
        &b,
        "npx_votes",
        &[
            ("vote_index", &["0", "1", "2"]),
            (
                "issuer_name",
                &[" n14506104 ", "MICROSOFT CORP", "PLACEHOLDER INC"],
            ),
            ("cusip", &[" n14506104 ", "594918104", "000000000"]),
            // White space removed and upper-cased; placeholders → NULL (§4.4).
            ("cusip_norm", &["N14506104", "594918104", "NULL"]),
            ("isin", &["NULL", "NULL", "NULL"]),
            ("figi", &["NULL", "BBG000BPH459", "NULL"]),
            ("meeting_date", &["2026-01-02", "2025-12-05", "NULL"]),
            ("vote_description", &["A", "B", "C"]),
            (
                "other_vote_description",
                &["NULL", "Report on climate lobbying", "NULL"],
            ),
            (
                "vote_categories",
                &[
                    CATEGORIES,
                    "[ENVIRONMENT OR CLIMATE, OTHER SOCIAL ISSUES]",
                    "[]",
                ],
            ),
            ("vote_source", &["ISSUER", "SECURITY HOLDER", "ISSUER"]),
            ("vote_series", &["S000000001", "S000000002", "NULL"]),
            // S16 keeps the 17 significant digits exactly (§8.5).
            (
                "shares_voted",
                &[
                    "10.0000000000000000",
                    "387225.3580000000100000",
                    "-5.0000000000000000",
                ],
            ),
            ("vote_other_managers", &["[1]", "[]", "[2, 3, 4]"]),
            ("vote_other_info", &["NULL", "Shareholder proposal", "NULL"]),
            ("record_count", &["0", "2", "1"]),
            ("has_parse_issues", &["false", "false", "false"]),
        ],
    );
    assert_columns(
        &b,
        "npx_vote_records",
        &[
            ("vote_index", &["1", "1", "2"]),
            ("record_index", &["0", "1", "0"]),
            ("vote_series", &["S000000002", "S000000002", "NULL"]),
            ("meeting_date", &["2025-12-05", "2025-12-05", "NULL"]),
            ("cusip_norm", &["594918104", "594918104", "NULL"]),
            (
                "vote_source",
                &["SECURITY HOLDER", "SECURITY HOLDER", "ISSUER"],
            ),
            (
                "vote_categories",
                &[
                    "[ENVIRONMENT OR CLIMATE, OTHER SOCIAL ISSUES]",
                    "[ENVIRONMENT OR CLIMATE, OTHER SOCIAL ISSUES]",
                    "[]",
                ],
            ),
            ("how_voted", &["AGAINST", "FOR", "ABSTAIN"]),
            (
                "shares_voted",
                &[
                    "387225.3580000000000000",
                    "0.0000000000000000",
                    "5.0000000000000000",
                ],
            ),
            ("management_recommendation", &["AGAINST", "AGAINST", "NONE"]),
            ("has_parse_issues", &["false", "true", "false"]),
        ],
    );
    // 1e-17 has a non-zero digit beyond scale 16: rounded (to 0), logged.
    let issues = b.issues();
    assert_eq!(issues.len(), 1);
    assert_eq!(
        (
            issues[0].table.as_str(),
            issues[0].column.as_str(),
            issues[0].index,
            issues[0].raw.as_str(),
            issues[0].issue.as_str()
        ),
        (
            "npx_vote_records",
            "shares_voted",
            [Some(1), Some(1), None],
            "0.00000000000000001",
            "rounded"
        )
    );
}

/// Every typed N-PX column logs its issues with the §4.6 key positions, in
/// mapping order (report, managers, then each vote and its records), and
/// flags exactly its own row.
#[test]
fn npx_parse_issues_are_keyed_and_flag_their_rows() {
    let cover = sec::NpxCoverPage {
        confidential_treatment: s("maybe"),
        explanatory_choice: s("N"),
        amendment_number: s("N/A"),
        other_included_managers_count: s("99999999999"),
        series_count: s("1.5"),
        other_managers: vec![manager("x1", "A", "")],
        summary_managers: vec![manager("1", "B", ""), manager(" 2 ", "C", "")],
        ..npx_cover("IM", "2026a")
    };
    let votes = vec![
        sec::ProxyVote {
            vote_other_managers: vec![s("1"), s("x"), s(""), s("3.0"), s("3000000000")],
            shares_on_loan: s("N/A"),
            ..vote(
                "A",
                "",
                "2026-02-30",
                "",
                "1.00000000000000005",
                &[("FOR", "1", ""), ("FOR", "abc", "")],
            )
        },
        vote("B", "", "6/30/2026", "", "1", &[("FOR", "-", "")]),
        vote("C", "", "6/30/2026", "", "2", &[("FOR", "2", "")]),
    ];
    let report = sec::NpxReport {
        filer_cik: s("0000000004"),
        cover_page: Some(cover),
        votes,
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![
            filing("N-CEN", Body::Ncen(sec::NcenReport::default())),
            filing("N-PX", Body::Npx(report)),
        ],
    )]);

    let issues: Vec<_> = b
        .issues()
        .into_iter()
        .map(|i| {
            assert_eq!(i.filing_index, Some(1));
            assert_eq!(i.block_num, BLOCK_NUM);
            (i.table, i.column, i.index, i.raw, i.issue)
        })
        .collect();
    let expected = [
        (
            "npx_reports",
            "report_calendar_year",
            [None, None, None],
            "2026a",
            "unparseable",
        ),
        (
            "npx_reports",
            "confidential_treatment",
            [None, None, None],
            "maybe",
            "unparseable",
        ),
        (
            "npx_reports",
            "amendment_number",
            [None, None, None],
            "N/A",
            "sentinel",
        ),
        (
            "npx_reports",
            "other_included_managers_count",
            [None, None, None],
            "99999999999",
            "out_of_range",
        ),
        (
            "npx_reports",
            "declared_series_count",
            [None, None, None],
            "1.5",
            "unparseable",
        ),
        (
            "npx_other_managers",
            "serial_number",
            [Some(0), None, None],
            "x1",
            "unparseable",
        ),
        (
            "npx_votes",
            "meeting_date",
            [Some(0), None, None],
            "2026-02-30",
            "out_of_range",
        ),
        (
            "npx_votes",
            "shares_voted",
            [Some(0), None, None],
            "1.00000000000000005",
            "rounded",
        ),
        (
            "npx_votes",
            "shares_on_loan",
            [Some(0), None, None],
            "N/A",
            "sentinel",
        ),
        (
            "npx_votes",
            "vote_other_managers",
            [Some(0), Some(1), None],
            "x",
            "unparseable",
        ),
        (
            "npx_votes",
            "vote_other_managers",
            [Some(0), Some(4), None],
            "3000000000",
            "out_of_range",
        ),
        (
            "npx_vote_records",
            "shares_voted",
            [Some(0), Some(1), None],
            "abc",
            "unparseable",
        ),
        (
            "npx_vote_records",
            "shares_voted",
            [Some(1), Some(0), None],
            "-",
            "sentinel",
        ),
    ]
    .map(|(t, c, index, raw, issue)| (s(t), s(c), index, s(raw), s(issue)));
    assert_eq!(issues, expected);

    assert_columns(
        &b,
        "npx_reports",
        &[
            ("report_calendar_year", &["NULL"]),
            ("confidential_treatment", &["NULL"]),
            ("explanatory_choice", &["false"]),
            ("amendment_number", &["NULL"]),
            ("other_included_managers_count", &["NULL"]),
            ("declared_series_count", &["NULL"]),
            ("has_parse_issues", &["true"]),
        ],
    );
    assert_columns(
        &b,
        "npx_other_managers",
        &[
            ("list_kind", &["cover", "summary", "summary"]),
            // Integers are trimmed (§4.3).
            ("serial_number", &["NULL", "1", "2"]),
            ("has_parse_issues", &["true", "false", "false"]),
        ],
    );
    assert_columns(
        &b,
        "npx_votes",
        &[
            ("meeting_date", &["NULL", "2026-06-30", "2026-06-30"]),
            (
                "shares_voted",
                &[
                    "1.0000000000000001",
                    "1.0000000000000000",
                    "2.0000000000000000",
                ],
            ),
            (
                "shares_on_loan",
                &["NULL", "0.0000000000000000", "0.0000000000000000"],
            ),
            // '' and unparseable items are NULL; only the latter is logged.
            (
                "vote_other_managers",
                &["[1, NULL, NULL, 3, NULL]", "[]", "[]"],
            ),
            ("has_parse_issues", &["true", "false", "false"]),
        ],
    );
    assert_columns(
        &b,
        "npx_vote_records",
        &[
            ("vote_index", &["0", "0", "1", "2"]),
            ("record_index", &["0", "1", "0", "0"]),
            (
                "shares_voted",
                &["1.0000000000000000", "NULL", "NULL", "2.0000000000000000"],
            ),
            ("has_parse_issues", &["false", "true", "true", "false"]),
            // The copies come from the parent rows, whose issues stay there.
            ("report_calendar_year", &["NULL"; 4]),
            (
                "meeting_date",
                &["NULL", "NULL", "2026-06-30", "2026-06-30"],
            ),
        ],
    );
    assert_columns(&b, "npx_votes", &[("filing_index", &["1", "1", "1"])]);
    assert_eq!(b.cell("filings", "has_parse_issues", 1), "false");
}

/// Several N-PX filings in one block: rows follow `filing_index`, and each
/// child copies its own report's keys.
#[test]
fn rows_follow_filing_index() {
    let report = |cik: &str, year: &str, issuer: &str| sec::NpxReport {
        filer_cik: s(cik),
        cover_page: Some(npx_cover("IM", year)),
        votes: vec![vote(
            issuer,
            "",
            "6/30/2026",
            "",
            "1",
            &[("FOR", "1", "FOR")],
        )],
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![
            filing("N-PX", Body::Npx(report("0000000010", "2025", "FIRST"))),
            filing(
                "N-CEN",
                Body::Ncen(sec::NcenReport {
                    filer_cik: s("0000000011"),
                    ..Default::default()
                }),
            ),
            filing("N-PX/A", Body::Npx(report("0000000012", "2026", "SECOND"))),
        ],
    )]);
    assert_columns(
        &b,
        "npx_reports",
        &[
            ("filing_index", &["0", "2"]),
            ("form_type", &["N-PX", "N-PX/A"]),
            ("filer_cik", &["0000000010", "0000000012"]),
        ],
    );
    for table in ["npx_votes", "npx_vote_records"] {
        assert_columns(
            &b,
            table,
            &[
                ("filing_index", &["0", "2"]),
                ("filer_cik", &["0000000010", "0000000012"]),
                ("report_calendar_year", &["2025", "2026"]),
                ("vote_index", &["0", "0"]),
            ],
        );
    }
    assert_columns(&b, "npx_votes", &[("issuer_name", &["FIRST", "SECOND"])]);
    assert_columns(
        &b,
        "ncen_reports",
        &[("filing_index", &["1"]), ("filer_cik", &["0000000011"])],
    );
}

// ---------------------------------------------------------------------------
// N-CEN
// ---------------------------------------------------------------------------

#[test]
fn ncen_amendment_with_previous_accession() {
    let fixture = ncen_amendment_fixture();
    let b = fixture.map();
    b.assert_rows(&[("ncen_reports", 1), ("npx_reports", 0)]);
    assert_full_row(
        &b,
        "ncen_reports",
        0,
        &fixture,
        &[
            ("filer_cik", "0000912900"),
            ("investment_company_type", "N-1A"),
            ("report_ending_period", "2025-12-31"),
            ("is_report_period_lt12", "false"),
            ("previous_accession_number", "0000910472-26-004038"),
            ("registrant_name", "Praxis Funds"),
            ("registrant_file_number", "811-08056"),
            ("registrant_cik", "0000912900"),
            ("registrant_lei", "5493000A2TZD2B7R2O62"),
            ("registrant_street1", "1110 N. Main Street"),
            ("registrant_street2", "NULL"),
            ("registrant_city", "Goshen"),
            ("registrant_state", "US-IN"),
            ("registrant_zip_code", "46528-2638"),
            ("registrant_state_description", "NULL"),
            ("registrant_country", "US"),
            ("registrant_non_us_state_territory", "NULL"),
            ("registrant_phone", "800-977-2947"),
            ("series_ids", "[]"),
            ("series_count", "0"),
            ("has_parse_issues", "false"),
        ],
    );
    assert!(b.issues().is_empty());
}

/// Series ids, a true `is_report_period_lt12`, an absent registrant (all
/// NULL), and the two date issues of `report_ending_period`.
#[test]
fn ncen_series_registrant_and_date_issues() {
    let ncen = |period: &str, registrant: Option<sec::NcenRegistrant>| sec::NcenReport {
        filer_cik: s("0001064046"),
        investment_company_type: s("N-2"),
        report_ending_period: s(period),
        is_report_period_lt12: true,
        registrant,
        series_ids: vec![s("S000004310"), s(""), s("S000004311")],
        previous_accession_number: String::new(),
    };
    let registrant = sec::NcenRegistrant {
        full_name: s("SELECT SECTOR SPDR TRUST"),
        address: Some(sec::Address {
            non_us_state_territory: s("GB-LND"),
            ..address("", "", "LONDON", "", "")
        }),
        ..Default::default()
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![
            filing("N-CEN", Body::Ncen(ncen("2025-12-31-05:00", None))),
            filing("N-CEN", Body::Ncen(ncen("N/A", Some(registrant)))),
            filing("N-CEN", Body::Ncen(ncen("12/31/2025", None))),
        ],
    )]);
    assert_columns(
        &b,
        "ncen_reports",
        &[
            ("filer_cik", &["0001064046"; 3]),
            ("investment_company_type", &["N-2"; 3]),
            // A zone suffix is dropped and logged, the date kept (§4.2).
            (
                "report_ending_period",
                &["2025-12-31", "NULL", "2025-12-31"],
            ),
            ("is_report_period_lt12", &["true"; 3]),
            ("previous_accession_number", &["NULL"; 3]),
            (
                "registrant_name",
                &["NULL", "SELECT SECTOR SPDR TRUST", "NULL"],
            ),
            ("registrant_file_number", &["NULL"; 3]),
            ("registrant_cik", &["NULL"; 3]),
            ("registrant_lei", &["NULL"; 3]),
            ("registrant_street1", &["NULL"; 3]),
            ("registrant_city", &["NULL", "LONDON", "NULL"]),
            ("registrant_state", &["NULL"; 3]),
            (
                "registrant_non_us_state_territory",
                &["NULL", "GB-LND", "NULL"],
            ),
            ("registrant_phone", &["NULL"; 3]),
            // List items are verbatim: '' stays ''.
            ("series_ids", &["[S000004310, , S000004311]"; 3]),
            ("series_count", &["3"; 3]),
            ("has_parse_issues", &["true", "true", "false"]),
        ],
    );
    let issues: Vec<_> = b
        .issues()
        .into_iter()
        .map(|i| (i.filing_index, i.table, i.column, i.index, i.raw, i.issue))
        .collect();
    assert_eq!(
        issues,
        [
            (Some(0), "2025-12-31-05:00", "tz_dropped"),
            (Some(1), "N/A", "sentinel"),
        ]
        .map(|(filing_index, raw, issue)| (
            filing_index,
            s("ncen_reports"),
            s("report_ending_period"),
            [None, None, None],
            s(raw),
            s(issue)
        ))
    );
}

// ---------------------------------------------------------------------------
// Real data (local only)
// ---------------------------------------------------------------------------

/// The prost mirrors above equal the real §8.7 filings on every column of this
/// group's tables (the real filing mapped alone, as filing 0 of its window).
/// `cargo test -p blocks --lib sec::tests::funds::real_fixtures -- --ignored --nocapture`
#[test]
#[ignore = "local: needs /tmp/sec-fireparq/fire-v013"]
fn real_fixtures_equal_their_mirrors() {
    for fixture in [
        split_vote_fixture(),
        frequency_vote_fixture(),
        notice_report_fixture(),
        ncen_amendment_fixture(),
    ] {
        let accession = &fixture.filing.accession_number;
        let (_, block) = fire::block(fixture.day, fixture.block);
        let real = block
            .filings
            .into_iter()
            .find(|f| &f.accession_number == accession)
            .unwrap_or_else(|| panic!("{accession} not in block {}", fixture.block));
        let real = map(&[window_block(fixture.block, vec![real])]);
        let mirror = fixture.map();
        for table in TABLES {
            assert_eq!(real.rows(table), mirror.rows(table), "{accession} {table}");
            for row in 0..real.rows(table) {
                assert_eq!(
                    real.row(table, row),
                    mirror.row(table, row),
                    "{accession} {table} row {row}"
                );
            }
        }
        println!("{accession}: mirror equals the real filing");
    }
}

/// Every row and column of this group's tables equals the prototype's on the
/// four sample days (`proto_map.py` NDJSON).
/// `cargo test -p blocks --lib sec::tests::funds::funds_match -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn funds_match_the_prototype() {
    oracle::assert_matches(&fire::DAYS, &TABLES, &[]);
}
