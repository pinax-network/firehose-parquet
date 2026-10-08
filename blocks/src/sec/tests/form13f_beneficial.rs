//! Value tests of `form13f_reports`, `form13f_other_managers`, `form13f_holdings`, `beneficial_reports`, `beneficial_reporting_persons`.
//! Owned by the `form13f-beneficial` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The fixtures mirror the §8.7 sample filings (real accessions, windows,
//! acceptance times and values; long free text shortened). Every test that
//! checks a whole row lists **every** column after [ID] and [FC] in schema
//! order ([`assert_columns`]), so a column cannot go untested.

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
    let mut form13f = symetra_combination_body();
    form13f.signature = Some(sec::Form13fSignature {
        name: s("Megan Hatt"),
        title: s("Director, Deputy CCO"),
        signature_date: s("08-13-2026"),
        ..Default::default()
    });
    vec![
        filing("13F-HR", Body::Form13f(form13f)),
        filing("SCHEDULE 13D", Body::Beneficial(unifirst_13d_body())),
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
// Helpers
// ---------------------------------------------------------------------------

/// The [ID] and [FC] columns every table of this group starts with.
const PREFIX_COLUMNS: usize = 7 + 5;

const FC_COLUMNS: [&str; 5] = [
    "filing_index",
    "accession_number",
    "form_type",
    "filing_date",
    "acceptance_datetime",
];

/// Assert one row of `table`: `expected` must list every column after [ID]
/// and [FC], in schema order, with its [`Batches::cell`] text.
fn assert_columns(batches: &Batches, table: &str, row: usize, expected: &[(&str, &str)]) {
    let schema = batches.table(table).schema();
    let columns: Vec<&str> = schema
        .fields()
        .iter()
        .skip(PREFIX_COLUMNS)
        .map(|field| field.name().as_str())
        .collect();
    let listed: Vec<&str> = expected.iter().map(|(column, _)| *column).collect();
    assert_eq!(listed, columns, "{table}: the expected columns differ");
    for (column, value) in expected {
        assert_eq!(
            batches.cell(table, column, row),
            *value,
            "{table}.{column} row {row}"
        );
    }
}

/// The [FC] columns (and `block_num`, `date`) of `table` row `row` equal the
/// `filings` row `filing`.
fn assert_fc(batches: &Batches, table: &str, row: usize, filing: usize) {
    for column in FC_COLUMNS.iter().chain(&["block_num", "date"]) {
        assert_eq!(
            batches.cell(table, column, row),
            batches.cell("filings", column, filing),
            "{table}.{column} row {row}"
        );
    }
}

/// A filing as firesec writes it: `accession`, `form_type`, `filing_date`,
/// filer `cik`/`company_name`, accepted `offset` seconds into window `block`.
fn real_filing(
    accession: &str,
    form_type: &str,
    filing_date: &str,
    (block, offset): (u64, i64),
    (cik, company_name): (&str, &str),
    body: Body,
) -> sec::Filing {
    sec::Filing {
        accession_number: s(accession),
        filing_date: s(filing_date),
        cik: s(cik),
        company_name: s(company_name),
        acceptance_datetime: Some(prost_types::Timestamp {
            seconds: window_seconds(block) + offset,
            nanos: 0,
        }),
        source_path: format!("{}.gz!{accession}", filing_date.replace('-', "")),
        ..filing(form_type, body)
    }
}

fn s(text: &str) -> String {
    text.to_string()
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| item.to_string()).collect()
}

fn address(street1: &str, street2: &str, city: &str, state: &str, zip_code: &str) -> sec::Address {
    sec::Address {
        street1: s(street1),
        street2: s(street2),
        city: s(city),
        state: s(state),
        zip_code: s(zip_code),
        ..Default::default()
    }
}

fn other_manager(cik: &str, name: &str, file_number: &str, sequence: &str) -> sec::OtherManager {
    sec::OtherManager {
        cik: s(cik),
        name: s(name),
        form13f_file_number: s(file_number),
        sequence_number: s(sequence),
        ..Default::default()
    }
}

/// An information-table entry with `SH` shares and voting authority
/// `(sole, shared, none)`.
fn holding(
    (issuer, class, cusip): (&str, &str, &str),
    (value, shares): (&str, &str),
    discretion: &str,
    ids: &[&str],
    (sole, shared, none): (&str, &str, &str),
) -> sec::InfoTableEntry {
    sec::InfoTableEntry {
        name_of_issuer: s(issuer),
        title_of_class: s(class),
        cusip: s(cusip),
        value: s(value),
        shares_or_principal: Some(sec::ShrsOrPrnAmt {
            amount: s(shares),
            r#type: s("SH"),
        }),
        investment_discretion: s(discretion),
        other_manager_ids: strings(ids),
        voting_authority: Some(sec::VotingAuthority {
            sole: s(sole),
            shared: s(shared),
            none: s(none),
        }),
        ..Default::default()
    }
}

/// `(table, column, index, raw_value, issue)` of one `parse_issues` row.
type Issue = (String, String, [Option<u32>; 3], String, String);

/// Every `parse_issues` row, in row order.
fn issue_tuples(batches: &Batches) -> Vec<Issue> {
    batches
        .issues()
        .into_iter()
        .map(|i| (i.table, i.column, i.index, i.raw, i.issue))
        .collect()
}

fn issue(table: &str, column: &str, index: [Option<u32>; 3], raw: &str, kind: &str) -> Issue {
    (s(table), s(column), index, s(raw), s(kind))
}

// ---------------------------------------------------------------------------
// §8.7 13F fixtures
// ---------------------------------------------------------------------------

/// 13F-HR `0000950103-26-012367` (Atairos, window 2977836): summary other
/// managers with sequence numbers, multi-id `other_manager_ids` (`1, 2, 3`).
fn atairos() -> sec::Filing {
    let entry = |issuer_class_cusip, value: &str, shares: &str| {
        holding(
            issuer_class_cusip,
            (value, shares),
            "DFND",
            &["1, 2, 3"],
            (shares, "0", "0"),
        )
    };
    let body = sec::Form13fReport {
        cover_page: Some(sec::CoverPage {
            report_type: s("13F HOLDINGS REPORT"),
            form13f_file_number: s("028-18539"),
            period_of_report: s("2026-06-30"),
            filing_manager: Some(sec::FilingManager {
                cik: s("0001671122"),
                name: s("Atairos Group, Inc."),
                address: Some(address("40 Morris Avenue", "", "Bryn Mawr", "PA", "19010")),
            }),
            provide_info_for_instruction5: s("Y"),
            additional_information: s("Michael J. Angelakis controls Atairos Partners GP, Inc."),
            ..Default::default()
        }),
        summary_page: Some(sec::SummaryPage {
            other_included_managers_count: 3,
            table_entry_total: 5,
            table_value_total: s("1755698676"),
            other_managers: vec![
                other_manager("0001671185", "Atairos Partners, L.P.", "028-18540", "1"),
                other_manager("0001671176", "Atairos Partners GP, Inc.", "028-23365", "2"),
                other_manager("0001393014", "Angelakis Michael J", "028-18537", "3"),
            ],
            is_confidential_omitted: None,
        }),
        holdings: vec![
            entry(
                ("TRINET GROUP INC", "COM", "896288107"),
                "895064906",
                "18085773",
            ),
            entry(
                ("LUCKY STRIKE ENTERTAINMENT CORP", "CL A COM", "10258P102"),
                "474424894",
                "63425788",
            ),
            entry(
                ("CLARIVATE PLC", "ORD SHS", "G21810109"),
                "26481270",
                "12259847",
            ),
            entry(
                ("XPONENTIAL FITNESS INC", "COM CL A", "98422X101"),
                "1560806",
                "225550",
            ),
            entry(
                ("LIFE TIME GROUP HOLDINGS, INC.", "COM", "53190C102"),
                "358166800",
                "8770000",
            ),
        ],
        signature: None,
    };
    real_filing(
        "0000950103-26-012367",
        "13F-HR",
        "2026-08-14",
        (2_977_836, 351),
        ("0001671122", "Atairos Group, Inc."),
        Body::Form13f(body),
    )
}

/// 13F-NT `0001323255-14-000015` (ABP, window 2346276): cover-page other
/// managers, no summary page, no holdings.
fn abp_notice() -> sec::Filing {
    let body = sec::Form13fReport {
        cover_page: Some(sec::CoverPage {
            report_type: s("13F NOTICE"),
            form13f_file_number: s("028-04817"),
            period_of_report: s("2014-06-30"),
            filing_manager: Some(sec::FilingManager {
                cik: s("0000918509"),
                name: s("STICHTING PENSIOENFONDS ABP"),
                address: Some(address(
                    "OUDE LINDESTRAAT 70",
                    "POSTBUS 6401",
                    "DL HEERLEN",
                    "P7",
                    "00000",
                )),
            }),
            provide_info_for_instruction5: s("Y"),
            additional_information: s("Signed pursuant to a Power of Attorney."),
            other_managers: vec![
                other_manager("0001434819", "APG Asset Management N.V.", "028-13074", ""),
                other_manager(
                    "0001323255",
                    "APG Asset Management US Inc.",
                    "028-11397",
                    "",
                ),
            ],
            ..Default::default()
        }),
        ..Default::default()
    };
    real_filing(
        "0001323255-14-000015",
        "13F-NT",
        "2014-08-11",
        (2_346_276, 163),
        ("0000918509", "STICHTING PENSIOENFONDS ABP"),
        Body::Form13f(body),
    )
}

/// 13F-HR `0000950123-14-008258` (Polaris, window 2346308): filed before
/// 2023-01-03 (`value_multiplier_rule` 1000), `is_confidential_omitted` false.
fn polaris() -> sec::Filing {
    let entry = |issuer: &str, cusip: &str, value: &str, shares: &str| {
        holding(
            (issuer, "Common Stock", cusip),
            (value, shares),
            "SOLE",
            &[],
            (shares, "0", "0"),
        )
    };
    let body = sec::Form13fReport {
        cover_page: Some(sec::CoverPage {
            report_type: s("13F HOLDINGS REPORT"),
            form13f_file_number: s("028-16125"),
            period_of_report: s("2014-06-30"),
            filing_manager: Some(sec::FilingManager {
                cik: s("0001439589"),
                name: s("Polaris Venture Management Co. V, L.L.C."),
                address: Some(address(
                    "1000 Winter Street",
                    "Suite 3350",
                    "Waltham",
                    "MA",
                    "02451",
                )),
            }),
            provide_info_for_instruction5: s("N"),
            ..Default::default()
        }),
        summary_page: Some(sec::SummaryPage {
            table_entry_total: 5,
            table_value_total: s("122723"),
            is_confidential_omitted: Some(false),
            ..Default::default()
        }),
        holdings: vec![
            entry("Bind Therapeutics, Inc.", "05548N107", "26600", "2018253"),
            entry("Fate Therapeutics, Inc.", "31189P102", "15606", "2473186"),
            entry("Genocea Biosciences, Inc.", "372427104", "39913", "2128678"),
            entry("Trevena, Inc.", "89532E109", "21536", "3811682"),
            entry("Cerulean Pharma Inc", "15708Q105", "19068", "3287529"),
        ],
        signature: None,
    };
    real_filing(
        "0000950123-14-008258",
        "13F-HR",
        "2014-08-11",
        (2_346_308, 96),
        ("0001439589", "Polaris Venture Management Co. V, L.L.C."),
        Body::Form13f(body),
    )
}

/// The body of 13F-HR `0001811513-26-000014` (Symetra, window 2977776): a
/// COMBINATION REPORT with a cover-page and a summary-page manager. Trimmed
/// to its first 2 of 6 holdings.
fn symetra_combination_body() -> sec::Form13fReport {
    let entry = |issuer_class_cusip, value: &str, shares: &str| {
        holding(
            issuer_class_cusip,
            (value, shares),
            "DFND",
            &["1"],
            (shares, "0", "0"),
        )
    };
    sec::Form13fReport {
        cover_page: Some(sec::CoverPage {
            report_type: s("13F COMBINATION REPORT"),
            form13f_file_number: s("028-20275"),
            period_of_report: s("2026-06-30"),
            filing_manager: Some(sec::FilingManager {
                cik: s("0001811513"),
                name: s("Symetra Investment Management Co"),
                address: Some(address(
                    "308 Farmington Ave",
                    "",
                    "Farmington",
                    "CT",
                    "06032",
                )),
            }),
            provide_info_for_instruction5: s("N"),
            other_managers: vec![other_manager(
                "",
                "Tortoise Capital Advisors LLC",
                "028-11123",
                "",
            )],
            ..Default::default()
        }),
        summary_page: Some(sec::SummaryPage {
            other_included_managers_count: 1,
            table_entry_total: 6,
            table_value_total: s("1144074"),
            other_managers: vec![other_manager(
                "",
                "Symetra Financial Corporation",
                "028-17231",
                "1",
            )],
            is_confidential_omitted: Some(false),
        }),
        holdings: vec![
            entry(
                ("VANGUARD", "INDEX FDS TOTAL STK MKT", "922908769"),
                "391095",
                "1056900",
            ),
            entry(
                ("BLACKROCK, INC.", "ISHARES TR IBOXX HI YD ETF", "464288513"),
                "90761",
                "1134937",
            ),
        ],
        signature: None,
    }
}

fn symetra() -> sec::Filing {
    real_filing(
        "0001811513-26-000014",
        "13F-HR",
        "2026-08-14",
        (2_977_776, 479),
        ("0001811513", "Symetra Investment Management Co"),
        Body::Form13f(symetra_combination_body()),
    )
}

/// 13F-HR `0001104659-26-097111` (MVM Partners, window 2977896): one holding,
/// CRD and SEC file numbers, `is_confidential_omitted` unset.
fn mvm() -> sec::Filing {
    let body = sec::Form13fReport {
        cover_page: Some(sec::CoverPage {
            report_type: s("13F HOLDINGS REPORT"),
            form13f_file_number: s("028-22913"),
            period_of_report: s("2026-06-30"),
            filing_manager: Some(sec::FilingManager {
                cik: s("0001947083"),
                name: s("MVM Partners, LLC"),
                address: Some(address(
                    "OLD CITY HALL",
                    "45 SCHOOL STREET",
                    "BOSTON",
                    "MA",
                    "02108",
                )),
            }),
            provide_info_for_instruction5: s("N"),
            crd_number: s("000159225"),
            sec_file_number: s("028-22913"),
            ..Default::default()
        }),
        summary_page: Some(sec::SummaryPage {
            table_entry_total: 1,
            table_value_total: s("1911206"),
            ..Default::default()
        }),
        holdings: vec![holding(
            ("MDXHealth SA", "SHS New", "B5950S113"),
            ("1911206", "4700457"),
            "SOLE",
            &[],
            ("4700457", "0", "0"),
        )],
        signature: None,
    };
    real_filing(
        "0001104659-26-097111",
        "13F-HR",
        "2026-08-14",
        (2_977_896, 407),
        ("0001947083", "MVM Partners, LLC"),
        Body::Form13f(body),
    )
}

/// 13F-HR/A `0001595082-26-000063` (Davidson Kempner, window 2977880): a NEW
/// HOLDINGS amendment, number 5.
fn davidson_kempner_amendment() -> sec::Filing {
    let body = sec::Form13fReport {
        cover_page: Some(sec::CoverPage {
            report_type: s("13F HOLDINGS REPORT"),
            form13f_file_number: s("028-16184"),
            period_of_report: s("2025-09-30"),
            filing_manager: Some(sec::FilingManager {
                cik: s("0001595082"),
                name: s("Davidson Kempner Capital Management LP"),
                address: Some(address("9 West 57th Street", "", "New York", "NY", "10019")),
            }),
            is_amendment: true,
            amendment_type: s("NEW HOLDINGS"),
            amendment_number: s("5"),
            provide_info_for_instruction5: s("N"),
            ..Default::default()
        }),
        summary_page: Some(sec::SummaryPage {
            table_entry_total: 1,
            table_value_total: s("390612740"),
            is_confidential_omitted: Some(false),
            ..Default::default()
        }),
        holdings: vec![holding(
            ("CHART INDS INC", "COM", "16115Q308"),
            ("390612740", "1951600"),
            "SOLE",
            &[],
            ("1951600", "0", "0"),
        )],
        signature: None,
    };
    sec::Filing {
        period_of_report: s("2025-09-30"),
        ..real_filing(
            "0001595082-26-000063",
            "13F-HR/A",
            "2026-08-14",
            (2_977_880, 465),
            ("0001595082", "DAVIDSON KEMPNER CAPITAL MANAGEMENT LP"),
            Body::Form13f(body),
        )
    }
}

/// A 13F-HR with `body` and the test filing envelope.
fn form13f(body: sec::Form13fReport) -> sec::Filing {
    filing("13F-HR", Body::Form13f(body))
}

// ---------------------------------------------------------------------------
// 13F tests
// ---------------------------------------------------------------------------

#[test]
fn form13f_summary_managers_and_sequence_numbers() {
    let batches = map(&[window_block(2_977_836, vec![atairos()])]);
    batches.assert_rows(&[
        ("form13f_reports", 1),
        ("form13f_other_managers", 3),
        ("form13f_holdings", 5),
        ("parse_issues", 0),
    ]);
    assert_fc(&batches, "form13f_reports", 0, 0);
    assert_eq!(
        batches.cell("form13f_reports", "acceptance_datetime", 0),
        "2026-08-14T10:05:51Z"
    );
    assert_columns(
        &batches,
        "form13f_reports",
        0,
        &[
            ("has_cover_page", "true"),
            ("manager_cik", "0001671122"),
            ("manager_name", "Atairos Group, Inc."),
            ("manager_street1", "40 Morris Avenue"),
            ("manager_street2", "NULL"),
            ("manager_city", "Bryn Mawr"),
            ("manager_state", "PA"),
            ("manager_zip_code", "19010"),
            ("manager_state_description", "NULL"),
            ("manager_country", "NULL"),
            ("manager_non_us_state_territory", "NULL"),
            ("report_type", "13F HOLDINGS REPORT"),
            ("form13f_file_number", "028-18539"),
            ("crd_number", "NULL"),
            ("sec_file_number", "NULL"),
            ("period_of_report", "2026-06-30"),
            ("cover_is_amendment", "false"),
            ("amendment_type", "NULL"),
            ("amendment_number", "NULL"),
            ("provide_info_for_instruction5", "true"),
            (
                "additional_information",
                "Michael J. Angelakis controls Atairos Partners GP, Inc.",
            ),
            ("has_summary_page", "true"),
            ("other_included_managers_count", "3"),
            ("table_entry_total", "5"),
            ("table_value_total", "1755698676"),
            ("is_confidential_omitted", "NULL"),
            ("value_multiplier_rule", "1"),
            ("holdings_count", "5"),
            ("holdings_value_sum", "1755698676"),
            ("holdings_complete", "true"),
            ("cover_other_manager_count", "0"),
            ("summary_other_manager_count", "3"),
            ("has_parse_issues", "false"),
        ],
    );

    for row in 0..3 {
        assert_fc(&batches, "form13f_other_managers", row, 0);
    }
    assert_columns(
        &batches,
        "form13f_other_managers",
        1,
        &[
            ("manager_cik", "0001671122"),
            ("period_of_report", "2026-06-30"),
            ("other_manager_index", "1"),
            ("list_kind", "summary"),
            ("sequence_number", "2"),
            ("other_manager_cik", "0001671176"),
            ("other_manager_name", "Atairos Partners GP, Inc."),
            ("form13f_file_number", "028-23365"),
            ("crd_number", "NULL"),
            ("sec_file_number", "NULL"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_eq!(
        batches.column("form13f_other_managers", "sequence_number"),
        ["1", "2", "3"]
    );
    assert_eq!(
        batches.column("form13f_other_managers", "other_manager_index"),
        ["0", "1", "2"]
    );

    for row in 0..5 {
        assert_fc(&batches, "form13f_holdings", row, 0);
    }
    assert_columns(
        &batches,
        "form13f_holdings",
        2,
        &[
            ("manager_cik", "0001671122"),
            ("manager_name", "Atairos Group, Inc."),
            ("period_of_report", "2026-06-30"),
            ("report_type", "13F HOLDINGS REPORT"),
            ("amendment_type", "NULL"),
            ("value_multiplier_rule", "1"),
            ("holding_index", "2"),
            ("issuer_name", "CLARIVATE PLC"),
            ("title_of_class", "ORD SHS"),
            ("cusip", "G21810109"),
            ("cusip_norm", "G21810109"),
            ("figi", "NULL"),
            ("value", "26481270"),
            ("shares_or_principal_amount", "12259847"),
            ("shares_or_principal_type", "SH"),
            ("put_call", "NULL"),
            ("put_call_norm", "NULL"),
            ("investment_discretion", "DFND"),
            ("other_manager_ids", "[1, 2, 3]"),
            ("other_manager_sequence_numbers", "[1, 2, 3]"),
            ("voting_authority_sole", "12259847"),
            ("voting_authority_shared", "0"),
            ("voting_authority_none", "0"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_eq!(
        batches.column("form13f_holdings", "holding_index"),
        ["0", "1", "2", "3", "4"]
    );
    assert_eq!(
        batches.column("form13f_holdings", "value"),
        ["895064906", "474424894", "26481270", "1560806", "358166800"]
    );
}

#[test]
fn form13f_notice_with_cover_managers_and_no_summary_page() {
    let batches = map(&[window_block(2_346_276, vec![abp_notice()])]);
    batches.assert_rows(&[
        ("form13f_reports", 1),
        ("form13f_other_managers", 2),
        ("form13f_holdings", 0),
        ("parse_issues", 0),
    ]);
    assert_fc(&batches, "form13f_reports", 0, 0);
    assert_columns(
        &batches,
        "form13f_reports",
        0,
        &[
            ("has_cover_page", "true"),
            ("manager_cik", "0000918509"),
            ("manager_name", "STICHTING PENSIOENFONDS ABP"),
            ("manager_street1", "OUDE LINDESTRAAT 70"),
            ("manager_street2", "POSTBUS 6401"),
            ("manager_city", "DL HEERLEN"),
            ("manager_state", "P7"),
            ("manager_zip_code", "00000"),
            ("manager_state_description", "NULL"),
            ("manager_country", "NULL"),
            ("manager_non_us_state_territory", "NULL"),
            ("report_type", "13F NOTICE"),
            ("form13f_file_number", "028-04817"),
            ("crd_number", "NULL"),
            ("sec_file_number", "NULL"),
            ("period_of_report", "2014-06-30"),
            ("cover_is_amendment", "false"),
            ("amendment_type", "NULL"),
            ("amendment_number", "NULL"),
            ("provide_info_for_instruction5", "true"),
            (
                "additional_information",
                "Signed pursuant to a Power of Attorney.",
            ),
            // No summary page: its uint32 and optional bool are NULL, not 0/false.
            ("has_summary_page", "false"),
            ("other_included_managers_count", "NULL"),
            ("table_entry_total", "NULL"),
            ("table_value_total", "NULL"),
            ("is_confidential_omitted", "NULL"),
            ("value_multiplier_rule", "1000"),
            ("holdings_count", "0"),
            // No holdings: NULL, not 0.
            ("holdings_value_sum", "NULL"),
            ("holdings_complete", "NULL"),
            ("cover_other_manager_count", "2"),
            ("summary_other_manager_count", "0"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_fc(&batches, "form13f_other_managers", 1, 0);
    assert_columns(
        &batches,
        "form13f_other_managers",
        1,
        &[
            ("manager_cik", "0000918509"),
            ("period_of_report", "2014-06-30"),
            ("other_manager_index", "1"),
            ("list_kind", "cover"),
            ("sequence_number", "NULL"),
            ("other_manager_cik", "0001323255"),
            ("other_manager_name", "APG Asset Management US Inc."),
            ("form13f_file_number", "028-11397"),
            ("crd_number", "NULL"),
            ("sec_file_number", "NULL"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_eq!(
        batches.column("form13f_other_managers", "list_kind"),
        ["cover", "cover"]
    );
}

#[test]
fn form13f_combination_report_counts_cover_then_summary_managers() {
    let batches = map(&[window_block(2_977_776, vec![symetra()])]);
    batches.assert_rows(&[("form13f_other_managers", 2), ("form13f_holdings", 2)]);
    let managers = |column: &str| batches.column("form13f_other_managers", column);
    assert_eq!(managers("other_manager_index"), ["0", "1"]);
    assert_eq!(managers("list_kind"), ["cover", "summary"]);
    assert_eq!(managers("sequence_number"), ["NULL", "1"]);
    assert_eq!(managers("other_manager_cik"), ["NULL", "NULL"]);
    assert_eq!(
        managers("other_manager_name"),
        [
            "Tortoise Capital Advisors LLC",
            "Symetra Financial Corporation"
        ]
    );
    assert_eq!(managers("form13f_file_number"), ["028-11123", "028-17231"]);
    let report = |column: &str| batches.cell("form13f_reports", column, 0);
    assert_eq!(report("report_type"), "13F COMBINATION REPORT");
    assert_eq!(report("cover_other_manager_count"), "1");
    assert_eq!(report("summary_other_manager_count"), "1");
    assert_eq!(report("other_included_managers_count"), "1");
    assert_eq!(report("is_confidential_omitted"), "false");
    // Trimmed to 2 of the 6 declared holdings: the QA flag catches it.
    assert_eq!(report("holdings_count"), "2");
    assert_eq!(report("table_entry_total"), "6");
    assert_eq!(report("holdings_complete"), "false");
    assert_eq!(report("holdings_value_sum"), "481856");
    assert_eq!(report("table_value_total"), "1144074");
    assert_eq!(
        batches.column("form13f_holdings", "other_manager_sequence_numbers"),
        ["[1]", "[1]"]
    );
}

#[test]
fn form13f_value_multiplier_rule_follows_the_filing_date_cutover() {
    let report = |filing_date: &str| sec::Filing {
        filing_date: s(filing_date),
        ..form13f(sec::Form13fReport::default())
    };
    // A 2026 window: before / on / after the cutover, and a NULL filing date
    // (empty or unparseable) that falls back to the 2026-08-28 partition.
    let late = window_block(
        BLOCK_NUM,
        vec![
            report("2023-01-02"),
            report("2023-01-03"),
            report("2026-08-27"),
            report(""),
            report("N/A"),
        ],
    );
    // A 2014 window: a NULL filing date falls back to the 2014-08-11 partition.
    let early = window_block(2_346_308, vec![polaris(), report("")]);
    let batches = map(&[late, early]);
    assert_eq!(
        batches.column("form13f_reports", "value_multiplier_rule"),
        ["1000", "1", "1", "1", "1", "1000", "1000"]
    );
    assert_eq!(
        batches.column("form13f_reports", "filing_date"),
        [
            "2023-01-02",
            "2023-01-03",
            "2026-08-27",
            "NULL",
            "NULL",
            "2014-08-11",
            "NULL"
        ]
    );
    // The rule is copied onto every holding of the 2014 report.
    assert_eq!(
        batches.column("form13f_holdings", "value_multiplier_rule"),
        ["1000"; 5]
    );
    // The unparseable filing date is a `filings` issue, not a 13F one.
    assert_eq!(
        issue_tuples(&batches),
        [issue(
            "filings",
            "filing_date",
            [None; 3],
            "N/A",
            "sentinel"
        )]
    );
}

#[test]
fn form13f_pre_cutover_report_keeps_raw_values() {
    let batches = map(&[window_block(2_346_308, vec![polaris()])]);
    assert_fc(&batches, "form13f_reports", 0, 0);
    assert_columns(
        &batches,
        "form13f_reports",
        0,
        &[
            ("has_cover_page", "true"),
            ("manager_cik", "0001439589"),
            ("manager_name", "Polaris Venture Management Co. V, L.L.C."),
            ("manager_street1", "1000 Winter Street"),
            ("manager_street2", "Suite 3350"),
            ("manager_city", "Waltham"),
            ("manager_state", "MA"),
            ("manager_zip_code", "02451"),
            ("manager_state_description", "NULL"),
            ("manager_country", "NULL"),
            ("manager_non_us_state_territory", "NULL"),
            ("report_type", "13F HOLDINGS REPORT"),
            ("form13f_file_number", "028-16125"),
            ("crd_number", "NULL"),
            ("sec_file_number", "NULL"),
            ("period_of_report", "2014-06-30"),
            ("cover_is_amendment", "false"),
            ("amendment_type", "NULL"),
            ("amendment_number", "NULL"),
            ("provide_info_for_instruction5", "false"),
            ("additional_information", "NULL"),
            ("has_summary_page", "true"),
            // A present summary page: proto3 uint32 0 is 0, not NULL.
            ("other_included_managers_count", "0"),
            ("table_entry_total", "5"),
            // RAW thousands; the view applies the multiplier.
            ("table_value_total", "122723"),
            ("is_confidential_omitted", "false"),
            ("value_multiplier_rule", "1000"),
            ("holdings_count", "5"),
            ("holdings_value_sum", "122723"),
            ("holdings_complete", "true"),
            ("cover_other_manager_count", "0"),
            ("summary_other_manager_count", "0"),
            ("has_parse_issues", "false"),
        ],
    );
    let holdings = |column: &str| batches.column("form13f_holdings", column);
    assert_eq!(
        holdings("cusip_norm"),
        [
            "05548N107",
            "31189P102",
            "372427104",
            "89532E109",
            "15708Q105"
        ]
    );
    assert_eq!(holdings("other_manager_ids"), ["[]"; 5]);
    assert_eq!(holdings("other_manager_sequence_numbers"), ["[]"; 5]);
    assert_eq!(holdings("investment_discretion"), ["SOLE"; 5]);
}

#[test]
fn form13f_single_holding_with_crd_and_sec_file_numbers() {
    let batches = map(&[window_block(2_977_896, vec![mvm()])]);
    let report = |column: &str| batches.cell("form13f_reports", column, 0);
    assert_eq!(report("crd_number"), "000159225");
    assert_eq!(report("sec_file_number"), "028-22913");
    assert_eq!(report("manager_street2"), "45 SCHOOL STREET");
    assert_eq!(report("is_confidential_omitted"), "NULL");
    assert_eq!(report("other_included_managers_count"), "0");
    assert_eq!(report("holdings_count"), "1");
    assert_eq!(report("holdings_value_sum"), "1911206");
    assert_eq!(report("holdings_complete"), "true");
    assert_eq!(report("value_multiplier_rule"), "1");
    let holding = |column: &str| batches.cell("form13f_holdings", column, 0);
    assert_eq!(holding("cusip_norm"), "B5950S113");
    assert_eq!(holding("issuer_name"), "MDXHealth SA");
    assert_eq!(holding("title_of_class"), "SHS New");
}

#[test]
fn form13f_new_holdings_amendment() {
    let batches = map(&[window_block(2_977_880, vec![davidson_kempner_amendment()])]);
    let report = |column: &str| batches.cell("form13f_reports", column, 0);
    assert_eq!(report("form_type"), "13F-HR/A");
    assert_eq!(report("cover_is_amendment"), "true");
    assert_eq!(report("amendment_type"), "NEW HOLDINGS");
    assert_eq!(report("amendment_number"), "5");
    assert_eq!(report("period_of_report"), "2025-09-30");
    assert_eq!(report("holdings_value_sum"), "390612740");
    assert_fc(&batches, "form13f_holdings", 0, 0);
    assert_columns(
        &batches,
        "form13f_holdings",
        0,
        &[
            ("manager_cik", "0001595082"),
            ("manager_name", "Davidson Kempner Capital Management LP"),
            ("period_of_report", "2025-09-30"),
            ("report_type", "13F HOLDINGS REPORT"),
            ("amendment_type", "NEW HOLDINGS"),
            ("value_multiplier_rule", "1"),
            ("holding_index", "0"),
            ("issuer_name", "CHART INDS INC"),
            ("title_of_class", "COM"),
            ("cusip", "16115Q308"),
            ("cusip_norm", "16115Q308"),
            ("figi", "NULL"),
            ("value", "390612740"),
            ("shares_or_principal_amount", "1951600"),
            ("shares_or_principal_type", "SH"),
            ("put_call", "NULL"),
            ("put_call_norm", "NULL"),
            ("investment_discretion", "SOLE"),
            ("other_manager_ids", "[]"),
            ("other_manager_sequence_numbers", "[]"),
            ("voting_authority_sole", "1951600"),
            ("voting_authority_shared", "0"),
            ("voting_authority_none", "0"),
            ("has_parse_issues", "false"),
        ],
    );
}

#[test]
fn form13f_without_cover_or_summary_page() {
    // A paper-era body: only holdings. Every cover column is NULL, including
    // the proto bool `cover_is_amendment` (NULL, not false), and the children
    // copy those NULLs.
    let body = sec::Form13fReport {
        holdings: vec![sec::InfoTableEntry {
            name_of_issuer: s("ACME"),
            value: s("10"),
            ..Default::default()
        }],
        ..Default::default()
    };
    let batches = map(&[window_block(BLOCK_NUM, vec![form13f(body)])]);
    assert_columns(
        &batches,
        "form13f_reports",
        0,
        &[
            ("has_cover_page", "false"),
            ("manager_cik", "NULL"),
            ("manager_name", "NULL"),
            ("manager_street1", "NULL"),
            ("manager_street2", "NULL"),
            ("manager_city", "NULL"),
            ("manager_state", "NULL"),
            ("manager_zip_code", "NULL"),
            ("manager_state_description", "NULL"),
            ("manager_country", "NULL"),
            ("manager_non_us_state_territory", "NULL"),
            ("report_type", "NULL"),
            ("form13f_file_number", "NULL"),
            ("crd_number", "NULL"),
            ("sec_file_number", "NULL"),
            ("period_of_report", "NULL"),
            ("cover_is_amendment", "NULL"),
            ("amendment_type", "NULL"),
            ("amendment_number", "NULL"),
            ("provide_info_for_instruction5", "NULL"),
            ("additional_information", "NULL"),
            ("has_summary_page", "false"),
            ("other_included_managers_count", "NULL"),
            ("table_entry_total", "NULL"),
            ("table_value_total", "NULL"),
            ("is_confidential_omitted", "NULL"),
            ("value_multiplier_rule", "1"),
            ("holdings_count", "1"),
            ("holdings_value_sum", "10"),
            ("holdings_complete", "NULL"),
            ("cover_other_manager_count", "0"),
            ("summary_other_manager_count", "0"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_columns(
        &batches,
        "form13f_holdings",
        0,
        &[
            ("manager_cik", "NULL"),
            ("manager_name", "NULL"),
            ("period_of_report", "NULL"),
            ("report_type", "NULL"),
            ("amendment_type", "NULL"),
            ("value_multiplier_rule", "1"),
            ("holding_index", "0"),
            ("issuer_name", "ACME"),
            ("title_of_class", "NULL"),
            ("cusip", "NULL"),
            ("cusip_norm", "NULL"),
            ("figi", "NULL"),
            ("value", "10"),
            // Absent sub-messages: every flattened column is NULL.
            ("shares_or_principal_amount", "NULL"),
            ("shares_or_principal_type", "NULL"),
            ("put_call", "NULL"),
            ("put_call_norm", "NULL"),
            ("investment_discretion", "NULL"),
            ("other_manager_ids", "[]"),
            ("other_manager_sequence_numbers", "[]"),
            ("voting_authority_sole", "NULL"),
            ("voting_authority_shared", "NULL"),
            ("voting_authority_none", "NULL"),
            ("has_parse_issues", "false"),
        ],
    );

    // A cover page whose manager has no address: the 8 address columns NULL;
    // the cover's proto bool is false (not NULL) once the cover exists.
    let body = sec::Form13fReport {
        cover_page: Some(sec::CoverPage {
            filing_manager: Some(sec::FilingManager {
                cik: s("0000000009"),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };
    let batches = map(&[window_block(BLOCK_NUM, vec![form13f(body)])]);
    let report = |column: &str| batches.cell("form13f_reports", column, 0);
    assert_eq!(report("has_cover_page"), "true");
    assert_eq!(report("cover_is_amendment"), "false");
    assert_eq!(report("manager_cik"), "0000000009");
    assert_eq!(report("manager_name"), "NULL");
    for suffix in crate::sec::build::ADDRESS_FIELDS {
        assert_eq!(report(&format!("manager_{suffix}")), "NULL", "{suffix}");
    }
}

#[test]
fn form13f_holdings_derived_join_keys() {
    let entry = |cusip: &str, put_call: &str, ids: &[&str]| sec::InfoTableEntry {
        cusip: s(cusip),
        put_call: s(put_call),
        other_manager_ids: strings(ids),
        value: s("1"),
        figi: s("BBG000BLNNH6"),
        shares_or_principal: Some(sec::ShrsOrPrnAmt {
            amount: s("100"),
            r#type: s("PRN"),
        }),
        ..Default::default()
    };
    let body = sec::Form13fReport {
        holdings: vec![
            entry(" g2181 0109", "Put", &["03,01", "1.0", "0", "N/A", "NONE"]),
            entry("000000000", " call ", &["2;4/5 6"]),
            entry("12345678", "Straddle", &["10000", "9999", "Manager A"]),
            entry("999999999", "", &[]),
            entry("ABCDEFGH*", "PUT", &[""]),
        ],
        ..Default::default()
    };
    let batches = map(&[window_block(BLOCK_NUM, vec![form13f(body)])]);
    let column = |name: &str| batches.column("form13f_holdings", name);
    assert_eq!(
        column("cusip"),
        [
            " g2181 0109",
            "000000000",
            "12345678",
            "999999999",
            "ABCDEFGH*"
        ]
    );
    assert_eq!(
        column("cusip_norm"),
        ["G21810109", "NULL", "NULL", "NULL", "NULL"]
    );
    assert_eq!(
        column("put_call"),
        ["Put", " call ", "Straddle", "NULL", "PUT"]
    );
    assert_eq!(
        column("put_call_norm"),
        ["PUT", "CALL", "NULL", "NULL", "PUT"]
    );
    assert_eq!(
        column("other_manager_ids"),
        [
            "[03,01, 1.0, 0, N/A, NONE]",
            "[2;4/5 6]",
            "[10000, 9999, Manager A]",
            "[]",
            "[]"
        ]
    );
    assert_eq!(
        column("other_manager_sequence_numbers"),
        ["[3, 1, 1]", "[2, 4, 5, 6]", "[9999]", "[]", "[]"]
    );
    assert_eq!(column("figi"), ["BBG000BLNNH6"; 5]);
    assert_eq!(column("shares_or_principal_type"), ["PRN"; 5]);
    assert_eq!(column("shares_or_principal_amount"), ["100"; 5]);
    // The derived keys never log an issue: the raw values sit next to them.
    assert_eq!(batches.rows("parse_issues"), 0);
    assert_eq!(
        batches.cell("form13f_reports", "holdings_value_sum", 0),
        "5"
    );
}

#[test]
fn form13f_parse_issues_are_keyed_and_ordered() {
    let body = sec::Form13fReport {
        cover_page: Some(sec::CoverPage {
            period_of_report: s("N/A"),
            amendment_number: s("1.5"),
            provide_info_for_instruction5: s("maybe"),
            other_managers: vec![other_manager("", "COVER MGR", "", "")],
            ..Default::default()
        }),
        summary_page: Some(sec::SummaryPage {
            table_entry_total: 3,
            table_value_total: s("12,345"),
            other_managers: vec![
                other_manager("", "GOOD", "", "01"),
                other_manager("", "BAD", "", "x"),
            ],
            ..Default::default()
        }),
        holdings: vec![
            sec::InfoTableEntry {
                value: s("100.00"),
                shares_or_principal: Some(sec::ShrsOrPrnAmt {
                    amount: s("1.0"),
                    r#type: s("SH"),
                }),
                ..Default::default()
            },
            sec::InfoTableEntry {
                value: s("N/A"),
                voting_authority: Some(sec::VotingAuthority {
                    sole: s("99999999999999999999"),
                    shared: s(""),
                    none: s("-"),
                }),
                ..Default::default()
            },
            sec::InfoTableEntry {
                value: s("7"),
                ..Default::default()
            },
        ],
        signature: None,
    };
    let batches = map(&[window_block(BLOCK_NUM, vec![form13f(body)])]);
    let report = |column: &str| batches.cell("form13f_reports", column, 0);
    assert_eq!(report("period_of_report"), "NULL");
    assert_eq!(report("amendment_number"), "NULL");
    assert_eq!(report("provide_info_for_instruction5"), "NULL");
    assert_eq!(report("table_value_total"), "NULL");
    // One holding value is NULL (a sentinel): the sum is NULL, with no issue.
    assert_eq!(report("holdings_value_sum"), "NULL");
    assert_eq!(report("holdings_complete"), "true");
    assert_eq!(report("has_parse_issues"), "true");
    // The copies on child rows are the parent's typed values.
    let managers = |column: &str| batches.column("form13f_other_managers", column);
    assert_eq!(managers("period_of_report"), ["NULL"; 3]);
    assert_eq!(managers("list_kind"), ["cover", "summary", "summary"]);
    assert_eq!(managers("sequence_number"), ["NULL", "1", "NULL"]);
    assert_eq!(managers("has_parse_issues"), ["false", "false", "true"]);
    let holdings = |column: &str| batches.column("form13f_holdings", column);
    assert_eq!(holdings("period_of_report"), ["NULL"; 3]);
    assert_eq!(holdings("value"), ["100", "NULL", "7"]);
    assert_eq!(
        holdings("shares_or_principal_amount"),
        ["1", "NULL", "NULL"]
    );
    assert_eq!(holdings("voting_authority_sole"), ["NULL"; 3]);
    assert_eq!(holdings("voting_authority_shared"), ["NULL"; 3]);
    assert_eq!(holdings("voting_authority_none"), ["NULL"; 3]);
    assert_eq!(holdings("has_parse_issues"), ["false", "true", "false"]);

    assert!(batches
        .issues()
        .iter()
        .all(|i| i.filing_index == Some(0) && i.block_num == BLOCK_NUM));
    assert_eq!(
        issue_tuples(&batches),
        [
            issue(
                "form13f_reports",
                "period_of_report",
                [None; 3],
                "N/A",
                "sentinel"
            ),
            issue(
                "form13f_reports",
                "amendment_number",
                [None; 3],
                "1.5",
                "unparseable"
            ),
            issue(
                "form13f_reports",
                "provide_info_for_instruction5",
                [None; 3],
                "maybe",
                "unparseable"
            ),
            issue(
                "form13f_reports",
                "table_value_total",
                [None; 3],
                "12,345",
                "unparseable"
            ),
            issue(
                "form13f_other_managers",
                "sequence_number",
                [Some(2), None, None],
                "x",
                "unparseable"
            ),
            issue(
                "form13f_holdings",
                "value",
                [Some(1), None, None],
                "N/A",
                "sentinel"
            ),
            issue(
                "form13f_holdings",
                "voting_authority_sole",
                [Some(1), None, None],
                "99999999999999999999",
                "out_of_range"
            ),
            issue(
                "form13f_holdings",
                "voting_authority_none",
                [Some(1), None, None],
                "-",
                "sentinel"
            ),
        ]
    );
}

#[test]
fn form13f_holdings_value_sum_overflow_is_an_issue_of_the_report() {
    let entry = |value: &str| sec::InfoTableEntry {
        value: s(value),
        ..Default::default()
    };
    let body = sec::Form13fReport {
        cover_page: Some(sec::CoverPage {
            amendment_number: s("x"),
            ..Default::default()
        }),
        holdings: vec![entry("9223372036854775807"), entry(" 1 ")],
        ..Default::default()
    };
    let batches = map(&[window_block(BLOCK_NUM, vec![form13f(body)])]);
    let report = |column: &str| batches.cell("form13f_reports", column, 0);
    assert_eq!(report("holdings_value_sum"), "NULL");
    assert_eq!(report("has_parse_issues"), "true");
    assert_eq!(
        batches.column("form13f_holdings", "value"),
        ["9223372036854775807", "1"]
    );
    assert_eq!(
        batches.column("form13f_holdings", "has_parse_issues"),
        ["false", "false"]
    );
    // Derived issues come after the row's parsed columns; `raw_value` is the
    // operands verbatim joined with ` + `, so the sum can be recomputed.
    assert_eq!(
        issue_tuples(&batches),
        [
            issue(
                "form13f_reports",
                "amendment_number",
                [None; 3],
                "x",
                "unparseable"
            ),
            issue(
                "form13f_reports",
                "holdings_value_sum",
                [None; 3],
                "9223372036854775807 +  1 ",
                "overflow"
            ),
        ]
    );
}

// ---------------------------------------------------------------------------
// §8.7 13D/G fixtures
// ---------------------------------------------------------------------------

fn authorized(
    name: &str,
    phone: &str,
    address: Option<sec::Address>,
) -> sec::BeneficialAuthorizedPerson {
    sec::BeneficialAuthorizedPerson {
        name: s(name),
        phone: s(phone),
        address,
    }
}

/// The body of SCHEDULE 13D `0000950103-26-003802` (Cintas on UniFirst,
/// window 2956129): authorized persons and Items 1–7. Free text shortened.
fn unifirst_13d_body() -> sec::BeneficialOwnershipReport {
    let davis_polk = || {
        Some(address(
            "Davis Polk & Wardwell LLP",
            "450 Lexington Avenue",
            "New York",
            "NY",
            "10017",
        ))
    };
    sec::BeneficialOwnershipReport {
        subject_company_cik: s("0000717954"),
        subject_company_name: s("UNIFIRST CORP"),
        cusip: s("904708104"),
        schedule_type: s("SCHEDULE 13D"),
        filer_cik: s("0000723254"),
        securities_class_title: s("Common Stock"),
        event_date: s("03/10/2026"),
        issuer_address: Some(address("68 JONSPIN RD", "", "WILMINGTON", "MA", "01887")),
        rules_designated: vec![],
        reporting_persons: vec![sec::ReportingPerson {
            cik: s("0000723254"),
            name: s("Cintas Corp"),
            fund_type: s("OO"),
            citizenship: s("WA"),
            sole_voting_power: s("0.00"),
            shared_voting_power: s("3374968.00"),
            sole_dispositive_power: s("0.00"),
            shared_dispositive_power: s("0.00"),
            aggregate_amount_owned: s("3374968.00"),
            percent_of_class: s("18.7"),
            type_of_reporting_person: s("CO"),
            member_of_group: String::new(),
            no_cik: Some(false),
            aggregate_excludes_shares: Some(false),
            legal_proceedings: Some(false),
            comment: s("Rows 8, 11 and 13."),
            fund_types: strings(&["OO"]),
            types_of_reporting_person: strings(&["CO"]),
        }],
        cusips: strings(&["904708104"]),
        previous_accession_number: String::new(),
        amendment_number: String::new(),
        previously_filed: Some(false),
        items_13d: Some(sec::Schedule13dItems {
            security_title: s("Common Stock"),
            filing_person_name: s("The information set forth in response to each separate Item."),
            principal_business_address: s("6800 Cintas Boulevard, Cincinnati, Ohio 45262-5737."),
            principal_job: s("Rental and servicing of uniforms."),
            has_been_convicted: s("During the last five years, none."),
            conviction_description: s("No civil proceeding."),
            citizenship: s("Washington"),
            funds_source: s("Items 4 and 5 are incorporated by reference."),
            transaction_purpose: s("The purpose of the Mergers is to acquire control."),
            percentage_of_class: s("18.7%"),
            number_of_shares: s("3,374,968"),
            transaction_description: s("None."),
            list_of_shareholders: s("No other person."),
            date_5_percent_ownership: s("Not applicable."),
            contract_description: s("Voting Agreements."),
            filed_exhibits: s("Exhibit 1 Agreement and Plan of Merger"),
            item1_comment: s("This statement relates to shares of Common Stock."),
            issuer_name: s("UNIFIRST CORP"),
            issuer_principal_address: Some(address(
                "68 JONSPIN RD",
                "",
                "WILMINGTON",
                "MA",
                "01887",
            )),
        }),
        items_13g: None,
        signatures: vec![sec::BeneficialSignature {
            reporting_person: s("Cintas Corp"),
            signature: s("/s/ Scott A. Garula"),
            title: s("Executive Vice President and Chief Financial Officer"),
            date: s("03/16/2026"),
        }],
        exhibit_info: String::new(),
        signature_comments: String::new(),
        authorized_persons: vec![
            authorized(
                "Scott A. Garula",
                "(513) 459-1200",
                Some(address(
                    "6800 Cintas Boulevard",
                    "P.O. Box 625737",
                    "Cincinnati",
                    "OH",
                    "45262-5737",
                )),
            ),
            authorized("James Dougherty", "(212) 450-4000", davis_polk()),
            authorized("Shanu Bajaj", "(212) 450-4000", davis_polk()),
        ],
    }
}

fn unifirst_13d() -> sec::Filing {
    real_filing(
        "0000950103-26-003802",
        "SCHEDULE 13D",
        "2026-03-16",
        (2_956_129, 511),
        ("0000717954", "UNIFIRST CORP"),
        Body::Beneficial(unifirst_13d_body()),
    )
}

/// SCHEDULE 13G/A `0001172661-26-003444` (Broad Bay on Atlanta Braves, window
/// 2977838): `amended_accession`, Rule 13d-1(b), Items 3–10.
fn braves_13g_amendment() -> sec::Filing {
    let body = sec::BeneficialOwnershipReport {
        subject_company_cik: s("0001958140"),
        subject_company_name: s("ATLANTA BRAVES HOLDINGS, INC."),
        cusip: s("047726302"),
        schedule_type: s("SCHEDULE 13G/A"),
        filer_cik: s("0001759115"),
        securities_class_title: s("Series C Common Stock"),
        event_date: s("06/30/2026"),
        issuer_address: Some(address(
            "755 Battery Avenue SE",
            "",
            "Atlanta",
            "GA",
            "30339",
        )),
        rules_designated: strings(&["Rule 13d-1(b)"]),
        reporting_persons: vec![sec::ReportingPerson {
            name: s("Broad Bay Capital Management, LP"),
            citizenship: s("DE"),
            sole_voting_power: s("2046239.00"),
            shared_voting_power: s("0.00"),
            sole_dispositive_power: s("2046239.00"),
            shared_dispositive_power: s("0.00"),
            aggregate_amount_owned: s("2046239.00"),
            percent_of_class: s("3.87"),
            type_of_reporting_person: s("IA"),
            aggregate_excludes_shares: Some(false),
            types_of_reporting_person: strings(&["IA"]),
            ..Default::default()
        }],
        cusips: strings(&["047726302"]),
        previous_accession_number: s("0001172661-26-002059"),
        amendment_number: s("1"),
        items_13g: Some(sec::Schedule13gItems {
            type_of_person_filing: strings(&["IA"]),
            amount_beneficially_owned: s("2,046,239"),
            class_percent: s("3.87%"),
            sole_voting_power: s("2,046,239"),
            shared_voting_power: s("0"),
            sole_dispositive_power: s("2,046,239"),
            shared_dispositive_power: s("0"),
            class_ownership_5_percent_or_less: Some(true),
            certifications: s("By signing below I certify that ..."),
            issuer_name: s("ATLANTA BRAVES HOLDINGS, INC."),
            issuer_principal_office_address: s("755 Battery Avenue SE, Atlanta, Georgia 30339"),
            filing_person_name: s("Broad Bay Capital Management, LP"),
            principal_business_office_address: s("920 Broadway, Fl 9, New York, NY 10010"),
            citizenship: s("Delaware USA"),
            ..Default::default()
        }),
        signatures: vec![sec::BeneficialSignature {
            reporting_person: s("Broad Bay Capital Management, LP"),
            signature: s("Richard Scott Greeder"),
            title: s("Richard Scott Greeder/Controlling Limited Partner"),
            date: s("08/14/2026"),
        }],
        ..Default::default()
    };
    sec::Filing {
        amended_accession: s("0001172661-26-002059"),
        ..real_filing(
            "0001172661-26-003444",
            "SCHEDULE 13G/A",
            "2026-08-14",
            (2_977_838, 280),
            ("0001958140", "Atlanta Braves Holdings, Inc."),
            Body::Beneficial(body),
        )
    }
}

/// Legacy SC 13G/A `0001140361-14-031610` (VOC Energy Trust, window 2346252):
/// metadata only.
fn voc_legacy_13g() -> sec::Filing {
    let body = sec::BeneficialOwnershipReport {
        subject_company_cik: s("0001505413"),
        subject_company_name: s("VOC Energy Trust"),
        schedule_type: s("SC 13G/A"),
        ..Default::default()
    };
    sec::Filing {
        primary_document: String::new(),
        group_members: strings(&["RICHARD A. KAYNE"]),
        ..real_filing(
            "0001140361-14-031610",
            "SC 13G/A",
            "2014-08-11",
            (2_346_252, 280),
            ("0001505413", "VOC Energy Trust"),
            Body::Beneficial(body),
        )
    }
}

// ---------------------------------------------------------------------------
// 13D/G tests
// ---------------------------------------------------------------------------

#[test]
fn beneficial_13d_with_authorized_persons_and_items() {
    let batches = map(&[window_block(2_956_129, vec![unifirst_13d()])]);
    batches.assert_rows(&[
        ("beneficial_reports", 1),
        ("beneficial_reporting_persons", 1),
        ("parse_issues", 0),
    ]);
    assert_fc(&batches, "beneficial_reports", 0, 0);
    assert_eq!(
        batches.cell("beneficial_reports", "acceptance_datetime", 0),
        "2026-03-16T16:18:31Z"
    );
    let persons = "[\
        {name: Scott A. Garula, phone: (513) 459-1200, street1: 6800 Cintas Boulevard, \
         street2: P.O. Box 625737, city: Cincinnati, state: OH, zip_code: 45262-5737, \
         state_description: NULL, country: NULL, non_us_state_territory: NULL}, \
        {name: James Dougherty, phone: (212) 450-4000, street1: Davis Polk & Wardwell LLP, \
         street2: 450 Lexington Avenue, city: New York, state: NY, zip_code: 10017, \
         state_description: NULL, country: NULL, non_us_state_territory: NULL}, \
        {name: Shanu Bajaj, phone: (212) 450-4000, street1: Davis Polk & Wardwell LLP, \
         street2: 450 Lexington Avenue, city: New York, state: NY, zip_code: 10017, \
         state_description: NULL, country: NULL, non_us_state_territory: NULL}]";
    assert_columns(
        &batches,
        "beneficial_reports",
        0,
        &[
            ("subject_company_cik", "0000717954"),
            ("subject_company_name", "UNIFIRST CORP"),
            ("cusip", "904708104"),
            ("cusips", "[904708104]"),
            ("cusip_norm", "904708104"),
            ("schedule_type", "SCHEDULE 13D"),
            ("schedule_kind", "13D"),
            ("filer_cik", "0000723254"),
            ("securities_class_title", "Common Stock"),
            ("event_date", "2026-03-10"),
            ("issuer_street1", "68 JONSPIN RD"),
            ("issuer_street2", "NULL"),
            ("issuer_city", "WILMINGTON"),
            ("issuer_state", "MA"),
            ("issuer_zip_code", "01887"),
            ("issuer_state_description", "NULL"),
            ("issuer_country", "NULL"),
            ("issuer_non_us_state_territory", "NULL"),
            ("rules_designated", "[]"),
            ("previous_accession_number", "NULL"),
            ("amendment_number", "NULL"),
            ("previously_filed", "false"),
            ("item13d_security_title", "Common Stock"),
            ("item13d_issuer_name", "UNIFIRST CORP"),
            ("item13d_issuer_principal_street1", "68 JONSPIN RD"),
            ("item13d_issuer_principal_street2", "NULL"),
            ("item13d_issuer_principal_city", "WILMINGTON"),
            ("item13d_issuer_principal_state", "MA"),
            ("item13d_issuer_principal_zip_code", "01887"),
            ("item13d_issuer_principal_state_description", "NULL"),
            ("item13d_issuer_principal_country", "NULL"),
            ("item13d_issuer_principal_non_us_state_territory", "NULL"),
            (
                "item13d_item1_comment",
                "This statement relates to shares of Common Stock.",
            ),
            (
                "item13d_filing_person_name",
                "The information set forth in response to each separate Item.",
            ),
            (
                "item13d_principal_business_address",
                "6800 Cintas Boulevard, Cincinnati, Ohio 45262-5737.",
            ),
            ("item13d_principal_job", "Rental and servicing of uniforms."),
            (
                "item13d_has_been_convicted",
                "During the last five years, none.",
            ),
            ("item13d_conviction_description", "No civil proceeding."),
            ("item13d_citizenship", "Washington"),
            (
                "item13d_funds_source",
                "Items 4 and 5 are incorporated by reference.",
            ),
            (
                "item13d_transaction_purpose",
                "The purpose of the Mergers is to acquire control.",
            ),
            ("item13d_percentage_of_class", "18.7%"),
            ("item13d_number_of_shares", "3,374,968"),
            ("item13d_transaction_description", "None."),
            ("item13d_list_of_shareholders", "No other person."),
            ("item13d_date_5_percent_ownership", "Not applicable."),
            ("item13d_contract_description", "Voting Agreements."),
            (
                "item13d_filed_exhibits",
                "Exhibit 1 Agreement and Plan of Merger",
            ),
            // No 13G items: NULL, the list `[]`, the optional bool NULL.
            ("item13g_type_of_person_filing", "[]"),
            ("item13g_other_type_of_person_filing", "NULL"),
            ("item13g_amount_beneficially_owned", "NULL"),
            ("item13g_class_percent", "NULL"),
            ("item13g_sole_voting_power", "NULL"),
            ("item13g_shared_voting_power", "NULL"),
            ("item13g_sole_dispositive_power", "NULL"),
            ("item13g_shared_dispositive_power", "NULL"),
            ("item13g_class_ownership_5_percent_or_less", "NULL"),
            ("item13g_ownership_on_behalf_of_another", "NULL"),
            ("item13g_subsidiary_identification", "NULL"),
            ("item13g_group_members_identification", "NULL"),
            ("item13g_group_dissolution_notice", "NULL"),
            ("item13g_certifications", "NULL"),
            ("item13g_issuer_name", "NULL"),
            ("item13g_issuer_principal_office_address", "NULL"),
            ("item13g_filing_person_name", "NULL"),
            ("item13g_principal_business_office_address", "NULL"),
            ("item13g_citizenship", "NULL"),
            ("exhibit_info", "NULL"),
            ("signature_comments", "NULL"),
            ("authorized_persons", persons),
            ("reporting_person_count", "1"),
            ("max_aggregate_amount_owned", "3374968.000000"),
            ("max_percent_of_class", "18.700000000000"),
            ("signature_count", "1"),
            ("has_parse_issues", "false"),
        ],
    );

    assert_fc(&batches, "beneficial_reporting_persons", 0, 0);
    assert_columns(
        &batches,
        "beneficial_reporting_persons",
        0,
        &[
            ("subject_company_cik", "0000717954"),
            ("subject_company_name", "UNIFIRST CORP"),
            ("cusip_norm", "904708104"),
            ("schedule_kind", "13D"),
            ("event_date", "2026-03-10"),
            ("person_index", "0"),
            ("person_cik", "0000723254"),
            ("person_name", "Cintas Corp"),
            // 13D optional bools set to false: false, not NULL.
            ("no_cik", "false"),
            ("member_of_group", "NULL"),
            ("fund_type", "OO"),
            ("fund_types", "[OO]"),
            ("citizenship", "WA"),
            ("sole_voting_power", "0.000000"),
            ("shared_voting_power", "3374968.000000"),
            ("sole_dispositive_power", "0.000000"),
            ("shared_dispositive_power", "0.000000"),
            ("aggregate_amount_owned", "3374968.000000"),
            ("aggregate_excludes_shares", "false"),
            ("percent_of_class", "18.700000000000"),
            ("type_of_reporting_person", "CO"),
            ("types_of_reporting_person", "[CO]"),
            ("legal_proceedings", "false"),
            ("comment", "Rows 8, 11 and 13."),
            ("has_parse_issues", "false"),
        ],
    );
}

#[test]
fn beneficial_13g_amendment() {
    let batches = map(&[window_block(2_977_838, vec![braves_13g_amendment()])]);
    batches.assert_rows(&[
        ("beneficial_reports", 1),
        ("beneficial_reporting_persons", 1),
        ("parse_issues", 0),
    ]);
    assert_eq!(
        batches.cell("filings", "amended_accession", 0),
        "0001172661-26-002059"
    );
    assert_fc(&batches, "beneficial_reports", 0, 0);
    assert_columns(
        &batches,
        "beneficial_reports",
        0,
        &[
            ("subject_company_cik", "0001958140"),
            ("subject_company_name", "ATLANTA BRAVES HOLDINGS, INC."),
            ("cusip", "047726302"),
            ("cusips", "[047726302]"),
            ("cusip_norm", "047726302"),
            ("schedule_type", "SCHEDULE 13G/A"),
            ("schedule_kind", "13G"),
            ("filer_cik", "0001759115"),
            ("securities_class_title", "Series C Common Stock"),
            ("event_date", "2026-06-30"),
            ("issuer_street1", "755 Battery Avenue SE"),
            ("issuer_street2", "NULL"),
            ("issuer_city", "Atlanta"),
            ("issuer_state", "GA"),
            ("issuer_zip_code", "30339"),
            ("issuer_state_description", "NULL"),
            ("issuer_country", "NULL"),
            ("issuer_non_us_state_territory", "NULL"),
            ("rules_designated", "[Rule 13d-1(b)]"),
            ("previous_accession_number", "0001172661-26-002059"),
            ("amendment_number", "1"),
            ("previously_filed", "NULL"),
            // No 13D items: every item13d column is NULL.
            ("item13d_security_title", "NULL"),
            ("item13d_issuer_name", "NULL"),
            ("item13d_issuer_principal_street1", "NULL"),
            ("item13d_issuer_principal_street2", "NULL"),
            ("item13d_issuer_principal_city", "NULL"),
            ("item13d_issuer_principal_state", "NULL"),
            ("item13d_issuer_principal_zip_code", "NULL"),
            ("item13d_issuer_principal_state_description", "NULL"),
            ("item13d_issuer_principal_country", "NULL"),
            ("item13d_issuer_principal_non_us_state_territory", "NULL"),
            ("item13d_item1_comment", "NULL"),
            ("item13d_filing_person_name", "NULL"),
            ("item13d_principal_business_address", "NULL"),
            ("item13d_principal_job", "NULL"),
            ("item13d_has_been_convicted", "NULL"),
            ("item13d_conviction_description", "NULL"),
            ("item13d_citizenship", "NULL"),
            ("item13d_funds_source", "NULL"),
            ("item13d_transaction_purpose", "NULL"),
            ("item13d_percentage_of_class", "NULL"),
            ("item13d_number_of_shares", "NULL"),
            ("item13d_transaction_description", "NULL"),
            ("item13d_list_of_shareholders", "NULL"),
            ("item13d_date_5_percent_ownership", "NULL"),
            ("item13d_contract_description", "NULL"),
            ("item13d_filed_exhibits", "NULL"),
            ("item13g_type_of_person_filing", "[IA]"),
            ("item13g_other_type_of_person_filing", "NULL"),
            ("item13g_amount_beneficially_owned", "2,046,239"),
            ("item13g_class_percent", "3.87%"),
            ("item13g_sole_voting_power", "2,046,239"),
            ("item13g_shared_voting_power", "0"),
            ("item13g_sole_dispositive_power", "2,046,239"),
            ("item13g_shared_dispositive_power", "0"),
            ("item13g_class_ownership_5_percent_or_less", "true"),
            ("item13g_ownership_on_behalf_of_another", "NULL"),
            ("item13g_subsidiary_identification", "NULL"),
            ("item13g_group_members_identification", "NULL"),
            ("item13g_group_dissolution_notice", "NULL"),
            (
                "item13g_certifications",
                "By signing below I certify that ...",
            ),
            ("item13g_issuer_name", "ATLANTA BRAVES HOLDINGS, INC."),
            (
                "item13g_issuer_principal_office_address",
                "755 Battery Avenue SE, Atlanta, Georgia 30339",
            ),
            (
                "item13g_filing_person_name",
                "Broad Bay Capital Management, LP",
            ),
            (
                "item13g_principal_business_office_address",
                "920 Broadway, Fl 9, New York, NY 10010",
            ),
            ("item13g_citizenship", "Delaware USA"),
            ("exhibit_info", "NULL"),
            ("signature_comments", "NULL"),
            ("authorized_persons", "[]"),
            ("reporting_person_count", "1"),
            ("max_aggregate_amount_owned", "2046239.000000"),
            ("max_percent_of_class", "3.870000000000"),
            ("signature_count", "1"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_fc(&batches, "beneficial_reporting_persons", 0, 0);
    assert_columns(
        &batches,
        "beneficial_reporting_persons",
        0,
        &[
            ("subject_company_cik", "0001958140"),
            ("subject_company_name", "ATLANTA BRAVES HOLDINGS, INC."),
            ("cusip_norm", "047726302"),
            ("schedule_kind", "13G"),
            ("event_date", "2026-06-30"),
            ("person_index", "0"),
            // 13G persons have no CIK and leave the 13D-only flags unset.
            ("person_cik", "NULL"),
            ("person_name", "Broad Bay Capital Management, LP"),
            ("no_cik", "NULL"),
            ("member_of_group", "NULL"),
            ("fund_type", "NULL"),
            ("fund_types", "[]"),
            ("citizenship", "DE"),
            ("sole_voting_power", "2046239.000000"),
            ("shared_voting_power", "0.000000"),
            ("sole_dispositive_power", "2046239.000000"),
            ("shared_dispositive_power", "0.000000"),
            ("aggregate_amount_owned", "2046239.000000"),
            ("aggregate_excludes_shares", "false"),
            ("percent_of_class", "3.870000000000"),
            ("type_of_reporting_person", "IA"),
            ("types_of_reporting_person", "[IA]"),
            ("legal_proceedings", "NULL"),
            ("comment", "NULL"),
            ("has_parse_issues", "false"),
        ],
    );
}

#[test]
fn beneficial_legacy_13g_is_metadata_only() {
    let batches = map(&[window_block(2_346_252, vec![voc_legacy_13g()])]);
    batches.assert_rows(&[
        ("beneficial_reports", 1),
        ("beneficial_reporting_persons", 0),
        ("parse_issues", 0),
    ]);
    assert_fc(&batches, "beneficial_reports", 0, 0);
    let mut expected: Vec<(&str, &str)> = vec![
        ("subject_company_cik", "0001505413"),
        ("subject_company_name", "VOC Energy Trust"),
        ("cusip", "NULL"),
        ("cusips", "[]"),
        ("cusip_norm", "NULL"),
        ("schedule_type", "SC 13G/A"),
        ("schedule_kind", "13G"),
    ];
    // Every other column: NULL, `[]` for lists, 0 for counts.
    let schema = batches.table("beneficial_reports").schema();
    for field in schema.fields().iter().skip(PREFIX_COLUMNS + expected.len()) {
        let value = match field.name().as_str() {
            "rules_designated" | "item13g_type_of_person_filing" | "authorized_persons" => "[]",
            "reporting_person_count" | "signature_count" => "0",
            "has_parse_issues" => "false",
            _ => "NULL",
        };
        expected.push((field.name().as_str(), value));
    }
    assert_columns(&batches, "beneficial_reports", 0, &expected);
}

#[test]
fn beneficial_schedule_kind_falls_back_to_the_form_type() {
    let report = |form_type: &str, schedule_type: &str| {
        filing(
            form_type,
            Body::Beneficial(sec::BeneficialOwnershipReport {
                schedule_type: s(schedule_type),
                ..Default::default()
            }),
        )
    };
    let batches = map(&[window_block(
        BLOCK_NUM,
        vec![
            report("SC 13D/A", ""),
            report("SCHEDULE 13G", ""),
            report("SCHEDULE 13G", "SC 13D"),
            report("SC 13D", "SC TO-T"),
            report("SC 14D9", ""),
        ],
    )]);
    assert_eq!(
        batches.column("beneficial_reports", "schedule_kind"),
        ["13D", "13G", "13D", "NULL", "NULL"]
    );
    assert_eq!(
        batches.column("beneficial_reports", "schedule_type"),
        ["NULL", "NULL", "SC 13D", "SC TO-T", "NULL"]
    );
}

#[test]
fn beneficial_persons_typed_values_maxima_and_issues() {
    let person = |name: &str, aggregate: &str, percent: &str| sec::ReportingPerson {
        name: s(name),
        aggregate_amount_owned: s(aggregate),
        percent_of_class: s(percent),
        ..Default::default()
    };
    let joint = sec::ReportingPerson {
        sole_voting_power: s("1,000"),
        shared_voting_power: s(".5"),
        sole_dispositive_power: s("+3.25"),
        shared_dispositive_power: s("5."),
        member_of_group: s("a"),
        no_cik: Some(true),
        legal_proceedings: Some(true),
        aggregate_excludes_shares: Some(true),
        fund_types: strings(&["WC", "AF"]),
        fund_type: s("WC"),
        ..person("JOINT", "250.5", "7.12345678901249")
    };
    let body = sec::BeneficialOwnershipReport {
        cusip: s("90470 8104"),
        cusips: strings(&["90470 8104", "904708203"]),
        event_date: s("2026-02-30"),
        amendment_number: s("N/A"),
        reporting_persons: vec![
            person("FUND", "100", "5.1"),
            joint,
            person("NONE", "N/A", ""),
        ],
        authorized_persons: vec![authorized("NO ADDRESS", "", None)],
        ..Default::default()
    };
    let batches = map(&[window_block(
        BLOCK_NUM,
        vec![filing("SCHEDULE 13D/A", Body::Beneficial(body))],
    )]);
    let report = |column: &str| batches.cell("beneficial_reports", column, 0);
    assert_eq!(report("cusip"), "90470 8104");
    assert_eq!(report("cusips"), "[90470 8104, 904708203]");
    assert_eq!(report("cusip_norm"), "904708104");
    assert_eq!(report("schedule_kind"), "13D");
    assert_eq!(report("event_date"), "NULL");
    assert_eq!(report("amendment_number"), "NULL");
    assert_eq!(report("reporting_person_count"), "3");
    assert_eq!(report("signature_count"), "0");
    // Max over the non-NULL typed values (the rounded percent included).
    assert_eq!(report("max_aggregate_amount_owned"), "250.500000");
    assert_eq!(report("max_percent_of_class"), "7.123456789012");
    assert_eq!(report("has_parse_issues"), "true");
    assert_eq!(
        report("authorized_persons"),
        "[{name: NO ADDRESS, phone: NULL, street1: NULL, street2: NULL, city: NULL, \
         state: NULL, zip_code: NULL, state_description: NULL, country: NULL, \
         non_us_state_territory: NULL}]"
    );

    let column = |name: &str| batches.column("beneficial_reporting_persons", name);
    assert_eq!(column("person_index"), ["0", "1", "2"]);
    assert_eq!(column("person_name"), ["FUND", "JOINT", "NONE"]);
    assert_eq!(column("cusip_norm"), ["904708104"; 3]);
    assert_eq!(column("schedule_kind"), ["13D"; 3]);
    assert_eq!(column("event_date"), ["NULL"; 3]);
    assert_eq!(
        column("aggregate_amount_owned"),
        ["100.000000", "250.500000", "NULL"]
    );
    assert_eq!(
        column("percent_of_class"),
        ["5.100000000000", "7.123456789012", "NULL"]
    );
    assert_eq!(column("sole_voting_power"), ["NULL"; 3]);
    assert_eq!(column("shared_voting_power"), ["NULL", "0.500000", "NULL"]);
    assert_eq!(
        column("sole_dispositive_power"),
        ["NULL", "3.250000", "NULL"]
    );
    assert_eq!(
        column("shared_dispositive_power"),
        ["NULL", "5.000000", "NULL"]
    );
    assert_eq!(column("member_of_group"), ["NULL", "a", "NULL"]);
    assert_eq!(column("no_cik"), ["NULL", "true", "NULL"]);
    assert_eq!(column("legal_proceedings"), ["NULL", "true", "NULL"]);
    assert_eq!(
        column("aggregate_excludes_shares"),
        ["NULL", "true", "NULL"]
    );
    assert_eq!(column("fund_type"), ["NULL", "WC", "NULL"]);
    assert_eq!(column("fund_types"), ["[]", "[WC, AF]", "[]"]);
    assert_eq!(column("has_parse_issues"), ["false", "true", "true"]);

    assert_eq!(
        issue_tuples(&batches),
        [
            issue(
                "beneficial_reports",
                "event_date",
                [None; 3],
                "2026-02-30",
                "out_of_range"
            ),
            issue(
                "beneficial_reports",
                "amendment_number",
                [None; 3],
                "N/A",
                "sentinel"
            ),
            issue(
                "beneficial_reporting_persons",
                "sole_voting_power",
                [Some(1), None, None],
                "1,000",
                "unparseable"
            ),
            issue(
                "beneficial_reporting_persons",
                "percent_of_class",
                [Some(1), None, None],
                "7.12345678901249",
                "rounded"
            ),
            issue(
                "beneficial_reporting_persons",
                "aggregate_amount_owned",
                [Some(2), None, None],
                "N/A",
                "sentinel"
            ),
        ]
    );
}

#[test]
fn beneficial_maxima_are_null_without_typed_values() {
    let body = sec::BeneficialOwnershipReport {
        reporting_persons: vec![sec::ReportingPerson {
            percent_of_class: s("N/A"),
            ..Default::default()
        }],
        ..Default::default()
    };
    let batches = map(&[window_block(
        BLOCK_NUM,
        vec![filing("SCHEDULE 13G", Body::Beneficial(body))],
    )]);
    let report = |column: &str| batches.cell("beneficial_reports", column, 0);
    assert_eq!(report("reporting_person_count"), "1");
    assert_eq!(report("max_aggregate_amount_owned"), "NULL");
    assert_eq!(report("max_percent_of_class"), "NULL");
    // The issue is the person's, not the report's.
    assert_eq!(report("has_parse_issues"), "false");
    assert_eq!(
        batches.cell("beneficial_reporting_persons", "has_parse_issues", 0),
        "true"
    );
}

// ---------------------------------------------------------------------------
// Several filings, options
// ---------------------------------------------------------------------------

#[test]
fn rows_follow_filing_order_and_carry_their_filing_context() {
    // 13F and 13D/G filings interleaved with another body: every child row
    // carries its own filing's context, positions restart per filing.
    let filings = vec![
        atairos(),
        filing("4", Body::Ownership(sec::OwnershipDocument::default())),
        braves_13g_amendment(),
        symetra(),
    ];
    let batches = map(&[window_block(2_977_836, filings)]);
    assert_eq!(
        batches.column("form13f_reports", "filing_index"),
        ["0", "3"]
    );
    assert_eq!(
        batches.column("form13f_other_managers", "filing_index"),
        ["0", "0", "0", "3", "3"]
    );
    assert_eq!(
        batches.column("form13f_other_managers", "other_manager_index"),
        ["0", "1", "2", "0", "1"]
    );
    assert_eq!(
        batches.column("form13f_holdings", "filing_index"),
        ["0", "0", "0", "0", "0", "3", "3"]
    );
    assert_eq!(
        batches.column("form13f_holdings", "holding_index"),
        ["0", "1", "2", "3", "4", "0", "1"]
    );
    assert_eq!(
        batches.column("form13f_holdings", "manager_cik"),
        [
            "0001671122",
            "0001671122",
            "0001671122",
            "0001671122",
            "0001671122",
            "0001811513",
            "0001811513"
        ]
    );
    assert_fc(&batches, "form13f_other_managers", 4, 3);
    assert_fc(&batches, "form13f_holdings", 6, 3);
    assert_fc(&batches, "beneficial_reports", 0, 2);
    assert_fc(&batches, "beneficial_reporting_persons", 0, 2);
    assert_eq!(batches.column("beneficial_reports", "filing_index"), ["2"]);
}

#[test]
fn fork_step_columns_come_last() {
    let batches = map_with(
        &[window_block(BLOCK_NUM, contract_filings())],
        true,
        EncodeBytes::Hex,
    );
    for table in TABLES {
        let schema = batches.table(table).schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names[names.len() - 2..],
            ["fork_step", "stream_ordinal"],
            "{table}"
        );
        assert!(batches.rows(table) > 0, "{table}");
    }
}

// ---------------------------------------------------------------------------
// Real data, local only
// ---------------------------------------------------------------------------

/// The §8.7 13F and 13D/G accessions in their real sample blocks.
/// `cargo test -p blocks --lib sec::tests::form13f_beneficial::real -- --ignored`
#[test]
#[ignore = "local: needs /tmp/sec-fireparq/fire-v013"]
fn real_fixture_filings() {
    let map_real = |day: &str, block_num: u64| {
        let (identity, block) = fire::block(day, block_num);
        let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
        mapper
            .map_block(&block.encode_to_vec(), &identity, StreamEvent::default())
            .unwrap();
        Batches::new(mapper.flush().unwrap())
    };
    let rows_of = |batches: &Batches, table: &str, accession: &str| -> Vec<usize> {
        let accessions = batches.column(table, "accession_number");
        (0..accessions.len())
            .filter(|&row| accessions[row] == accession)
            .collect()
    };

    let b = map_real("2026-08-14", 2_977_836);
    let atairos = "0000950103-26-012367";
    let report = rows_of(&b, "form13f_reports", atairos)[0];
    let cell = |column: &str| b.cell("form13f_reports", column, report);
    assert_eq!(cell("holdings_value_sum"), "1755698676");
    assert_eq!(cell("holdings_complete"), "true");
    let sequence: Vec<String> = rows_of(&b, "form13f_other_managers", atairos)
        .into_iter()
        .map(|row| b.cell("form13f_other_managers", "sequence_number", row))
        .collect();
    assert_eq!(sequence, ["1", "2", "3"]);
    for row in rows_of(&b, "form13f_holdings", atairos) {
        assert_eq!(
            b.cell("form13f_holdings", "other_manager_sequence_numbers", row),
            "[1, 2, 3]"
        );
    }

    let b = map_real("2014-08-11", 2_346_308);
    let report = rows_of(&b, "form13f_reports", "0000950123-14-008258")[0];
    assert_eq!(
        b.cell("form13f_reports", "value_multiplier_rule", report),
        "1000"
    );

    let b = map_real("2014-08-11", 2_346_276);
    let notice = "0001323255-14-000015";
    let report = rows_of(&b, "form13f_reports", notice)[0];
    assert_eq!(
        b.cell("form13f_reports", "has_summary_page", report),
        "false"
    );
    let kinds: Vec<String> = rows_of(&b, "form13f_other_managers", notice)
        .into_iter()
        .map(|row| b.cell("form13f_other_managers", "list_kind", row))
        .collect();
    assert_eq!(kinds, ["cover", "cover"]);

    let b = map_real("2026-08-14", 2_977_776);
    let kinds: Vec<String> = rows_of(&b, "form13f_other_managers", "0001811513-26-000014")
        .into_iter()
        .map(|row| b.cell("form13f_other_managers", "list_kind", row))
        .collect();
    assert_eq!(kinds, ["cover", "summary"]);

    let b = map_real("2026-08-14", 2_977_896);
    assert_eq!(
        rows_of(&b, "form13f_holdings", "0001104659-26-097111").len(),
        1
    );

    let b = map_real("2026-08-14", 2_977_880);
    let report = rows_of(&b, "form13f_reports", "0001595082-26-000063")[0];
    assert_eq!(
        b.cell("form13f_reports", "amendment_type", report),
        "NEW HOLDINGS"
    );

    let b = map_real("2026-03-16", 2_956_129);
    let report = rows_of(&b, "beneficial_reports", "0000950103-26-003802")[0];
    let cell = |column: &str| b.cell("beneficial_reports", column, report);
    assert_eq!(cell("schedule_kind"), "13D");
    assert!(
        cell("authorized_persons").starts_with("[{name: Scott A. Garula, phone: (513) 459-1200")
    );
    assert_eq!(cell("max_percent_of_class"), "18.700000000000");

    let b = map_real("2026-08-14", 2_977_838);
    let report = rows_of(&b, "beneficial_reports", "0001172661-26-003444")[0];
    assert_eq!(
        b.cell("beneficial_reports", "previous_accession_number", report),
        "0001172661-26-002059"
    );

    let b = map_real("2014-08-11", 2_346_252);
    let report = rows_of(&b, "beneficial_reports", "0001140361-14-031610")[0];
    let cell = |column: &str| b.cell("beneficial_reports", column, report);
    assert_eq!(cell("reporting_person_count"), "0");
    assert_eq!(cell("schedule_kind"), "13G");
}

/// This group's tables equal the prototype's on every sample day present
/// locally, and none of them logs a `parse_issues` row (the prototype logs
/// none for them either).
/// `cargo test -p blocks --lib --release sec::tests::form13f_beneficial::matches -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn matches_the_prototype() {
    oracle::assert_matches(&fire::DAYS, &TABLES, &[]);
    for day in fire::DAYS {
        if !fire::path(day).exists() {
            continue;
        }
        let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
        let mut ours = 0;
        let mut blocks = fire::blocks(day).peekable();
        while let Some((identity, payload)) = blocks.next() {
            mapper
                .map_block_bytes(payload.into(), &identity, StreamEvent::default())
                .unwrap();
            if mapper.total_rows() < 500_000 && blocks.peek().is_some() {
                continue;
            }
            let batches = Batches::new(mapper.flush().unwrap());
            ours += batches
                .column("parse_issues", "table_name")
                .iter()
                .filter(|table| TABLES.contains(&table.as_str()))
                .count();
        }
        println!("{day}: {ours} parse_issues rows in this group's tables");
        assert_eq!(ours, 0, "{day}");
    }
}
