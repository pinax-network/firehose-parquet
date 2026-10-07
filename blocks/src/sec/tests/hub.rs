//! Value tests of the hub tables `blocks` and `filings`, of the filing context
//! [FC], and of `parse_issues` plumbing.

use super::*;

/// The hub's filings in the contract fixture: a raw body, an EDGAR deletion
/// notice and a filing with an unparseable `filing_date` and a sentinel
/// `period_of_report` (two `parse_issues` rows).
pub(crate) fn contract_filings() -> Vec<sec::Filing> {
    vec![
        sec::Filing {
            filing_date: "2026-08-2x".to_string(),
            period_of_report: "N/A".to_string(),
            ..filing(
                "N-PX",
                Body::Raw(sec::RawFiling {
                    reason: "no_xml".to_string(),
                    detail: String::new(),
                }),
            )
        },
        deletion_notice(),
    ]
}

fn deletion_notice() -> sec::Filing {
    sec::Filing {
        accession_number: "0002147005-26-000004".to_string(),
        dissemination_flags: vec!["CORRECTION".to_string(), "DELETION".to_string()],
        dissemination_timestamp: "20260827:173012".to_string(),
        ..filing(
            "4",
            Body::Raw(sec::RawFiling {
                reason: "deletion".to_string(),
                detail: String::new(),
            }),
        )
    }
}

fn party(role: &str, cik: &str, name: &str) -> sec::FilingParty {
    sec::FilingParty {
        role: role.to_string(),
        cik: cik.to_string(),
        name: name.to_string(),
        ..Default::default()
    }
}

#[test]
fn blocks_row_carries_feed_date_count_and_issue_flag() {
    let mut bad_feed = window_block(BLOCK_NUM + 1, vec![]);
    bad_feed.header.as_mut().unwrap().feed_date = "2026/08/28".to_string();
    let batches = map(&[window_block(BLOCK_NUM, contract_filings()), bad_feed]);
    assert_eq!(
        batches.column("blocks", "feed_date"),
        ["2026-08-28", "NULL"]
    );
    assert_eq!(batches.column("blocks", "filing_count"), ["2", "0"]);
    assert_eq!(
        batches.column("blocks", "has_parse_issues"),
        ["false", "true"]
    );
    assert_eq!(
        batches.column("blocks", "block_id"),
        [BLOCK_NUM.to_string(), (BLOCK_NUM + 1).to_string()]
    );
    assert_eq!(
        batches.cell("blocks", "timestamp", 0),
        "2026-08-28T12:30:00Z"
    );

    let issues = batches.issues();
    let block_issue = issues
        .iter()
        .find(|issue| issue.table == "blocks")
        .expect("feed_date issue");
    assert_eq!(block_issue.block_num, BLOCK_NUM + 1);
    assert_eq!(block_issue.filing_index, None);
    assert_eq!(block_issue.accession_number, None);
    assert_eq!(block_issue.column, "feed_date");
    assert_eq!(block_issue.index, [None, None, None]);
    assert_eq!(block_issue.raw, "2026/08/28");
    assert_eq!(block_issue.issue, "unparseable");

    let filing_issues: Vec<(&str, &str, &str)> = issues
        .iter()
        .filter(|issue| issue.table == "filings")
        .map(|issue| {
            (
                issue.column.as_str(),
                issue.raw.as_str(),
                issue.issue.as_str(),
            )
        })
        .collect();
    assert_eq!(
        filing_issues,
        [
            ("filing_date", "2026-08-2x", "unparseable"),
            ("period_of_report", "N/A", "sentinel"),
        ]
    );
    assert_eq!(
        batches.column("filings", "has_parse_issues"),
        ["true", "false"]
    );
}

#[test]
fn filings_envelope_columns() {
    let filed = sec::Filing {
        accession_number: "0001339459-26-000007".to_string(),
        cik_role: "ISSUER".to_string(),
        period_of_report: "2026-08-25".to_string(),
        amended_accession: String::new(),
        group_members: vec!["MEMBER ONE".to_string(), String::new()],
        raw_xml: b"<ownershipDocument/>".to_vec().into(),
        parties: vec![
            party("REPORTING-OWNER", "0002000001", "LANE RYAN"),
            party("REPORTING-OWNER", "0002000002", "FUND LP"),
            party("ISSUER", "0001000001", "EMPD CORP"),
        ],
        documents: vec![sec::SubmissionDocument::default(); 2],
        ..filing("4/A", Body::Ownership(sec::OwnershipDocument::default()))
    };
    let batches = map(&[window_block(BLOCK_NUM, vec![filed, deletion_notice()])]);
    let cell = |column: &str, row: usize| batches.cell("filings", column, row);

    assert_eq!(cell("filing_index", 0), "0");
    assert_eq!(cell("filing_index", 1), "1");
    assert_eq!(cell("accession_number", 0), "0001339459-26-000007");
    assert_eq!(cell("form_type", 0), "4/A");
    assert_eq!(cell("base_form_type", 0), "4");
    assert_eq!(cell("is_amendment", 0), "true");
    assert_eq!(cell("body_kind", 0), "ownership");
    assert_eq!(cell("body_kind", 1), "raw");
    assert_eq!(cell("cik", 0), "0000000001");
    assert_eq!(cell("cik_role", 0), "ISSUER");
    assert_eq!(cell("cik_role", 1), "NULL");
    assert_eq!(cell("issuer_cik", 0), "0001000001");
    assert_eq!(cell("issuer_name", 0), "EMPD CORP");
    assert_eq!(cell("filer_cik", 0), "0002000001");
    assert_eq!(cell("filer_name", 0), "LANE RYAN");
    assert_eq!(cell("issuer_cik", 1), "NULL");
    assert_eq!(cell("filing_date", 0), "2026-08-27");
    assert_eq!(cell("period_of_report", 0), "2026-08-25");
    assert_eq!(cell("period_of_report", 1), "NULL");
    assert_eq!(cell("acceptance_datetime", 0), "2026-08-28T12:31:00Z");
    assert_eq!(cell("acceptance_in_block_window", 0), "true");
    assert_eq!(cell("dissemination_lag_days", 0), "1");
    assert_eq!(cell("primary_document", 0), "primary_doc.xml");
    assert_eq!(cell("amended_accession", 0), "NULL");
    assert_eq!(cell("dissemination_flags", 0), "[]");
    assert_eq!(cell("dissemination_flags", 1), "[CORRECTION, DELETION]");
    assert_eq!(cell("dissemination_timestamp", 1), "20260827:173012");
    assert_eq!(cell("is_deletion_notice", 0), "false");
    assert_eq!(cell("is_deletion_notice", 1), "true");
    assert_eq!(cell("group_members", 0), "[MEMBER ONE, ]");
    assert_eq!(cell("party_count", 0), "3");
    assert_eq!(cell("document_count", 0), "2");
    assert_eq!(cell("series_count", 0), "0");
    assert_eq!(cell("raw_reason", 0), "NULL");
    assert_eq!(cell("raw_reason", 1), "deletion");
    assert_eq!(cell("raw_detail", 1), "NULL");
    assert_eq!(cell("has_raw_xml", 0), "true");
    assert_eq!(cell("raw_xml_size", 0), "20");
    assert_eq!(cell("has_raw_xml", 1), "false");
    assert_eq!(cell("raw_xml_size", 1), "0");
    assert_eq!(cell("has_parse_issues", 0), "false");
}

#[test]
fn issuer_falls_back_to_the_filer_only_for_self_filed_forms() {
    let filer = vec![party("FILER", "0003000001", "STARTUP INC")];
    let filings = vec![
        sec::Filing {
            parties: filer.clone(),
            ..filing("D/A", Body::FormD(sec::FormDNotice::default()))
        },
        sec::Filing {
            parties: filer.clone(),
            ..filing("C-U", Body::FormC(sec::FormCNotice::default()))
        },
        sec::Filing {
            parties: filer.clone(),
            ..filing("13F-HR", Body::Form13f(sec::Form13fReport::default()))
        },
        sec::Filing {
            parties: vec![
                party("FILED-BY", "0004000001", "HOLDER LP"),
                party("SUBJECT-COMPANY", "0005000001", "TARGET CO"),
            ],
            ..filing(
                "SCHEDULE 13G",
                Body::Beneficial(sec::BeneficialOwnershipReport::default()),
            )
        },
    ];
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    assert_eq!(
        batches.column("filings", "issuer_cik"),
        ["0003000001", "0003000001", "NULL", "0005000001"]
    );
    assert_eq!(
        batches.column("filings", "filer_cik"),
        ["0003000001", "0003000001", "0003000001", "0004000001"]
    );
}

#[test]
fn acceptance_outside_the_window_and_redissemination_lag() {
    let early = sec::Filing {
        filing_date: "2004-06-30".to_string(),
        acceptance_datetime: Some(prost_types::Timestamp {
            seconds: window_seconds(BLOCK_NUM) - 1,
            nanos: 0,
        }),
        ..filing("4", Body::Ownership(sec::OwnershipDocument::default()))
    };
    let at_end = sec::Filing {
        acceptance_datetime: Some(prost_types::Timestamp {
            seconds: window_seconds(BLOCK_NUM) + 600,
            nanos: 0,
        }),
        ..filing("4", Body::Ownership(sec::OwnershipDocument::default()))
    };
    let none = sec::Filing {
        acceptance_datetime: None,
        filing_date: String::new(),
        ..filing("4", Body::Ownership(sec::OwnershipDocument::default()))
    };
    let batches = map(&[window_block(BLOCK_NUM, vec![early, at_end, none])]);
    assert_eq!(
        batches.column("filings", "acceptance_in_block_window"),
        ["false", "false", "NULL"]
    );
    assert_eq!(
        batches.column("filings", "dissemination_lag_days"),
        ["8094", "1", "NULL"]
    );
    assert_eq!(batches.cell("filings", "acceptance_datetime", 2), "NULL");
}

#[test]
fn filing_context_is_copied_from_the_filings_row() {
    // The FC columns of every child table equal the `filings` row; the
    // envelope's raw XML table is the first child the hub fixture fills.
    let batches = map(&[window_block(BLOCK_NUM, envelope::contract_filings())]);
    if batches.rows("filing_raw_xml") > 0 {
        for column in [
            "filing_index",
            "accession_number",
            "form_type",
            "filing_date",
            "acceptance_datetime",
        ] {
            assert_eq!(
                batches.cell("filing_raw_xml", column, 0),
                batches.cell("filings", column, 0),
                "{column}"
            );
        }
    }
}

#[test]
fn filing_issues_are_keyed_and_flag_their_row() {
    let filings = vec![
        filing("4", Body::Ownership(sec::OwnershipDocument::default())),
        sec::Filing {
            filing_date: "2014-08-11-05:00".to_string(),
            period_of_report: "N/A".to_string(),
            ..filing("4", Body::Ownership(sec::OwnershipDocument::default()))
        },
    ];
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    assert_eq!(
        batches.column("filings", "has_parse_issues"),
        ["false", "true"]
    );
    assert_eq!(batches.cell("filings", "filing_date", 1), "2014-08-11");
    let issues = batches.issues();
    assert_eq!(issues.len(), 2);
    assert_eq!(issues[0].filing_index, Some(1));
    assert_eq!(
        issues[0].accession_number.as_deref(),
        Some("0000000000-26-000001")
    );
    assert_eq!(
        (
            issues[0].table.as_str(),
            issues[0].column.as_str(),
            issues[0].issue.as_str()
        ),
        ("filings", "filing_date", "tz_dropped")
    );
    assert_eq!(issues[0].raw, "2014-08-11-05:00");
    assert_eq!(
        (
            issues[1].column.as_str(),
            issues[1].issue.as_str(),
            issues[1].raw.as_str()
        ),
        ("period_of_report", "sentinel", "N/A")
    );
    assert_eq!(issues[1].index, [None, None, None]);
}

/// Real sample blocks: the filings of the deletion-notice window.
#[test]
#[ignore = "local: needs /tmp/sec-fireparq/fire-v013"]
fn real_deletion_notice_window() {
    let (identity, block) = fire::block("2026-08-28", 2_979_792);
    let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
    mapper
        .map_block(&block.encode_to_vec(), &identity, StreamEvent::default())
        .unwrap();
    let batches = Batches::new(mapper.flush().unwrap());
    let accessions = batches.column("filings", "accession_number");
    let row = accessions
        .iter()
        .position(|a| a == "0002147005-26-000004")
        .expect("deletion notice");
    assert_eq!(batches.cell("filings", "is_deletion_notice", row), "true");
    assert_eq!(batches.cell("filings", "raw_reason", row), "deletion");
}

/// The hub tables equal the prototype's on every sample day present locally.
/// `cargo test -p blocks --lib sec::tests::hub::hub_matches -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn hub_matches_the_prototype() {
    oracle::assert_matches(&fire::DAYS, &["blocks", "filings"], &[]);
}

/// Every `parse_issues` row equals the prototype's, in order, on every sample
/// day present locally.
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn parse_issues_match_the_prototype() {
    oracle::assert_matches(&fire::DAYS, &["parse_issues"], &[]);
}

/// All 43 tables equal the prototype's, row for row and column for column, on
/// every sample day present locally (one mapping pass per day).
/// `cargo test --release -p blocks --lib sec::tests::hub::all_tables -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn all_tables_match_the_prototype() {
    oracle::assert_matches(&fire::DAYS, &crate::sec::schema::TABLE_NAMES, &[]);
}
