//! Value tests of `filing_raw_xml`, `filing_parties`, `filing_documents`, `filing_series`, `filing_series_classes`, `filing_signatures`.
//! Owned by the `envelope` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The fixtures mirror real 0.13.0 filings of the four sample days (final-spec
//! §8.7 accessions; values copied verbatim from the samples, lists trimmed).
//! The `#[ignore]`d tests at the end compare every row with the prototype on
//! the local sample files.

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
    vec![
        sec::Filing {
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
        },
        // Signatures: a Form C with both of its sources (issuer first).
        filing(
            "C",
            Body::FormC(sec::FormCNotice {
                issuer_signature: Some(form_c_signature(
                    "Kokopelli Outdoor, Inc.",
                    "Patrick Smith",
                    "President",
                    "",
                )),
                person_signatures: vec![form_c_signature(
                    "",
                    "Steven Folse",
                    "Managing Director",
                    "08-12-2026",
                )],
                ..Default::default()
            }),
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

#[test]
fn contract_filings_fill_every_table_of_this_group() {
    let batches = map(&[window_block(BLOCK_NUM, contract_filings())]);
    for table in TABLES {
        assert!(batches.rows(table) > 0, "{table} has no rows");
    }
    // The contract fixture adds no parse issue of this group.
    assert!(batches
        .issues()
        .iter()
        .all(|issue| !TABLES.contains(&issue.table.as_str())));
}

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn text(value: &str) -> String {
    value.to_string()
}

fn address(street1: &str, street2: &str, city: &str, state: &str, zip: &str) -> sec::Address {
    sec::Address {
        street1: text(street1),
        street2: text(street2),
        city: text(city),
        state: text(state),
        zip_code: text(zip),
        ..Default::default()
    }
}

fn former(name: &str, date_changed: &str) -> sec::FormerName {
    sec::FormerName {
        name: text(name),
        date_changed: text(date_changed),
    }
}

fn document(
    sequence: &str,
    kind: &str,
    filename: &str,
    description: &str,
) -> sec::SubmissionDocument {
    sec::SubmissionDocument {
        sequence: text(sequence),
        r#type: text(kind),
        filename: text(filename),
        description: text(description),
    }
}

fn class(class_id: &str, class_name: &str, ticker_symbol: &str) -> sec::FundClass {
    sec::FundClass {
        class_id: text(class_id),
        class_name: text(class_name),
        ticker_symbol: text(ticker_symbol),
    }
}

fn form_c_signature(issuer: &str, signature: &str, title: &str, date: &str) -> sec::FormCSignature {
    sec::FormCSignature {
        issuer: text(issuer),
        signature: text(signature),
        title: text(title),
        date: text(date),
    }
}

/// `filing(form_type, body)` with a real accession.
fn real(accession: &str, form_type: &str, body: Body) -> sec::Filing {
    sec::Filing {
        accession_number: text(accession),
        ..filing(form_type, body)
    }
}

/// Every child row's [FC] columns equal its `filings` row (one block, so the
/// `filings` row number is the `filing_index`).
fn assert_filing_context(batches: &Batches, table: &str) {
    for row in 0..batches.rows(table) {
        let filing_row: usize = batches
            .cell(table, "filing_index", row)
            .parse()
            .expect("filing_index");
        for column in [
            "block_num",
            "block_id",
            "timestamp",
            "date",
            "filing_index",
            "accession_number",
            "form_type",
            "filing_date",
            "acceptance_datetime",
        ] {
            assert_eq!(
                batches.cell(table, column, row),
                batches.cell("filings", column, filing_row),
                "{table}.{column} row {row}"
            );
        }
    }
}

/// The parties of the joint Form 4 `0001339459-26-000007` (2026-08-14, block
/// 2977907): two reporting owners (the first without a business address) and
/// the issuer.
fn thunder_bridge_parties() -> Vec<sec::FilingParty> {
    let great_falls = || {
        Some(address(
            "9912 GEORGETOWN PIKE",
            "SUITE D203",
            "GREAT FALLS",
            "VA",
            "22066",
        ))
    };
    vec![
        sec::FilingParty {
            role: text("REPORTING-OWNER"),
            cik: text("0001339459"),
            name: text("Simanson Gary A"),
            form_type: text("4"),
            act: text("34"),
            file_number: text("001-43446"),
            film_number: text("261284087"),
            mail_address: Some(address("717 KING STREET", "", "ALEXANDRIA", "VA", "22314")),
            ..Default::default()
        },
        sec::FilingParty {
            role: text("REPORTING-OWNER"),
            cik: text("0002142210"),
            name: text("TBCP V, LLC"),
            state_of_incorporation: text("DE"),
            fiscal_year_end: text("1231"),
            form_type: text("4"),
            act: text("34"),
            file_number: text("001-43446"),
            film_number: text("261284086"),
            business_address: great_falls(),
            business_phone: text("(202) 431-0507"),
            mail_address: great_falls(),
            ..Default::default()
        },
        sec::FilingParty {
            role: text("ISSUER"),
            cik: text("0002140030"),
            name: text("Thunder Bridge Capital Partners V, Ltd."),
            assigned_sic: text("6770"),
            organization_name: text("05 Real Estate & Construction"),
            irs_number: text("000000000"),
            state_of_incorporation: text("E9"),
            fiscal_year_end: text("1231"),
            business_address: great_falls(),
            business_phone: text("(202) 431-0507"),
            mail_address: great_falls(),
            ..Default::default()
        },
    ]
}

/// The filer of the 13F-NT `0001323255-14-000015` (2014-08-11, block
/// 2346276): a foreign filer (`P7`) with two former names.
fn abp_party() -> sec::FilingParty {
    let heerlen = || {
        Some(address(
            "OUDE LINDESTRAAT 70",
            "POSTBUS 6401",
            "DL HEERLEN",
            "P7",
            "00000",
        ))
    };
    sec::FilingParty {
        role: text("FILER"),
        cik: text("0000918509"),
        name: text("STICHTING PENSIOENFONDS ABP"),
        irs_number: text("980140331"),
        state_of_incorporation: text("P7"),
        fiscal_year_end: text("1231"),
        form_type: text("13F-NT"),
        act: text("34"),
        file_number: text("028-04817"),
        film_number: text("141029229"),
        business_address: heerlen(),
        business_phone: text("0113145798022"),
        mail_address: heerlen(),
        former_names: vec![
            former("STICHTING PENSIOENFONDS  ABP", "2001-10-11"),
            former("ALGEMEEN BURGERLIJK PENSIOENFONDS", "1994-02-02"),
        ],
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// filing_raw_xml (§3.3)
// ---------------------------------------------------------------------------

#[test]
fn raw_xml_rows_only_for_non_empty_raw_xml() {
    let filings = vec![
        sec::Filing {
            raw_xml: b"<ownershipDocument/>".to_vec().into(),
            primary_document: text("form4a.xml"),
            ..filing("4/A", Body::Ownership(sec::OwnershipDocument::default()))
        },
        // No `--include-raw`: no row.
        filing("4", Body::Ownership(sec::OwnershipDocument::default())),
        sec::Filing {
            raw_xml: vec![0x3c, 0x00, 0xff].into(),
            primary_document: String::new(),
            ..filing("13F-HR", Body::Form13f(sec::Form13fReport::default()))
        },
    ];
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    batches.assert_rows(&[("filings", 3), ("filing_raw_xml", 2)]);
    let t = "filing_raw_xml";
    assert_eq!(batches.column(t, "filing_index"), ["0", "2"]);
    assert_eq!(batches.column(t, "form_type"), ["4/A", "13F-HR"]);
    assert_eq!(
        batches.column(t, "primary_document"),
        ["form4a.xml", "NULL"]
    );
    assert_eq!(
        batches.column(t, "raw_xml"),
        [hex_of(b"<ownershipDocument/>"), "3c00ff".to_string()]
    );
    assert_eq!(
        batches.column("filings", "has_raw_xml"),
        ["true", "false", "true"]
    );
    assert_filing_context(&batches, t);
}

#[test]
fn raw_xml_stays_binary_under_every_id_encoding_and_fork_step_is_last() {
    let filed = sec::Filing {
        raw_xml: b"<x/>".to_vec().into(),
        ..filing("4", Body::Ownership(sec::OwnershipDocument::default()))
    };
    for encoding in [EncodeBytes::Hex, EncodeBytes::Binary] {
        let batches = map_with(
            &[window_block(BLOCK_NUM, vec![filed.clone()])],
            true,
            encoding,
        );
        assert_eq!(
            batches.cell("filing_raw_xml", "raw_xml", 0),
            hex_of(b"<x/>")
        );
        for table in TABLES {
            let schema = batches.table(table).schema();
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(
                names[names.len() - 2..],
                ["fork_step", "stream_ordinal"],
                "{table}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// filing_parties (§3.4)
// ---------------------------------------------------------------------------

#[test]
fn parties_map_every_column_verbatim() {
    let mut parties = thunder_bridge_parties();
    // Every remaining column: LEI, the Forms 3/4/5 and D address members, and
    // former names with a blank name and a blank date.
    parties.push(sec::FilingParty {
        role: text("FILER"),
        lei: text("549300EXAMPLE0000042"),
        business_address: Some(sec::Address {
            state_description: text("ENGLAND"),
            country: text("UNITED KINGDOM"),
            non_us_state_territory: text("LONDON"),
            ..Default::default()
        }),
        former_names: vec![former("", "2001-01-02"), former("OLD NAME", "")],
        ..Default::default()
    });
    let filings = vec![
        real(
            "0001339459-26-000007",
            "4",
            Body::Ownership(Default::default()),
        ),
        sec::Filing {
            parties,
            ..real(
                "0001339459-26-000007",
                "4",
                Body::Ownership(Default::default()),
            )
        },
    ];
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    let t = "filing_parties";
    batches.assert_rows(&[(t, 4)]);
    let col = |column: &str| batches.column(t, column);
    assert_eq!(col("filing_index"), ["1", "1", "1", "1"]);
    assert_eq!(col("party_index"), ["0", "1", "2", "3"]);
    assert_eq!(
        col("role"),
        ["REPORTING-OWNER", "REPORTING-OWNER", "ISSUER", "FILER"]
    );
    assert_eq!(
        col("cik"),
        ["0001339459", "0002142210", "0002140030", "NULL"]
    );
    assert_eq!(
        col("name"),
        [
            "Simanson Gary A",
            "TBCP V, LLC",
            "Thunder Bridge Capital Partners V, Ltd.",
            "NULL"
        ]
    );
    assert_eq!(col("assigned_sic"), ["NULL", "NULL", "6770", "NULL"]);
    assert_eq!(
        col("organization_name"),
        ["NULL", "NULL", "05 Real Estate & Construction", "NULL"]
    );
    assert_eq!(col("irs_number"), ["NULL", "NULL", "000000000", "NULL"]);
    assert_eq!(col("state_of_incorporation"), ["NULL", "DE", "E9", "NULL"]);
    assert_eq!(col("fiscal_year_end"), ["NULL", "1231", "1231", "NULL"]);
    assert_eq!(col("lei"), ["NULL", "NULL", "NULL", "549300EXAMPLE0000042"]);
    assert_eq!(col("party_form_type"), ["4", "4", "NULL", "NULL"]);
    assert_eq!(col("act"), ["34", "34", "NULL", "NULL"]);
    assert_eq!(
        col("file_number"),
        ["001-43446", "001-43446", "NULL", "NULL"]
    );
    assert_eq!(
        col("film_number"),
        ["261284087", "261284086", "NULL", "NULL"]
    );
    assert_eq!(
        col("business_street1"),
        [
            "NULL",
            "9912 GEORGETOWN PIKE",
            "9912 GEORGETOWN PIKE",
            "NULL"
        ]
    );
    assert_eq!(
        col("business_street2"),
        ["NULL", "SUITE D203", "SUITE D203", "NULL"]
    );
    assert_eq!(
        col("business_city"),
        ["NULL", "GREAT FALLS", "GREAT FALLS", "NULL"]
    );
    assert_eq!(col("business_state"), ["NULL", "VA", "VA", "NULL"]);
    assert_eq!(col("business_zip_code"), ["NULL", "22066", "22066", "NULL"]);
    assert_eq!(
        col("business_state_description"),
        ["NULL", "NULL", "NULL", "ENGLAND"]
    );
    assert_eq!(
        col("business_country"),
        ["NULL", "NULL", "NULL", "UNITED KINGDOM"]
    );
    assert_eq!(
        col("business_non_us_state_territory"),
        ["NULL", "NULL", "NULL", "LONDON"]
    );
    assert_eq!(
        col("business_phone"),
        ["NULL", "(202) 431-0507", "(202) 431-0507", "NULL"]
    );
    assert_eq!(
        col("mail_street1"),
        [
            "717 KING STREET",
            "9912 GEORGETOWN PIKE",
            "9912 GEORGETOWN PIKE",
            "NULL"
        ]
    );
    assert_eq!(
        col("mail_street2"),
        ["NULL", "SUITE D203", "SUITE D203", "NULL"]
    );
    assert_eq!(
        col("mail_city"),
        ["ALEXANDRIA", "GREAT FALLS", "GREAT FALLS", "NULL"]
    );
    assert_eq!(col("mail_state"), ["VA", "VA", "VA", "NULL"]);
    assert_eq!(col("mail_zip_code"), ["22314", "22066", "22066", "NULL"]);
    for column in [
        "mail_state_description",
        "mail_country",
        "mail_non_us_state_territory",
    ] {
        assert_eq!(col(column), ["NULL", "NULL", "NULL", "NULL"], "{column}");
    }
    assert_eq!(
        col("former_names"),
        [
            "[]",
            "[]",
            "[]",
            "[{name: NULL, date_changed: 2001-01-02}, {name: OLD NAME, date_changed: NULL}]"
        ]
    );
    assert_eq!(
        col("has_parse_issues"),
        ["false", "false", "false", "false"]
    );
    assert_filing_context(&batches, t);
    assert!(batches.issues().is_empty());
}

#[test]
fn former_names_are_a_list_of_structs_in_header_order() {
    let filings = vec![sec::Filing {
        parties: vec![abp_party()],
        ..real(
            "0001323255-14-000015",
            "13F-NT",
            Body::Form13f(sec::Form13fReport::default()),
        )
    }];
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    let t = "filing_parties";
    assert_eq!(
        batches.cell(t, "former_names", 0),
        "[{name: STICHTING PENSIOENFONDS  ABP, date_changed: 2001-10-11}, \
         {name: ALGEMEEN BURGERLIJK PENSIOENFONDS, date_changed: 1994-02-02}]"
    );
    assert_eq!(batches.cell(t, "state_of_incorporation", 0), "P7");
    assert_eq!(batches.cell(t, "business_street2", 0), "POSTBUS 6401");
    assert_eq!(batches.cell(t, "business_zip_code", 0), "00000");
    assert_eq!(batches.cell(t, "business_phone", 0), "0113145798022");
    assert_eq!(batches.cell(t, "has_parse_issues", 0), "false");
}

#[test]
fn former_name_date_issues_are_keyed_by_party_and_element() {
    let mut bad = abp_party();
    bad.former_names = vec![
        former("A", "2001-10-11"),
        former("B", "N/A"),
        former("C", "2014-06-30-05:00"),
        former("D", ""),
        former("E", "1999-02-30"),
        former("F", "19990228"),
    ];
    let filings = vec![sec::Filing {
        parties: vec![abp_party(), bad],
        ..filing("13F-NT", Body::Form13f(sec::Form13fReport::default()))
    }];
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    let t = "filing_parties";
    assert_eq!(batches.column(t, "has_parse_issues"), ["false", "true"]);
    assert_eq!(
        batches.cell(t, "former_names", 1),
        "[{name: A, date_changed: 2001-10-11}, {name: B, date_changed: NULL}, \
         {name: C, date_changed: 2014-06-30}, {name: D, date_changed: NULL}, \
         {name: E, date_changed: NULL}, {name: F, date_changed: NULL}]"
    );
    let issues = batches.issues();
    let got: Vec<_> = issues
        .iter()
        .map(|i| {
            (
                i.filing_index,
                i.table.as_str(),
                i.column.as_str(),
                i.index,
                i.raw.as_str(),
                i.issue.as_str(),
            )
        })
        .collect();
    let row = |element: u32, raw: &'static str, issue: &'static str| {
        (
            Some(0),
            "filing_parties",
            "former_names.date_changed",
            [Some(1), Some(element), None],
            raw,
            issue,
        )
    };
    assert_eq!(
        got,
        [
            row(1, "N/A", "sentinel"),
            row(2, "2014-06-30-05:00", "tz_dropped"),
            row(4, "1999-02-30", "out_of_range"),
            row(5, "19990228", "unparseable"),
        ]
    );
    assert_eq!(
        issues[0].accession_number.as_deref(),
        Some("0000000000-26-000006")
    );
}

// ---------------------------------------------------------------------------
// filing_documents (§3.5)
// ---------------------------------------------------------------------------

#[test]
fn documents_map_every_column_in_submission_order() {
    // SCHEDULE 13D `0000950103-26-003802` (2026-03-16, block 2956129), plus an
    // all-blank document.
    let filings = vec![sec::Filing {
        documents: vec![
            document("1", "SCHEDULE 13D", "primary_doc.xml", ""),
            document("2", "EX-99.1", "dp243449_ex9901.htm", "SCHEDULE I"),
            sec::SubmissionDocument::default(),
        ],
        ..real(
            "0000950103-26-003802",
            "SCHEDULE 13D",
            Body::Beneficial(sec::BeneficialOwnershipReport::default()),
        )
    }];
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    let t = "filing_documents";
    let col = |column: &str| batches.column(t, column);
    assert_eq!(col("document_index"), ["0", "1", "2"]);
    assert_eq!(col("sequence"), ["1", "2", "NULL"]);
    assert_eq!(col("document_type"), ["SCHEDULE 13D", "EX-99.1", "NULL"]);
    assert_eq!(
        col("filename"),
        ["primary_doc.xml", "dp243449_ex9901.htm", "NULL"]
    );
    assert_eq!(col("description"), ["NULL", "SCHEDULE I", "NULL"]);
    assert_eq!(batches.cell("filings", "document_count", 0), "3");
    assert_filing_context(&batches, t);
}

// ---------------------------------------------------------------------------
// filing_series (§3.6) and filing_series_classes (§3.7)
// ---------------------------------------------------------------------------

/// N-CEN/A `0000910472-26-004047` (2026-03-16, block 2956117), trimmed to
/// its first series and its last (whose class has no ticker), plus a series
/// without classes and one with a blank series id.
fn praxis_series() -> Vec<sec::FundSeries> {
    let existing = text("EXISTING-SERIES-AND-CLASSES-CONTRACTS");
    vec![
        sec::FundSeries {
            owner_cik: text("0000912900"),
            series_id: text("S000003160"),
            series_name: text("Praxis Impact Bond Fund"),
            classes: vec![
                class("C000008547", "Praxis Impact Bond Fund Class A", "MIIAX"),
                class("C000035282", "Praxis Impact Bond Fund Class I", "MIIIX"),
            ],
            status: existing.clone(),
        },
        sec::FundSeries {
            owner_cik: text("0000912900"),
            series_id: text("S000104970"),
            series_name: text("Praxis Impact International ETF"),
            classes: vec![class("C000275643", "Praxis Impact International ETF", "")],
            status: existing,
        },
        sec::FundSeries {
            series_id: text("S000000009"),
            ..Default::default()
        },
        sec::FundSeries {
            classes: vec![class("C000000009", "", "TSTX")],
            ..Default::default()
        },
    ]
}

#[test]
fn series_and_classes_map_every_column() {
    let filings = vec![
        filing("NPORT-P", Body::Nport(sec::NportReport::default())),
        sec::Filing {
            series: praxis_series(),
            ..real(
                "0000910472-26-004047",
                "N-CEN/A",
                Body::Ncen(sec::NcenReport::default()),
            )
        },
    ];
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    batches.assert_rows(&[("filing_series", 4), ("filing_series_classes", 4)]);

    let t = "filing_series";
    let col = |column: &str| batches.column(t, column);
    assert_eq!(col("filing_index"), ["1", "1", "1", "1"]);
    assert_eq!(col("series_index"), ["0", "1", "2", "3"]);
    assert_eq!(
        col("owner_cik"),
        ["0000912900", "0000912900", "NULL", "NULL"]
    );
    assert_eq!(
        col("series_id"),
        ["S000003160", "S000104970", "S000000009", "NULL"]
    );
    assert_eq!(
        col("series_name"),
        [
            "Praxis Impact Bond Fund",
            "Praxis Impact International ETF",
            "NULL",
            "NULL"
        ]
    );
    assert_eq!(
        col("status"),
        [
            "EXISTING-SERIES-AND-CLASSES-CONTRACTS",
            "EXISTING-SERIES-AND-CLASSES-CONTRACTS",
            "NULL",
            "NULL"
        ]
    );
    assert_eq!(col("class_count"), ["2", "1", "0", "1"]);
    assert_filing_context(&batches, t);

    let t = "filing_series_classes";
    let col = |column: &str| batches.column(t, column);
    assert_eq!(col("series_index"), ["0", "0", "1", "3"]);
    assert_eq!(col("class_index"), ["0", "1", "0", "0"]);
    // A copy of the parent's series_id ('' → NULL there too).
    assert_eq!(
        col("series_id"),
        ["S000003160", "S000003160", "S000104970", "NULL"]
    );
    assert_eq!(
        col("class_id"),
        ["C000008547", "C000035282", "C000275643", "C000000009"]
    );
    assert_eq!(
        col("class_name"),
        [
            "Praxis Impact Bond Fund Class A",
            "Praxis Impact Bond Fund Class I",
            "Praxis Impact International ETF",
            "NULL"
        ]
    );
    assert_eq!(col("ticker_symbol"), ["MIIAX", "MIIIX", "NULL", "TSTX"]);
    assert_filing_context(&batches, t);
    assert_eq!(batches.column("filings", "series_count"), ["0", "4"]);
}

// ---------------------------------------------------------------------------
// filing_signatures (§3.8)
// ---------------------------------------------------------------------------

/// One filing per signature source, mirroring real filings: Form 4
/// `0001339459-26-000007`, 13F-HR `0000950103-26-012367`, SCHEDULE 13D
/// `0000950103-26-003802`, NPORT-P `0000940400-26-035739`, Form D/A
/// `0002048118-26-000003`, N-PX `0001193125-26-372353` and Form C
/// `0001720779-26-000005` (issuer signature without a date).
fn signature_filings() -> Vec<sec::Filing> {
    let investcorp = |issuer: &str| sec::FormDSignature {
        issuer_name: text(issuer),
        signature_name: text("/s/ Emily Tibbetts"),
        name_of_signer: text("Emily Tibbetts"),
        signature_title: text("Director of the issuer's GP"),
        signature_date: text("2026-03-16"),
    };
    let nelson = |for_whom: &str| sec::Signature {
        name: format!(
            "/s/ Nelson Mullins Riley & Scarborough LLP, Attorney-in-Fact for {for_whom}"
        ),
        date: text("2026-08-14"),
    };
    vec![
        real(
            "0001339459-26-000007",
            "4",
            Body::Ownership(sec::OwnershipDocument {
                owner_signatures: vec![nelson("Gary A. Simanson"), nelson("TBCP V, LLC")],
                ..Default::default()
            }),
        ),
        real(
            "0000950103-26-012367",
            "13F-HR",
            Body::Form13f(sec::Form13fReport {
                signature: Some(sec::Form13fSignature {
                    name: text("David Caplan"),
                    title: text("Vice President and General Counsel"),
                    phone: text("646-690-5220"),
                    signature: text("/s/ David Caplan"),
                    city: text("New York"),
                    state: text("NY"),
                    signature_date: text("08-14-2026"),
                }),
                ..Default::default()
            }),
        ),
        real(
            "0000950103-26-003802",
            "SCHEDULE 13D",
            Body::Beneficial(sec::BeneficialOwnershipReport {
                signatures: vec![sec::BeneficialSignature {
                    reporting_person: text("Cintas Corp"),
                    signature: text("/s/ Scott A. Garula"),
                    title: text(
                        "Scott A. Garula / Executive Vice President and Chief Financial Officer",
                    ),
                    date: text("03/16/2026"),
                }],
                ..Default::default()
            }),
        ),
        real(
            "0000940400-26-035739",
            "NPORT-P",
            Body::Nport(sec::NportReport {
                signature: Some(sec::NportSignature {
                    date_signed: text("2026-08-28"),
                    name_of_applicant: text("USCF ETF Trust"),
                    signature: text("Kenneth A. Kalina"),
                    signer_name: text("Kenneth A. Kalina"),
                    title: text("CCO"),
                }),
                ..Default::default()
            }),
        ),
        real(
            "0002048118-26-000003",
            "D/A",
            Body::FormD(sec::FormDNotice {
                signatures: vec![
                    investcorp("Investcorp North American Private Equity Fund II, L.P."),
                    investcorp("Investcorp North American Private Equity Parallel Fund II, L.P."),
                ],
                ..Default::default()
            }),
        ),
        real(
            "0001193125-26-372353",
            "N-PX",
            Body::Npx(sec::NpxReport {
                cover_page: Some(sec::NpxCoverPage {
                    signatures: vec![sec::NpxSignature {
                        reporting_person: text("Pamplona Capital Management, LLC"),
                        signature: text("Stephen Gauci"),
                        printed_signature: text("Stephen Gauci"),
                        title: text("Director of Managing Member"),
                        date: text("08/28/2026"),
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            }),
        ),
        real(
            "0001720779-26-000005",
            "C",
            Body::FormC(sec::FormCNotice {
                // Proto order is irrelevant: the issuer signature comes first.
                person_signatures: vec![
                    form_c_signature("", "Patrick Smith", "President", "08-12-2026"),
                    form_c_signature("", "Steven Folse", "Managing Director", "08-12-2026"),
                ],
                issuer_signature: Some(form_c_signature(
                    "Kokopelli Outdoor, Inc.",
                    "Patrick Smith",
                    "President",
                    "",
                )),
                ..Default::default()
            }),
        ),
    ]
}

#[test]
fn signatures_map_every_source_per_the_matrix() {
    let batches = map(&[window_block(BLOCK_NUM, signature_filings())]);
    let t = "filing_signatures";
    batches.assert_rows(&[(t, 11)]);
    let col = |column: &str| batches.column(t, column);
    assert_eq!(
        col("filing_index"),
        ["0", "0", "1", "2", "3", "4", "4", "5", "6", "6", "6"]
    );
    assert_eq!(
        col("signature_index"),
        ["0", "1", "0", "0", "0", "0", "1", "0", "0", "1", "2"]
    );
    assert_eq!(
        col("signature_source"),
        [
            "ownership",
            "ownership",
            "form13f",
            "beneficial",
            "nport",
            "form_d",
            "form_d",
            "npx",
            "form_c_issuer",
            "form_c_person",
            "form_c_person"
        ]
    );
    assert_eq!(
        col("signed_for"),
        [
            "NULL",
            "NULL",
            "NULL",
            "Cintas Corp",
            "USCF ETF Trust",
            "Investcorp North American Private Equity Fund II, L.P.",
            "Investcorp North American Private Equity Parallel Fund II, L.P.",
            "Pamplona Capital Management, LLC",
            "Kokopelli Outdoor, Inc.",
            "NULL",
            "NULL"
        ]
    );
    assert_eq!(
        col("signer_name"),
        [
            "NULL",
            "NULL",
            "David Caplan",
            "NULL",
            "Kenneth A. Kalina",
            "Emily Tibbetts",
            "Emily Tibbetts",
            "Stephen Gauci",
            "NULL",
            "NULL",
            "NULL"
        ]
    );
    assert_eq!(
        col("signature_text"),
        [
            "/s/ Nelson Mullins Riley & Scarborough LLP, Attorney-in-Fact for Gary A. Simanson",
            "/s/ Nelson Mullins Riley & Scarborough LLP, Attorney-in-Fact for TBCP V, LLC",
            "/s/ David Caplan",
            "/s/ Scott A. Garula",
            "Kenneth A. Kalina",
            "/s/ Emily Tibbetts",
            "/s/ Emily Tibbetts",
            "Stephen Gauci",
            "Patrick Smith",
            "Patrick Smith",
            "Steven Folse"
        ]
    );
    assert_eq!(
        col("title"),
        [
            "NULL",
            "NULL",
            "Vice President and General Counsel",
            "Scott A. Garula / Executive Vice President and Chief Financial Officer",
            "CCO",
            "Director of the issuer's GP",
            "Director of the issuer's GP",
            "Director of Managing Member",
            "President",
            "President",
            "Managing Director"
        ]
    );
    let only_13f = |value: &str| {
        let mut expected = vec!["NULL".to_string(); 11];
        expected[2] = value.to_string();
        expected
    };
    assert_eq!(col("phone"), only_13f("646-690-5220"));
    assert_eq!(col("city"), only_13f("New York"));
    assert_eq!(col("state"), only_13f("NY"));
    // ISO, MM-DD-YYYY, MM/DD/YYYY; the Form C issuer signature has no date.
    assert_eq!(
        col("signature_date"),
        [
            "2026-08-14",
            "2026-08-14",
            "2026-08-14",
            "2026-03-16",
            "2026-08-28",
            "2026-03-16",
            "2026-03-16",
            "2026-08-28",
            "NULL",
            "2026-08-12",
            "2026-08-12"
        ]
    );
    assert_eq!(col("has_parse_issues"), vec!["false"; 11]);
    assert_filing_context(&batches, t);
    assert!(batches.issues().is_empty());
}

#[test]
fn absent_signature_messages_write_no_row_and_empty_ones_do() {
    let filings = vec![
        // Absent singular messages and an absent N-PX cover page: no row.
        filing("13F-HR", Body::Form13f(sec::Form13fReport::default())),
        filing("NPORT-P", Body::Nport(sec::NportReport::default())),
        filing("N-PX", Body::Npx(sec::NpxReport::default())),
        filing(
            "N-PX",
            Body::Npx(sec::NpxReport {
                cover_page: Some(sec::NpxCoverPage::default()),
                ..Default::default()
            }),
        ),
        // Bodies without a filing_signatures source.
        filing("144", Body::Form144(sec::Form144Notice::default())),
        filing("N-CEN", Body::Ncen(sec::NcenReport::default())),
        filing("4", Body::Raw(sec::RawFiling::default())),
        sec::Filing {
            body: None,
            ..filing("4", Body::Raw(sec::RawFiling::default()))
        },
        // A present but empty message gives an all-NULL row.
        filing(
            "13F-HR",
            Body::Form13f(sec::Form13fReport {
                signature: Some(sec::Form13fSignature::default()),
                ..Default::default()
            }),
        ),
        // Form C without an issuer signature: the person signatures start at 0.
        filing(
            "C-U",
            Body::FormC(sec::FormCNotice {
                person_signatures: vec![sec::FormCSignature::default()],
                ..Default::default()
            }),
        ),
        filing(
            "NPORT-P",
            Body::Nport(sec::NportReport {
                signature: Some(sec::NportSignature::default()),
                ..Default::default()
            }),
        ),
    ];
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    let t = "filing_signatures";
    batches.assert_rows(&[("filings", 11), (t, 3)]);
    assert_eq!(batches.column(t, "filing_index"), ["8", "9", "10"]);
    assert_eq!(batches.column(t, "signature_index"), ["0", "0", "0"]);
    assert_eq!(
        batches.column(t, "signature_source"),
        ["form13f", "form_c_person", "nport"]
    );
    for column in [
        "signed_for",
        "signer_name",
        "signature_text",
        "title",
        "phone",
        "city",
        "state",
        "signature_date",
    ] {
        assert_eq!(
            batches.column(t, column),
            ["NULL", "NULL", "NULL"],
            "{column}"
        );
    }
    assert_eq!(
        batches.column(t, "has_parse_issues"),
        ["false", "false", "false"]
    );
}

#[test]
fn signature_date_issues_are_keyed_by_signature_index() {
    // Form 4/A `0001477932-14-004179` (2014-08-11, block 2346291): the 2014
    // owner signature date carries a timezone suffix (`tz_dropped`, the date
    // is kept); the other three dates are synthetic.
    let filings = vec![real(
        "0001477932-14-004179",
        "4/A",
        Body::Ownership(sec::OwnershipDocument {
            owner_signatures: vec![
                sec::Signature {
                    name: text("/s/ Leonard Friedman"),
                    date: text("2014-08-11-05:00"),
                },
                sec::Signature {
                    name: text("/s/ Second Owner"),
                    date: text("2014-08-11"),
                },
                sec::Signature {
                    name: text("/s/ Third Owner"),
                    date: text("N/A"),
                },
                sec::Signature {
                    name: text("/s/ Fourth Owner"),
                    date: text("13/01/2014"),
                },
            ],
            ..Default::default()
        }),
    )];
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    let t = "filing_signatures";
    assert_eq!(
        batches.column(t, "signature_date"),
        ["2014-08-11", "2014-08-11", "NULL", "NULL"]
    );
    assert_eq!(
        batches.column(t, "has_parse_issues"),
        ["true", "false", "true", "true"]
    );
    let issues = batches.issues();
    let got: Vec<_> = issues
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
        .collect();
    assert_eq!(
        got,
        [
            (
                "filing_signatures",
                "signature_date",
                [Some(0), None, None],
                "2014-08-11-05:00",
                "tz_dropped"
            ),
            (
                "filing_signatures",
                "signature_date",
                [Some(2), None, None],
                "N/A",
                "sentinel"
            ),
            (
                "filing_signatures",
                "signature_date",
                [Some(3), None, None],
                "13/01/2014",
                "out_of_range"
            ),
        ]
    );
    assert!(issues
        .iter()
        .all(|i| i.accession_number.as_deref() == Some("0001477932-14-004179")));
}

// ---------------------------------------------------------------------------
// Issue order across the envelope
// ---------------------------------------------------------------------------

#[test]
fn envelope_issues_follow_the_filings_row_in_table_order() {
    let mut first = abp_party();
    first.former_names = vec![former("X", "N/A")];
    let mut second = abp_party();
    second.former_names = vec![former("Y", "2014-06-30-05:00"), former("Z", "NONE")];
    let filings = vec![
        filing("4", Body::Ownership(Default::default())),
        sec::Filing {
            filing_date: text("2014-08-11-05:00"),
            parties: vec![first, abp_party(), second],
            documents: vec![document("1", "4", "x.xml", "")],
            series: praxis_series(),
            ..filing(
                "4",
                Body::Ownership(sec::OwnershipDocument {
                    owner_signatures: vec![sec::Signature {
                        name: text("/s/ A"),
                        date: text("NULL"),
                    }],
                    ..Default::default()
                }),
            )
        },
    ];
    let batches = map(&[window_block(BLOCK_NUM, filings)]);
    let got: Vec<_> = batches
        .issues()
        .into_iter()
        .map(|i| (i.filing_index, i.table, i.column, i.index, i.issue))
        .collect();
    let row = |table: &str, column: &str, index: [Option<u32>; 3], issue: &str| {
        (
            Some(1),
            table.to_string(),
            column.to_string(),
            index,
            issue.to_string(),
        )
    };
    assert_eq!(
        got,
        [
            row("filings", "filing_date", [None, None, None], "tz_dropped"),
            row(
                "filing_parties",
                "former_names.date_changed",
                [Some(0), Some(0), None],
                "sentinel"
            ),
            row(
                "filing_parties",
                "former_names.date_changed",
                [Some(2), Some(0), None],
                "tz_dropped"
            ),
            row(
                "filing_parties",
                "former_names.date_changed",
                [Some(2), Some(1), None],
                "sentinel"
            ),
            row(
                "filing_signatures",
                "signature_date",
                [Some(0), None, None],
                "sentinel"
            ),
        ]
    );
    assert_eq!(
        batches.column("filing_parties", "has_parse_issues"),
        ["true", "false", "true"]
    );
}

// ---------------------------------------------------------------------------
// Real data (local only)
// ---------------------------------------------------------------------------

/// The §8.7 2014 Form 4/A on its real block: envelope rows and the
/// `tz_dropped` signature date.
/// `cargo test -p blocks --lib sec::tests::envelope::real_ -- --ignored --nocapture`
#[test]
#[ignore = "local: needs /tmp/sec-fireparq/fire-v013"]
fn real_2014_form4_envelope() {
    let (identity, block) = fire::block("2014-08-11", 2_346_291);
    let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
    mapper
        .map_block(&block.encode_to_vec(), &identity, StreamEvent::default())
        .unwrap();
    let batches = Batches::new(mapper.flush().unwrap());
    let accession = "0001477932-14-004179";
    let index = batches
        .column("filings", "accession_number")
        .iter()
        .position(|a| a == accession)
        .expect("fixture filing")
        .to_string();
    let rows_of = |table: &str| -> Vec<usize> {
        (0..batches.rows(table))
            .filter(|&row| batches.cell(table, "filing_index", row) == index)
            .collect()
    };
    let parties = rows_of("filing_parties");
    assert_eq!(parties.len(), 2);
    assert_eq!(batches.cell("filing_parties", "role", parties[0]), "ISSUER");
    assert_eq!(
        batches.cell("filing_parties", "business_street2", parties[0]),
        "405 LEXINGTON AVENUE, 26TH FLOOR"
    );
    assert_eq!(
        batches.cell("filing_parties", "name", parties[1]),
        "Friedman Leonard"
    );
    let documents = rows_of("filing_documents");
    assert_eq!(
        batches.cell("filing_documents", "description", documents[0]),
        "FORM 4 A"
    );
    let signatures = rows_of("filing_signatures");
    assert_eq!(signatures.len(), 1);
    assert_eq!(
        batches.cell("filing_signatures", "signature_text", signatures[0]),
        "/s/ Leonard Friedman"
    );
    assert_eq!(
        batches.cell("filing_signatures", "signature_date", signatures[0]),
        "2014-08-11"
    );
    let issue = batches
        .issues()
        .into_iter()
        .find(|i| i.table == "filing_signatures")
        .expect("tz_dropped issue");
    assert_eq!(issue.accession_number.as_deref(), Some(accession));
    assert_eq!(
        (
            issue.column.as_str(),
            issue.index,
            issue.raw.as_str(),
            issue.issue.as_str()
        ),
        (
            "signature_date",
            [Some(0), None, None],
            "2014-08-11-05:00",
            "tz_dropped"
        )
    );
}

/// The envelope tables equal the prototype's on every sample day present
/// locally (every column of every row).
/// `cargo test -p blocks --lib sec::tests::envelope::envelope_matches -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn envelope_matches_the_prototype() {
    oracle::assert_matches(&fire::DAYS, &TABLES, &[]);
}

/// The `parse_issues` rows of the envelope tables equal the prototype's, in
/// order (the full `parse_issues` comparison needs every group).
/// `cargo test -p blocks --lib sec::tests::envelope::envelope_parse_issues -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn envelope_parse_issues_match_the_prototype() {
    use std::io::BufRead;

    let ours = |row: &serde_json::Value| {
        row["table_name"]
            .as_str()
            .is_some_and(|table| TABLES.contains(&table))
    };
    for day in fire::DAYS {
        if !fire::path(day).exists() || !oracle::dir(day).exists() {
            continue;
        }
        let mut rust = Vec::new();
        let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
        let mut collect = |mapper: &mut SecBlockMapper| {
            let batches = mapper.flush().unwrap();
            let batch = &batches["parse_issues"];
            let schema = batch.schema();
            for row in 0..batch.num_rows() {
                let value: serde_json::Map<String, serde_json::Value> = schema
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
                let value = serde_json::Value::Object(value);
                if ours(&value) {
                    rust.push(value);
                }
            }
        };
        for (identity, payload) in fire::blocks(day) {
            mapper
                .map_block_bytes(payload.into(), &identity, StreamEvent::default())
                .unwrap();
            if mapper.total_rows() > 500_000 {
                collect(&mut mapper);
            }
        }
        collect(&mut mapper);

        let file = std::fs::File::open(oracle::dir(day).join("parse_issues.ndjson")).unwrap();
        let expected: Vec<serde_json::Value> = std::io::BufReader::new(file)
            .lines()
            .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
            .filter(|row| ours(row))
            .collect();
        println!(
            "{day}: envelope parse_issues rust {} oracle {}",
            rust.len(),
            expected.len()
        );
        assert_eq!(rust, expected, "{day}");
    }
}
