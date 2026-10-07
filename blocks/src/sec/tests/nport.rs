//! Value tests of `nport_reports`, `nport_monthly_returns`, `nport_monthly_activity`, `nport_holdings`, `nport_debt_reference_instruments`, `nport_debt_conversion_currencies`, `nport_derivatives`, `nport_derivative_swap_legs`, `nport_derivative_index_components`.
//! Owned by the `nport` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The fixture tests mirror the N-PORT filings of final-spec §8.7 as prost
//! structs (values copied from the 2026-08-28 sample, lists trimmed); the
//! expected values are the prototype's (`proto/sec.duckdb`). Every table has at
//! least one [`assert_full_row`], which names every column after [ID] + [FC] in
//! schema order.

use super::*;

/// This group's tables.
pub(crate) const TABLES: [&str; 9] = [
    "nport_reports",
    "nport_monthly_returns",
    "nport_monthly_activity",
    "nport_holdings",
    "nport_debt_reference_instruments",
    "nport_debt_conversion_currencies",
    "nport_derivatives",
    "nport_derivative_swap_legs",
    "nport_derivative_index_components",
];

/// This group's filings in the cross-table contract fixture
/// (`make_every_body_block`): together they must give every table of
/// [`TABLES`] at least one row (and contribute signatures where the body has
/// them). Ordinals are reassigned by position.
pub(crate) fn contract_filings() -> Vec<sec::Filing> {
    vec![nport_filing("0000000000-26-900001", contract_report())]
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
    // The N-PORT signature lands in `filing_signatures` (envelope group).
    assert!(contract_report().signature.is_some());
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn st(text: &str) -> String {
    text.to_string()
}

fn other_id(description: &str, value: &str) -> sec::OtherIdentifier {
    sec::OtherIdentifier {
        description: st(description),
        value: st(value),
    }
}

/// An NPORT-P filing with `report` as its body.
fn nport_filing(accession: &str, report: sec::NportReport) -> sec::Filing {
    sec::Filing {
        accession_number: st(accession),
        filing_date: st("2026-08-28"),
        cik_role: st("FILER"),
        ..filing("NPORT-P", Body::Nport(report))
    }
}

/// Map one N-PORT filing in window [`BLOCK_NUM`].
fn map_report(accession: &str, report: sec::NportReport) -> Batches {
    map(&[window_block(
        BLOCK_NUM,
        vec![nport_filing(accession, report)],
    )])
}

/// Assert the listed cells of one row.
fn assert_row(batches: &Batches, table: &str, row: usize, expected: &[(&str, &str)]) {
    for (column, value) in expected {
        assert_eq!(
            batches.cell(table, column, row),
            *value,
            "{table}.{column}, row {row}"
        );
    }
}

/// Number of [ID] + [FC] columns before a table's own columns.
const ID_FC_COLUMNS: usize = 12;

/// [`assert_row`], where `expected` names every column after [ID] + [FC], in
/// schema order.
fn assert_full_row(batches: &Batches, table: &str, row: usize, expected: &[(&str, &str)]) {
    let schema = batches.table(table).schema();
    let columns: Vec<&str> = schema.fields()[ID_FC_COLUMNS..]
        .iter()
        .map(|field| field.name().as_str())
        .collect();
    let named: Vec<&str> = expected.iter().map(|(column, _)| *column).collect();
    assert_eq!(
        named, columns,
        "{table}: the expected row names every column"
    );
    assert_row(batches, table, row, expected);
}

type IssueTuple = (String, String, [Option<u32>; 3], String, String);

/// The `parse_issues` rows as `(table, column, index, raw, issue)`.
fn issue_tuples(batches: &Batches) -> Vec<IssueTuple> {
    batches
        .issues()
        .into_iter()
        .map(|i| (i.table, i.column, i.index, i.raw, i.issue))
        .collect()
}

fn issue(table: &str, column: &str, index: [Option<u32>; 3], raw: &str, kind: &str) -> IssueTuple {
    (st(table), st(column), index, st(raw), st(kind))
}

// ---------------------------------------------------------------------------
// The contract filing: every table, a 3-level derivative chain, issues
// ---------------------------------------------------------------------------

/// One NPORT-P with every repeated child: a convertible bond (reference
/// instrument, conversion currency, lending) and a swaption whose nested swap
/// (level 1) has legs and basket components and itself nests a future
/// (level 2). Carries a few unparseable values for the issue addressing tests.
fn contract_report() -> sec::NportReport {
    let future = sec::Derivative {
        category: st("FUT"),
        expiration_date: st("someday"),
        ..Default::default()
    };
    let swap = sec::Derivative {
        category: st("SWP"),
        counterparty_name: st("BANK OF TESTS"),
        swap_flag: st("maybe"),
        legs: vec![
            sec::SwapLeg {
                kind: st("fixedRecDesc"),
                fixed_or_floating: st("Fixed"),
                currency: st("USD"),
                amount: st("1000.5"),
                fixed_rate: st("4.25"),
                ..Default::default()
            },
            sec::SwapLeg {
                kind: st("floatingPmntDesc"),
                fixed_or_floating: st("Floating"),
                currency: st("USD"),
                amount: st("N/A"),
                floating_rate_index: st("SOFR"),
                floating_rate_spread: st("0.5"),
                payment_amount: st("12"),
                ..Default::default()
            },
        ],
        ref_index_name: st("CUSTOM BASKET"),
        ref_index_components: vec![sec::IndexBasketComponent {
            name: st("APPLE INC"),
            cusip: st("037833100"),
            isin: st("US0378331005"),
            ticker: st("AAPL"),
            other_identifiers: vec![other_id("SEDOL", "2046251")],
            notional_amount: st("1,000"),
            currency: st("USD"),
            value: st("250.75"),
            issue_currency: st("USD"),
        }],
        nested: Some(Box::new(future)),
        additional_info: Some(sec::DerivativeAdditionalInfo {
            name: st("SWAP LEG HOLDER"),
            balance: st("3"),
            ..Default::default()
        }),
        ..Default::default()
    };
    sec::NportReport {
        filer_cik: st("0000000042"),
        general_info: Some(sec::GeneralInfo {
            reg_name: st("TEST TRUST"),
            series_name: st("TEST FUND"),
            series_id: st("S000000042"),
            rep_period_end: st("2026-12-31"),
            rep_period_date: st("2026-06-30"),
            ..Default::default()
        }),
        fund_info: Some(sec::FundInfo {
            net_assets: st("1000000"),
            monthly_total_returns: vec![sec::MonthlyReturn {
                class_id: st("C000000042"),
                return_month1: st("1.5"),
                return_month2: st("-0.25"),
                return_month3: st("0"),
            }],
            monthly_activity: vec![sec::MonthlyFundActivity {
                month: 3,
                sales: st("10"),
                ..Default::default()
            }],
            ..Default::default()
        }),
        holdings: vec![
            sec::PortfolioHolding {
                name: st("CONVERTIBLE ISSUER"),
                cusip: st("70932AAH6"),
                balance: st("100"),
                security_lending: Some(sec::SecurityLending {
                    is_loan_by_fund: true,
                    loan_value: st("5"),
                    ..Default::default()
                }),
                debt_security: Some(sec::DebtSecurity {
                    maturity_date: st("2029-06-01"),
                    is_contingent_convertible: Some(true),
                    reference_instruments: vec![sec::DebtReferenceInstrument {
                        name: st("REFERENCE EQUITY"),
                        cusip: st("70931T103"),
                        other_identifiers: vec![other_id("FIGI", "BBG000000001")],
                        ..Default::default()
                    }],
                    conversion_currencies: vec![sec::ConversionCurrency {
                        currency: st("USD"),
                        conversion_ratio: st("N/A"),
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            sec::PortfolioHolding {
                name: st("SWAPTION ISSUER"),
                derivative: Some(sec::Derivative {
                    category: st("SWO"),
                    put_or_call: st("Put"),
                    nested: Some(Box::new(swap)),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
        explanatory_notes: vec![sec::NportExplanatoryNote {
            note_item: st("B.5.a"),
            note: st("Returns exclude sales loads."),
        }],
        signature: Some(sec::NportSignature {
            date_signed: st("2026-08-28"),
            name_of_applicant: st("TEST TRUST"),
            signature: st("/s/ Jane Doe"),
            signer_name: st("Jane Doe"),
            title: st("Treasurer"),
        }),
    }
}

#[test]
fn a_derivative_chain_is_one_row_per_level_with_its_children_keyed_by_level() {
    let b = map_report("0000000000-26-900001", contract_report());
    b.assert_rows(&[
        ("nport_reports", 1),
        ("nport_monthly_returns", 1),
        ("nport_monthly_activity", 1),
        ("nport_holdings", 2),
        ("nport_debt_reference_instruments", 1),
        ("nport_debt_conversion_currencies", 1),
        ("nport_derivatives", 3),
        ("nport_derivative_swap_legs", 2),
        ("nport_derivative_index_components", 1),
    ]);
    let t = "nport_derivatives";
    assert_eq!(b.column(t, "holding_index"), ["1", "1", "1"]);
    assert_eq!(b.column(t, "nesting_level"), ["0", "1", "2"]);
    assert_eq!(b.column(t, "category"), ["SWO", "SWP", "FUT"]);
    assert_eq!(b.column(t, "has_nested"), ["true", "true", "false"]);
    assert_eq!(
        b.column(t, "has_additional_info"),
        ["false", "true", "false"]
    );
    assert_eq!(
        b.column(t, "addl_name"),
        ["NULL", "SWAP LEG HOLDER", "NULL"]
    );
    assert_eq!(
        b.column(t, "addl_balance"),
        ["NULL", "3.0000000000", "NULL"]
    );
    assert_eq!(b.column(t, "swap_leg_count"), ["0", "2", "0"]);
    assert_eq!(b.column(t, "index_component_count"), ["0", "1", "0"]);
    assert_eq!(b.column(t, "swap_flag"), ["NULL", "NULL", "NULL"]);
    assert_eq!(b.column(t, "has_parse_issues"), ["false", "true", "true"]);
    // The holding copies the outer category only.
    assert_eq!(
        b.column("nport_holdings", "has_derivative"),
        ["false", "true"]
    );
    assert_eq!(
        b.column("nport_holdings", "derivative_category"),
        ["NULL", "SWO"]
    );
    assert_eq!(
        b.column("nport_holdings", "lending_is_loan_by_fund"),
        ["true", "NULL"]
    );
    assert_eq!(
        b.column("nport_holdings", "lending_loan_value"),
        ["5.0000000000", "NULL"]
    );

    let t = "nport_derivative_swap_legs";
    assert_eq!(b.column(t, "holding_index"), ["1", "1"]);
    assert_eq!(b.column(t, "nesting_level"), ["1", "1"]);
    assert_eq!(b.column(t, "leg_index"), ["0", "1"]);
    assert_eq!(b.column(t, "amount"), ["1000.5000000000", "NULL"]);
    assert_eq!(b.column(t, "fixed_rate"), ["4.250000000000", "NULL"]);
    assert_eq!(b.column(t, "has_parse_issues"), ["false", "true"]);

    let t = "nport_derivative_index_components";
    assert_full_row(
        &b,
        t,
        0,
        &[
            ("series_id", "S000000042"),
            ("as_of_date", "2026-06-30"),
            ("holding_index", "1"),
            ("nesting_level", "1"),
            ("component_index", "0"),
            ("component_name", "APPLE INC"),
            ("cusip", "037833100"),
            ("cusip_norm", "037833100"),
            ("isin", "US0378331005"),
            ("ticker", "AAPL"),
            (
                "other_identifiers",
                "[{description: SEDOL, value: 2046251}]",
            ),
            ("notional_amount", "NULL"),
            ("currency", "USD"),
            ("value", "250.7500000000"),
            ("issue_currency", "USD"),
            ("has_parse_issues", "true"),
        ],
    );
    assert_row(
        &b,
        "nport_debt_reference_instruments",
        0,
        &[(
            "other_identifiers",
            "[{description: FIGI, value: BBG000000001}]",
        )],
    );
    assert_row(
        &b,
        "nport_debt_conversion_currencies",
        0,
        &[("conversion_ratio", "NULL"), ("has_parse_issues", "true")],
    );

    // Issues in emit order: per holding the holding, its conversion
    // currencies, then per level the derivative, its legs, its components.
    assert_eq!(
        issue_tuples(&b),
        vec![
            issue(
                "nport_debt_conversion_currencies",
                "conversion_ratio",
                [Some(0), Some(0), None],
                "N/A",
                "sentinel"
            ),
            issue(
                "nport_derivatives",
                "swap_flag",
                [Some(1), Some(1), None],
                "maybe",
                "unparseable"
            ),
            issue(
                "nport_derivative_swap_legs",
                "amount",
                [Some(1), Some(1), Some(1)],
                "N/A",
                "sentinel"
            ),
            issue(
                "nport_derivative_index_components",
                "notional_amount",
                [Some(1), Some(1), Some(0)],
                "1,000",
                "unparseable"
            ),
            issue(
                "nport_derivatives",
                "expiration_date",
                [Some(1), Some(2), None],
                "someday",
                "unparseable"
            ),
        ]
    );
    let issues = b.issues();
    assert!(issues.iter().all(|i| i.filing_index == Some(0)
        && i.accession_number.as_deref() == Some("0000000000-26-900001")));
}

#[test]
fn child_rows_copy_the_filing_context_and_the_report_keys() {
    let b = map_report("0000000000-26-900001", contract_report());
    let acceptance = utc_millis_text((window_seconds(BLOCK_NUM) + 60) * 1000);
    let block_num = BLOCK_NUM.to_string();
    for table in TABLES {
        assert_row(
            &b,
            table,
            0,
            &[
                ("block_num", block_num.as_str()),
                ("filing_index", "0"),
                ("accession_number", "0000000000-26-900001"),
                ("form_type", "NPORT-P"),
                ("filing_date", "2026-08-28"),
                ("acceptance_datetime", acceptance.as_str()),
            ],
        );
        if table != "nport_reports" {
            assert_row(
                &b,
                table,
                0,
                &[("series_id", "S000000042"), ("as_of_date", "2026-06-30")],
            );
        }
    }
    assert_row(
        &b,
        "nport_holdings",
        1,
        &[
            ("registrant_name", "TEST TRUST"),
            ("series_name", "TEST FUND"),
        ],
    );
}

// ---------------------------------------------------------------------------
// §8.7 fixtures
// ---------------------------------------------------------------------------

fn uscf_general_info() -> sec::GeneralInfo {
    sec::GeneralInfo {
        reg_name: st("USCF ETF Trust"),
        reg_file_number: st("811-22930"),
        reg_cik: st("0001597389"),
        reg_lei: st("549300MH2TWHBRNV2U91"),
        series_name: st("USCF Gold Strategy Plus Income Fund"),
        series_id: st("S000072135"),
        series_lei: st("54930038N10WS1HN8Y79"),
        rep_period_end: st("2026-06-30"),
        rep_period_date: st("2026-06-30"),
        is_final_filing: false,
        reg_address: Some(sec::Address {
            street1: st("1850 MT.Diablo Boulevard, Suite 640"),
            city: st("Walnut Creek"),
            state: st("US-CA"),
            zip_code: st("94596"),
            country: st("US"),
            ..Default::default()
        }),
        reg_phone: st("1-800-920-0259"),
    }
}

fn uscf_flows(month: u32, redemption: &str) -> sec::MonthlyFundActivity {
    sec::MonthlyFundActivity {
        month,
        sales: st("0.00000000"),
        reinvestment: st("0.00000000"),
        redemption: st(redemption),
        net_realized_gain: st("0.00000000"),
        net_unrealized_appreciation: st("0.00000000"),
    }
}

/// `0000940400-26-035739` (block 2979910): a gold future, and a written call
/// on a gold future (nested, with `additional_info`).
fn uscf_gold_strategy() -> sec::NportReport {
    let gold_ref = || vec![other_id("INTERNAL", "GOLD1")];
    sec::NportReport {
        filer_cik: st("0001597389"),
        general_info: Some(uscf_general_info()),
        fund_info: Some(sec::FundInfo {
            total_assets: st("9911858.78"),
            total_liabilities: st("1125923.67"),
            net_assets: st("8785935.11"),
            monthly_total_returns: vec![sec::MonthlyReturn {
                class_id: st("C000227919"),
                return_month1: st("-0.16000000"),
                return_month2: st("-1.05000000"),
                return_month3: st("-11.40000000"),
            }],
            assets_invested: st("1677382.37000000"),
            misc_securities_assets: st("0.00000000"),
            cash_not_reported: st("9882161.47000000"),
            monthly_activity: vec![
                uscf_flows(1, "1838934.75000000"),
                uscf_flows(2, "932046.60000000"),
                uscf_flows(3, "1620013.00000000"),
            ],
        }),
        holdings: vec![
            sec::PortfolioHolding {
                name: st("CME Group Inc."),
                lei: st("KJNXBSWZVIKEX4NFOL81"),
                title: st("Gold Futures"),
                cusip: st("000000000"),
                ticker: st("GC Q26"),
                other_identifier: st("BBG01PZXTQX7"),
                balance: st("22.00000000"),
                units: st("NC"),
                currency: st("USD"),
                value_usd: st("-1092390.00000000"),
                pct_value: st("-12.4333948102"),
                payoff_profile: st("N/A"),
                asset_category: st("DCO"),
                issuer_category: st("CORP"),
                investment_country: st("US"),
                fair_value_level: st("1"),
                derivative: Some(sec::Derivative {
                    category: st("FUT"),
                    counterparty_name: st("CME Group Inc."),
                    counterparty_lei: st("KJNXBSWZVIKEX4NFOL81"),
                    ref_instrument_name: st("GOLD"),
                    ref_instrument_title: st("GOLD"),
                    notional_amount: st("9977090.00000000"),
                    expiration_date: st("2026-08-27"),
                    unrealized_appreciation: st("-1092390.00000000"),
                    currency: st("USD"),
                    payoff_profile: st("Long"),
                    ref_cusip: st("000000000"),
                    ref_other_identifiers: gold_ref(),
                    ..Default::default()
                }),
                security_lending: Some(sec::SecurityLending::default()),
                other_identifiers: vec![other_id("FIGI", "BBG01PZXTQX7")],
                ..Default::default()
            },
            sec::PortfolioHolding {
                name: st("COMMODITIES EXCHANGE CENTER"),
                lei: st("5493008GFNDTXFPHWI47"),
                title: st("Gold Option"),
                cusip: st("000000000"),
                ticker: st("OGQ6 C4400"),
                other_identifier: st("BBG01WMKJQJ1"),
                balance: st("-22.00000000"),
                units: st("NC"),
                currency: st("USD"),
                value_usd: st("-29700.00000000"),
                pct_value: st("-0.33804028402"),
                payoff_profile: st("N/A"),
                asset_category: st("DCO"),
                issuer_category: st("CORP"),
                investment_country: st("US"),
                fair_value_level: st("1"),
                derivative: Some(sec::Derivative {
                    category: st("OPT"),
                    counterparty_name: st("COMMODITIES EXCHANGE CENTER"),
                    counterparty_lei: st("5493008GFNDTXFPHWI47"),
                    put_or_call: st("Call"),
                    written_or_purchased: st("Written"),
                    ref_ticker: st("GCQ6"),
                    share_no: st("100.00000000"),
                    exercise_price: st("4400.00000000"),
                    exercise_currency: st("USD"),
                    expiration_date: st("2026-07-28"),
                    delta: st("XXXX"),
                    unrealized_appreciation: st("12830.16000000"),
                    nested: Some(Box::new(sec::Derivative {
                        category: st("FUT"),
                        counterparty_name: st("COMMODITIES EXCHANGE CENTER"),
                        counterparty_lei: st("5493008GFNDTXFPHWI47"),
                        ref_instrument_name: st("GOLD"),
                        ref_instrument_title: st("GOLD"),
                        notional_amount: st("0.00000000"),
                        expiration_date: st("2026-08-27"),
                        currency: st("USD"),
                        payoff_profile: st("Long"),
                        ref_cusip: st("000000000"),
                        ref_other_identifiers: gold_ref(),
                        additional_info: Some(sec::DerivativeAdditionalInfo {
                            name: st("COMMODITIES EXCHANGE CENTER"),
                            lei: st("5493008GFNDTXFPHWI47"),
                            title: st("Gold Futures"),
                            cusip: st("000000000"),
                            ticker: st("GCQ6"),
                            other_identifiers: vec![other_id("FIGI", "BBG01PZXTQX7")],
                            balance: st("0.00000000"),
                            units: st("NC"),
                            currency: st("USD"),
                            value_usd: st("0.00000000"),
                            pct_value: st("0.00000000"),
                            asset_category: st("DIR"),
                            issuer_category: st("CORP"),
                            investment_country: st("US"),
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                security_lending: Some(sec::SecurityLending::default()),
                other_identifiers: vec![other_id("FIGI", "BBG01WMKJQJ1")],
                ..Default::default()
            },
        ],
        explanatory_notes: Vec::new(),
        signature: Some(sec::NportSignature {
            date_signed: st("2026-08-28"),
            name_of_applicant: st("USCF ETF Trust"),
            signature: st("Kenneth A. Kalina"),
            signer_name: st("Kenneth A. Kalina"),
            title: st("CCO"),
        }),
    }
}

#[test]
fn fixture_nested_derivative_with_additional_info() {
    let b = map_report("0000940400-26-035739", uscf_gold_strategy());
    b.assert_rows(&[
        ("nport_reports", 1),
        ("nport_monthly_returns", 1),
        ("nport_monthly_activity", 3),
        ("nport_holdings", 2),
        ("nport_debt_reference_instruments", 0),
        ("nport_debt_conversion_currencies", 0),
        ("nport_derivatives", 3),
        ("nport_derivative_swap_legs", 0),
        ("nport_derivative_index_components", 0),
    ]);
    assert!(b.issues().is_empty());

    assert_full_row(
        &b,
        "nport_reports",
        0,
        &[
            ("filer_cik", "0001597389"),
            ("registrant_name", "USCF ETF Trust"),
            ("registrant_file_number", "811-22930"),
            ("registrant_cik", "0001597389"),
            ("registrant_lei", "549300MH2TWHBRNV2U91"),
            ("registrant_street1", "1850 MT.Diablo Boulevard, Suite 640"),
            ("registrant_street2", "NULL"),
            ("registrant_city", "Walnut Creek"),
            ("registrant_state", "US-CA"),
            ("registrant_zip_code", "94596"),
            ("registrant_state_description", "NULL"),
            ("registrant_country", "US"),
            ("registrant_non_us_state_territory", "NULL"),
            ("registrant_phone", "1-800-920-0259"),
            ("series_name", "USCF Gold Strategy Plus Income Fund"),
            ("series_id", "S000072135"),
            ("series_lei", "54930038N10WS1HN8Y79"),
            ("fiscal_year_end", "2026-06-30"),
            ("as_of_date", "2026-06-30"),
            ("is_final_filing", "false"),
            ("total_assets", "9911858.7800000000"),
            ("total_liabilities", "1125923.6700000000"),
            ("net_assets", "8785935.1100000000"),
            ("assets_invested", "1677382.3700000000"),
            ("misc_securities_assets", "0.0000000000"),
            ("cash_not_reported", "9882161.4700000000"),
            ("explanatory_notes", "[]"),
            ("holdings_count", "2"),
            ("derivative_holding_count", "2"),
            ("debt_holding_count", "0"),
            ("monthly_return_class_count", "1"),
            ("has_parse_issues", "false"),
        ],
    );

    assert_full_row(
        &b,
        "nport_monthly_returns",
        0,
        &[
            ("series_id", "S000072135"),
            ("as_of_date", "2026-06-30"),
            ("return_index", "0"),
            ("class_id", "C000227919"),
            ("return_month1", "-0.160000000000"),
            ("return_month2", "-1.050000000000"),
            ("return_month3", "-11.400000000000"),
            ("month1_end", "2026-04-30"),
            ("month2_end", "2026-05-31"),
            ("month3_end", "2026-06-30"),
            ("has_parse_issues", "false"),
        ],
    );

    assert_full_row(
        &b,
        "nport_monthly_activity",
        0,
        &[
            ("series_id", "S000072135"),
            ("as_of_date", "2026-06-30"),
            ("activity_index", "0"),
            ("month", "1"),
            ("month_end", "2026-04-30"),
            ("sales", "0.0000000000"),
            ("reinvestment", "0.0000000000"),
            ("redemption", "1838934.7500000000"),
            ("net_realized_gain", "0.0000000000"),
            ("net_unrealized_appreciation", "0.0000000000"),
            ("has_parse_issues", "false"),
        ],
    );
    let t = "nport_monthly_activity";
    assert_eq!(b.column(t, "activity_index"), ["0", "1", "2"]);
    assert_eq!(b.column(t, "month"), ["1", "2", "3"]);
    assert_eq!(
        b.column(t, "month_end"),
        ["2026-04-30", "2026-05-31", "2026-06-30"]
    );
    assert_eq!(
        b.column(t, "redemption"),
        [
            "1838934.7500000000",
            "932046.6000000000",
            "1620013.0000000000"
        ]
    );

    assert_full_row(
        &b,
        "nport_holdings",
        0,
        &[
            ("registrant_name", "USCF ETF Trust"),
            ("series_id", "S000072135"),
            ("series_name", "USCF Gold Strategy Plus Income Fund"),
            ("as_of_date", "2026-06-30"),
            ("holding_index", "0"),
            ("issuer_name", "CME Group Inc."),
            ("issuer_lei", "KJNXBSWZVIKEX4NFOL81"),
            ("issuer_lei_norm", "KJNXBSWZVIKEX4NFOL81"),
            ("issue_title", "Gold Futures"),
            ("cusip", "000000000"),
            ("cusip_norm", "NULL"),
            ("isin", "NULL"),
            ("ticker", "GC Q26"),
            ("other_identifier", "BBG01PZXTQX7"),
            (
                "other_identifiers",
                "[{description: FIGI, value: BBG01PZXTQX7}]",
            ),
            ("balance", "22.0000000000"),
            ("units", "NC"),
            ("units_description", "NULL"),
            ("currency", "USD"),
            ("exchange_rate", "NULL"),
            ("value_usd", "-1092390.0000000000"),
            ("pct_value", "-12.433394810200"),
            ("payoff_profile", "N/A"),
            ("asset_category", "DCO"),
            ("asset_category_description", "NULL"),
            ("issuer_category", "CORP"),
            ("issuer_category_description", "NULL"),
            ("investment_country", "US"),
            ("fair_value_level", "1"),
            ("is_restricted", "false"),
            ("has_debt_security", "false"),
            ("debt_maturity_date", "NULL"),
            ("debt_coupon_kind", "NULL"),
            ("debt_annualized_rate", "NULL"),
            ("debt_is_default", "NULL"),
            ("debt_are_interest_payments_in_arrears", "NULL"),
            ("debt_is_paid_in_kind", "NULL"),
            ("debt_is_mandatory_convertible", "NULL"),
            ("debt_is_contingent_convertible", "NULL"),
            ("debt_delta", "NULL"),
            ("debt_reference_instrument_count", "0"),
            ("debt_conversion_currency_count", "0"),
            ("has_derivative", "true"),
            ("derivative_category", "FUT"),
            // `<securityLending/>` present but empty: false, not NULL.
            ("has_security_lending", "true"),
            ("lending_is_cash_collateral", "false"),
            ("lending_is_non_cash_collateral", "false"),
            ("lending_is_loan_by_fund", "false"),
            ("lending_loan_value", "NULL"),
            ("lending_cash_collateral_value", "NULL"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_row(
        &b,
        "nport_holdings",
        1,
        &[
            ("holding_index", "1"),
            ("issuer_name", "COMMODITIES EXCHANGE CENTER"),
            ("balance", "-22.0000000000"),
            ("pct_value", "-0.338040284020"),
            ("derivative_category", "OPT"),
        ],
    );

    let t = "nport_derivatives";
    assert_eq!(b.column(t, "holding_index"), ["0", "1", "1"]);
    assert_eq!(b.column(t, "nesting_level"), ["0", "0", "1"]);
    assert_full_row(
        &b,
        t,
        1,
        &[
            ("series_id", "S000072135"),
            ("as_of_date", "2026-06-30"),
            ("holding_index", "1"),
            ("nesting_level", "0"),
            ("category", "OPT"),
            ("counterparty_name", "COMMODITIES EXCHANGE CENTER"),
            ("counterparty_lei", "5493008GFNDTXFPHWI47"),
            ("put_or_call", "Call"),
            ("written_or_purchased", "Written"),
            ("payoff_profile", "NULL"),
            ("ref_instrument_name", "NULL"),
            ("ref_instrument_title", "NULL"),
            ("ref_cusip", "NULL"),
            ("ref_cusip_norm", "NULL"),
            ("ref_isin", "NULL"),
            ("ref_ticker", "GCQ6"),
            ("ref_other_identifiers", "[]"),
            ("ref_index_name", "NULL"),
            ("ref_index_identifier", "NULL"),
            ("ref_index_description", "NULL"),
            ("other_description", "NULL"),
            ("share_no", "100.0000000000"),
            ("principal_amount", "NULL"),
            ("notional_amount", "NULL"),
            ("exercise_price", "4400.0000000000"),
            ("exercise_currency", "USD"),
            ("expiration_date", "2026-07-28"),
            // `delta` is text: `XXXX` is not a sentinel issue.
            ("delta", "XXXX"),
            ("unrealized_appreciation", "12830.1600000000"),
            ("currency", "NULL"),
            ("termination_date", "NULL"),
            ("amount_currency_sold", "NULL"),
            ("currency_sold", "NULL"),
            ("amount_currency_purchased", "NULL"),
            ("currency_purchased", "NULL"),
            ("settlement_date", "NULL"),
            ("upfront_payment", "NULL"),
            ("upfront_receipt", "NULL"),
            ("payment_currency", "NULL"),
            ("receipt_currency", "NULL"),
            ("swap_flag", "NULL"),
            ("swap_leg_count", "0"),
            ("index_component_count", "0"),
            ("has_nested", "true"),
            ("has_additional_info", "false"),
            ("addl_name", "NULL"),
            ("addl_lei", "NULL"),
            ("addl_title", "NULL"),
            ("addl_cusip", "NULL"),
            ("addl_isin", "NULL"),
            ("addl_ticker", "NULL"),
            ("addl_other_identifiers", "[]"),
            ("addl_balance", "NULL"),
            ("addl_units", "NULL"),
            ("addl_units_description", "NULL"),
            ("addl_currency", "NULL"),
            ("addl_exchange_rate", "NULL"),
            ("addl_value_usd", "NULL"),
            ("addl_pct_value", "NULL"),
            ("addl_asset_category", "NULL"),
            ("addl_asset_category_description", "NULL"),
            ("addl_issuer_category", "NULL"),
            ("addl_issuer_category_description", "NULL"),
            ("addl_investment_country", "NULL"),
            ("addl_other_investment_country", "NULL"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_full_row(
        &b,
        t,
        2,
        &[
            ("series_id", "S000072135"),
            ("as_of_date", "2026-06-30"),
            ("holding_index", "1"),
            ("nesting_level", "1"),
            ("category", "FUT"),
            ("counterparty_name", "COMMODITIES EXCHANGE CENTER"),
            ("counterparty_lei", "5493008GFNDTXFPHWI47"),
            ("put_or_call", "NULL"),
            ("written_or_purchased", "NULL"),
            ("payoff_profile", "Long"),
            ("ref_instrument_name", "GOLD"),
            ("ref_instrument_title", "GOLD"),
            ("ref_cusip", "000000000"),
            ("ref_cusip_norm", "NULL"),
            ("ref_isin", "NULL"),
            ("ref_ticker", "NULL"),
            (
                "ref_other_identifiers",
                "[{description: INTERNAL, value: GOLD1}]",
            ),
            ("ref_index_name", "NULL"),
            ("ref_index_identifier", "NULL"),
            ("ref_index_description", "NULL"),
            ("other_description", "NULL"),
            ("share_no", "NULL"),
            ("principal_amount", "NULL"),
            ("notional_amount", "0.0000000000"),
            ("exercise_price", "NULL"),
            ("exercise_currency", "NULL"),
            ("expiration_date", "2026-08-27"),
            ("delta", "NULL"),
            ("unrealized_appreciation", "NULL"),
            ("currency", "USD"),
            ("termination_date", "NULL"),
            ("amount_currency_sold", "NULL"),
            ("currency_sold", "NULL"),
            ("amount_currency_purchased", "NULL"),
            ("currency_purchased", "NULL"),
            ("settlement_date", "NULL"),
            ("upfront_payment", "NULL"),
            ("upfront_receipt", "NULL"),
            ("payment_currency", "NULL"),
            ("receipt_currency", "NULL"),
            ("swap_flag", "NULL"),
            ("swap_leg_count", "0"),
            ("index_component_count", "0"),
            ("has_nested", "false"),
            ("has_additional_info", "true"),
            ("addl_name", "COMMODITIES EXCHANGE CENTER"),
            ("addl_lei", "5493008GFNDTXFPHWI47"),
            ("addl_title", "Gold Futures"),
            ("addl_cusip", "000000000"),
            ("addl_isin", "NULL"),
            ("addl_ticker", "GCQ6"),
            (
                "addl_other_identifiers",
                "[{description: FIGI, value: BBG01PZXTQX7}]",
            ),
            ("addl_balance", "0.0000000000"),
            ("addl_units", "NC"),
            ("addl_units_description", "NULL"),
            ("addl_currency", "USD"),
            ("addl_exchange_rate", "NULL"),
            ("addl_value_usd", "0.0000000000"),
            ("addl_pct_value", "0.000000000000"),
            ("addl_asset_category", "DIR"),
            ("addl_asset_category_description", "NULL"),
            ("addl_issuer_category", "CORP"),
            ("addl_issuer_category_description", "NULL"),
            ("addl_investment_country", "US"),
            ("addl_other_investment_country", "NULL"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_row(
        &b,
        t,
        0,
        &[
            ("category", "FUT"),
            ("counterparty_name", "CME Group Inc."),
            ("notional_amount", "9977090.0000000000"),
            ("unrealized_appreciation", "-1092390.0000000000"),
            ("has_nested", "false"),
        ],
    );
}

fn krane_general_info() -> sec::GeneralInfo {
    sec::GeneralInfo {
        reg_name: st("Krane Shares Trust"),
        reg_file_number: st("811-22698"),
        reg_cik: st("0001547576"),
        reg_lei: st("2549008E1DI99JHCCA80"),
        series_name: st("KraneShares 2x Long BABA Daily ETF"),
        series_id: st("S000089268"),
        series_lei: st("254900KD5E7J1YE53M51"),
        rep_period_end: st("2027-03-31"),
        rep_period_date: st("2026-06-30"),
        is_final_filing: false,
        reg_address: Some(sec::Address {
            street1: st("280 Park Ave"),
            street2: st("32nd Floor"),
            city: st("NEW YORK"),
            state: st("US-NY"),
            zip_code: st("10017"),
            country: st("US"),
            ..Default::default()
        }),
        reg_phone: st("2129330393"),
    }
}

/// `0002048251-26-007235` (block 2979888): a collateral balance and a total
/// return swap with a fixed receive leg and a floating pay leg. `fund_info`
/// and the signature are left out of the mirror (not under test here).
fn krane_baba_swap() -> sec::NportReport {
    sec::NportReport {
        filer_cik: st("0001547576"),
        general_info: Some(krane_general_info()),
        fund_info: None,
        holdings: vec![
            sec::PortfolioHolding {
                name: st("N/A"),
                lei: st("N/A"),
                title: st("MAREX COLLATERAL BALANCE"),
                cusip: st("N/A"),
                other_identifier: st("CASH-MX"),
                balance: st("2380873.80000000"),
                units: st("PA"),
                currency: st("USD"),
                value_usd: st("2380873.80000000"),
                pct_value: st("109.7099584035"),
                payoff_profile: st("Long"),
                asset_category: st("STIV"),
                issuer_category: st("OTHER"),
                investment_country: st("US"),
                fair_value_level: st("2"),
                security_lending: Some(sec::SecurityLending::default()),
                other_identifiers: vec![other_id("All Others", "CASH-MX")],
                issuer_category_description: st("N/A"),
                ..Default::default()
            },
            sec::PortfolioHolding {
                name: st("REF: 2X BABA US EQUITY"),
                lei: st("N/A"),
                title: st("REF: 2X BABA US EQUITY BRK :MAREX"),
                cusip: st("01609W102"),
                other_identifier: st("01609W102"),
                balance: st("45004.00000000"),
                units: st("NC"),
                currency: st("USD"),
                value_usd: st("-393325.87000000"),
                pct_value: st("-18.1243394071"),
                payoff_profile: st("N/A"),
                asset_category: st("DE"),
                issuer_category: st("OTHER"),
                investment_country: st("US"),
                fair_value_level: st("2"),
                derivative: Some(sec::Derivative {
                    category: st("SWP"),
                    counterparty_name: st("REF: 2X BABA US EQUITY"),
                    counterparty_lei: st("N/A"),
                    notional_amount: st("-4712809.79000000"),
                    unrealized_appreciation: st("-393325.87000000"),
                    currency: st("USD"),
                    termination_date: st("2027-05-12"),
                    upfront_payment: st("0.00000000"),
                    upfront_receipt: st("0.00000000"),
                    payment_currency: st("USD"),
                    receipt_currency: st("USD"),
                    swap_flag: st("Y"),
                    legs: vec![
                        sec::SwapLeg {
                            kind: st("fixedRecDesc"),
                            fixed_or_floating: st("Fixed"),
                            currency: st("USD"),
                            amount: st("53584.65000000"),
                            fixed_rate: st("0.00000000"),
                            ..Default::default()
                        },
                        sec::SwapLeg {
                            kind: st("floatingPmntDesc"),
                            fixed_or_floating: st("Floating"),
                            currency: st("USD"),
                            floating_rate_index: st("N/A"),
                            floating_rate_spread: st("0.00000000"),
                            payment_amount: st("0.00000000"),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }),
                security_lending: Some(sec::SecurityLending::default()),
                other_identifiers: vec![other_id("All Others", "01609W102")],
                issuer_category_description: st("N/A"),
                ..Default::default()
            },
        ],
        explanatory_notes: Vec::new(),
        signature: None,
    }
}

#[test]
fn fixture_two_leg_swap() {
    let b = map_report("0002048251-26-007235", krane_baba_swap());
    assert!(b.issues().is_empty());
    // No `fund_info`: no monthly rows, NULL totals.
    b.assert_rows(&[
        ("nport_monthly_returns", 0),
        ("nport_monthly_activity", 0),
        ("nport_holdings", 2),
        ("nport_derivatives", 1),
        ("nport_derivative_swap_legs", 2),
    ]);
    assert_row(
        &b,
        "nport_reports",
        0,
        &[
            ("registrant_street2", "32nd Floor"),
            ("fiscal_year_end", "2027-03-31"),
            ("as_of_date", "2026-06-30"),
            ("total_assets", "NULL"),
            ("cash_not_reported", "NULL"),
            ("holdings_count", "2"),
            ("derivative_holding_count", "1"),
            ("monthly_return_class_count", "0"),
        ],
    );
    let t = "nport_holdings";
    assert_eq!(b.column(t, "has_derivative"), ["false", "true"]);
    assert_eq!(b.column(t, "derivative_category"), ["NULL", "SWP"]);
    // Text columns keep `N/A` verbatim (no parse, no issue); the LEI and CUSIP
    // join keys drop it.
    assert_eq!(b.column(t, "issuer_lei"), ["N/A", "N/A"]);
    assert_eq!(b.column(t, "issuer_lei_norm"), ["NULL", "NULL"]);
    assert_eq!(b.column(t, "cusip_norm"), ["NULL", "01609W102"]);
    assert_eq!(b.column(t, "issuer_category_description"), ["N/A", "N/A"]);
    assert_eq!(
        b.column(t, "pct_value"),
        ["109.709958403500", "-18.124339407100"]
    );

    assert_row(
        &b,
        "nport_derivatives",
        0,
        &[
            ("holding_index", "1"),
            ("nesting_level", "0"),
            ("category", "SWP"),
            ("counterparty_lei", "N/A"),
            ("notional_amount", "-4712809.7900000000"),
            ("termination_date", "2027-05-12"),
            ("upfront_payment", "0.0000000000"),
            ("upfront_receipt", "0.0000000000"),
            ("payment_currency", "USD"),
            ("receipt_currency", "USD"),
            ("swap_flag", "true"),
            ("swap_leg_count", "2"),
            ("has_nested", "false"),
        ],
    );

    let t = "nport_derivative_swap_legs";
    assert_full_row(
        &b,
        t,
        0,
        &[
            ("series_id", "S000089268"),
            ("as_of_date", "2026-06-30"),
            ("holding_index", "1"),
            ("nesting_level", "0"),
            ("leg_index", "0"),
            ("leg_kind", "fixedRecDesc"),
            ("fixed_or_floating", "Fixed"),
            ("currency", "USD"),
            ("amount", "53584.6500000000"),
            ("fixed_rate", "0.000000000000"),
            ("floating_rate_index", "NULL"),
            ("floating_rate_spread", "NULL"),
            ("payment_amount", "NULL"),
            ("description", "NULL"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_full_row(
        &b,
        t,
        1,
        &[
            ("series_id", "S000089268"),
            ("as_of_date", "2026-06-30"),
            ("holding_index", "1"),
            ("nesting_level", "0"),
            ("leg_index", "1"),
            ("leg_kind", "floatingPmntDesc"),
            ("fixed_or_floating", "Floating"),
            ("currency", "USD"),
            ("amount", "NULL"),
            ("fixed_rate", "NULL"),
            ("floating_rate_index", "N/A"),
            ("floating_rate_spread", "0.000000000000"),
            ("payment_amount", "0.0000000000"),
            ("description", "NULL"),
            ("has_parse_issues", "false"),
        ],
    );
}

fn basket_component(
    name: &str,
    isin: &str,
    notional: &str,
    value: &str,
) -> sec::IndexBasketComponent {
    sec::IndexBasketComponent {
        name: st(name),
        isin: st(isin),
        notional_amount: st(notional),
        currency: st("USD"),
        value: st(value),
        issue_currency: st("USD"),
        ..Default::default()
    }
}

/// `0002048251-26-007523` (block 2979897), trimmed to its basket swap (holding
/// 26 of 670) and the first 3 of its 50 basket components.
fn fs_multi_strategy_basket() -> sec::NportReport {
    sec::NportReport {
        filer_cik: st("0001593547"),
        general_info: Some(sec::GeneralInfo {
            reg_name: st("ADVISORS' INNER CIRCLE III"),
            reg_file_number: st("811-22920"),
            reg_cik: st("0001593547"),
            reg_lei: st("549300TG800HOPJVWT31"),
            series_name: st("FS Multi-Strategy Alternatives Fund"),
            series_id: st("S000075444"),
            series_lei: st("549300E0668BUMX3FG88"),
            rep_period_end: st("2026-12-31"),
            rep_period_date: st("2026-06-30"),
            is_final_filing: false,
            reg_address: Some(sec::Address {
                street1: st("ONE FREEDOM VALLEY DRIVE"),
                city: st("OAKS"),
                state: st("US-PA"),
                zip_code: st("19456"),
                country: st("US"),
                ..Default::default()
            }),
            reg_phone: st("8774463863"),
        }),
        fund_info: None,
        holdings: vec![sec::PortfolioHolding {
            name: st("Morgan Stanley"),
            lei: st("9R7GPTSO7KV3UQJZQ078"),
            title: st("AI Services TRS"),
            cusip: st("TFS22VL04"),
            other_identifier: st("TFS22VL04"),
            balance: st("466187.00000000"),
            units: st("NC"),
            currency: st("USD"),
            value_usd: st("-382459.81000000"),
            pct_value: st("-0.02609648374"),
            payoff_profile: st("N/A"),
            asset_category: st("DE"),
            issuer_category: st("OTHER"),
            investment_country: st("US"),
            fair_value_level: st("2"),
            derivative: Some(sec::Derivative {
                category: st("SWP"),
                counterparty_name: st("MORGAN STANLEY & CO. LLC"),
                counterparty_lei: st("9R7GPTSO7KV3UQJZQ078"),
                notional_amount: st("-46977850.46000000"),
                unrealized_appreciation: st("-382459.81000000"),
                currency: st("USD"),
                termination_date: st("2027-12-07"),
                upfront_payment: st("0.00000000"),
                upfront_receipt: st("0.00000000"),
                payment_currency: st("USD"),
                receipt_currency: st("USD"),
                swap_flag: st("Y"),
                legs: vec![
                    sec::SwapLeg {
                        kind: st("otherRecDesc"),
                        fixed_or_floating: st("Other"),
                        description: st("equity-performance leg"),
                        ..Default::default()
                    },
                    sec::SwapLeg {
                        kind: st("floatingPmntDesc"),
                        fixed_or_floating: st("Floating"),
                        currency: st("USD"),
                        floating_rate_index: st("1D FF"),
                        floating_rate_spread: st("0.30000000"),
                        payment_amount: st("0.00000000"),
                        ..Default::default()
                    },
                ],
                ref_index_name: st("AI Services"),
                ref_index_identifier: st("MSFS22VL Index"),
                ref_index_description: st("N/A"),
                ref_index_components: vec![
                    basket_component(
                        "Alaska Air Group Inc",
                        "US0116591092",
                        "5933.36261021",
                        "309721.52825312",
                    ),
                    basket_component(
                        "Allegiant Travel Co",
                        "US01748X1028",
                        "2736.68391084",
                        "321834.02791429",
                    ),
                    basket_component(
                        "American Airlines Group Inc",
                        "US02376R1023",
                        "17826.50993473",
                        "322125.03452050",
                    ),
                ],
                ..Default::default()
            }),
            security_lending: Some(sec::SecurityLending::default()),
            other_identifiers: vec![other_id("All Others", "TFS22VL04")],
            issuer_category_description: st("N/A"),
            ..Default::default()
        }],
        explanatory_notes: vec![sec::NportExplanatoryNote {
            note_item: st("B.5.a"),
            note: st("The presentation of the Class A returns in Item B.5.a, includes the sales load associated with the share class."),
        }],
        signature: None,
    }
}

#[test]
fn fixture_index_basket_swap() {
    let b = map_report("0002048251-26-007523", fs_multi_strategy_basket());
    assert!(b.issues().is_empty());
    // The holding's CUSIP is an internal swap id that happens to have the
    // CUSIP shape: `cusip_norm` keeps it (§4.4 is a pure shape rule).
    assert_row(
        &b,
        "nport_holdings",
        0,
        &[("cusip", "TFS22VL04"), ("cusip_norm", "TFS22VL04")],
    );
    assert_row(
        &b,
        "nport_derivatives",
        0,
        &[
            ("ref_index_name", "AI Services"),
            ("ref_index_identifier", "MSFS22VL Index"),
            ("ref_index_description", "N/A"),
            ("notional_amount", "-46977850.4600000000"),
            ("swap_leg_count", "2"),
            ("index_component_count", "3"),
        ],
    );
    let t = "nport_derivative_swap_legs";
    assert_eq!(
        b.column(t, "leg_kind"),
        ["otherRecDesc", "floatingPmntDesc"]
    );
    assert_eq!(b.column(t, "fixed_or_floating"), ["Other", "Floating"]);
    assert_eq!(
        b.column(t, "description"),
        ["equity-performance leg", "NULL"]
    );
    assert_eq!(b.column(t, "floating_rate_index"), ["NULL", "1D FF"]);
    assert_eq!(
        b.column(t, "floating_rate_spread"),
        ["NULL", "0.300000000000"]
    );

    let t = "nport_derivative_index_components";
    assert_eq!(b.rows(t), 3);
    assert_eq!(b.column(t, "component_index"), ["0", "1", "2"]);
    assert_full_row(
        &b,
        t,
        1,
        &[
            ("series_id", "S000075444"),
            ("as_of_date", "2026-06-30"),
            ("holding_index", "0"),
            ("nesting_level", "0"),
            ("component_index", "1"),
            ("component_name", "Allegiant Travel Co"),
            ("cusip", "NULL"),
            ("cusip_norm", "NULL"),
            ("isin", "US01748X1028"),
            ("ticker", "NULL"),
            ("other_identifiers", "[]"),
            ("notional_amount", "2736.6839108400"),
            ("currency", "USD"),
            ("value", "321834.0279142900"),
            ("issue_currency", "USD"),
            ("has_parse_issues", "false"),
        ],
    );
    assert_eq!(
        b.column(t, "value"),
        [
            "309721.5282531200",
            "321834.0279142900",
            "322125.0345205000"
        ]
    );
}

fn monthly_return(class_id: &str, m1: &str, m2: &str, m3: &str) -> sec::MonthlyReturn {
    sec::MonthlyReturn {
        class_id: st(class_id),
        return_month1: st(m1),
        return_month2: st(m2),
        return_month3: st(m3),
    }
}

/// `0000910472-26-013496` (block 2979883), trimmed to its first convertible
/// bond (holding 6 of 24) and without monthly activity.
fn catalyst_convertible() -> sec::NportReport {
    sec::NportReport {
        filer_cik: st("0001355064"),
        general_info: Some(sec::GeneralInfo {
            reg_name: st("MUTUAL FUND SERIES TRUST"),
            reg_file_number: st("811-21872"),
            reg_cik: st("0001355064"),
            reg_lei: st("5493002ZGLQMLR4QMA96"),
            series_name: st("Catalyst Insider Income Fund"),
            series_id: st("S000045921"),
            series_lei: st("549300WPJ6UUCQURTQ64"),
            rep_period_end: st("2026-06-30"),
            rep_period_date: st("2026-06-30"),
            is_final_filing: false,
            reg_address: Some(sec::Address {
                street1: st("C/O GEMINI FUND SERVICES LLC"),
                street2: st("4221 North 203rd Street, Suite 100"),
                city: st("ELKHORN"),
                state: st("US-NE"),
                zip_code: st("68022"),
                country: st("US"),
                ..Default::default()
            }),
            reg_phone: st("631-470-2600"),
        }),
        fund_info: Some(sec::FundInfo {
            total_assets: st("67333282.38"),
            total_liabilities: st("351978.56"),
            net_assets: st("66981303.82"),
            monthly_total_returns: vec![
                monthly_return("C000143111", "0.81000000", "0.26000000", "0.29000000"),
                monthly_return("C000143110", "0.73000000", "0.07000000", "0.31000000"),
                monthly_return("C000143109", "0.79000000", "0.13000000", "0.38000000"),
            ],
            assets_invested: st("0.00000000"),
            misc_securities_assets: st("0.00000000"),
            cash_not_reported: st("0.00000000"),
            monthly_activity: Vec::new(),
        }),
        holdings: vec![sec::PortfolioHolding {
            name: st("PENNYMAC CORP"),
            lei: st("EWN0I878407TKQNXZ933"),
            title: st("PMT 8 1/2 06/01/29"),
            cusip: st("70932AAH6"),
            isin: st("US70932AAH68"),
            balance: st("4000000.00000000"),
            units: st("PA"),
            currency: st("USD"),
            value_usd: st("4093200.00000000"),
            pct_value: st("6.110958978940"),
            payoff_profile: st("Long"),
            asset_category: st("DBT"),
            issuer_category: st("CORP"),
            investment_country: st("US"),
            fair_value_level: st("2"),
            security_lending: Some(sec::SecurityLending::default()),
            debt_security: Some(sec::DebtSecurity {
                maturity_date: st("2029-06-01"),
                coupon_kind: st("Fixed"),
                annualized_rate: st("8.50000000"),
                is_default: Some(false),
                are_interest_payments_in_arrears: Some(false),
                is_paid_in_kind: Some(false),
                is_mandatory_convertible: Some(false),
                is_contingent_convertible: Some(true),
                reference_instruments: vec![sec::DebtReferenceInstrument {
                    name: st("PennyMac Mortgage Investment Trust"),
                    title: st("PennyMac Mortgage Investment Trust COM"),
                    currency: st("USD"),
                    cusip: st("70931T103"),
                    ..Default::default()
                }],
                conversion_currencies: vec![sec::ConversionCurrency {
                    currency: st("USD"),
                    conversion_ratio: st("63.33320000"),
                }],
                delta: st("XXXX"),
            }),
            ..Default::default()
        }],
        explanatory_notes: vec![sec::NportExplanatoryNote {
            note_item: st("B.5.a"),
            note: st(
                "Returns are reported without deducting sales loads and redemption fees, if any.",
            ),
        }],
        signature: None,
    }
}

#[test]
fn fixture_convertible_bond() {
    let b = map_report("0000910472-26-013496", catalyst_convertible());
    assert!(b.issues().is_empty());
    assert_row(
        &b,
        "nport_reports",
        0,
        &[
            (
                "explanatory_notes",
                "[{note_item: B.5.a, note: Returns are reported without deducting sales loads and redemption fees, if any.}]",
            ),
            ("holdings_count", "1"),
            ("derivative_holding_count", "0"),
            ("debt_holding_count", "1"),
            ("monthly_return_class_count", "3"),
        ],
    );
    let t = "nport_monthly_returns";
    assert_eq!(b.column(t, "return_index"), ["0", "1", "2"]);
    assert_eq!(
        b.column(t, "class_id"),
        ["C000143111", "C000143110", "C000143109"]
    );
    assert_eq!(
        b.column(t, "return_month3"),
        ["0.290000000000", "0.310000000000", "0.380000000000"]
    );
    assert_eq!(b.column(t, "month1_end"), ["2026-04-30"; 3]);

    assert_row(
        &b,
        "nport_holdings",
        0,
        &[
            ("issuer_lei_norm", "EWN0I878407TKQNXZ933"),
            ("cusip_norm", "70932AAH6"),
            ("isin", "US70932AAH68"),
            ("other_identifier", "NULL"),
            ("other_identifiers", "[]"),
            ("value_usd", "4093200.0000000000"),
            ("pct_value", "6.110958978940"),
            ("has_debt_security", "true"),
            ("debt_maturity_date", "2029-06-01"),
            ("debt_coupon_kind", "Fixed"),
            ("debt_annualized_rate", "8.500000000000"),
            ("debt_is_default", "false"),
            ("debt_are_interest_payments_in_arrears", "false"),
            ("debt_is_paid_in_kind", "false"),
            ("debt_is_mandatory_convertible", "false"),
            ("debt_is_contingent_convertible", "true"),
            ("debt_delta", "XXXX"),
            ("debt_reference_instrument_count", "1"),
            ("debt_conversion_currency_count", "1"),
            ("has_derivative", "false"),
            ("derivative_category", "NULL"),
        ],
    );

    assert_full_row(
        &b,
        "nport_debt_reference_instruments",
        0,
        &[
            ("series_id", "S000045921"),
            ("as_of_date", "2026-06-30"),
            ("holding_index", "0"),
            ("reference_index", "0"),
            ("reference_name", "PennyMac Mortgage Investment Trust"),
            ("reference_title", "PennyMac Mortgage Investment Trust COM"),
            ("currency", "USD"),
            ("cusip", "70931T103"),
            ("cusip_norm", "70931T103"),
            ("isin", "NULL"),
            ("ticker", "NULL"),
            ("other_identifiers", "[]"),
        ],
    );
    assert_full_row(
        &b,
        "nport_debt_conversion_currencies",
        0,
        &[
            ("series_id", "S000045921"),
            ("as_of_date", "2026-06-30"),
            ("holding_index", "0"),
            ("conversion_index", "0"),
            ("currency", "USD"),
            ("conversion_ratio", "63.333200000000"),
            ("has_parse_issues", "false"),
        ],
    );
}

fn vanguard_flows(
    month: u32,
    sales: &str,
    redemption: &str,
    gain: &str,
    appreciation: &str,
) -> sec::MonthlyFundActivity {
    sec::MonthlyFundActivity {
        month,
        sales: st(sales),
        reinvestment: st("0.00000000"),
        redemption: st(redemption),
        net_realized_gain: st(gain),
        net_unrealized_appreciation: st(appreciation),
    }
}

/// `0000857490-26-000627` (block 2979913), trimmed to holdings 0 (EUR equity,
/// restricted), 8 (CHF forward, `exchange_rate` `N/A`) and 31 (securities
/// lending cash collateral).
fn vanguard_international() -> sec::NportReport {
    sec::NportReport {
        filer_cik: st("0000857490"),
        general_info: Some(sec::GeneralInfo {
            reg_name: st("VANGUARD VARIABLE INSURANCE FUNDS"),
            reg_file_number: st("811-05962"),
            reg_cik: st("0000857490"),
            reg_lei: st("549300N9FZ0B1IR95761"),
            series_name: st("INTERNATIONAL PORTFOLIO"),
            series_id: st("S000004403"),
            series_lei: st("OT40UIT5RF6NRFJZGB59"),
            rep_period_end: st("2026-12-31"),
            rep_period_date: st("2026-06-30"),
            is_final_filing: false,
            reg_address: Some(sec::Address {
                street1: st("100 Vanguard Boulevard"),
                city: st("Malvern"),
                state: st("US-PA"),
                zip_code: st("19355"),
                country: st("US"),
                ..Default::default()
            }),
            reg_phone: st("610-669-1000"),
        }),
        fund_info: Some(sec::FundInfo {
            total_assets: st("3424093431.03"),
            total_liabilities: st("5710538.41"),
            net_assets: st("3418382892.62"),
            monthly_total_returns: vec![monthly_return(
                "C000012159",
                "7.79220779",
                "2.62869660",
                "0.81821416",
            )],
            assets_invested: st("0.00000000"),
            misc_securities_assets: st("0.00000000"),
            cash_not_reported: st("18733341.05000000"),
            monthly_activity: vec![
                vanguard_flows(
                    1,
                    "29798066.30999950",
                    "51070235.68000000",
                    "10214424.77000000",
                    "221794704.78000000",
                ),
                vanguard_flows(
                    2,
                    "24251890.86000060",
                    "50204127.24000000",
                    "55529822.93000000",
                    "22344057.59000000",
                ),
                vanguard_flows(
                    3,
                    "14900743.42000010",
                    "43405798.26000000",
                    "26610565.88000000",
                    "-1080301.29000000",
                ),
            ],
        }),
        holdings: vec![
            sec::PortfolioHolding {
                name: st("Adyen NV"),
                lei: st("724500973ODKK3IFQ447"),
                title: st("ADYEN NV"),
                cusip: st("N/A"),
                isin: st("NL0012969182"),
                balance: st("53222.00000000"),
                units: st("NS"),
                currency: st("EUR"),
                value_usd: st("49928479.14000000"),
                pct_value: st("1.460587672837"),
                payoff_profile: st("Long"),
                asset_category: st("EC"),
                issuer_category: st("CORP"),
                investment_country: st("NL"),
                fair_value_level: st("2"),
                is_restricted: true,
                security_lending: Some(sec::SecurityLending::default()),
                exchange_rate: st("1.14260000"),
                ..Default::default()
            },
            sec::PortfolioHolding {
                name: st("N/A"),
                lei: st("N/A"),
                title: st("CHF/USD FWD 20260916"),
                cusip: st("N/A"),
                ticker: st("CHF"),
                balance: st("1.00000000"),
                units: st("NC"),
                currency: st("CHF"),
                value_usd: st("2131.61000000"),
                pct_value: st("0.000062357262"),
                payoff_profile: st("N/A"),
                asset_category: st("DFE"),
                issuer_category: st("OTHER"),
                investment_country: st("N/A"),
                fair_value_level: st("2"),
                derivative: Some(sec::Derivative {
                    category: st("FWD"),
                    counterparty_name: st("STATE STREET BANK AND TRUST COMPANY"),
                    counterparty_lei: st("571474TGEMMWANRLN572"),
                    unrealized_appreciation: st("2131.61000000"),
                    amount_currency_sold: st("97200.00000000"),
                    currency_sold: st("CHF"),
                    amount_currency_purchased: st("123463.06000000"),
                    currency_purchased: st("USD"),
                    settlement_date: st("2026-09-16"),
                    ..Default::default()
                }),
                security_lending: Some(sec::SecurityLending::default()),
                exchange_rate: st("N/A"),
                issuer_category_description: st("N/A"),
                ..Default::default()
            },
            sec::PortfolioHolding {
                name: st("Vanguard Cmt Funds-Vanguard Market Liquidity Fund"),
                lei: st("1I6HV0TLSTR3A4XQ6L78"),
                title: st("Vanguard Market Liquidity Fund"),
                cusip: st("N/A"),
                other_identifier: st("SLBBH1142"),
                balance: st("142015.06000000"),
                units: st("NS"),
                currency: st("USD"),
                value_usd: st("14200085.85000000"),
                pct_value: st("0.415403607379"),
                payoff_profile: st("Long"),
                asset_category: st("STIV"),
                issuer_category: st("CORP"),
                investment_country: st("US"),
                fair_value_level: st("1"),
                security_lending: Some(sec::SecurityLending {
                    is_cash_collateral: true,
                    cash_collateral_value: st("14200085.85000000"),
                    ..Default::default()
                }),
                other_identifiers: vec![other_id("FAID", "SLBBH1142")],
                ..Default::default()
            },
        ],
        explanatory_notes: Vec::new(),
        signature: None,
    }
}

#[test]
fn fixture_foreign_currency_forward_lending_and_sentinel() {
    let b = map_report("0000857490-26-000627", vanguard_international());
    assert_eq!(
        issue_tuples(&b),
        vec![issue(
            "nport_holdings",
            "exchange_rate",
            [Some(1), None, None],
            "N/A",
            "sentinel"
        )]
    );
    let t = "nport_holdings";
    assert_eq!(b.column(t, "currency"), ["EUR", "CHF", "USD"]);
    assert_eq!(
        b.column(t, "exchange_rate"),
        ["1.142600000000", "NULL", "NULL"]
    );
    assert_eq!(b.column(t, "has_parse_issues"), ["false", "true", "false"]);
    assert_eq!(b.column(t, "is_restricted"), ["true", "false", "false"]);
    assert_eq!(
        b.column(t, "issuer_lei_norm"),
        ["724500973ODKK3IFQ447", "NULL", "1I6HV0TLSTR3A4XQ6L78"]
    );
    assert_eq!(b.column(t, "investment_country"), ["NL", "N/A", "US"]);
    assert_eq!(
        b.column(t, "pct_value"),
        ["1.460587672837", "0.000062357262", "0.415403607379"]
    );
    assert_eq!(b.column(t, "has_security_lending"), ["true"; 3]);
    assert_eq!(
        b.column(t, "lending_is_cash_collateral"),
        ["false", "false", "true"]
    );
    assert_eq!(b.column(t, "lending_is_non_cash_collateral"), ["false"; 3]);
    assert_eq!(b.column(t, "lending_is_loan_by_fund"), ["false"; 3]);
    assert_eq!(b.column(t, "lending_loan_value"), ["NULL"; 3]);
    assert_eq!(
        b.column(t, "lending_cash_collateral_value"),
        ["NULL", "NULL", "14200085.8500000000"]
    );

    assert_row(
        &b,
        "nport_derivatives",
        0,
        &[
            ("holding_index", "1"),
            ("category", "FWD"),
            ("counterparty_lei", "571474TGEMMWANRLN572"),
            ("unrealized_appreciation", "2131.6100000000"),
            ("amount_currency_sold", "97200.0000000000"),
            ("currency_sold", "CHF"),
            ("amount_currency_purchased", "123463.0600000000"),
            ("currency_purchased", "USD"),
            ("settlement_date", "2026-09-16"),
            ("swap_flag", "NULL"),
        ],
    );
    assert_row(
        &b,
        "nport_reports",
        0,
        &[
            ("total_assets", "3424093431.0300000000"),
            ("net_assets", "3418382892.6200000000"),
            ("cash_not_reported", "18733341.0500000000"),
            ("has_parse_issues", "false"),
        ],
    );
    let t = "nport_monthly_activity";
    assert_eq!(
        b.column(t, "sales"),
        [
            "29798066.3099995000",
            "24251890.8600006000",
            "14900743.4200001000"
        ]
    );
    assert_eq!(
        b.column(t, "net_unrealized_appreciation"),
        [
            "221794704.7800000000",
            "22344057.5900000000",
            "-1080301.2900000000"
        ]
    );
}

/// One of the three swaps of [`tidal_dram`].
struct TidalSwap {
    id: &'static str,
    pct: &'static str,
    value: &'static str,
    counterparty: (&'static str, &'static str),
    notional: &'static str,
    termination: &'static str,
    spread: &'static str,
    payment: &'static str,
}

fn tidal_swap(s: &TidalSwap) -> sec::PortfolioHolding {
    sec::PortfolioHolding {
        name: st("N/A"),
        lei: st("N/A"),
        title: st("SAMSUNG ASSET MANAGEMENT CO. - KODEX FN WEBTOON AN"),
        cusip: st("N/A"),
        other_identifier: st(s.id),
        balance: st("1"),
        units: st("NC"),
        currency: st("USD"),
        value_usd: st(s.value),
        pct_value: st(s.pct),
        payoff_profile: st("N/A"),
        asset_category: st("DE"),
        issuer_category: st("OTHER"),
        investment_country: st("US"),
        fair_value_level: st("2"),
        derivative: Some(sec::Derivative {
            category: st("SWP"),
            counterparty_name: st(s.counterparty.0),
            counterparty_lei: st(s.counterparty.1),
            ref_instrument_name: st("N/A"),
            ref_instrument_title: st("common stock"),
            notional_amount: st(s.notional),
            unrealized_appreciation: st(s.value),
            currency: st("USD"),
            termination_date: st(s.termination),
            upfront_payment: st("0"),
            upfront_receipt: st("0"),
            payment_currency: st("USD"),
            receipt_currency: st("USD"),
            swap_flag: st("Y"),
            legs: vec![
                sec::SwapLeg {
                    kind: st("otherRecDesc"),
                    fixed_or_floating: st("Other"),
                    description: st("OTHER"),
                    ..Default::default()
                },
                sec::SwapLeg {
                    kind: st("floatingPmntDesc"),
                    fixed_or_floating: st("Floating"),
                    currency: st("USD"),
                    floating_rate_index: st("OBFR01 INDEX"),
                    floating_rate_spread: st(s.spread),
                    payment_amount: st(s.payment),
                    ..Default::default()
                },
            ],
            ref_other_identifiers: vec![other_id("Custom Identifier", s.id)],
            ..Default::default()
        }),
        security_lending: Some(sec::SecurityLending::default()),
        other_identifiers: vec![other_id("USER DEFINED", s.id)],
        issuer_category_description: st("SWP"),
        ..Default::default()
    }
}

fn tidal_flows(month: u32, sales: &str, other: &str) -> sec::MonthlyFundActivity {
    sec::MonthlyFundActivity {
        month,
        sales: st(sales),
        reinvestment: st(other),
        redemption: st(other),
        net_realized_gain: st("0"),
        net_unrealized_appreciation: st("0"),
    }
}

/// `0002000324-26-004161` (block 2979907): three swaps whose `pct_value` has
/// 16-17 decimals (rounded at R12), and `N/A` returns and flows.
fn tidal_dram() -> sec::NportReport {
    let swaps = [
        TidalSwap {
            id: "77926X320-TRS-03/31/33-L-DRAL",
            pct: "-1.1729354470093785",
            value: "-33421.05",
            counterparty: (
                "NOMURA SECURITIES INTERNATIONAL INC.",
                "OXTKY6Q8X53C9ILVV871",
            ),
            notional: "1477000",
            termination: "2033-03-31",
            spread: "16.5",
            payment: "4104.85",
        },
        TidalSwap {
            id: "77926X320-TRS-06/27/33-L-DRAL",
            pct: "-1.0567760201464529",
            value: "-30111.26",
            counterparty: ("MAREX CAPITAL MARKETS INC.", "5493006BWPDUCYG6EQ34"),
            notional: "1477000",
            termination: "2033-06-27",
            spread: "5",
            payment: "1800.26",
        },
        TidalSwap {
            id: "77926X320-TRS-08/15/28-L-DRAL",
            pct: "-0.20258155753198523",
            value: "-5772.26",
            counterparty: ("CLEAR STREET LLC", "549300KNQS43Y7TO3X67"),
            notional: "2603877.15",
            termination: "2028-08-15",
            spread: "3",
            payment: "1925.46",
        },
    ];
    sec::NportReport {
        filer_cik: st("0001924868"),
        general_info: Some(sec::GeneralInfo {
            reg_name: st("Tidal Trust II"),
            reg_file_number: st("811-23793"),
            reg_cik: st("0001924868"),
            reg_lei: st("549300BGXECFCIZF2P89"),
            series_name: st("Defiance Daily Target 2x Long Dram ETF"),
            series_id: st("S000105968"),
            series_lei: st("254900P0FWM8IRH2VB14"),
            rep_period_end: st("2027-03-31"),
            rep_period_date: st("2026-06-30"),
            is_final_filing: false,
            reg_address: Some(sec::Address {
                street1: st("234 West Florida Street"),
                street2: st("Suite 700"),
                city: st("Milwaukee"),
                state: st("US-WI"),
                zip_code: st("53204"),
                country: st("US"),
                ..Default::default()
            }),
            reg_phone: st("8449867676"),
        }),
        fund_info: Some(sec::FundInfo {
            total_assets: st("2936646.93"),
            total_liabilities: st("87295.75"),
            net_assets: st("2849351.18"),
            monthly_total_returns: vec![monthly_return("C000276783", "N/A", "N/A", "9.59")],
            assets_invested: st("0"),
            misc_securities_assets: st("0"),
            cash_not_reported: st("2319000"),
            monthly_activity: vec![
                tidal_flows(1, "N/A", "N/A"),
                tidal_flows(2, "N/A", "N/A"),
                tidal_flows(3, "2943843", "0"),
            ],
        }),
        holdings: swaps.iter().map(tidal_swap).collect(),
        explanatory_notes: Vec::new(),
        signature: None,
    }
}

#[test]
fn fixture_rounded_pct_value_and_sentinel_flows() {
    let b = map_report("0002000324-26-004161", tidal_dram());
    let at = |i| [Some(i), None, None];
    let (r, a, h) = (
        "nport_monthly_returns",
        "nport_monthly_activity",
        "nport_holdings",
    );
    assert_eq!(
        issue_tuples(&b),
        vec![
            issue(r, "return_month1", at(0), "N/A", "sentinel"),
            issue(r, "return_month2", at(0), "N/A", "sentinel"),
            issue(a, "sales", at(0), "N/A", "sentinel"),
            issue(a, "reinvestment", at(0), "N/A", "sentinel"),
            issue(a, "redemption", at(0), "N/A", "sentinel"),
            issue(a, "sales", at(1), "N/A", "sentinel"),
            issue(a, "reinvestment", at(1), "N/A", "sentinel"),
            issue(a, "redemption", at(1), "N/A", "sentinel"),
            issue(h, "pct_value", at(0), "-1.1729354470093785", "rounded"),
            issue(h, "pct_value", at(1), "-1.0567760201464529", "rounded"),
            issue(h, "pct_value", at(2), "-0.20258155753198523", "rounded"),
        ]
    );
    assert_eq!(b.column("nport_reports", "has_parse_issues"), ["false"]);
    assert_row(
        &b,
        "nport_reports",
        0,
        &[
            ("assets_invested", "0.0000000000"),
            ("cash_not_reported", "2319000.0000000000"),
        ],
    );
    assert_row(
        &b,
        r,
        0,
        &[
            ("return_month1", "NULL"),
            ("return_month2", "NULL"),
            ("return_month3", "9.590000000000"),
            ("has_parse_issues", "true"),
        ],
    );
    assert_eq!(b.column(a, "sales"), ["NULL", "NULL", "2943843.0000000000"]);
    assert_eq!(b.column(a, "redemption"), ["NULL", "NULL", "0.0000000000"]);
    assert_eq!(b.column(a, "has_parse_issues"), ["true", "true", "false"]);

    // Rounded half away from zero at 12 decimals; the raw text is the issue's.
    assert_eq!(
        b.column(h, "pct_value"),
        ["-1.172935447009", "-1.056776020146", "-0.202581557532"]
    );
    assert_eq!(b.column(h, "balance"), ["1.0000000000"; 3]);
    assert_eq!(b.column(h, "has_parse_issues"), ["true"; 3]);
    assert_eq!(b.column(h, "issuer_category_description"), ["SWP"; 3]);

    let t = "nport_derivatives";
    assert_eq!(b.column(t, "has_parse_issues"), ["false"; 3]);
    assert_eq!(
        b.column(t, "notional_amount"),
        [
            "1477000.0000000000",
            "1477000.0000000000",
            "2603877.1500000000"
        ]
    );
    assert_eq!(
        b.column(t, "ref_other_identifiers"),
        [
            "[{description: Custom Identifier, value: 77926X320-TRS-03/31/33-L-DRAL}]",
            "[{description: Custom Identifier, value: 77926X320-TRS-06/27/33-L-DRAL}]",
            "[{description: Custom Identifier, value: 77926X320-TRS-08/15/28-L-DRAL}]",
        ]
    );
    let t = "nport_derivative_swap_legs";
    assert_eq!(b.rows(t), 6);
    assert_eq!(b.column(t, "holding_index"), ["0", "0", "1", "1", "2", "2"]);
    assert_eq!(b.column(t, "leg_index"), ["0", "1", "0", "1", "0", "1"]);
    assert_eq!(
        b.column(t, "floating_rate_spread"),
        [
            "NULL",
            "16.500000000000",
            "NULL",
            "5.000000000000",
            "NULL",
            "3.000000000000"
        ]
    );
    assert_eq!(
        b.column(t, "payment_amount"),
        [
            "NULL",
            "4104.8500000000",
            "NULL",
            "1800.2600000000",
            "NULL",
            "1925.4600000000"
        ]
    );
}

// ---------------------------------------------------------------------------
// Synthetic edge cases
// ---------------------------------------------------------------------------

#[test]
fn absent_sub_messages_give_nulls_and_present_empty_ones_give_false() {
    let report = sec::NportReport {
        holdings: vec![
            // Every optional sub-message present but empty.
            sec::PortfolioHolding {
                derivative: Some(sec::Derivative::default()),
                debt_security: Some(sec::DebtSecurity::default()),
                security_lending: Some(sec::SecurityLending::default()),
                ..Default::default()
            },
            // Every optional sub-message absent.
            sec::PortfolioHolding::default(),
        ],
        ..Default::default()
    };
    let b = map_report("0000000000-26-900002", report);
    assert!(b.issues().is_empty());
    b.assert_rows(&[
        ("nport_reports", 1),
        ("nport_monthly_returns", 0),
        ("nport_monthly_activity", 0),
        ("nport_holdings", 2),
        ("nport_debt_reference_instruments", 0),
        ("nport_debt_conversion_currencies", 0),
        ("nport_derivatives", 1),
        ("nport_derivative_swap_legs", 0),
        ("nport_derivative_index_components", 0),
    ]);
    // No `general_info`: NULL registrant, series and `is_final_filing`.
    assert_row(
        &b,
        "nport_reports",
        0,
        &[
            ("filer_cik", "NULL"),
            ("registrant_name", "NULL"),
            ("registrant_street1", "NULL"),
            ("registrant_non_us_state_territory", "NULL"),
            ("series_id", "NULL"),
            ("as_of_date", "NULL"),
            ("is_final_filing", "NULL"),
            ("total_assets", "NULL"),
            ("explanatory_notes", "[]"),
            ("holdings_count", "2"),
            ("derivative_holding_count", "1"),
            ("debt_holding_count", "1"),
            ("monthly_return_class_count", "0"),
            ("has_parse_issues", "false"),
        ],
    );
    let t = "nport_holdings";
    assert_eq!(b.column(t, "registrant_name"), ["NULL"; 2]);
    assert_eq!(b.column(t, "as_of_date"), ["NULL"; 2]);
    assert_eq!(b.column(t, "is_restricted"), ["false"; 2]);
    assert_eq!(b.column(t, "other_identifiers"), ["[]"; 2]);
    assert_eq!(b.column(t, "has_debt_security"), ["true", "false"]);
    // Proto `optional bool`: NULL when unset, whether or not the message is set.
    assert_eq!(b.column(t, "debt_is_default"), ["NULL"; 2]);
    assert_eq!(b.column(t, "debt_is_contingent_convertible"), ["NULL"; 2]);
    assert_eq!(b.column(t, "debt_reference_instrument_count"), ["0"; 2]);
    assert_eq!(b.column(t, "has_derivative"), ["true", "false"]);
    assert_eq!(b.column(t, "derivative_category"), ["NULL"; 2]);
    // Proto bools of `SecurityLending`: NULL exactly when it is absent.
    assert_eq!(b.column(t, "has_security_lending"), ["true", "false"]);
    assert_eq!(b.column(t, "lending_is_cash_collateral"), ["false", "NULL"]);
    assert_eq!(
        b.column(t, "lending_is_non_cash_collateral"),
        ["false", "NULL"]
    );
    assert_eq!(b.column(t, "lending_is_loan_by_fund"), ["false", "NULL"]);
    assert_eq!(b.column(t, "has_parse_issues"), ["false"; 2]);

    assert_row(
        &b,
        "nport_derivatives",
        0,
        &[
            ("holding_index", "0"),
            ("nesting_level", "0"),
            ("category", "NULL"),
            ("ref_other_identifiers", "[]"),
            ("swap_flag", "NULL"),
            ("swap_leg_count", "0"),
            ("index_component_count", "0"),
            ("has_nested", "false"),
            ("has_additional_info", "false"),
            ("addl_other_identifiers", "[]"),
            ("has_parse_issues", "false"),
        ],
    );
}

#[test]
fn month_ends_count_back_from_the_as_of_month() {
    let report = |as_of: &str| sec::NportReport {
        general_info: Some(sec::GeneralInfo {
            rep_period_date: st(as_of),
            ..Default::default()
        }),
        fund_info: Some(sec::FundInfo {
            monthly_total_returns: vec![sec::MonthlyReturn::default()],
            monthly_activity: [1, 2, 3, 0, 4]
                .into_iter()
                .map(|month| sec::MonthlyFundActivity {
                    month,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    };
    let blocks = vec![window_block(
        BLOCK_NUM,
        vec![
            nport_filing("0000000000-26-900003", report("2026-01-31")),
            nport_filing("0000000000-26-900004", report("2024-03-15")),
            nport_filing("0000000000-26-900005", report("")),
        ],
    )];
    let b = map(&blocks);
    let t = "nport_monthly_returns";
    assert_eq!(
        b.column(t, "month1_end"),
        ["2025-11-30", "2024-01-31", "NULL"]
    );
    assert_eq!(
        b.column(t, "month2_end"),
        ["2025-12-31", "2024-02-29", "NULL"]
    );
    // The as-of month's last day, whatever the as-of day.
    assert_eq!(
        b.column(t, "month3_end"),
        ["2026-01-31", "2024-03-31", "NULL"]
    );
    // All-empty returns are NULL without issues.
    assert_eq!(b.column(t, "return_month1"), ["NULL"; 3]);
    assert_eq!(b.column(t, "has_parse_issues"), ["false"; 3]);

    let t = "nport_monthly_activity";
    let month = b.column(t, "month");
    assert_eq!(month[..5], ["1", "2", "3", "0", "4"]);
    // §4.4 (C3): `0` (proto3 unset) and months outside 1..=3 name no month.
    assert_eq!(
        b.column(t, "month_end"),
        [
            "2025-11-30",
            "2025-12-31",
            "2026-01-31",
            "NULL",
            "NULL",
            "2024-01-31",
            "2024-02-29",
            "2024-03-31",
            "NULL",
            "NULL",
            "NULL",
            "NULL",
            "NULL",
            "NULL",
            "NULL",
        ]
    );
    assert_eq!(
        b.column(t, "activity_index")[5..10],
        ["0", "1", "2", "3", "4"]
    );
    assert!(b.issues().is_empty());
}

#[test]
fn identifier_join_keys_are_normalized_next_to_the_raw_values() {
    let holding = |lei: &str, cusip: &str| sec::PortfolioHolding {
        lei: st(lei),
        cusip: st(cusip),
        ..Default::default()
    };
    let report = sec::NportReport {
        holdings: vec![
            holding(" kjnxbswzvikex4nfol81 ", " 037833 10 0 "),
            holding("00000000000000000000", "999999999"),
            holding("N/A", "12345678"),
            holding("KJNXBSWZVIKEX4NFOL8", "abc123def"),
            sec::PortfolioHolding {
                debt_security: Some(sec::DebtSecurity {
                    reference_instruments: vec![sec::DebtReferenceInstrument {
                        cusip: st("70931t103"),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                derivative: Some(sec::Derivative {
                    ref_cusip: st("N/A"),
                    nested: Some(Box::new(sec::Derivative {
                        ref_cusip: st("01609w102"),
                        additional_info: Some(sec::DerivativeAdditionalInfo {
                            cusip: st("01609w102"),
                            ..Default::default()
                        }),
                        ref_index_components: vec![sec::IndexBasketComponent {
                            cusip: st("000000000"),
                            ..Default::default()
                        }],
                        ..Default::default()
                    })),
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let b = map_report("0000000000-26-900006", report);
    assert!(b.issues().is_empty());
    let t = "nport_holdings";
    assert_eq!(
        b.column(t, "issuer_lei"),
        [
            " kjnxbswzvikex4nfol81 ",
            "00000000000000000000",
            "N/A",
            "KJNXBSWZVIKEX4NFOL8",
            "NULL"
        ]
    );
    assert_eq!(
        b.column(t, "issuer_lei_norm"),
        ["KJNXBSWZVIKEX4NFOL81", "NULL", "NULL", "NULL", "NULL"]
    );
    assert_eq!(
        b.column(t, "cusip_norm"),
        ["037833100", "NULL", "NULL", "ABC123DEF", "NULL"]
    );
    assert_eq!(
        b.column("nport_debt_reference_instruments", "cusip_norm"),
        ["70931T103"]
    );
    let t = "nport_derivatives";
    assert_eq!(b.column(t, "ref_cusip"), ["N/A", "01609w102"]);
    assert_eq!(b.column(t, "ref_cusip_norm"), ["NULL", "01609W102"]);
    // `addl_cusip` has no normalized sibling (§4.4); it stays verbatim.
    assert_eq!(b.column(t, "addl_cusip"), ["NULL", "01609w102"]);
    let t = "nport_derivative_index_components";
    assert_eq!(b.column(t, "cusip"), ["000000000"]);
    assert_eq!(b.column(t, "cusip_norm"), ["NULL"]);
    assert_eq!(b.column(t, "nesting_level"), ["1"]);
}

/// A report whose every typed source value is `bad<n>` (unparseable).
fn all_unparseable_report() -> sec::NportReport {
    let bad = |n: u32| format!("bad{n}");
    sec::NportReport {
        general_info: Some(sec::GeneralInfo {
            rep_period_end: bad(1),
            rep_period_date: bad(2),
            ..Default::default()
        }),
        fund_info: Some(sec::FundInfo {
            total_assets: bad(3),
            total_liabilities: bad(4),
            net_assets: bad(5),
            assets_invested: bad(6),
            misc_securities_assets: bad(7),
            cash_not_reported: bad(8),
            monthly_total_returns: vec![sec::MonthlyReturn {
                return_month1: bad(9),
                return_month2: bad(10),
                return_month3: bad(11),
                ..Default::default()
            }],
            monthly_activity: vec![sec::MonthlyFundActivity {
                month: 1,
                sales: bad(12),
                reinvestment: bad(13),
                redemption: bad(14),
                net_realized_gain: bad(15),
                net_unrealized_appreciation: bad(16),
            }],
        }),
        holdings: vec![sec::PortfolioHolding {
            balance: bad(17),
            exchange_rate: bad(18),
            value_usd: bad(19),
            pct_value: bad(20),
            debt_security: Some(sec::DebtSecurity {
                maturity_date: bad(21),
                annualized_rate: bad(22),
                conversion_currencies: vec![sec::ConversionCurrency {
                    conversion_ratio: bad(23),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            security_lending: Some(sec::SecurityLending {
                loan_value: bad(24),
                cash_collateral_value: bad(25),
                ..Default::default()
            }),
            derivative: Some(sec::Derivative {
                share_no: bad(26),
                principal_amount: bad(27),
                notional_amount: bad(28),
                exercise_price: bad(29),
                expiration_date: bad(30),
                unrealized_appreciation: bad(31),
                termination_date: bad(32),
                amount_currency_sold: bad(33),
                amount_currency_purchased: bad(34),
                settlement_date: bad(35),
                upfront_payment: bad(36),
                upfront_receipt: bad(37),
                swap_flag: bad(38),
                additional_info: Some(sec::DerivativeAdditionalInfo {
                    balance: bad(39),
                    exchange_rate: bad(40),
                    value_usd: bad(41),
                    pct_value: bad(42),
                    ..Default::default()
                }),
                legs: vec![sec::SwapLeg {
                    amount: bad(43),
                    fixed_rate: bad(44),
                    floating_rate_spread: bad(45),
                    payment_amount: bad(46),
                    ..Default::default()
                }],
                ref_index_components: vec![sec::IndexBasketComponent {
                    notional_amount: bad(47),
                    value: bad(48),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[test]
fn every_typed_column_logs_its_issue_with_the_row_keys() {
    let b = map_report("0000000000-26-900007", all_unparseable_report());
    let k0 = [None, None, None];
    let k1 = [Some(0), None, None];
    let k2 = [Some(0), Some(0), None];
    let k3 = [Some(0), Some(0), Some(0)];
    // Emit order: report, returns, activity, then the holding (its own
    // columns in schema order), its conversion currency, its derivative, the
    // derivative's leg and component. `bad<n>` numbers the source fields.
    let expected = [
        ("nport_reports", "fiscal_year_end", k0, 1),
        ("nport_reports", "as_of_date", k0, 2),
        ("nport_reports", "total_assets", k0, 3),
        ("nport_reports", "total_liabilities", k0, 4),
        ("nport_reports", "net_assets", k0, 5),
        ("nport_reports", "assets_invested", k0, 6),
        ("nport_reports", "misc_securities_assets", k0, 7),
        ("nport_reports", "cash_not_reported", k0, 8),
        ("nport_monthly_returns", "return_month1", k1, 9),
        ("nport_monthly_returns", "return_month2", k1, 10),
        ("nport_monthly_returns", "return_month3", k1, 11),
        ("nport_monthly_activity", "sales", k1, 12),
        ("nport_monthly_activity", "reinvestment", k1, 13),
        ("nport_monthly_activity", "redemption", k1, 14),
        ("nport_monthly_activity", "net_realized_gain", k1, 15),
        (
            "nport_monthly_activity",
            "net_unrealized_appreciation",
            k1,
            16,
        ),
        ("nport_holdings", "balance", k1, 17),
        ("nport_holdings", "exchange_rate", k1, 18),
        ("nport_holdings", "value_usd", k1, 19),
        ("nport_holdings", "pct_value", k1, 20),
        ("nport_holdings", "debt_maturity_date", k1, 21),
        ("nport_holdings", "debt_annualized_rate", k1, 22),
        ("nport_holdings", "lending_loan_value", k1, 24),
        ("nport_holdings", "lending_cash_collateral_value", k1, 25),
        (
            "nport_debt_conversion_currencies",
            "conversion_ratio",
            k2,
            23,
        ),
        ("nport_derivatives", "share_no", k2, 26),
        ("nport_derivatives", "principal_amount", k2, 27),
        ("nport_derivatives", "notional_amount", k2, 28),
        ("nport_derivatives", "exercise_price", k2, 29),
        ("nport_derivatives", "expiration_date", k2, 30),
        ("nport_derivatives", "unrealized_appreciation", k2, 31),
        ("nport_derivatives", "termination_date", k2, 32),
        ("nport_derivatives", "amount_currency_sold", k2, 33),
        ("nport_derivatives", "amount_currency_purchased", k2, 34),
        ("nport_derivatives", "settlement_date", k2, 35),
        ("nport_derivatives", "upfront_payment", k2, 36),
        ("nport_derivatives", "upfront_receipt", k2, 37),
        ("nport_derivatives", "swap_flag", k2, 38),
        ("nport_derivatives", "addl_balance", k2, 39),
        ("nport_derivatives", "addl_exchange_rate", k2, 40),
        ("nport_derivatives", "addl_value_usd", k2, 41),
        ("nport_derivatives", "addl_pct_value", k2, 42),
        ("nport_derivative_swap_legs", "amount", k3, 43),
        ("nport_derivative_swap_legs", "fixed_rate", k3, 44),
        ("nport_derivative_swap_legs", "floating_rate_spread", k3, 45),
        ("nport_derivative_swap_legs", "payment_amount", k3, 46),
        (
            "nport_derivative_index_components",
            "notional_amount",
            k3,
            47,
        ),
        ("nport_derivative_index_components", "value", k3, 48),
    ];
    let expected: Vec<IssueTuple> = expected
        .iter()
        .map(|(table, column, index, n)| {
            issue(table, column, *index, &format!("bad{n}"), "unparseable")
        })
        .collect();
    assert_eq!(issue_tuples(&b), expected);
    // Every typed value is NULL and every row is flagged.
    for table in TABLES {
        if table == "nport_debt_reference_instruments" {
            continue;
        }
        assert_eq!(b.column(table, "has_parse_issues"), ["true"], "{table}");
    }
    assert_row(
        &b,
        "nport_reports",
        0,
        &[
            ("fiscal_year_end", "NULL"),
            ("as_of_date", "NULL"),
            ("net_assets", "NULL"),
        ],
    );
    assert_row(
        &b,
        "nport_monthly_returns",
        0,
        &[("month1_end", "NULL"), ("month3_end", "NULL")],
    );
    assert_row(&b, "nport_monthly_activity", 0, &[("month_end", "NULL")]);
    assert_row(
        &b,
        "nport_derivatives",
        0,
        &[("swap_flag", "NULL"), ("addl_pct_value", "NULL")],
    );
}

#[test]
fn swap_flag_reads_y_n_text() {
    let derivative = |flag: &str| sec::PortfolioHolding {
        derivative: Some(sec::Derivative {
            swap_flag: st(flag),
            ..Default::default()
        }),
        ..Default::default()
    };
    let report = sec::NportReport {
        holdings: ["Y", "N", " yes ", "false", ""]
            .into_iter()
            .map(derivative)
            .collect(),
        ..Default::default()
    };
    let b = map_report("0000000000-26-900008", report);
    assert_eq!(
        b.column("nport_derivatives", "swap_flag"),
        ["true", "false", "true", "false", "NULL"]
    );
    assert!(b.issues().is_empty());
}

#[test]
fn rows_of_several_filings_keep_filing_then_position_order() {
    let blocks = vec![window_block(
        BLOCK_NUM,
        vec![
            nport_filing("0000940400-26-035739", uscf_gold_strategy()),
            nport_filing("0002048251-26-007235", krane_baba_swap()),
        ],
    )];
    let b = map(&blocks);
    let t = "nport_derivatives";
    assert_eq!(b.column(t, "filing_index"), ["0", "0", "0", "1"]);
    assert_eq!(b.column(t, "holding_index"), ["0", "1", "1", "1"]);
    assert_eq!(b.column(t, "nesting_level"), ["0", "0", "1", "0"]);
    assert_eq!(
        b.column(t, "series_id"),
        ["S000072135", "S000072135", "S000072135", "S000089268"]
    );
    let t = "nport_holdings";
    assert_eq!(b.column(t, "filing_index"), ["0", "0", "1", "1"]);
    assert_eq!(b.column(t, "holding_index"), ["0", "1", "0", "1"]);
    assert_eq!(
        b.column(t, "accession_number"),
        [
            "0000940400-26-035739",
            "0000940400-26-035739",
            "0002048251-26-007235",
            "0002048251-26-007235"
        ]
    );
}

#[test]
fn fork_step_columns_follow_the_nport_columns() {
    let blocks = vec![window_block(
        BLOCK_NUM,
        vec![nport_filing("0000000000-26-900001", contract_report())],
    )];
    let b = map_with(&blocks, true, EncodeBytes::Hex);
    for table in TABLES {
        let schema = b.table(table).schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names[names.len() - 2..],
            ["fork_step", "stream_ordinal"],
            "{table}"
        );
        assert!(b.rows(table) > 0, "{table}");
    }
}

// ---------------------------------------------------------------------------
// Real data (local only)
// ---------------------------------------------------------------------------

/// The mirrored prost structs above equal the real filings of the sample FIRE
/// file, up to the documented trims.
/// `cargo test -p blocks --lib sec::tests::nport::real_fixture -- --ignored --nocapture`
#[test]
#[ignore = "local: needs /tmp/sec-fireparq/fire-v013"]
fn real_fixture_filings_match_their_mirrors() {
    /// One §8.7 filing: its block, accession, mirror, the holdings the
    /// mirror keeps and how many basket components it keeps.
    struct Case {
        block_num: u64,
        accession: &'static str,
        mirror: fn() -> sec::NportReport,
        kept: &'static [usize],
        components: Option<usize>,
    }
    let case = |block_num, accession, mirror, kept, components| Case {
        block_num,
        accession,
        mirror,
        kept,
        components,
    };
    let cases = [
        case(
            2_979_910,
            "0000940400-26-035739",
            uscf_gold_strategy,
            &[0, 1],
            None,
        ),
        case(
            2_979_888,
            "0002048251-26-007235",
            krane_baba_swap,
            &[0, 1],
            None,
        ),
        case(
            2_979_897,
            "0002048251-26-007523",
            fs_multi_strategy_basket,
            &[26],
            Some(3),
        ),
        case(
            2_979_883,
            "0000910472-26-013496",
            catalyst_convertible,
            &[6],
            None,
        ),
        case(
            2_979_913,
            "0000857490-26-000627",
            vanguard_international,
            &[0, 8, 31],
            None,
        ),
        case(
            2_979_907,
            "0002000324-26-004161",
            tidal_dram,
            &[0, 1, 2],
            None,
        ),
    ];
    let wanted: Vec<u64> = cases.iter().map(|case| case.block_num).collect();
    let blocks: std::collections::HashMap<u64, sec::Block> = fire::blocks("2026-08-28")
        .filter(|(identity, _)| wanted.contains(&identity.block_num))
        .map(|(identity, payload)| {
            let block = <sec::Block as Message>::decode(payload.as_slice()).unwrap();
            (identity.block_num, block)
        })
        .collect();
    for Case {
        block_num,
        accession,
        mirror,
        kept,
        components,
    } in cases
    {
        let block = &blocks[&block_num];
        let filing = block
            .filings
            .iter()
            .find(|f| f.accession_number == accession)
            .unwrap_or_else(|| panic!("{accession} not in block {block_num}"));
        let Some(Body::Nport(real)) = &filing.body else {
            panic!("{accession}: not an N-PORT body");
        };
        let mut trimmed = real.clone();
        trimmed.holdings = kept.iter().map(|&i| real.holdings[i].clone()).collect();
        if let Some(n) = components {
            for holding in &mut trimmed.holdings {
                if let Some(derivative) = holding.derivative.as_mut() {
                    derivative.ref_index_components.truncate(n);
                }
            }
        }
        let expected = mirror();
        // Parts the mirrors leave out.
        if expected.fund_info.is_none() {
            trimmed.fund_info = None;
        } else if let (Some(t), Some(e)) = (trimmed.fund_info.as_mut(), expected.fund_info.as_ref())
        {
            if e.monthly_activity.is_empty() {
                t.monthly_activity.clear();
            }
        }
        if expected.signature.is_none() {
            trimmed.signature = None;
        }
        assert_eq!(trimmed, expected, "{accession}");
    }
}

/// Every column of every row of this group's tables equals the prototype's on
/// the four sample days.
/// `cargo test -p blocks --lib sec::tests::nport::nport_matches -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn nport_matches_the_prototype() {
    oracle::assert_matches(&fire::DAYS, &TABLES, &[]);
}

/// The `parse_issues` rows of this group's tables equal the prototype's, in
/// order (the other groups' rows are filtered out on both sides).
/// `cargo test -p blocks --lib sec::tests::nport::nport_parse_issues -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn nport_parse_issues_match_the_prototype() {
    use std::io::BufRead;
    let mut total = 0usize;
    let mut failures = Vec::new();
    for day in fire::DAYS {
        if !fire::path(day).exists() || !oracle::dir(day).exists() {
            continue;
        }
        let path = oracle::dir(day).join("parse_issues.ndjson");
        let file = std::fs::File::open(&path).unwrap();
        let mut expected = std::io::BufReader::new(file)
            .lines()
            .map(|line| line.unwrap())
            .filter(|line| line.contains("\"table_name\":\"nport_"))
            .map(|line| serde_json::from_str::<serde_json::Value>(&line).unwrap());
        let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
        let mut blocks = fire::blocks(day).peekable();
        let mut rows = 0usize;
        while let Some((identity, payload)) = blocks.next() {
            mapper
                .map_block_bytes(payload.into(), &identity, StreamEvent::default())
                .unwrap();
            if mapper.total_rows() < 500_000 && blocks.peek().is_some() {
                continue;
            }
            let batches = mapper.flush().unwrap();
            let batch = &batches["parse_issues"];
            let schema = batch.schema();
            let table = batch.column_by_name("table_name").unwrap();
            for row in 0..batch.num_rows() {
                let name = oracle::json_value(table.as_ref(), row);
                if !name.as_str().unwrap_or("").starts_with("nport_") {
                    continue;
                }
                rows += 1;
                let actual: serde_json::Map<String, serde_json::Value> = schema
                    .fields()
                    .iter()
                    .zip(batch.columns())
                    .map(|(f, c)| (f.name().clone(), oracle::json_value(c.as_ref(), row)))
                    .collect();
                let actual = serde_json::Value::Object(actual);
                match expected.next() {
                    Some(e) if e == actual => {}
                    other => {
                        if failures.len() < 10 {
                            failures
                                .push(format!("{day} #{rows}: rust {actual} != oracle {other:?}"));
                        }
                    }
                }
            }
        }
        let left = expected.count();
        println!("{day}: {rows} nport parse_issues rows, {left} oracle rows left");
        if left > 0 {
            failures.push(format!("{day}: {left} oracle rows not produced"));
        }
        total += rows;
    }
    println!("total {total} nport parse_issues rows");
    assert!(failures.is_empty(), "{failures:#?}");
}
