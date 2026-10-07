//! Value tests of `form144_notices`, `form144_securities_information`, `form144_securities_to_be_sold`, `form144_sales_past_3_months`, `form_d_notices`, `form_d_co_issuers`, `form_d_related_persons`, `form_d_sales_recipients`, `form_c_notices`, `form_c_co_issuers`.
//! Owned by the `smallforms` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! The fixture bodies mirror the §8.7 sample filings exactly
//! ([`fixture_bodies_equal_the_real_filings`] checks them against the local
//! FIRE files); the expected values are the prototype's rows for them.

use super::*;
use crate::sec::build::ADDRESS_FIELDS;

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
    // The Investcorp D/A has a co-issuer and related persons; give it the JS
    // Venture sales recipients too, so one Form D fills all four tables.
    let mut form_d = investcorp_form_d();
    form_d.sales_compensation_recipients = js_venture_form_d().sales_compensation_recipients;
    vec![
        sample(HALLIBURTON, "144", Body::Form144(halliburton_144())),
        sample(INVESTCORP, "D/A", Body::FormD(form_d)),
        sample(KOKOPELLI, "C", Body::FormC(kokopelli_form_c())),
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

fn s(text: &str) -> String {
    text.to_string()
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| item.to_string()).collect()
}

/// A US address as the samples have it (no country, no non-US territory).
fn address(
    street1: &str,
    street2: &str,
    city: &str,
    state: &str,
    zip_code: &str,
    state_description: &str,
) -> Option<sec::Address> {
    Some(sec::Address {
        street1: s(street1),
        street2: s(street2),
        city: s(city),
        state: s(state),
        zip_code: s(zip_code),
        state_description: s(state_description),
        country: String::new(),
        non_us_state_territory: String::new(),
    })
}

/// The expected 8 address columns of [`address`] (`""` → `NULL`).
fn addr6<'x>(
    street1: &'x str,
    street2: &'x str,
    city: &'x str,
    state: &'x str,
    zip_code: &'x str,
    state_description: &'x str,
) -> [&'x str; 8] {
    [
        street1,
        street2,
        city,
        state,
        zip_code,
        state_description,
        "",
        "",
    ]
}

const NO_ADDRESS: [&str; 8] = [""; 8];

/// A sample filing: the test envelope with the real accession number.
fn sample(accession: &str, form_type: &str, body: Body) -> sec::Filing {
    sec::Filing {
        accession_number: s(accession),
        ..filing(form_type, body)
    }
}

/// The expected domain columns of one row: every column after [ID] and [FC],
/// in schema order. `""` means NULL for addresses; elsewhere write `NULL`.
#[derive(Default)]
struct Expect(Vec<(String, String)>);

impl Expect {
    fn new() -> Self {
        Self::default()
    }

    fn c(mut self, column: &str, value: &str) -> Self {
        self.0.push((s(column), s(value)));
        self
    }

    fn addr(mut self, prefix: &str, values: [&str; 8]) -> Self {
        for (suffix, value) in ADDRESS_FIELDS.iter().zip(values) {
            let value = if value.is_empty() { "NULL" } else { value };
            self.0.push((format!("{prefix}_{suffix}"), s(value)));
        }
        self
    }

    /// Assert that the expectation names every domain column of `table`, in
    /// order, and that row `row` holds exactly these values.
    fn assert(&self, b: &Batches, table: &str, row: usize) {
        let schema = b.table(table).schema();
        let domain: Vec<&str> = schema
            .fields()
            .iter()
            .skip(7 + 5)
            .map(|field| field.name().as_str())
            .collect();
        let named: Vec<&str> = self.0.iter().map(|(column, _)| column.as_str()).collect();
        assert_eq!(
            named, domain,
            "{table}: the expectation must list every column"
        );
        let differences: Vec<String> = self
            .0
            .iter()
            .filter_map(|(column, expected)| {
                let actual = b.cell(table, column, row);
                (&actual != expected).then(|| format!("{column}: {actual} != {expected}"))
            })
            .collect();
        assert!(
            differences.is_empty(),
            "{table} row {row}: {differences:#?}"
        );
    }
}

/// The [FC] columns of row `row`: filing position, accession and form type
/// (the test envelope's filing date and acceptance).
fn assert_fc(b: &Batches, table: &str, row: usize, filing_index: u32, accession: &str, form: &str) {
    assert_eq!(b.cell(table, "filing_index", row), filing_index.to_string());
    assert_eq!(b.cell(table, "accession_number", row), accession);
    assert_eq!(b.cell(table, "form_type", row), form);
    assert_eq!(b.cell(table, "filing_date", row), "2026-08-27");
    assert_eq!(
        b.cell(table, "acceptance_datetime", row),
        "2026-08-28T12:31:00Z"
    );
    assert_eq!(b.cell(table, "block_num", row), BLOCK_NUM.to_string());
}

/// Every `parse_issues` row as `filing table.column [i1,i2,i3] raw issue`.
fn issue_lines(b: &Batches) -> Vec<String> {
    b.issues()
        .into_iter()
        .map(|issue| {
            let index: Vec<String> = issue
                .index
                .iter()
                .map(|i| i.map_or_else(|| s("-"), |i| i.to_string()))
                .collect();
            format!(
                "{} {}.{} [{}] {} {}",
                issue.filing_index.map_or_else(|| s("-"), |i| i.to_string()),
                issue.table,
                issue.column,
                index.join(","),
                issue.raw,
                issue.issue
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// §8.7 fixture bodies (exact mirrors of the sample filings)
// ---------------------------------------------------------------------------

/// Form 144 with a 10b5-1 plan adoption date and a past-3-month sale
/// (2026-03-16, block 2956120).
const HALLIBURTON: &str = "0001959173-26-002331";
/// Form 144 with 3 `securities_information` lots (2026-03-16, block 2956154).
const MOHAWK: &str = "0000011790-26-000005";
/// Form D/A with `Indefinite` totals and a co-issuer (2026-03-16, block 2956120).
const INVESTCORP: &str = "0002048118-26-000003";
/// Form D/A with `previous_accession_number` and sales recipients
/// (2026-03-16, block 2956120).
const JS_VENTURE: &str = "0002111624-26-000002";
/// Form C with financials and a co-issuer (2026-08-14, block 2977781).
const KOKOPELLI: &str = "0001720779-26-000005";
/// Form C-U, a progress update (2026-08-14, block 2977899).
const CLASSIC_COFFEE: &str = "0002143684-26-000004";

const HALLIBURTON_SIGNATURE: &str = "/s/ Jennifer Ruchti, as a duly authorized representative of Fidelity Brokerage Services LLC, as attorney-in-fact for J.Shannon Slocum";

fn halliburton_144() -> sec::Form144Notice {
    let entry = sec::SecuritiesInformation {
        securities_class_title: s("Common"),
        broker: Some(sec::BrokerInfo {
            name: s("Fidelity Brokerage Services LLC"),
            address: address("900 Salem Street", "", "Smithfield", "RI", "02917", ""),
        }),
        units_sold: s("5441"),
        aggregate_market_value: s("184014.62"),
        units_outstanding: s("837548345"),
        approx_sale_date: s("03/16/2026"),
        securities_exchange_name: s("NYSE"),
    };
    sec::Form144Notice {
        filer_cik: s("0001970357"),
        issuer: Some(sec::Issuer144 {
            issuer_cik: s("0000045012"),
            issuer_name: s("HALLIBURTON CO"),
            sec_file_number: s("001-03492"),
            issuer_address: address(
                "3000 NORTH SAM HOUSTON PARKWAY EAST",
                "3000 NORTH SAM HOUSTON PARKWAY EAST",
                "HOUSTON",
                "TX",
                "77032",
                "",
            ),
            issuer_contact_phone: s("2818712699"),
            person_for_whose_account: s("Slocum Jeffrey Shannon"),
            relationships_to_issuer: strings(&["Officer", "Director"]),
        }),
        securities_information: Some(entry.clone()),
        securities_to_be_sold: vec![sec::SecuritiesToBeSold {
            securities_class_title: s("Common"),
            acquired_date: s("02/27/2026"),
            nature_of_acquisition: s("Restricted Stock Vesting"),
            acquired_from: s("Issuer"),
            is_gift: false,
            amount_acquired: s("5441"),
            payment_date: s("02/27/2026"),
            nature_of_payment: s("Compensation"),
            donor_acquired_date: String::new(),
        }],
        nothing_sold_past_3_months: false,
        securities_sold_past_3_months: vec![sec::SecuritiesSoldPast3Months {
            seller_name: s("J.Shannon Slocum"),
            seller_address: address(
                "3000 North Sam Houston Parkway East",
                "",
                "Houston",
                "TX",
                "77032",
                "",
            ),
            securities_class_title: s("Common"),
            sale_date: s("01/09/2026"),
            amount_sold: s("23895"),
            gross_proceeds: s("771808.50"),
        }],
        remarks: String::new(),
        signature: Some(sec::NoticeSignature {
            notice_date: s("03/16/2026"),
            signature: s(HALLIBURTON_SIGNATURE),
            plan_adoption_dates: strings(&["08/07/2025"]),
        }),
        securities_information_entries: vec![entry],
        previous_accession_number: String::new(),
    }
}

fn mohawk_144() -> sec::Form144Notice {
    let entry = |city: &str, units: &str, value: &str| sec::SecuritiesInformation {
        securities_class_title: s("COMMON"),
        broker: Some(sec::BrokerInfo {
            name: s("TD SECURITIES (USA) LLC"),
            address: address("125 PARK AVE, 20TH FLOOR", "", city, "NY", "10017", ""),
        }),
        units_sold: s(units),
        aggregate_market_value: s(value),
        units_outstanding: s("62520000"),
        approx_sale_date: s("03/16/2026"),
        securities_exchange_name: s("NYSE"),
    };
    let sale = |name: &str, street: &str, date: &str, amount: &str, proceeds: &str| {
        sec::SecuritiesSoldPast3Months {
            seller_name: s(name),
            seller_address: address(street, "", "CALHOUN", "GA", "30701", ""),
            securities_class_title: s("COMMON"),
            sale_date: s(date),
            amount_sold: s(amount),
            gross_proceeds: s(proceeds),
        }
    };
    const FAM: &str = "LORBERBAUM FAM PAST S HELEN CUST";
    const FAMILY: &str = "LORBERBAUM FAMILY PAST S HELEN CUST";
    sec::Form144Notice {
        filer_cik: s("0001271373"),
        issuer: Some(sec::Issuer144 {
            issuer_cik: s("0000851968"),
            issuer_name: s("MOHAWK INDUSTRIES INC"),
            sec_file_number: s("001-13697"),
            issuer_address: address(
                "160 S INDUSTRIAL BLVD",
                "PO BOX 12069",
                "CALHOUN",
                "GA",
                "30701",
                "",
            ),
            issuer_contact_phone: s("706-624-2032"),
            person_for_whose_account: s("HELEN SUZANNE L"),
            relationships_to_issuer: strings(&["Affiliate"]),
        }),
        securities_information: Some(entry("NEW YORK", "1000", "103164.16")),
        securities_to_be_sold: vec![sec::SecuritiesToBeSold {
            securities_class_title: s("COMMON"),
            acquired_date: s("02/29/2012"),
            nature_of_acquisition: s("COMPANY DISBURSEMENT"),
            acquired_from: s("MOHAWK"),
            is_gift: false,
            amount_acquired: s("395202"),
            payment_date: s("02/29/2012"),
            nature_of_payment: s("FREE RECEIVE"),
            donor_acquired_date: String::new(),
        }],
        nothing_sold_past_3_months: false,
        securities_sold_past_3_months: vec![
            sale(
                FAM,
                "160 S. INDUSTRIAL BLVD S.W",
                "12/16/2025",
                "2000",
                "221377.60",
            ),
            sale(
                FAM,
                "160 S. INDUSTRIAL BLVD S.W.",
                "12/17/2025",
                "3500",
                "386229.55",
            ),
            sale(
                FAM,
                "160S. INDUSTRIAL BLVD S.W.",
                "12/18/2025",
                "2700",
                "294275.08",
            ),
            sale(
                FAM,
                "160 S. INDUSTRIAL BLVD S.W.",
                "02/27/2026",
                "300",
                "37163.01",
            ),
            sale(
                FAMILY,
                "160 S. INDUSTRIAL BLVD S.W.",
                "03/10/2026",
                "5658",
                "595079.37",
            ),
            sale(
                FAMILY,
                "160 S. INDUSTRIAL BLVD S.W,.",
                "03/11/2026",
                "1042",
                "113970.42",
            ),
        ],
        remarks: String::new(),
        signature: Some(sec::NoticeSignature {
            notice_date: s("03/16/2026"),
            signature: s("/S/ SUZANNE L HELEN"),
            plan_adoption_dates: Vec::new(),
        }),
        securities_information_entries: vec![
            entry("NEW YORK", "1000", "103164.16"),
            entry("NEW YORK", "500", "51730.00"),
            entry("BROOKLYN", "500", "51717.50"),
        ],
        previous_accession_number: String::new(),
    }
}

/// The address every Investcorp party shares (upper-case for the issuers).
fn park_avenue(upper: bool) -> Option<sec::Address> {
    if upper {
        address(
            "280 PARK AVENUE",
            "36TH FLOOR",
            "NEW YORK",
            "NY",
            "10017",
            "NEW YORK",
        )
    } else {
        address(
            "280 Park Avenue",
            "36th Floor",
            "New York",
            "NY",
            "10017",
            "NEW YORK",
        )
    }
}

fn investcorp_form_d() -> sec::FormDNotice {
    let person =
        |first: &str, last: &str, relationship: &str, clarification: &str| sec::RelatedPerson {
            first_name: s(first),
            middle_name: String::new(),
            last_name: s(last),
            address: park_avenue(false),
            relationships: strings(&[relationship]),
            relationship_clarification: s(clarification),
        };
    let signature = |issuer: &str| sec::FormDSignature {
        issuer_name: s(issuer),
        signature_name: s("/s/ Emily Tibbetts"),
        name_of_signer: s("Emily Tibbetts"),
        signature_title: s("Director of the issuer's GP"),
        signature_date: s("2026-03-16"),
    };
    sec::FormDNotice {
        schema_version: s("X0708"),
        submission_type: s("D/A"),
        primary_issuer: Some(sec::IssuerD {
            cik: s("0002048118"),
            entity_name: s("Investcorp North American Private Equity Fund II, L.P."),
            address: park_avenue(true),
            phone: s("(212) 599-4700"),
            jurisdiction_of_inc: s("DELAWARE"),
            entity_type: s("Limited Partnership"),
            year_of_inc: s("2024"),
            entity_type_other_desc: String::new(),
            year_of_inc_status: s("withinFiveYears"),
            previous_names: Vec::new(),
            edgar_previous_names: Vec::new(),
        }),
        related_persons: vec![
            person(
                "Nicholas",
                "McGrane",
                "Executive Officer",
                "Executive Officer of the Manager of the Issuer",
            ),
            person(
                "N/A",
                "Investcorp Investment Advisers LLC",
                "Promoter",
                "Manager of the Issuer",
            ),
            person(
                "N/A",
                "Investcorp North American Private Equity Fund II GP, L.P.",
                "Promoter",
                "General Partner of the Issuer",
            ),
        ],
        offering: Some(sec::OfferingData {
            industry_group: s("Pooled Investment Fund"),
            is_amendment: true,
            date_of_first_sale: s("2025-02-13"),
            more_than_one_year: true,
            is_equity_type: true,
            federal_exemptions: strings(&["06b", "3C", "3C.7"]),
            minimum_investment: s("0"),
            total_offering_amount: s("Indefinite"),
            total_amount_sold: s("251371785"),
            total_remaining: s("Indefinite"),
            has_non_accredited_investors: false,
            total_already_invested: s("24"),
            sales_commissions: s("0"),
            finders_fees: s("0"),
            gross_proceeds_used: s("0"),
            investment_fund_type: s("Private Equity Fund"),
            is_40_act: Some(false),
            revenue_range: s("Decline to Disclose"),
            is_business_combination: Some(false),
            securities_types: strings(&["isEquityType", "isPooledInvestmentFundType"]),
            ..Default::default()
        }),
        previous_accession_number: s("0002048118-25-000001"),
        issuers: vec![sec::IssuerD {
            cik: s("0002048215"),
            entity_name: s("Investcorp North American Private Equity Parallel Fund II, L.P."),
            address: park_avenue(true),
            phone: s("(212) 599-4700"),
            jurisdiction_of_inc: s("CAYMAN ISLANDS"),
            entity_type: s("Limited Partnership"),
            year_of_inc: s("2024"),
            entity_type_other_desc: String::new(),
            year_of_inc_status: s("withinFiveYears"),
            previous_names: Vec::new(),
            edgar_previous_names: Vec::new(),
        }],
        sales_compensation_recipients: Vec::new(),
        signatures: vec![
            signature("Investcorp North American Private Equity Fund II, L.P."),
            signature("Investcorp North American Private Equity Parallel Fund II, L.P."),
        ],
        authorized_representative: Some(false),
    }
}

const JS_SALES_CLARIFICATION: &str = "The value reflected in the commissions field includes a 2% compliance and organizational fee paid to the placement agents.";
const JS_PROCEEDS_CLARIFICATION: &str =
    "Represents a Management Fee payable to Global Venture Management LLC.";

fn js_venture_form_d() -> sec::FormDNotice {
    let stewart = || {
        address(
            "585 STEWART AVE",
            "SUITE L60C",
            "GARDEN CITY",
            "NY",
            "11530",
            "NEW YORK",
        )
    };
    sec::FormDNotice {
        schema_version: s("X0708"),
        submission_type: s("D/A"),
        primary_issuer: Some(sec::IssuerD {
            cik: s("0002111624"),
            entity_name: s("JS VENTURE FUND LLC SERIES A36"),
            address: stewart(),
            phone: s("516-257-7001"),
            jurisdiction_of_inc: s("DELAWARE"),
            entity_type: s("Limited Liability Company"),
            year_of_inc: s("2026"),
            entity_type_other_desc: String::new(),
            year_of_inc_status: s("withinFiveYears"),
            previous_names: Vec::new(),
            edgar_previous_names: Vec::new(),
        }),
        related_persons: vec![sec::RelatedPerson {
            first_name: s("DAMIAN"),
            middle_name: String::new(),
            last_name: s("MAGGIO"),
            address: stewart(),
            relationships: strings(&["Executive Officer"]),
            relationship_clarification: s(
                "Managing Member, Global Venture Management LLC, its Manager.",
            ),
        }],
        offering: Some(sec::OfferingData {
            industry_group: s("Pooled Investment Fund"),
            is_amendment: true,
            date_of_first_sale: s("2026-02-13"),
            federal_exemptions: strings(&["06b", "3C", "3C.7"]),
            minimum_investment: s("25000"),
            total_offering_amount: s("Indefinite"),
            total_amount_sold: s("2755000"),
            total_remaining: s("Indefinite"),
            total_already_invested: s("26"),
            sales_commissions: s("244825"),
            finders_fees: s("0"),
            gross_proceeds_used: s("68875"),
            investment_fund_type: s("Other Investment Fund"),
            is_40_act: Some(false),
            aggregate_net_asset_value_range: s("Decline to Disclose"),
            is_business_combination: Some(false),
            securities_types: strings(&["isPooledInvestmentFundType"]),
            sales_commissions_is_estimate: Some(true),
            finders_fees_is_estimate: Some(true),
            gross_proceeds_used_is_estimate: Some(true),
            sales_commissions_clarification: s(JS_SALES_CLARIFICATION),
            use_of_proceeds_clarification: s(JS_PROCEEDS_CLARIFICATION),
            ..Default::default()
        }),
        previous_accession_number: s("0002111624-26-000001"),
        issuers: Vec::new(),
        sales_compensation_recipients: vec![
            sec::SalesCompensationRecipient {
                name: s("VCS VENTURE SECURITIES"),
                crd_number: s("127921"),
                associated_bd_name: s("None"),
                associated_bd_crd_number: s("None"),
                address: address(
                    "29 BROADWAY",
                    "SUITE 1502",
                    "NEW YORK",
                    "NY",
                    "10006",
                    "NEW YORK",
                ),
                states_of_solicitation: strings(&[
                    "AL", "AZ", "CA", "CO", "FL", "IL", "IN", "KS", "ME", "MD", "MI", "NY", "TX",
                ]),
                foreign_solicitation: Some(false),
            },
            sec::SalesCompensationRecipient {
                name: s("JOSEPH STONE CAPITAL L.L.C."),
                crd_number: s("159744"),
                associated_bd_name: s("None"),
                associated_bd_crd_number: s("None"),
                address: stewart(),
                states_of_solicitation: Vec::new(),
                foreign_solicitation: Some(true),
            },
        ],
        signatures: vec![sec::FormDSignature {
            issuer_name: s("JS VENTURE FUND LLC SERIES A36"),
            signature_name: s("/s/ Damian Maggio"),
            name_of_signer: s("Damian Maggio"),
            signature_title: s("Manager, Global Venture Management LLC, its Manager"),
            signature_date: s("2026-03-16"),
        }],
        authorized_representative: Some(false),
    }
}

const KOKOPELLI_COMPENSATION: &str = "7.9% of the offering amount upon a successful fundraise, and be entitled to reimbursement for out-of-pocket third party expenses it pays or incurs on behalf of the Issuer in connection with the offering.";
const KOKOPELLI_PRICE_METHOD: &str = "Pro-rated portion of the total principal value of $50,000; interests will be sold in increments of $1; each investment is convertible to one share of stock as described under Item 13.";
const KOKOPELLI_JURISDICTIONS: &str = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY B5 GU PR VI 1V";
const COFFEE_COMPENSATION: &str = "8.5% of the total offering amount upon a successful raise. $500 Platform Fee. 3% investment fee capped at $75. Credit Card: 5.5% + $2; Honeycomb Wallet: 0%; Hybrid (HW + ACH): 2%, capped at $30. A Loan Servicing Fee of .25% assessed monthly.";
const COFFEE_JURISDICTIONS: &str = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA PR RI SC SD TN TX UT VT VA WA WV WI WY A0 A1 A2 A3 A4 A5 A6 A7 A8 A9 B0 Z4";

/// `[a, b, …]`: how `cell` prints a `List<Utf8>` of space-separated `items`.
fn list_text(items: &str) -> String {
    format!("[{}]", items.split(' ').collect::<Vec<_>>().join(", "))
}

/// Form C financial statements from their 19 source strings, in proto order
/// (`current_employees` first).
fn financials(values: [&str; 19]) -> sec::FormCFinancials {
    let [current_employees, ta1, ta0, ce1, ce0, ar1, ar0, sd1, sd0, ld1, ld0, rv1, rv0, cg1, cg0, tp1, tp0, ni1, ni0] =
        values;
    sec::FormCFinancials {
        current_employees: s(current_employees),
        total_assets_most_recent_fy: s(ta1),
        total_assets_prior_fy: s(ta0),
        cash_equivalents_most_recent_fy: s(ce1),
        cash_equivalents_prior_fy: s(ce0),
        accounts_receivable_most_recent_fy: s(ar1),
        accounts_receivable_prior_fy: s(ar0),
        short_term_debt_most_recent_fy: s(sd1),
        short_term_debt_prior_fy: s(sd0),
        long_term_debt_most_recent_fy: s(ld1),
        long_term_debt_prior_fy: s(ld0),
        revenue_most_recent_fy: s(rv1),
        revenue_prior_fy: s(rv0),
        cost_goods_sold_most_recent_fy: s(cg1),
        cost_goods_sold_prior_fy: s(cg0),
        tax_paid_most_recent_fy: s(tp1),
        tax_paid_prior_fy: s(tp0),
        net_income_most_recent_fy: s(ni1),
        net_income_prior_fy: s(ni0),
    }
}

fn kokopelli_form_c() -> sec::FormCNotice {
    sec::FormCNotice {
        filer_cik: s("0001720779"),
        issuer: Some(sec::FormCIssuer {
            name: s("Kokopelli Outdoor, Inc."),
            legal_status_form: s("Corporation"),
            jurisdiction: s("DE"),
            date_incorporation: s("01-01-2018"),
            address: address("3863 Steele Street", "", "Denver", "CO", "80205", ""),
            website: s("https://rovrproducts.com"),
            legal_status_other_desc: String::new(),
        }),
        intermediary_company_name: s("Wefunder Portal LLC"),
        intermediary_cik: s("0001670254"),
        intermediary_file_number: s("007-00033"),
        offering: Some(sec::FormCOffering {
            security_type: s("Other"),
            num_securities_offered: s("50000"),
            price: s("1.00000"),
            offering_amount: s("50000.00"),
            maximum_offering_amount: s("900000.00"),
            over_subscription_accepted: true,
            deadline_date: s("04-30-2027"),
            compensation_amount: s(KOKOPELLI_COMPENSATION),
            security_offered_other_desc: s("Simple Agreement for Future Equity (SAFE)"),
            over_subscription_allocation_type: s("Other"),
            desc_over_subscription: s("As determined by the issuer"),
            price_determination_method: s(KOKOPELLI_PRICE_METHOD),
            financial_interest: s("No"),
        }),
        financials: Some(financials([
            "8",
            "6597473.00",
            "4843898.00",
            "140183.00",
            "69416.00",
            "682217.00",
            "197943.00",
            "2277644.00",
            "1232634.00",
            "4241552.00",
            "2687652.00",
            "6194049.00",
            "4423014.00",
            "3155803.00",
            "2164763.00",
            "0.00",
            "0.00",
            "-2371578.00",
            "-2439688.00",
        ])),
        intermediary_crd_number: s("283503"),
        is_amendment: None,
        nature_of_amendment: String::new(),
        progress_update: String::new(),
        is_co_issuer: Some(true),
        co_issuers: vec![sec::FormCIssuer {
            name: s("ROVR Products IV, a series of Wefunder SPV, LLC"),
            legal_status_form: s("Limited Liability Company"),
            jurisdiction: s("DE"),
            date_incorporation: s("08-13-2026"),
            address: address(
                "1887 Whitney Mesa Dr",
                "NUM 8885",
                "Henderson",
                "NV",
                "89014",
                "",
            ),
            website: s("https://wefunder.com/"),
            legal_status_other_desc: String::new(),
        }],
        offering_jurisdictions: strings(&KOKOPELLI_JURISDICTIONS.split(' ').collect::<Vec<_>>()),
        issuer_signature: Some(sec::FormCSignature {
            issuer: s("Kokopelli Outdoor, Inc."),
            signature: s("Patrick Smith"),
            title: s("President"),
            date: String::new(),
        }),
        person_signatures: vec![
            sec::FormCSignature {
                issuer: String::new(),
                signature: s("Patrick Smith"),
                title: s("President"),
                date: s("08-12-2026"),
            },
            sec::FormCSignature {
                issuer: String::new(),
                signature: s("Steven Folse"),
                title: s("Managing Director"),
                date: s("08-12-2026"),
            },
        ],
        period: String::new(),
    }
}

fn classic_coffee_form_c() -> sec::FormCNotice {
    sec::FormCNotice {
        filer_cik: s("0002143684"),
        issuer: Some(sec::FormCIssuer {
            name: s("Classic Coffee and Tea, LLC"),
            legal_status_form: s("Limited Liability Company"),
            jurisdiction: s("VA"),
            date_incorporation: s("01-01-2023"),
            address: address(
                "8247 RAVEN RUN DRIVE",
                "",
                "MECHANICSVILLE",
                "PA",
                "23111",
                "",
            ),
            website: s("https://www.classiccoffeeteabooks.com"),
            legal_status_other_desc: String::new(),
        }),
        intermediary_company_name: s("Honeycomb Portal LLC"),
        intermediary_cik: s("0001705726"),
        intermediary_file_number: s("007-00119"),
        offering: Some(sec::FormCOffering {
            security_type: s("Debt"),
            num_securities_offered: String::new(),
            price: s("1.00000"),
            offering_amount: s("15000.00"),
            maximum_offering_amount: s("30000.00"),
            over_subscription_accepted: true,
            deadline_date: s("08-13-2026"),
            compensation_amount: s(COFFEE_COMPENSATION),
            security_offered_other_desc: String::new(),
            over_subscription_allocation_type: s("First-come, first-served basis"),
            desc_over_subscription: String::new(),
            price_determination_method: s("Pro-rated portion of the total principal"),
            financial_interest: s("None"),
        }),
        financials: Some(financials([
            "4.00",
            "26293.00",
            "47323.00",
            "489.00",
            "7000.00",
            "0.00",
            "0.00",
            "13677.00",
            "2464.00",
            "36247.00",
            "41939.00",
            "206160.00",
            "119811.00",
            "70640.00",
            "57118.00",
            "0.00",
            "0.00",
            "39937.00",
            "-22626.00",
        ])),
        intermediary_crd_number: s("289015"),
        is_amendment: None,
        nature_of_amendment: String::new(),
        progress_update: s("Offering closed unsuccessfully"),
        is_co_issuer: Some(false),
        co_issuers: Vec::new(),
        offering_jurisdictions: strings(&COFFEE_JURISDICTIONS.split(' ').collect::<Vec<_>>()),
        issuer_signature: Some(sec::FormCSignature {
            issuer: s("Classic Coffee and Tea, LLC"),
            signature: s("Wayne Wright"),
            title: s("CEO"),
            date: String::new(),
        }),
        person_signatures: vec![sec::FormCSignature {
            issuer: String::new(),
            signature: s("Wayne Wright"),
            title: s("CEO"),
            date: s("08-14-2026"),
        }],
        period: String::new(),
    }
}

// ---------------------------------------------------------------------------
// Form 144
// ---------------------------------------------------------------------------

fn form144_fixture_batches() -> Batches {
    map(&[window_block(
        BLOCK_NUM,
        vec![
            sample(HALLIBURTON, "144", Body::Form144(halliburton_144())),
            sample(MOHAWK, "144", Body::Form144(mohawk_144())),
        ],
    )])
}

#[test]
fn form144_notices_of_the_fixture_filings() {
    let b = form144_fixture_batches();
    b.assert_rows(&[
        ("form144_notices", 2),
        ("form144_securities_information", 1 + 3),
        ("form144_securities_to_be_sold", 1 + 1),
        ("form144_sales_past_3_months", 1 + 6),
        ("parse_issues", 0),
    ]);
    let table = "form144_notices";
    assert_fc(&b, table, 0, 0, HALLIBURTON, "144");
    Expect::new()
        .c("filer_cik", "0001970357")
        .c("issuer_cik", "0000045012")
        .c("issuer_name", "HALLIBURTON CO")
        .c("issuer_sec_file_number", "001-03492")
        .addr(
            "issuer",
            addr6(
                "3000 NORTH SAM HOUSTON PARKWAY EAST",
                "3000 NORTH SAM HOUSTON PARKWAY EAST",
                "HOUSTON",
                "TX",
                "77032",
                "",
            ),
        )
        .c("issuer_contact_phone", "2818712699")
        .c("person_for_whose_account", "Slocum Jeffrey Shannon")
        .c("relationships_to_issuer", "[Officer, Director]")
        .c("nothing_sold_past_3_months", "false")
        .c("remarks", "NULL")
        .c("previous_accession_number", "NULL")
        .c("notice_date", "2026-03-16")
        .c("signature_text", HALLIBURTON_SIGNATURE)
        .c("plan_adoption_dates", "[2025-08-07]")
        .c("securities_information_count", "1")
        .c("total_units_sold", "5441.000000")
        .c("total_aggregate_market_value", "184014.62")
        .c("securities_to_be_sold_count", "1")
        .c("sales_past_3_months_count", "1")
        .c("has_parse_issues", "false")
        .assert(&b, table, 0);
    assert_fc(&b, table, 1, 1, MOHAWK, "144");
    Expect::new()
        .c("filer_cik", "0001271373")
        .c("issuer_cik", "0000851968")
        .c("issuer_name", "MOHAWK INDUSTRIES INC")
        .c("issuer_sec_file_number", "001-13697")
        .addr(
            "issuer",
            addr6(
                "160 S INDUSTRIAL BLVD",
                "PO BOX 12069",
                "CALHOUN",
                "GA",
                "30701",
                "",
            ),
        )
        .c("issuer_contact_phone", "706-624-2032")
        .c("person_for_whose_account", "HELEN SUZANNE L")
        .c("relationships_to_issuer", "[Affiliate]")
        .c("nothing_sold_past_3_months", "false")
        .c("remarks", "NULL")
        .c("previous_accession_number", "NULL")
        .c("notice_date", "2026-03-16")
        .c("signature_text", "/S/ SUZANNE L HELEN")
        .c("plan_adoption_dates", "[]")
        .c("securities_information_count", "3")
        // 1000 + 500 + 500 and 103164.16 + 51730.00 + 51717.50.
        .c("total_units_sold", "2000.000000")
        .c("total_aggregate_market_value", "206611.66")
        .c("securities_to_be_sold_count", "1")
        .c("sales_past_3_months_count", "6")
        .c("has_parse_issues", "false")
        .assert(&b, table, 1);
}

#[test]
fn form144_child_tables_of_the_fixture_filings() {
    let b = form144_fixture_batches();

    let table = "form144_securities_information";
    assert_fc(&b, table, 0, 0, HALLIBURTON, "144");
    Expect::new()
        .c("issuer_cik", "0000045012")
        .c("filer_cik", "0001970357")
        .c("entry_index", "0")
        .c("securities_class_title", "Common")
        .c("broker_name", "Fidelity Brokerage Services LLC")
        .addr(
            "broker",
            addr6("900 Salem Street", "", "Smithfield", "RI", "02917", ""),
        )
        .c("units_sold", "5441.000000")
        .c("aggregate_market_value", "184014.62")
        .c("units_outstanding", "837548345.000000")
        .c("approx_sale_date", "2026-03-16")
        .c("securities_exchange_name", "NYSE")
        .c("has_parse_issues", "false")
        .assert(&b, table, 0);
    // Mohawk's three lots: the entries, not the singular copy of the first.
    assert_eq!(b.column(table, "filing_index"), ["0", "1", "1", "1"]);
    assert_eq!(b.column(table, "entry_index"), ["0", "0", "1", "2"]);
    assert_eq!(
        b.column(table, "broker_city"),
        ["Smithfield", "NEW YORK", "NEW YORK", "BROOKLYN"]
    );
    assert_eq!(
        b.column(table, "units_sold"),
        ["5441.000000", "1000.000000", "500.000000", "500.000000"]
    );
    assert_eq!(
        b.column(table, "aggregate_market_value"),
        ["184014.62", "103164.16", "51730.00", "51717.50"]
    );
    assert_eq!(b.column(table, "issuer_cik")[3], "0000851968");
    assert_eq!(b.column(table, "filer_cik")[3], "0001271373");

    let table = "form144_securities_to_be_sold";
    assert_fc(&b, table, 1, 1, MOHAWK, "144");
    Expect::new()
        .c("issuer_cik", "0000851968")
        .c("filer_cik", "0001271373")
        .c("lot_index", "0")
        .c("securities_class_title", "COMMON")
        .c("acquired_date", "2012-02-29")
        .c("nature_of_acquisition", "COMPANY DISBURSEMENT")
        .c("acquired_from", "MOHAWK")
        .c("is_gift", "false")
        .c("donor_acquired_date", "NULL")
        .c("amount_acquired", "395202.000000")
        .c("payment_date", "2012-02-29")
        .c("nature_of_payment", "FREE RECEIVE")
        .c("has_parse_issues", "false")
        .assert(&b, table, 1);
    assert_eq!(b.cell(table, "acquired_date", 0), "2026-02-27");
    assert_eq!(b.cell(table, "nature_of_payment", 0), "Compensation");

    let table = "form144_sales_past_3_months";
    assert_fc(&b, table, 0, 0, HALLIBURTON, "144");
    Expect::new()
        .c("issuer_cik", "0000045012")
        .c("filer_cik", "0001970357")
        .c("sale_index", "0")
        .c("seller_name", "J.Shannon Slocum")
        .addr(
            "seller",
            addr6(
                "3000 North Sam Houston Parkway East",
                "",
                "Houston",
                "TX",
                "77032",
                "",
            ),
        )
        .c("securities_class_title", "Common")
        .c("sale_date", "2026-01-09")
        .c("amount_sold", "23895.000000")
        .c("gross_proceeds", "771808.50")
        .c("has_parse_issues", "false")
        .assert(&b, table, 0);
    assert_eq!(
        b.column(table, "sale_index"),
        ["0", "0", "1", "2", "3", "4", "5"]
    );
    assert_eq!(
        b.column(table, "sale_date")[1..],
        [
            "2025-12-16",
            "2025-12-17",
            "2025-12-18",
            "2026-02-27",
            "2026-03-10",
            "2026-03-11"
        ]
    );
    assert_eq!(
        b.column(table, "gross_proceeds")[1..],
        [
            "221377.60",
            "386229.55",
            "294275.08",
            "37163.01",
            "595079.37",
            "113970.42"
        ]
    );
    assert_eq!(
        b.cell(table, "seller_street1", 6),
        "160 S. INDUSTRIAL BLVD S.W,."
    );
    assert_eq!(
        b.cell(table, "seller_name", 6),
        "LORBERBAUM FAMILY PAST S HELEN CUST"
    );
}

#[test]
fn form144_singular_securities_information_is_entry_0_only_without_entries() {
    let single = |units: &str, value: &str| sec::SecuritiesInformation {
        securities_class_title: s("Common"),
        units_sold: s(units),
        aggregate_market_value: s(value),
        ..Default::default()
    };
    let notice = |singular: Option<sec::SecuritiesInformation>,
                  entries: Vec<sec::SecuritiesInformation>| {
        Body::Form144(sec::Form144Notice {
            securities_information: singular,
            securities_information_entries: entries,
            ..Default::default()
        })
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![
            // A pre-0.13 block: only the singular → entry 0.
            filing("144", notice(Some(single("10", "1.50")), Vec::new())),
            // 0.13.0: the entries win; the singular is never written too.
            filing(
                "144",
                notice(
                    Some(single("999", "999.00")),
                    vec![single("1", "0.10"), single("2", "0.20")],
                ),
            ),
            // Neither: no row.
            filing("144", notice(None, Vec::new())),
        ],
    )]);
    let table = "form144_securities_information";
    assert_eq!(b.column(table, "filing_index"), ["0", "1", "1"]);
    assert_eq!(b.column(table, "entry_index"), ["0", "0", "1"]);
    assert_eq!(
        b.column(table, "units_sold"),
        ["10.000000", "1.000000", "2.000000"]
    );
    // The broker is absent: its name and address are NULL.
    assert_eq!(b.cell(table, "broker_name", 0), "NULL");
    assert_eq!(b.cell(table, "broker_street1", 0), "NULL");
    let table = "form144_notices";
    assert_eq!(
        b.column(table, "securities_information_count"),
        ["1", "2", "0"]
    );
    assert_eq!(
        b.column(table, "total_units_sold"),
        ["10.000000", "3.000000", "NULL"]
    );
    assert_eq!(
        b.column(table, "total_aggregate_market_value"),
        ["1.50", "0.30", "NULL"]
    );
}

#[test]
fn form144_absent_messages_give_nulls_false_and_empty_lists() {
    let body = sec::Form144Notice {
        securities_to_be_sold: vec![sec::SecuritiesToBeSold::default()],
        securities_sold_past_3_months: vec![sec::SecuritiesSoldPast3Months::default()],
        securities_information_entries: vec![sec::SecuritiesInformation::default()],
        ..Default::default()
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![filing("144", Body::Form144(body))],
    )]);
    Expect::new()
        .c("filer_cik", "NULL")
        .c("issuer_cik", "NULL")
        .c("issuer_name", "NULL")
        .c("issuer_sec_file_number", "NULL")
        .addr("issuer", NO_ADDRESS)
        .c("issuer_contact_phone", "NULL")
        .c("person_for_whose_account", "NULL")
        .c("relationships_to_issuer", "[]")
        .c("nothing_sold_past_3_months", "false")
        .c("remarks", "NULL")
        .c("previous_accession_number", "NULL")
        .c("notice_date", "NULL")
        .c("signature_text", "NULL")
        .c("plan_adoption_dates", "[]")
        .c("securities_information_count", "1")
        // The one entry has no units: the totals are NULL, with no issue.
        .c("total_units_sold", "NULL")
        .c("total_aggregate_market_value", "NULL")
        .c("securities_to_be_sold_count", "1")
        .c("sales_past_3_months_count", "1")
        .c("has_parse_issues", "false")
        .assert(&b, "form144_notices", 0);
    Expect::new()
        .c("issuer_cik", "NULL")
        .c("filer_cik", "NULL")
        .c("entry_index", "0")
        .c("securities_class_title", "NULL")
        .c("broker_name", "NULL")
        .addr("broker", NO_ADDRESS)
        .c("units_sold", "NULL")
        .c("aggregate_market_value", "NULL")
        .c("units_outstanding", "NULL")
        .c("approx_sale_date", "NULL")
        .c("securities_exchange_name", "NULL")
        .c("has_parse_issues", "false")
        .assert(&b, "form144_securities_information", 0);
    Expect::new()
        .c("issuer_cik", "NULL")
        .c("filer_cik", "NULL")
        .c("lot_index", "0")
        .c("securities_class_title", "NULL")
        .c("acquired_date", "NULL")
        .c("nature_of_acquisition", "NULL")
        .c("acquired_from", "NULL")
        .c("is_gift", "false")
        .c("donor_acquired_date", "NULL")
        .c("amount_acquired", "NULL")
        .c("payment_date", "NULL")
        .c("nature_of_payment", "NULL")
        .c("has_parse_issues", "false")
        .assert(&b, "form144_securities_to_be_sold", 0);
    Expect::new()
        .c("issuer_cik", "NULL")
        .c("filer_cik", "NULL")
        .c("sale_index", "0")
        .c("seller_name", "NULL")
        .addr("seller", NO_ADDRESS)
        .c("securities_class_title", "NULL")
        .c("sale_date", "NULL")
        .c("amount_sold", "NULL")
        .c("gross_proceeds", "NULL")
        .c("has_parse_issues", "false")
        .assert(&b, "form144_sales_past_3_months", 0);
    assert!(b.issues().is_empty());
}

#[test]
fn form144_flags_gifts_amendments_and_nothing_sold() {
    let body = sec::Form144Notice {
        nothing_sold_past_3_months: true,
        remarks: s("Gift to a family trust."),
        previous_accession_number: s("0001959173-26-002330"),
        securities_to_be_sold: vec![sec::SecuritiesToBeSold {
            is_gift: true,
            acquired_date: s("06/30/2026"),
            donor_acquired_date: s("1/2/2001"),
            ..Default::default()
        }],
        ..Default::default()
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![filing("144/A", Body::Form144(body))],
    )]);
    let table = "form144_notices";
    assert_eq!(b.cell(table, "nothing_sold_past_3_months", 0), "true");
    assert_eq!(b.cell(table, "remarks", 0), "Gift to a family trust.");
    assert_eq!(
        b.cell(table, "previous_accession_number", 0),
        "0001959173-26-002330"
    );
    let table = "form144_securities_to_be_sold";
    assert_eq!(b.cell(table, "is_gift", 0), "true");
    assert_eq!(b.cell(table, "acquired_date", 0), "2026-06-30");
    assert_eq!(b.cell(table, "donor_acquired_date", 0), "2001-01-02");
    assert_eq!(b.rows("form144_sales_past_3_months"), 0);
}

#[test]
fn form144_issues_are_keyed_by_position_in_emit_order() {
    let entry = |units: &str, date: &str| sec::SecuritiesInformation {
        units_sold: s(units),
        aggregate_market_value: s("1.00"),
        approx_sale_date: s(date),
        ..Default::default()
    };
    let body = sec::Form144Notice {
        signature: Some(sec::NoticeSignature {
            notice_date: s("2026-13-01"),
            signature: s("/s/ X"),
            plan_adoption_dates: strings(&["08/07/2025", "soon", "", "2025-01-02+05:00"]),
        }),
        securities_information_entries: vec![entry("1", "03/16/2026"), entry("2", "N/A")],
        securities_to_be_sold: vec![sec::SecuritiesToBeSold {
            amount_acquired: s("1,000"),
            ..Default::default()
        }],
        securities_sold_past_3_months: vec![
            sec::SecuritiesSoldPast3Months::default(),
            sec::SecuritiesSoldPast3Months::default(),
            sec::SecuritiesSoldPast3Months {
                gross_proceeds: s("1.005"),
                amount_sold: s("10"),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![
            filing("4", Body::Ownership(Default::default())),
            filing("144", Body::Form144(body)),
        ],
    )]);
    assert_eq!(
        issue_lines(&b),
        [
            "1 form144_notices.notice_date [-,-,-] 2026-13-01 out_of_range",
            "1 form144_notices.plan_adoption_dates [1,-,-] soon unparseable",
            "1 form144_notices.plan_adoption_dates [3,-,-] 2025-01-02+05:00 tz_dropped",
            "1 form144_securities_information.approx_sale_date [1,-,-] N/A sentinel",
            "1 form144_securities_to_be_sold.amount_acquired [0,-,-] 1,000 unparseable",
            "1 form144_sales_past_3_months.gross_proceeds [2,-,-] 1.005 rounded",
        ]
    );
    let table = "form144_notices";
    assert_eq!(b.cell(table, "notice_date", 0), "NULL");
    // Items keep their positions: an unparseable item and `""` are NULL items.
    assert_eq!(
        b.cell(table, "plan_adoption_dates", 0),
        "[2025-08-07, NULL, NULL, 2025-01-02]"
    );
    assert_eq!(b.cell(table, "total_units_sold", 0), "3.000000");
    assert_eq!(b.cell(table, "has_parse_issues", 0), "true");
    let table = "form144_securities_information";
    assert_eq!(b.column(table, "has_parse_issues"), ["false", "true"]);
    assert_eq!(b.column(table, "approx_sale_date"), ["2026-03-16", "NULL"]);
    let table = "form144_securities_to_be_sold";
    assert_eq!(b.cell(table, "amount_acquired", 0), "NULL");
    assert_eq!(b.cell(table, "has_parse_issues", 0), "true");
    let table = "form144_sales_past_3_months";
    assert_eq!(
        b.column(table, "has_parse_issues"),
        ["false", "false", "true"]
    );
    // Half away from zero at the M2 scale.
    assert_eq!(b.cell(table, "gross_proceeds", 2), "1.01");
    assert_eq!(b.cell(table, "amount_sold", 2), "10.000000");
}

#[test]
fn form144_totals_are_null_with_a_null_entry_and_log_an_overflow() {
    // 38 digits at scale 6: the largest Q6 value.
    const Q6_MAX: &str = "99999999999999999999999999999999.999999";
    const M2_MAX: &str = "999999999999999999999999999999999999.99";
    let entry = |units: &str, value: &str| sec::SecuritiesInformation {
        units_sold: s(units),
        aggregate_market_value: s(value),
        ..Default::default()
    };
    let notice = |entries| {
        Body::Form144(sec::Form144Notice {
            securities_information_entries: entries,
            ..Default::default()
        })
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![
            // A missing and an unparseable operand: NULL totals, no total issue.
            filing("144", notice(vec![entry("1", "1.00"), entry("", "x")])),
            // Both totals overflow 38 digits; each operand alone fits.
            filing(
                "144",
                notice(vec![entry(Q6_MAX, M2_MAX), entry("0.000001", "+0.01")]),
            ),
            // A sentinel operand.
            filing(
                "144",
                notice(vec![entry("N/A", "2.00"), entry("5", "3.00")]),
            ),
        ],
    )]);
    let table = "form144_notices";
    assert_eq!(
        b.column(table, "total_units_sold"),
        ["NULL", "NULL", "NULL"]
    );
    assert_eq!(
        b.column(table, "total_aggregate_market_value"),
        ["NULL", "NULL", "5.00"]
    );
    assert_eq!(
        b.column(table, "has_parse_issues"),
        ["false", "true", "false"]
    );
    assert_eq!(
        issue_lines(&b),
        [
            "0 form144_securities_information.aggregate_market_value [1,-,-] x unparseable",
            format!("1 form144_notices.total_units_sold [-,-,-] {Q6_MAX} + 0.000001 overflow")
                .as_str(),
            format!(
                "1 form144_notices.total_aggregate_market_value [-,-,-] {M2_MAX} + +0.01 overflow"
            )
            .as_str(),
            "2 form144_securities_information.units_sold [0,-,-] N/A sentinel",
        ]
    );
    // The entries themselves hold their exact values.
    assert_eq!(
        b.column("form144_securities_information", "units_sold")[2..4],
        [Q6_MAX, "0.000001"]
    );
}

// ---------------------------------------------------------------------------
// Form D
// ---------------------------------------------------------------------------

fn form_d_fixture_batches() -> Batches {
    map(&[window_block(
        BLOCK_NUM,
        vec![
            sample(JS_VENTURE, "D/A", Body::FormD(js_venture_form_d())),
            sample(INVESTCORP, "D/A", Body::FormD(investcorp_form_d())),
        ],
    )])
}

#[test]
fn form_d_notices_of_the_fixture_filings() {
    let b = form_d_fixture_batches();
    b.assert_rows(&[
        ("form_d_notices", 2),
        ("form_d_co_issuers", 1),
        ("form_d_related_persons", 1 + 3),
        ("form_d_sales_recipients", 2),
        ("parse_issues", 0),
    ]);
    let table = "form_d_notices";
    assert_fc(&b, table, 0, 0, JS_VENTURE, "D/A");
    Expect::new()
        .c("schema_version", "X0708")
        .c("submission_type", "D/A")
        .c("previous_accession_number", "0002111624-26-000001")
        .c("issuer_cik", "0002111624")
        .c("issuer_name", "JS VENTURE FUND LLC SERIES A36")
        .addr(
            "issuer",
            addr6(
                "585 STEWART AVE",
                "SUITE L60C",
                "GARDEN CITY",
                "NY",
                "11530",
                "NEW YORK",
            ),
        )
        .c("issuer_phone", "516-257-7001")
        .c("jurisdiction_of_inc", "DELAWARE")
        .c("entity_type", "Limited Liability Company")
        .c("entity_type_other_desc", "NULL")
        .c("year_of_inc", "2026")
        .c("year_of_inc_status", "withinFiveYears")
        .c("issuer_previous_names", "[]")
        .c("issuer_edgar_previous_names", "[]")
        .c("industry_group", "Pooled Investment Fund")
        .c("investment_fund_type", "Other Investment Fund")
        .c("is_40_act", "false")
        .c("revenue_range", "NULL")
        .c("aggregate_net_asset_value_range", "Decline to Disclose")
        .c("offering_is_amendment", "true")
        .c("date_of_first_sale", "2026-02-13")
        .c("date_of_first_sale_yet_to_occur", "NULL")
        .c("more_than_one_year", "false")
        .c("is_equity_type", "false")
        .c("securities_types", "[isPooledInvestmentFundType]")
        .c("description_of_other_type", "NULL")
        .c("is_business_combination", "false")
        .c("business_combination_clarification", "NULL")
        .c("federal_exemptions", "[06b, 3C, 3C.7]")
        .c("minimum_investment", "25000.00")
        .c("total_offering_amount", "NULL")
        .c("total_offering_amount_is_indefinite", "true")
        .c("total_amount_sold", "2755000.00")
        .c("total_remaining", "NULL")
        .c("total_remaining_is_indefinite", "true")
        .c("offering_sales_amounts_clarification", "NULL")
        .c("has_non_accredited_investors", "false")
        .c("number_non_accredited_investors", "NULL")
        .c("total_number_already_invested", "26")
        .c("sales_commissions", "244825.00")
        .c("sales_commissions_is_estimate", "true")
        .c("finders_fees", "0.00")
        .c("finders_fees_is_estimate", "true")
        .c("sales_commissions_clarification", JS_SALES_CLARIFICATION)
        .c("gross_proceeds_used", "68875.00")
        .c("gross_proceeds_used_is_estimate", "true")
        .c("use_of_proceeds_clarification", JS_PROCEEDS_CLARIFICATION)
        .c("authorized_representative", "false")
        .c("co_issuer_count", "0")
        .c("related_person_count", "1")
        .c("sales_recipient_count", "2")
        .c("has_parse_issues", "false")
        .assert(&b, table, 0);
    assert_fc(&b, table, 1, 1, INVESTCORP, "D/A");
    Expect::new()
        .c("schema_version", "X0708")
        .c("submission_type", "D/A")
        .c("previous_accession_number", "0002048118-25-000001")
        .c("issuer_cik", "0002048118")
        .c(
            "issuer_name",
            "Investcorp North American Private Equity Fund II, L.P.",
        )
        .addr(
            "issuer",
            addr6(
                "280 PARK AVENUE",
                "36TH FLOOR",
                "NEW YORK",
                "NY",
                "10017",
                "NEW YORK",
            ),
        )
        .c("issuer_phone", "(212) 599-4700")
        .c("jurisdiction_of_inc", "DELAWARE")
        .c("entity_type", "Limited Partnership")
        .c("entity_type_other_desc", "NULL")
        .c("year_of_inc", "2024")
        .c("year_of_inc_status", "withinFiveYears")
        .c("issuer_previous_names", "[]")
        .c("issuer_edgar_previous_names", "[]")
        .c("industry_group", "Pooled Investment Fund")
        .c("investment_fund_type", "Private Equity Fund")
        .c("is_40_act", "false")
        .c("revenue_range", "Decline to Disclose")
        .c("aggregate_net_asset_value_range", "NULL")
        .c("offering_is_amendment", "true")
        .c("date_of_first_sale", "2025-02-13")
        .c("date_of_first_sale_yet_to_occur", "NULL")
        .c("more_than_one_year", "true")
        .c("is_equity_type", "true")
        .c(
            "securities_types",
            "[isEquityType, isPooledInvestmentFundType]",
        )
        .c("description_of_other_type", "NULL")
        .c("is_business_combination", "false")
        .c("business_combination_clarification", "NULL")
        .c("federal_exemptions", "[06b, 3C, 3C.7]")
        .c("minimum_investment", "0.00")
        .c("total_offering_amount", "NULL")
        .c("total_offering_amount_is_indefinite", "true")
        .c("total_amount_sold", "251371785.00")
        .c("total_remaining", "NULL")
        .c("total_remaining_is_indefinite", "true")
        .c("offering_sales_amounts_clarification", "NULL")
        .c("has_non_accredited_investors", "false")
        .c("number_non_accredited_investors", "NULL")
        .c("total_number_already_invested", "24")
        .c("sales_commissions", "0.00")
        .c("sales_commissions_is_estimate", "NULL")
        .c("finders_fees", "0.00")
        .c("finders_fees_is_estimate", "NULL")
        .c("sales_commissions_clarification", "NULL")
        .c("gross_proceeds_used", "0.00")
        .c("gross_proceeds_used_is_estimate", "NULL")
        .c("use_of_proceeds_clarification", "NULL")
        .c("authorized_representative", "false")
        .c("co_issuer_count", "1")
        .c("related_person_count", "3")
        .c("sales_recipient_count", "0")
        .c("has_parse_issues", "false")
        .assert(&b, table, 1);
}

#[test]
fn form_d_child_tables_of_the_fixture_filings() {
    let b = form_d_fixture_batches();

    let table = "form_d_co_issuers";
    assert_fc(&b, table, 0, 1, INVESTCORP, "D/A");
    Expect::new()
        .c("issuer_cik", "0002048118")
        .c("co_issuer_index", "0")
        .c("co_issuer_cik", "0002048215")
        .c(
            "co_issuer_name",
            "Investcorp North American Private Equity Parallel Fund II, L.P.",
        )
        .addr(
            "co_issuer",
            addr6(
                "280 PARK AVENUE",
                "36TH FLOOR",
                "NEW YORK",
                "NY",
                "10017",
                "NEW YORK",
            ),
        )
        .c("co_issuer_phone", "(212) 599-4700")
        .c("jurisdiction_of_inc", "CAYMAN ISLANDS")
        .c("entity_type", "Limited Partnership")
        .c("entity_type_other_desc", "NULL")
        .c("year_of_inc", "2024")
        .c("year_of_inc_status", "withinFiveYears")
        .c("previous_names", "[]")
        .c("edgar_previous_names", "[]")
        .c("has_parse_issues", "false")
        .assert(&b, table, 0);

    let table = "form_d_related_persons";
    assert_fc(&b, table, 0, 0, JS_VENTURE, "D/A");
    Expect::new()
        .c("issuer_cik", "0002111624")
        .c("person_index", "0")
        .c("first_name", "DAMIAN")
        .c("middle_name", "NULL")
        .c("last_name", "MAGGIO")
        .addr(
            "person",
            addr6(
                "585 STEWART AVE",
                "SUITE L60C",
                "GARDEN CITY",
                "NY",
                "11530",
                "NEW YORK",
            ),
        )
        .c("relationships", "[Executive Officer]")
        .c(
            "relationship_clarification",
            "Managing Member, Global Venture Management LLC, its Manager.",
        )
        .assert(&b, table, 0);
    assert_fc(&b, table, 3, 1, INVESTCORP, "D/A");
    Expect::new()
        .c("issuer_cik", "0002048118")
        .c("person_index", "2")
        .c("first_name", "N/A")
        .c("middle_name", "NULL")
        .c(
            "last_name",
            "Investcorp North American Private Equity Fund II GP, L.P.",
        )
        .addr(
            "person",
            addr6(
                "280 Park Avenue",
                "36th Floor",
                "New York",
                "NY",
                "10017",
                "NEW YORK",
            ),
        )
        .c("relationships", "[Promoter]")
        .c(
            "relationship_clarification",
            "General Partner of the Issuer",
        )
        .assert(&b, table, 3);
    assert_eq!(b.column(table, "person_index"), ["0", "0", "1", "2"]);
    assert_eq!(
        b.column(table, "last_name")[1..3],
        ["McGrane", "Investcorp Investment Advisers LLC"]
    );

    let table = "form_d_sales_recipients";
    assert_fc(&b, table, 0, 0, JS_VENTURE, "D/A");
    Expect::new()
        .c("issuer_cik", "0002111624")
        .c("recipient_index", "0")
        .c("recipient_name", "VCS VENTURE SECURITIES")
        .c("recipient_crd_number", "127921")
        .c("associated_bd_name", "None")
        .c("associated_bd_crd_number", "None")
        .addr(
            "recipient",
            addr6(
                "29 BROADWAY",
                "SUITE 1502",
                "NEW YORK",
                "NY",
                "10006",
                "NEW YORK",
            ),
        )
        .c(
            "states_of_solicitation",
            "[AL, AZ, CA, CO, FL, IL, IN, KS, ME, MD, MI, NY, TX]",
        )
        .c("foreign_solicitation", "false")
        .assert(&b, table, 0);
    Expect::new()
        .c("issuer_cik", "0002111624")
        .c("recipient_index", "1")
        .c("recipient_name", "JOSEPH STONE CAPITAL L.L.C.")
        .c("recipient_crd_number", "159744")
        .c("associated_bd_name", "None")
        .c("associated_bd_crd_number", "None")
        .addr(
            "recipient",
            addr6(
                "585 STEWART AVE",
                "SUITE L60C",
                "GARDEN CITY",
                "NY",
                "11530",
                "NEW YORK",
            ),
        )
        .c("states_of_solicitation", "[]")
        .c("foreign_solicitation", "true")
        .assert(&b, table, 1);
}

#[test]
fn form_d_indefinite_is_a_flag_not_an_issue() {
    let offering = |total: &str, remaining: &str| {
        Body::FormD(sec::FormDNotice {
            offering: Some(sec::OfferingData {
                total_offering_amount: s(total),
                total_remaining: s(remaining),
                ..Default::default()
            }),
            ..Default::default()
        })
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![
            filing("D", offering("Indefinite", " INDEFINITE ")),
            filing("D", offering("indefinite", "1500000")),
            filing("D", offering("", "Indefinitely")),
            filing("D", offering("N/A", "0")),
        ],
    )]);
    let table = "form_d_notices";
    assert_eq!(
        b.column(table, "total_offering_amount"),
        ["NULL", "NULL", "NULL", "NULL"]
    );
    assert_eq!(
        b.column(table, "total_offering_amount_is_indefinite"),
        ["true", "true", "false", "false"]
    );
    assert_eq!(
        b.column(table, "total_remaining"),
        ["NULL", "1500000.00", "NULL", "0.00"]
    );
    assert_eq!(
        b.column(table, "total_remaining_is_indefinite"),
        ["true", "false", "false", "false"]
    );
    assert_eq!(
        b.column(table, "has_parse_issues"),
        ["false", "false", "true", "true"]
    );
    assert_eq!(
        issue_lines(&b),
        [
            "2 form_d_notices.total_remaining [-,-,-] Indefinitely unparseable",
            "3 form_d_notices.total_offering_amount [-,-,-] N/A sentinel",
        ]
    );
}

#[test]
fn form_d_bools_are_null_exactly_when_their_message_or_option_is_absent() {
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![
            // No primary issuer, no offering.
            filing("D", Body::FormD(sec::FormDNotice::default())),
            // An empty offering: its proto bools are false, its optional bools NULL.
            filing(
                "D",
                Body::FormD(sec::FormDNotice {
                    offering: Some(sec::OfferingData::default()),
                    authorized_representative: Some(true),
                    ..Default::default()
                }),
            ),
            // Optional bools set.
            filing(
                "D",
                Body::FormD(sec::FormDNotice {
                    offering: Some(sec::OfferingData {
                        is_40_act: Some(true),
                        date_of_first_sale_yet_to_occur: Some(true),
                        is_business_combination: Some(true),
                        sales_commissions_is_estimate: Some(false),
                        finders_fees_is_estimate: Some(false),
                        gross_proceeds_used_is_estimate: Some(false),
                        has_non_accredited_investors: true,
                        ..Default::default()
                    }),
                    sales_compensation_recipients: vec![Default::default()],
                    ..Default::default()
                }),
            ),
        ],
    )]);
    let table = "form_d_notices";
    let column = |name: &str| b.column(table, name);
    for name in [
        "offering_is_amendment",
        "more_than_one_year",
        "is_equity_type",
    ] {
        assert_eq!(column(name), ["NULL", "false", "false"], "{name}");
    }
    assert_eq!(
        column("has_non_accredited_investors"),
        ["NULL", "false", "true"]
    );
    for name in [
        "is_40_act",
        "date_of_first_sale_yet_to_occur",
        "is_business_combination",
    ] {
        assert_eq!(column(name), ["NULL", "NULL", "true"], "{name}");
    }
    for name in [
        "sales_commissions_is_estimate",
        "finders_fees_is_estimate",
        "gross_proceeds_used_is_estimate",
    ] {
        assert_eq!(column(name), ["NULL", "NULL", "false"], "{name}");
    }
    assert_eq!(
        column("authorized_representative"),
        ["NULL", "true", "NULL"]
    );
    assert_eq!(
        column("total_offering_amount_is_indefinite"),
        ["false", "false", "false"]
    );
    for name in [
        "securities_types",
        "federal_exemptions",
        "issuer_previous_names",
    ] {
        assert_eq!(column(name), ["[]", "[]", "[]"], "{name}");
    }
    for name in [
        "issuer_cik",
        "issuer_street1",
        "industry_group",
        "year_of_inc",
    ] {
        assert_eq!(column(name), ["NULL", "NULL", "NULL"], "{name}");
    }
    assert_eq!(column("sales_recipient_count"), ["0", "0", "1"]);
    // A recipient without the optional flag, address or states.
    let table = "form_d_sales_recipients";
    assert_eq!(b.cell(table, "foreign_solicitation", 0), "NULL");
    assert_eq!(b.cell(table, "recipient_street1", 0), "NULL");
    assert_eq!(b.cell(table, "states_of_solicitation", 0), "[]");
    assert_eq!(b.cell(table, "issuer_cik", 0), "NULL");
    assert!(b.issues().is_empty());
}

#[test]
fn form_d_issues_are_keyed_by_position_in_emit_order() {
    let co_issuer = |year: &str| sec::IssuerD {
        year_of_inc: s(year),
        previous_names: strings(&["OLD NAME LLC", ""]),
        edgar_previous_names: strings(&["OLDER NAME LLC"]),
        ..Default::default()
    };
    let body = sec::FormDNotice {
        primary_issuer: Some(sec::IssuerD {
            cik: s("0000000042"),
            year_of_inc: s("2O24"),
            previous_names: strings(&["FORMER CO"]),
            edgar_previous_names: strings(&["FORMER CO", "FORMER CO INC"]),
            ..Default::default()
        }),
        offering: Some(sec::OfferingData {
            date_of_first_sale: s("2026-02-30"),
            minimum_investment: s("$25,000"),
            number_non_accredited_investors: s("99999999999999999999"),
            total_already_invested: s("12.0"),
            sales_commissions: s("10.555"),
            ..Default::default()
        }),
        issuers: vec![co_issuer("2020"), co_issuer("N/A")],
        related_persons: vec![Default::default()],
        ..Default::default()
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![filing("D", Body::FormD(body))],
    )]);
    assert_eq!(
        issue_lines(&b),
        [
            "0 form_d_notices.year_of_inc [-,-,-] 2O24 unparseable",
            "0 form_d_notices.date_of_first_sale [-,-,-] 2026-02-30 out_of_range",
            "0 form_d_notices.minimum_investment [-,-,-] $25,000 unparseable",
            "0 form_d_notices.number_non_accredited_investors [-,-,-] 99999999999999999999 out_of_range",
            "0 form_d_notices.sales_commissions [-,-,-] 10.555 rounded",
            "0 form_d_co_issuers.year_of_inc [1,-,-] N/A sentinel",
        ]
    );
    let table = "form_d_notices";
    assert_eq!(b.cell(table, "year_of_inc", 0), "NULL");
    assert_eq!(b.cell(table, "total_number_already_invested", 0), "12");
    assert_eq!(b.cell(table, "sales_commissions", 0), "10.56");
    assert_eq!(b.cell(table, "issuer_previous_names", 0), "[FORMER CO]");
    assert_eq!(
        b.cell(table, "issuer_edgar_previous_names", 0),
        "[FORMER CO, FORMER CO INC]"
    );
    assert_eq!(b.cell(table, "has_parse_issues", 0), "true");
    let table = "form_d_co_issuers";
    assert_eq!(b.column(table, "co_issuer_index"), ["0", "1"]);
    assert_eq!(b.column(table, "year_of_inc"), ["2020", "NULL"]);
    assert_eq!(b.column(table, "has_parse_issues"), ["false", "true"]);
    // The parent's issuer CIK is copied; `""` list items stay `""`.
    assert_eq!(b.column(table, "issuer_cik"), ["0000000042", "0000000042"]);
    assert_eq!(b.cell(table, "previous_names", 0), "[OLD NAME LLC, ]");
    assert_eq!(b.cell(table, "edgar_previous_names", 1), "[OLDER NAME LLC]");
    let table = "form_d_related_persons";
    assert_eq!(b.cell(table, "issuer_cik", 0), "0000000042");
    assert_eq!(b.cell(table, "relationships", 0), "[]");
    assert_eq!(b.cell(table, "person_street1", 0), "NULL");
}

// ---------------------------------------------------------------------------
// Form C
// ---------------------------------------------------------------------------

#[test]
fn form_c_notices_of_the_fixture_filings() {
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![
            sample(KOKOPELLI, "C", Body::FormC(kokopelli_form_c())),
            sample(CLASSIC_COFFEE, "C-U", Body::FormC(classic_coffee_form_c())),
        ],
    )]);
    b.assert_rows(&[
        ("form_c_notices", 2),
        ("form_c_co_issuers", 1),
        ("parse_issues", 0),
    ]);
    let table = "form_c_notices";
    assert_fc(&b, table, 0, 0, KOKOPELLI, "C");
    Expect::new()
        .c("filer_cik", "0001720779")
        .c("issuer_name", "Kokopelli Outdoor, Inc.")
        .c("issuer_legal_status_form", "Corporation")
        .c("issuer_legal_status_other_desc", "NULL")
        .c("issuer_jurisdiction", "DE")
        .c("issuer_date_incorporation", "2018-01-01")
        .addr(
            "issuer",
            addr6("3863 Steele Street", "", "Denver", "CO", "80205", ""),
        )
        .c("issuer_website", "https://rovrproducts.com")
        .c("intermediary_company_name", "Wefunder Portal LLC")
        .c("intermediary_cik", "0001670254")
        .c("intermediary_file_number", "007-00033")
        .c("intermediary_crd_number", "283503")
        .c("issuer_info_is_amendment", "NULL")
        .c("nature_of_amendment", "NULL")
        .c("progress_update", "NULL")
        .c("is_co_issuer", "true")
        .c("period", "NULL")
        .c("security_type", "Other")
        .c(
            "security_offered_other_desc",
            "Simple Agreement for Future Equity (SAFE)",
        )
        .c("num_securities_offered", "50000.000000")
        .c("price", "1.000000")
        .c("price_determination_method", KOKOPELLI_PRICE_METHOD)
        .c("offering_amount", "50000.00")
        .c("maximum_offering_amount", "900000.00")
        .c("over_subscription_accepted", "true")
        .c("over_subscription_allocation_type", "Other")
        .c("desc_over_subscription", "As determined by the issuer")
        .c("deadline_date", "2027-04-30")
        .c("compensation_amount", KOKOPELLI_COMPENSATION)
        .c("financial_interest", "No")
        .c(
            "offering_jurisdictions",
            &list_text(KOKOPELLI_JURISDICTIONS),
        )
        .c("has_financials", "true")
        .c("current_employees", "8")
        .c("total_assets_most_recent_fy", "6597473.00")
        .c("total_assets_prior_fy", "4843898.00")
        .c("cash_equivalents_most_recent_fy", "140183.00")
        .c("cash_equivalents_prior_fy", "69416.00")
        .c("accounts_receivable_most_recent_fy", "682217.00")
        .c("accounts_receivable_prior_fy", "197943.00")
        .c("short_term_debt_most_recent_fy", "2277644.00")
        .c("short_term_debt_prior_fy", "1232634.00")
        .c("long_term_debt_most_recent_fy", "4241552.00")
        .c("long_term_debt_prior_fy", "2687652.00")
        .c("revenue_most_recent_fy", "6194049.00")
        .c("revenue_prior_fy", "4423014.00")
        .c("cost_goods_sold_most_recent_fy", "3155803.00")
        .c("cost_goods_sold_prior_fy", "2164763.00")
        .c("tax_paid_most_recent_fy", "0.00")
        .c("tax_paid_prior_fy", "0.00")
        .c("net_income_most_recent_fy", "-2371578.00")
        .c("net_income_prior_fy", "-2439688.00")
        .c("co_issuer_count", "1")
        .c("has_parse_issues", "false")
        .assert(&b, table, 0);
    assert_fc(&b, table, 1, 1, CLASSIC_COFFEE, "C-U");
    Expect::new()
        .c("filer_cik", "0002143684")
        .c("issuer_name", "Classic Coffee and Tea, LLC")
        .c("issuer_legal_status_form", "Limited Liability Company")
        .c("issuer_legal_status_other_desc", "NULL")
        .c("issuer_jurisdiction", "VA")
        .c("issuer_date_incorporation", "2023-01-01")
        .addr(
            "issuer",
            addr6(
                "8247 RAVEN RUN DRIVE",
                "",
                "MECHANICSVILLE",
                "PA",
                "23111",
                "",
            ),
        )
        .c("issuer_website", "https://www.classiccoffeeteabooks.com")
        .c("intermediary_company_name", "Honeycomb Portal LLC")
        .c("intermediary_cik", "0001705726")
        .c("intermediary_file_number", "007-00119")
        .c("intermediary_crd_number", "289015")
        .c("issuer_info_is_amendment", "NULL")
        .c("nature_of_amendment", "NULL")
        .c("progress_update", "Offering closed unsuccessfully")
        .c("is_co_issuer", "false")
        .c("period", "NULL")
        .c("security_type", "Debt")
        .c("security_offered_other_desc", "NULL")
        .c("num_securities_offered", "NULL")
        .c("price", "1.000000")
        .c(
            "price_determination_method",
            "Pro-rated portion of the total principal",
        )
        .c("offering_amount", "15000.00")
        .c("maximum_offering_amount", "30000.00")
        .c("over_subscription_accepted", "true")
        .c(
            "over_subscription_allocation_type",
            "First-come, first-served basis",
        )
        .c("desc_over_subscription", "NULL")
        .c("deadline_date", "2026-08-13")
        .c("compensation_amount", COFFEE_COMPENSATION)
        .c("financial_interest", "None")
        .c("offering_jurisdictions", &list_text(COFFEE_JURISDICTIONS))
        .c("has_financials", "true")
        // `4.00`: an all-zero fraction is an exact integer.
        .c("current_employees", "4")
        .c("total_assets_most_recent_fy", "26293.00")
        .c("total_assets_prior_fy", "47323.00")
        .c("cash_equivalents_most_recent_fy", "489.00")
        .c("cash_equivalents_prior_fy", "7000.00")
        .c("accounts_receivable_most_recent_fy", "0.00")
        .c("accounts_receivable_prior_fy", "0.00")
        .c("short_term_debt_most_recent_fy", "13677.00")
        .c("short_term_debt_prior_fy", "2464.00")
        .c("long_term_debt_most_recent_fy", "36247.00")
        .c("long_term_debt_prior_fy", "41939.00")
        .c("revenue_most_recent_fy", "206160.00")
        .c("revenue_prior_fy", "119811.00")
        .c("cost_goods_sold_most_recent_fy", "70640.00")
        .c("cost_goods_sold_prior_fy", "57118.00")
        .c("tax_paid_most_recent_fy", "0.00")
        .c("tax_paid_prior_fy", "0.00")
        .c("net_income_most_recent_fy", "39937.00")
        .c("net_income_prior_fy", "-22626.00")
        .c("co_issuer_count", "0")
        .c("has_parse_issues", "false")
        .assert(&b, table, 1);

    let table = "form_c_co_issuers";
    assert_fc(&b, table, 0, 0, KOKOPELLI, "C");
    Expect::new()
        // Copied from the notice: the primary issuer (critic fix C9).
        .c("filer_cik", "0001720779")
        .c("co_issuer_index", "0")
        .c(
            "co_issuer_name",
            "ROVR Products IV, a series of Wefunder SPV, LLC",
        )
        .c("legal_status_form", "Limited Liability Company")
        .c("legal_status_other_desc", "NULL")
        .c("jurisdiction", "DE")
        .c("date_incorporation", "2026-08-13")
        .addr(
            "co_issuer",
            addr6(
                "1887 Whitney Mesa Dr",
                "NUM 8885",
                "Henderson",
                "NV",
                "89014",
                "",
            ),
        )
        .c("website", "https://wefunder.com/")
        .c("has_parse_issues", "false")
        .assert(&b, table, 0);
}

#[test]
fn form_c_presence_of_offering_financials_and_optional_bools() {
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![
            filing("C", Body::FormC(sec::FormCNotice::default())),
            filing(
                "C/A",
                Body::FormC(sec::FormCNotice {
                    offering: Some(sec::FormCOffering::default()),
                    financials: Some(sec::FormCFinancials::default()),
                    is_amendment: Some(true),
                    nature_of_amendment: s("Updated financials"),
                    is_co_issuer: Some(false),
                    period: s("12-31-2025"),
                    issuer: Some(sec::FormCIssuer {
                        legal_status_form: s("Other"),
                        legal_status_other_desc: s("Benefit corporation"),
                        ..Default::default()
                    }),
                    co_issuers: vec![sec::FormCIssuer::default()],
                    ..Default::default()
                }),
            ),
        ],
    )]);
    let table = "form_c_notices";
    let column = |name: &str| b.column(table, name);
    assert_eq!(column("has_financials"), ["false", "true"]);
    assert_eq!(column("over_subscription_accepted"), ["NULL", "false"]);
    assert_eq!(column("issuer_info_is_amendment"), ["NULL", "true"]);
    assert_eq!(column("is_co_issuer"), ["NULL", "false"]);
    assert_eq!(
        column("nature_of_amendment"),
        ["NULL", "Updated financials"]
    );
    assert_eq!(column("period"), ["NULL", "2025-12-31"]);
    assert_eq!(
        column("issuer_legal_status_other_desc"),
        ["NULL", "Benefit corporation"]
    );
    assert_eq!(column("offering_jurisdictions"), ["[]", "[]"]);
    assert_eq!(column("co_issuer_count"), ["0", "1"]);
    for name in crate::sec::prepare::formc::FINANCIAL_COLUMNS
        .into_iter()
        .chain([
            "current_employees",
            "price",
            "security_type",
            "issuer_street1",
        ])
    {
        assert_eq!(column(name), ["NULL", "NULL"], "{name}");
    }
    let table = "form_c_co_issuers";
    assert_eq!(b.cell(table, "filer_cik", 0), "NULL");
    assert_eq!(b.cell(table, "co_issuer_street1", 0), "NULL");
    assert_eq!(b.cell(table, "date_incorporation", 0), "NULL");
    assert!(b.issues().is_empty());
}

#[test]
fn form_c_financial_columns_follow_the_schema_and_their_sources() {
    let schema = crate::sec::schema::table_schema("form_c_notices", false, &EncodeBytes::Hex);
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    let first = names
        .iter()
        .position(|name| *name == "current_employees")
        .unwrap()
        + 1;
    assert_eq!(
        names[first..first + 18],
        crate::sec::prepare::formc::FINANCIAL_COLUMNS
    );
    // Distinct values through the mapper: each column gets its own source.
    let values: Vec<String> = (1..=18).map(|i| format!("{i}.{i:02}")).collect();
    let mut sources = ["7"; 19];
    for (slot, value) in sources[1..].iter_mut().zip(&values) {
        *slot = value.as_str();
    }
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![filing(
            "C-AR",
            Body::FormC(sec::FormCNotice {
                financials: Some(financials(sources)),
                ..Default::default()
            }),
        )],
    )]);
    for (column, value) in crate::sec::prepare::formc::FINANCIAL_COLUMNS
        .into_iter()
        .zip(&values)
    {
        assert_eq!(&b.cell("form_c_notices", column, 0), value, "{column}");
    }
    assert_eq!(b.cell("form_c_notices", "current_employees", 0), "7");
}

#[test]
fn form_c_issues_are_keyed_by_position_in_emit_order() {
    let mut statements = financials(["4.5"; 19]);
    statements.total_assets_most_recent_fy = s("100");
    statements.revenue_prior_fy = s("N/A");
    for value in [
        &mut statements.total_assets_prior_fy,
        &mut statements.cash_equivalents_most_recent_fy,
        &mut statements.cash_equivalents_prior_fy,
        &mut statements.accounts_receivable_most_recent_fy,
        &mut statements.accounts_receivable_prior_fy,
        &mut statements.short_term_debt_most_recent_fy,
        &mut statements.short_term_debt_prior_fy,
        &mut statements.long_term_debt_most_recent_fy,
        &mut statements.long_term_debt_prior_fy,
        &mut statements.revenue_most_recent_fy,
        &mut statements.cost_goods_sold_most_recent_fy,
        &mut statements.cost_goods_sold_prior_fy,
        &mut statements.tax_paid_most_recent_fy,
        &mut statements.tax_paid_prior_fy,
        &mut statements.net_income_most_recent_fy,
    ] {
        value.clear();
    }
    statements.net_income_prior_fy = s("-1.5");
    let body = sec::FormCNotice {
        filer_cik: s("0000000077"),
        issuer: Some(sec::FormCIssuer {
            date_incorporation: s("2018"),
            ..Default::default()
        }),
        period: s("12-31-2025"),
        offering: Some(sec::FormCOffering {
            price: s("1.0000005"),
            num_securities_offered: s("1e6"),
            deadline_date: s("04/30/2027"),
            offering_amount: s("-"),
            ..Default::default()
        }),
        financials: Some(statements),
        co_issuers: vec![
            sec::FormCIssuer {
                date_incorporation: s("13-01-2020"),
                ..Default::default()
            },
            sec::FormCIssuer {
                date_incorporation: s("8-1-2026"),
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let b = map(&[window_block(
        BLOCK_NUM,
        vec![filing("C", Body::FormC(body))],
    )]);
    assert_eq!(
        issue_lines(&b),
        [
            "0 form_c_notices.issuer_date_incorporation [-,-,-] 2018 unparseable",
            "0 form_c_notices.num_securities_offered [-,-,-] 1e6 unparseable",
            "0 form_c_notices.price [-,-,-] 1.0000005 rounded",
            "0 form_c_notices.offering_amount [-,-,-] - sentinel",
            "0 form_c_notices.current_employees [-,-,-] 4.5 unparseable",
            "0 form_c_notices.revenue_prior_fy [-,-,-] N/A sentinel",
            "0 form_c_co_issuers.date_incorporation [0,-,-] 13-01-2020 out_of_range",
        ]
    );
    let table = "form_c_notices";
    assert_eq!(b.cell(table, "price", 0), "1.000001");
    assert_eq!(b.cell(table, "period", 0), "2025-12-31");
    assert_eq!(b.cell(table, "deadline_date", 0), "2027-04-30");
    assert_eq!(b.cell(table, "current_employees", 0), "NULL");
    assert_eq!(b.cell(table, "total_assets_most_recent_fy", 0), "100.00");
    assert_eq!(b.cell(table, "total_assets_prior_fy", 0), "NULL");
    assert_eq!(b.cell(table, "net_income_prior_fy", 0), "-1.50");
    assert_eq!(b.cell(table, "has_parse_issues", 0), "true");
    let table = "form_c_co_issuers";
    assert_eq!(b.column(table, "co_issuer_index"), ["0", "1"]);
    assert_eq!(
        b.column(table, "date_incorporation"),
        ["NULL", "2026-08-01"]
    );
    assert_eq!(b.column(table, "has_parse_issues"), ["true", "false"]);
    assert_eq!(b.column(table, "filer_cik"), ["0000000077", "0000000077"]);
}

// ---------------------------------------------------------------------------
// Shared behavior
// ---------------------------------------------------------------------------

#[test]
fn smallforms_tables_end_with_the_fork_step_columns() {
    let b = map_with(
        &[window_block(BLOCK_NUM, contract_filings())],
        true,
        EncodeBytes::Hex,
    );
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

#[test]
fn smallforms_rows_follow_their_filings_across_blocks() {
    let b = map(&[
        window_block(
            BLOCK_NUM,
            vec![
                filing("4", Body::Ownership(Default::default())),
                sample(MOHAWK, "144", Body::Form144(mohawk_144())),
            ],
        ),
        window_block(
            BLOCK_NUM + 1,
            vec![sample(INVESTCORP, "D/A", Body::FormD(investcorp_form_d()))],
        ),
    ]);
    let table = "form144_sales_past_3_months";
    assert_eq!(b.column(table, "block_num"), vec![BLOCK_NUM.to_string(); 6]);
    assert_eq!(b.column(table, "filing_index"), ["1"; 6]);
    let table = "form_d_related_persons";
    assert_eq!(
        b.column(table, "block_num"),
        vec![(BLOCK_NUM + 1).to_string(); 3]
    );
    assert_eq!(b.column(table, "filing_index"), ["0"; 3]);
    assert_eq!(
        b.column(table, "block_id"),
        vec![(BLOCK_NUM + 1).to_string(); 3]
    );
}

// ---------------------------------------------------------------------------
// Real data (local only)
// ---------------------------------------------------------------------------

/// The fixture bodies are exact copies of the sample filings.
/// `cargo test -p blocks --lib sec::tests::smallforms::fixture_bodies -- --ignored`
#[test]
#[ignore = "local: needs /tmp/sec-fireparq/fire-v013"]
fn fixture_bodies_equal_the_real_filings() {
    let cases: [(&str, u64, &str, Body); 6] = [
        (
            "2026-03-16",
            2_956_120,
            HALLIBURTON,
            Body::Form144(halliburton_144()),
        ),
        ("2026-03-16", 2_956_154, MOHAWK, Body::Form144(mohawk_144())),
        (
            "2026-03-16",
            2_956_120,
            INVESTCORP,
            Body::FormD(investcorp_form_d()),
        ),
        (
            "2026-03-16",
            2_956_120,
            JS_VENTURE,
            Body::FormD(js_venture_form_d()),
        ),
        (
            "2026-08-14",
            2_977_781,
            KOKOPELLI,
            Body::FormC(kokopelli_form_c()),
        ),
        (
            "2026-08-14",
            2_977_899,
            CLASSIC_COFFEE,
            Body::FormC(classic_coffee_form_c()),
        ),
    ];
    for (day, block_num, accession, body) in cases {
        if !fire::path(day).exists() {
            println!("skipping {day}: no local sample");
            continue;
        }
        let (_, block) = fire::block(day, block_num);
        let real = block
            .filings
            .iter()
            .find(|f| f.accession_number == accession)
            .unwrap_or_else(|| panic!("{accession} not in block {block_num}"));
        assert_eq!(real.body.as_ref(), Some(&body), "{accession}");
    }
}

/// Every column of every row of this group's tables on the four sample days
/// equals the prototype (`proto_map.py`). `form_c_co_issuers.filer_cik` has no
/// oracle value (critic fix C9) and is reported as absent.
/// `cargo test -p blocks --lib sec::tests::smallforms::smallforms_match -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn smallforms_match_the_prototype() {
    oracle::assert_matches(&fire::DAYS, &TABLES, &[]);
}

/// `form_c_co_issuers.filer_cik` (critic fix C9, absent from the oracle) equals
/// the prototype's `filer_cik` of the parent `form_c_notices` row, on every
/// sample co-issuer.
/// `cargo test -p blocks --lib sec::tests::smallforms::form_c_co_issuer_filer_cik -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn form_c_co_issuer_filer_cik_matches_the_prototype_parent() {
    use std::collections::BTreeMap;
    use std::io::BufRead;
    let read = |day: &str, table: &str| -> Vec<serde_json::Value> {
        let path = oracle::dir(day).join(format!("{table}.ndjson"));
        let file = std::fs::File::open(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
        std::io::BufReader::new(file)
            .lines()
            .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
            .collect()
    };
    let mut checked = 0;
    for day in fire::DAYS {
        if !fire::path(day).exists() || !oracle::dir(day).exists() {
            println!("skipping {day}: no local sample");
            continue;
        }
        let key = |row: &serde_json::Value| {
            (
                row["block_num"].as_u64().unwrap(),
                row["filing_index"].as_u64().unwrap(),
            )
        };
        let parents: BTreeMap<(u64, u64), String> = read(day, "form_c_notices")
            .iter()
            .map(|row| {
                (
                    key(row),
                    row["filer_cik"].as_str().unwrap_or("NULL").to_string(),
                )
            })
            .collect();
        let children = read(day, "form_c_co_issuers");
        let blocks: std::collections::BTreeSet<u64> =
            children.iter().map(|row| key(row).0).collect();
        let mut expected: Vec<String> = children
            .iter()
            .map(|row| format!("{:?} {}", key(row), parents[&key(row)]))
            .collect();
        let mut actual = Vec::new();
        for (identity, payload) in
            fire::blocks(day).filter(|(id, _)| blocks.contains(&id.block_num))
        {
            let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
            mapper
                .map_block_bytes(payload.into(), &identity, StreamEvent::default())
                .unwrap();
            let b = Batches::new(mapper.flush().unwrap());
            let table = "form_c_co_issuers";
            for row in 0..b.rows(table) {
                let filing_index: u64 = b.cell(table, "filing_index", row).parse().unwrap();
                actual.push(format!(
                    "{:?} {}",
                    (identity.block_num, filing_index),
                    b.cell(table, "filer_cik", row)
                ));
            }
        }
        expected.sort();
        actual.sort();
        println!("{day}: {} co-issuers: {actual:?}", actual.len());
        assert_eq!(actual, expected, "{day}");
        checked += actual.len();
    }
    println!("{checked} co-issuer rows checked");
}

/// The `parse_issues` rows of this group's tables equal the prototype's on the
/// four sample days (none: the samples have no issue in these tables).
/// `cargo test -p blocks --lib sec::tests::smallforms::smallforms_parse_issues -- --ignored --nocapture`
#[test]
#[ignore = "local: needs the sample FIRE files and the prototype NDJSON"]
fn smallforms_parse_issues_match_the_prototype() {
    use std::io::BufRead;
    for day in fire::DAYS {
        if !fire::path(day).exists() || !oracle::dir(day).exists() {
            println!("skipping {day}: no local sample");
            continue;
        }
        let path = oracle::dir(day).join("parse_issues.ndjson");
        let file = std::fs::File::open(&path).unwrap();
        let expected: Vec<String> = std::io::BufReader::new(file)
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(&line.unwrap()).unwrap())
            .filter(|row| TABLES.contains(&row["table_name"].as_str().unwrap()))
            .map(|row| {
                format!(
                    "{} {} {}.{} [{},{},{}] {} {}",
                    row["block_num"],
                    row["filing_index"],
                    row["table_name"].as_str().unwrap(),
                    row["column_name"].as_str().unwrap(),
                    row["index_1"],
                    row["index_2"],
                    row["index_3"],
                    row["raw_value"].as_str().unwrap(),
                    row["issue"].as_str().unwrap()
                )
            })
            .collect();
        let mut mapper = SecBlockMapper::new(false, EncodeBytes::Hex);
        let mut actual = Vec::new();
        let mut blocks = fire::blocks(day).peekable();
        while let Some((identity, payload)) = blocks.next() {
            mapper
                .map_block_bytes(payload.into(), &identity, StreamEvent::default())
                .unwrap_or_else(|e| panic!("{day} block {}: {e:#}", identity.block_num));
            if mapper.total_rows() < 500_000 && blocks.peek().is_some() {
                continue;
            }
            let b = Batches::new(mapper.flush().unwrap());
            for issue in b.issues() {
                if !TABLES.contains(&issue.table.as_str()) {
                    continue;
                }
                let index: Vec<String> = issue
                    .index
                    .iter()
                    .map(|i| i.map_or_else(|| s("null"), |i| i.to_string()))
                    .collect();
                actual.push(format!(
                    "{} {} {}.{} [{}] {} {}",
                    issue.block_num,
                    issue.filing_index.unwrap(),
                    issue.table,
                    issue.column,
                    index.join(","),
                    issue.raw,
                    issue.issue
                ));
            }
        }
        println!(
            "{day}: {} rust issue rows, {} prototype issue rows in this group's tables",
            actual.len(),
            expected.len()
        );
        assert_eq!(actual, expected, "{day}");
    }
}
