//! Table specs generated from the final specification §3 (`final-spec.md`).
//! Edit only to fix a divergence from the specification; regenerate
//! `docs/schemas/sec.md` and re-pin the chain schema digests afterwards.

#[allow(unused_imports)]
use super::{Col, Family, Member, TableSpec, Ty};

/// §3.19 `form144_notices`.
pub(crate) const FORM144_NOTICES: TableSpec = TableSpec {
    name: super::FORM144_NOTICES,
    doc: "Form 144 notices of proposed sale.",
    filing_context: true,
    cols: &[
        Col::new(
            "filer_cik",
            Ty::Utf8,
            true,
            "`Filing.body.form144.filer_cik`. The seller (≠ filings.cik).",
        ),
        Col::new(
            "issuer_cik",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.issuer_cik`.",
        ),
        Col::new(
            "issuer_name",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.issuer_name`.",
        ),
        Col::new(
            "issuer_sec_file_number",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.sec_file_number`.",
        ),
        Col::new(
            "issuer_street1",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.issuer_address.street1`.",
        ),
        Col::new(
            "issuer_street2",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.issuer_address.street2`.",
        ),
        Col::new(
            "issuer_city",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.issuer_address.city`.",
        ),
        Col::new(
            "issuer_state",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.issuer_address.state`. EDGAR state/country code.",
        ),
        Col::new(
            "issuer_zip_code",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.issuer_address.zip_code`. Text (keeps leading zeros).",
        ),
        Col::new(
            "issuer_state_description",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.issuer_address.state_description`. Filled only by Forms 3/4/5 and D.",
        ),
        Col::new(
            "issuer_country",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.issuer_address.country`.",
        ),
        Col::new(
            "issuer_non_us_state_territory",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.issuer_address.non_us_state_territory`.",
        ),
        Col::new(
            "issuer_contact_phone",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.issuer_contact_phone`.",
        ),
        Col::new(
            "person_for_whose_account",
            Ty::Utf8,
            true,
            "`Filing.body.form144.issuer.person_for_whose_account`. The insider selling.",
        ),
        Col::new(
            "relationships_to_issuer",
            Ty::ListUtf8,
            false,
            "`Filing.body.form144.issuer.relationships_to_issuer[]`: verbatim items in document order; [] when empty. Free text: `Officer`, `Director`, `10% Stockholder`…",
        ),
        Col::new(
            "nothing_sold_past_3_months",
            Ty::Boolean,
            false,
            "`Filing.body.form144.nothing_sold_past_3_months`: proto bool (false = false or absent).",
        ),
        Col::new(
            "remarks",
            Ty::Utf8,
            true,
            "`Filing.body.form144.remarks`.",
        ),
        Col::new(
            "previous_accession_number",
            Ty::Utf8,
            true,
            "`Filing.body.form144.previous_accession_number`. 144/A.",
        ),
        Col::new(
            "notice_date",
            Ty::Date32,
            true,
            "`Filing.body.form144.signature.notice_date`: date (§4.2).",
        ),
        Col::new(
            "signature_text",
            Ty::Utf8,
            true,
            "`Filing.body.form144.signature.signature`. Proto `signature` (renamed: Bloom name).",
        ),
        Col::new(
            "plan_adoption_dates",
            Ty::ListDate32,
            false,
            "`Filing.body.form144.signature.plan_adoption_dates[]`: each item: date (§4.2); [] when empty. Rule 10b5-1 plan adoption date(s).",
        ),
        Col::new(
            "securities_information_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.body.form144.securities_information_entries[]`: rows written to `form144_securities_information` (§3.20).",
        ),
        Col::new(
            "total_units_sold",
            Ty::Decimal(Family::Q6),
            true,
            "Derived: Σ `form144_securities_information.units_sold`, checked; NULL when there is no entry, when any is NULL, or on overflow (+ `overflow` issue). Shares to be sold.",
        ),
        Col::new(
            "total_aggregate_market_value",
            Ty::Decimal(Family::M2),
            true,
            "Derived: Σ `aggregate_market_value`, checked; NULL when there is no entry, when any is NULL, or on overflow (+ `overflow` issue). USD.",
        ),
        Col::new(
            "securities_to_be_sold_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.body.form144.securities_to_be_sold[]`: length of the list.",
        ),
        Col::new(
            "sales_past_3_months_count",
            Ty::UInt32,
            false,
            "Derived from `Filing.body.form144.securities_sold_past_3_months[]`: length of the list.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

/// §3.20 `form144_securities_information`.
pub(crate) const FORM144_SECURITIES_INFORMATION: TableSpec = TableSpec {
    name: super::FORM144_SECURITIES_INFORMATION,
    doc: "The planned sale(s) of a Form 144: one row per class/broker block.",
    filing_context: true,
    cols: &[
        Col::new(
            "issuer_cik",
            Ty::Utf8,
            true,
            "Copy of `form144_notices.issuer_cik`.",
        ),
        Col::new(
            "filer_cik",
            Ty::Utf8,
            true,
            "Copy of `form144_notices.filer_cik`.",
        ),
        Col::new(
            "entry_index",
            Ty::UInt32,
            false,
            "Derived: position in `securities_information_entries` (or 0 for the singular fallback, §3.20).",
        ),
        Col::new(
            "securities_class_title",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_information_entries[].securities_class_title`.",
        ),
        Col::new(
            "broker_name",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_information_entries[].broker.name`.",
        ),
        Col::new(
            "broker_street1",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_information_entries[].broker.address.street1`.",
        ),
        Col::new(
            "broker_street2",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_information_entries[].broker.address.street2`.",
        ),
        Col::new(
            "broker_city",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_information_entries[].broker.address.city`.",
        ),
        Col::new(
            "broker_state",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_information_entries[].broker.address.state`. EDGAR state/country code.",
        ),
        Col::new(
            "broker_zip_code",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_information_entries[].broker.address.zip_code`. Text (keeps leading zeros).",
        ),
        Col::new(
            "broker_state_description",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_information_entries[].broker.address.state_description`. Filled only by Forms 3/4/5 and D.",
        ),
        Col::new(
            "broker_country",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_information_entries[].broker.address.country`.",
        ),
        Col::new(
            "broker_non_us_state_territory",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_information_entries[].broker.address.non_us_state_territory`.",
        ),
        Col::new(
            "units_sold",
            Ty::Decimal(Family::Q6),
            true,
            "`Filing.body.form144.securities_information_entries[].units_sold`: decimal s=6 (§4.3, Q6). Shares TO BE sold.",
        ),
        Col::new(
            "aggregate_market_value",
            Ty::Decimal(Family::M2),
            true,
            "`Filing.body.form144.securities_information_entries[].aggregate_market_value`: decimal s=2 (§4.3, M2). USD.",
        ),
        Col::new(
            "units_outstanding",
            Ty::Decimal(Family::Q6),
            true,
            "`Filing.body.form144.securities_information_entries[].units_outstanding`: decimal s=6 (§4.3, Q6). Issuer shares outstanding.",
        ),
        Col::new(
            "approx_sale_date",
            Ty::Date32,
            true,
            "`Filing.body.form144.securities_information_entries[].approx_sale_date`: date (§4.2).",
        ),
        Col::new(
            "securities_exchange_name",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_information_entries[].securities_exchange_name`.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

/// §3.21 `form144_securities_to_be_sold`.
pub(crate) const FORM144_SECURITIES_TO_BE_SOLD: TableSpec = TableSpec {
    name: super::FORM144_SECURITIES_TO_BE_SOLD,
    doc: "How the seller acquired the securities to be sold.",
    filing_context: true,
    cols: &[
        Col::new(
            "issuer_cik",
            Ty::Utf8,
            true,
            "Copy of `form144_notices.issuer_cik`.",
        ),
        Col::new(
            "filer_cik",
            Ty::Utf8,
            true,
            "Copy of `form144_notices.filer_cik`.",
        ),
        Col::new(
            "lot_index",
            Ty::UInt32,
            false,
            "Derived: position in `securities_to_be_sold`.",
        ),
        Col::new(
            "securities_class_title",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_to_be_sold[].securities_class_title`.",
        ),
        Col::new(
            "acquired_date",
            Ty::Date32,
            true,
            "`Filing.body.form144.securities_to_be_sold[].acquired_date`: date (§4.2).",
        ),
        Col::new(
            "nature_of_acquisition",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_to_be_sold[].nature_of_acquisition`.",
        ),
        Col::new(
            "acquired_from",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_to_be_sold[].acquired_from`.",
        ),
        Col::new(
            "is_gift",
            Ty::Boolean,
            false,
            "`Filing.body.form144.securities_to_be_sold[].is_gift`: proto bool (false = false or absent).",
        ),
        Col::new(
            "donor_acquired_date",
            Ty::Date32,
            true,
            "`Filing.body.form144.securities_to_be_sold[].donor_acquired_date`: date (§4.2). Gifts: when the donor acquired.",
        ),
        Col::new(
            "amount_acquired",
            Ty::Decimal(Family::Q6),
            true,
            "`Filing.body.form144.securities_to_be_sold[].amount_acquired`: decimal s=6 (§4.3, Q6). Shares.",
        ),
        Col::new(
            "payment_date",
            Ty::Date32,
            true,
            "`Filing.body.form144.securities_to_be_sold[].payment_date`: date (§4.2).",
        ),
        Col::new(
            "nature_of_payment",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_to_be_sold[].nature_of_payment`.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};

/// §3.22 `form144_sales_past_3_months`.
pub(crate) const FORM144_SALES_PAST_3_MONTHS: TableSpec = TableSpec {
    name: super::FORM144_SALES_PAST_3_MONTHS,
    doc: "Recent sales disclosed on a Form 144 (repeat across notices: never sum across filings).",
    filing_context: true,
    cols: &[
        Col::new(
            "issuer_cik",
            Ty::Utf8,
            true,
            "Copy of `form144_notices.issuer_cik`.",
        ),
        Col::new(
            "filer_cik",
            Ty::Utf8,
            true,
            "Copy of `form144_notices.filer_cik`.",
        ),
        Col::new(
            "sale_index",
            Ty::UInt32,
            false,
            "Derived: position in `securities_sold_past_3_months`.",
        ),
        Col::new(
            "seller_name",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].seller_name`.",
        ),
        Col::new(
            "seller_street1",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].seller_address.street1`.",
        ),
        Col::new(
            "seller_street2",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].seller_address.street2`.",
        ),
        Col::new(
            "seller_city",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].seller_address.city`.",
        ),
        Col::new(
            "seller_state",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].seller_address.state`. EDGAR state/country code.",
        ),
        Col::new(
            "seller_zip_code",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].seller_address.zip_code`. Text (keeps leading zeros).",
        ),
        Col::new(
            "seller_state_description",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].seller_address.state_description`. Filled only by Forms 3/4/5 and D.",
        ),
        Col::new(
            "seller_country",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].seller_address.country`.",
        ),
        Col::new(
            "seller_non_us_state_territory",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].seller_address.non_us_state_territory`.",
        ),
        Col::new(
            "securities_class_title",
            Ty::Utf8,
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].securities_class_title`.",
        ),
        Col::new(
            "sale_date",
            Ty::Date32,
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].sale_date`: date (§4.2).",
        ),
        Col::new(
            "amount_sold",
            Ty::Decimal(Family::Q6),
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].amount_sold`: decimal s=6 (§4.3, Q6). Shares.",
        ),
        Col::new(
            "gross_proceeds",
            Ty::Decimal(Family::M2),
            true,
            "`Filing.body.form144.securities_sold_past_3_months[].gross_proceeds`: decimal s=2 (§4.3, M2). USD.",
        ),
        Col::new(
            "has_parse_issues",
            Ty::Boolean,
            false,
            "Derived: true iff this row wrote ≥1 `parse_issues` row (§4.6).",
        ),
    ],
};
