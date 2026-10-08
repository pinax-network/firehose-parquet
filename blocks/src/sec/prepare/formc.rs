//! Preflight of `form_c_notices`, `form_c_co_issuers` (§3.41, §3.42).
//! Owned by the `smallforms` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.
//!
//! Issue order (§4.6, `proto_map.py`): the notice, then the co-issuers in
//! position order.

use anyhow::Result;

use super::idx;
use crate::sec::issues::IssueSink;
use crate::sec::parse::Family;
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

/// The 18 financial-statement columns of `form_c_notices` (all M2), in
/// schema order; [`financial_values`] gives their sources in the same order.
pub(crate) const FINANCIAL_COLUMNS: [&str; 18] = [
    "total_assets_most_recent_fy",
    "total_assets_prior_fy",
    "cash_equivalents_most_recent_fy",
    "cash_equivalents_prior_fy",
    "accounts_receivable_most_recent_fy",
    "accounts_receivable_prior_fy",
    "short_term_debt_most_recent_fy",
    "short_term_debt_prior_fy",
    "long_term_debt_most_recent_fy",
    "long_term_debt_prior_fy",
    "revenue_most_recent_fy",
    "revenue_prior_fy",
    "cost_goods_sold_most_recent_fy",
    "cost_goods_sold_prior_fy",
    "tax_paid_most_recent_fy",
    "tax_paid_prior_fy",
    "net_income_most_recent_fy",
    "net_income_prior_fy",
];

/// The source strings of [`FINANCIAL_COLUMNS`], in the same order.
pub(crate) fn financial_values(f: &sec::FormCFinancials) -> [&str; 18] {
    [
        &f.total_assets_most_recent_fy,
        &f.total_assets_prior_fy,
        &f.cash_equivalents_most_recent_fy,
        &f.cash_equivalents_prior_fy,
        &f.accounts_receivable_most_recent_fy,
        &f.accounts_receivable_prior_fy,
        &f.short_term_debt_most_recent_fy,
        &f.short_term_debt_prior_fy,
        &f.long_term_debt_most_recent_fy,
        &f.long_term_debt_prior_fy,
        &f.revenue_most_recent_fy,
        &f.revenue_prior_fy,
        &f.cost_goods_sold_most_recent_fy,
        &f.cost_goods_sold_prior_fy,
        &f.tax_paid_most_recent_fy,
        &f.tax_paid_prior_fy,
        &f.net_income_most_recent_fy,
        &f.net_income_prior_fy,
    ]
}

/// The parsed and derived values of `form_c_notices`, `form_c_co_issuers` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedFormC<'a> {
    /// Boxed: keeps `PreparedBody` variants small.
    pub notice: Box<PreparedNotice>,
    /// One per `co_issuers` element, in order.
    pub co_issuers: Vec<PreparedCoIssuer>,
    _borrows: std::marker::PhantomData<&'a ()>,
}

/// The typed values of the `form_c_notices` row.
#[derive(Debug, Default)]
pub(crate) struct PreparedNotice {
    pub issuer_date_incorporation: Option<i32>,
    pub period: Option<i32>,
    pub num_securities_offered: Option<i128>,
    pub price: Option<i128>,
    pub offering_amount: Option<i128>,
    pub maximum_offering_amount: Option<i128>,
    pub deadline_date: Option<i32>,
    pub current_employees: Option<i64>,
    /// M2 mantissas of [`FINANCIAL_COLUMNS`], in order; all NULL without
    /// `financials`.
    pub financials: [Option<i128>; 18],
    pub co_issuer_count: u32,
    pub has_parse_issues: bool,
}

/// The typed values of one `form_c_co_issuers` row.
#[derive(Debug, Default)]
pub(crate) struct PreparedCoIssuer {
    pub co_issuer_index: u32,
    pub date_incorporation: Option<i32>,
    pub has_parse_issues: bool,
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::FormCNotice,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedFormC<'a>> {
    let _ = fc;
    let issuer = body.issuer.as_ref();
    let offering = body.offering.as_ref();
    let of = |field: fn(&'a sec::FormCOffering) -> &'a str| offering.map_or("", field);

    // form_c_notices: parsed columns in schema order.
    let mut row = issues.row(schema::FORM_C_NOTICES, &[]);
    let issuer_date_incorporation = row.date(
        "issuer_date_incorporation",
        issuer.map_or("", |i| i.date_incorporation.as_str()),
    );
    let period = row.date("period", &body.period);
    let num_securities_offered = row.decimal(
        "num_securities_offered",
        of(|o| o.num_securities_offered.as_str()),
        Family::Q6,
    );
    let price = row.decimal("price", of(|o| o.price.as_str()), Family::Q6);
    let offering_amount = row.decimal(
        "offering_amount",
        of(|o| o.offering_amount.as_str()),
        Family::M2,
    );
    let maximum_offering_amount = row.decimal(
        "maximum_offering_amount",
        of(|o| o.maximum_offering_amount.as_str()),
        Family::M2,
    );
    let deadline_date = row.date("deadline_date", of(|o| o.deadline_date.as_str()));
    let financials = body.financials.as_ref();
    let current_employees = row.int::<i64>(
        "current_employees",
        financials.map_or("", |f| f.current_employees.as_str()),
    );
    let mut values = [None; 18];
    if let Some(financials) = financials {
        for ((value, column), raw) in values
            .iter_mut()
            .zip(FINANCIAL_COLUMNS)
            .zip(financial_values(financials))
        {
            *value = row.decimal(column, raw, Family::M2);
        }
    }
    let notice = Box::new(PreparedNotice {
        issuer_date_incorporation,
        period,
        num_securities_offered,
        price,
        offering_amount,
        maximum_offering_amount,
        deadline_date,
        current_employees,
        financials: values,
        co_issuer_count: idx(body.co_issuers.len())?,
        has_parse_issues: row.finish(),
    });

    // form_c_co_issuers
    let mut co_issuers = Vec::with_capacity(body.co_issuers.len());
    for (position, co_issuer) in body.co_issuers.iter().enumerate() {
        let co_issuer_index = idx(position)?;
        let mut row = issues.row(schema::FORM_C_CO_ISSUERS, &[co_issuer_index]);
        co_issuers.push(PreparedCoIssuer {
            co_issuer_index,
            date_incorporation: row.date("date_incorporation", &co_issuer.date_incorporation),
            has_parse_issues: row.finish(),
        });
    }

    Ok(PreparedFormC {
        notice,
        co_issuers,
        _borrows: std::marker::PhantomData,
    })
}
