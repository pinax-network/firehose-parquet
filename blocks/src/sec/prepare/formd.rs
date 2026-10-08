//! Preflight of `form_d_notices`, `form_d_co_issuers`, `form_d_related_persons`, `form_d_sales_recipients` (§3.32, §3.33, §3.34, §3.35).
//! Owned by the `smallforms` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.
//!
//! Issue order (§4.6, `proto_map.py`): the notice, then the co-issuers in
//! position order. Related persons and sales recipients have no typed column.
//! `Indefinite` amounts are not issues: they set the `*_is_indefinite` flag.

use anyhow::Result;

use super::idx;
use crate::sec::issues::{IssueSink, RowIssues};
use crate::sec::parse::{self, Family};
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

/// The parsed and derived values of `form_d_notices`, `form_d_co_issuers`, `form_d_related_persons`, `form_d_sales_recipients` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedFormD<'a> {
    /// Boxed: keeps `PreparedBody` variants small.
    pub notice: Box<PreparedNotice>,
    /// One per `issuers` element, in order.
    pub co_issuers: Vec<PreparedCoIssuer>,
    _borrows: std::marker::PhantomData<&'a ()>,
}

/// The typed and derived values of the `form_d_notices` row. Every offering
/// value is NULL (and both `*_is_indefinite` flags false) without `offering`.
#[derive(Debug, Default)]
pub(crate) struct PreparedNotice {
    pub year_of_inc: Option<i32>,
    pub date_of_first_sale: Option<i32>,
    pub minimum_investment: Option<i128>,
    pub total_offering_amount: Option<i128>,
    pub total_offering_amount_is_indefinite: bool,
    pub total_amount_sold: Option<i128>,
    pub total_remaining: Option<i128>,
    pub total_remaining_is_indefinite: bool,
    pub number_non_accredited_investors: Option<i64>,
    pub total_number_already_invested: Option<i64>,
    pub sales_commissions: Option<i128>,
    pub finders_fees: Option<i128>,
    pub gross_proceeds_used: Option<i128>,
    pub co_issuer_count: u32,
    /// The length of `related_persons`; their positions are `0..count`.
    pub related_person_count: u32,
    /// The length of `sales_compensation_recipients`; their positions are
    /// `0..count`.
    pub sales_recipient_count: u32,
    pub has_parse_issues: bool,
}

/// The typed values of one `form_d_co_issuers` row.
#[derive(Debug, Default)]
pub(crate) struct PreparedCoIssuer {
    pub co_issuer_index: u32,
    pub year_of_inc: Option<i32>,
    pub has_parse_issues: bool,
}

/// An M2 amount that may be `Indefinite` (§4.3): `Indefinite`
/// (case-insensitive, trimmed) is NULL with the flag set and no issue;
/// anything else parses as a decimal.
fn amount_or_indefinite<'a>(
    row: &mut RowIssues<'_, 'a>,
    column: &'static str,
    raw: &'a str,
) -> (Option<i128>, bool) {
    if parse::is_indefinite(raw) {
        (None, true)
    } else {
        (row.decimal(column, raw, Family::M2), false)
    }
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::FormDNotice,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedFormD<'a>> {
    let _ = fc;
    let issuer = body.primary_issuer.as_ref();
    let offering = body.offering.as_ref();
    let of = |field: fn(&'a sec::OfferingData) -> &'a str| offering.map_or("", field);

    // form_d_notices: parsed columns in schema order.
    let mut row = issues.row(schema::FORM_D_NOTICES, &[]);
    let year_of_inc = row.int::<i32>("year_of_inc", issuer.map_or("", |i| i.year_of_inc.as_str()));
    let date_of_first_sale = row.date("date_of_first_sale", of(|o| o.date_of_first_sale.as_str()));
    let minimum_investment = row.decimal(
        "minimum_investment",
        of(|o| o.minimum_investment.as_str()),
        Family::M2,
    );
    let (total_offering_amount, total_offering_amount_is_indefinite) = amount_or_indefinite(
        &mut row,
        "total_offering_amount",
        of(|o| o.total_offering_amount.as_str()),
    );
    let total_amount_sold = row.decimal(
        "total_amount_sold",
        of(|o| o.total_amount_sold.as_str()),
        Family::M2,
    );
    let (total_remaining, total_remaining_is_indefinite) = amount_or_indefinite(
        &mut row,
        "total_remaining",
        of(|o| o.total_remaining.as_str()),
    );
    let number_non_accredited_investors = row.int::<i64>(
        "number_non_accredited_investors",
        of(|o| o.number_non_accredited_investors.as_str()),
    );
    let total_number_already_invested = row.int::<i64>(
        "total_number_already_invested",
        of(|o| o.total_already_invested.as_str()),
    );
    let sales_commissions = row.decimal(
        "sales_commissions",
        of(|o| o.sales_commissions.as_str()),
        Family::M2,
    );
    let finders_fees = row.decimal("finders_fees", of(|o| o.finders_fees.as_str()), Family::M2);
    let gross_proceeds_used = row.decimal(
        "gross_proceeds_used",
        of(|o| o.gross_proceeds_used.as_str()),
        Family::M2,
    );
    let notice = Box::new(PreparedNotice {
        year_of_inc,
        date_of_first_sale,
        minimum_investment,
        total_offering_amount,
        total_offering_amount_is_indefinite,
        total_amount_sold,
        total_remaining,
        total_remaining_is_indefinite,
        number_non_accredited_investors,
        total_number_already_invested,
        sales_commissions,
        finders_fees,
        gross_proceeds_used,
        co_issuer_count: idx(body.issuers.len())?,
        related_person_count: idx(body.related_persons.len())?,
        sales_recipient_count: idx(body.sales_compensation_recipients.len())?,
        has_parse_issues: row.finish(),
    });

    // form_d_co_issuers
    let mut co_issuers = Vec::with_capacity(body.issuers.len());
    for (position, co_issuer) in body.issuers.iter().enumerate() {
        let co_issuer_index = idx(position)?;
        let mut row = issues.row(schema::FORM_D_CO_ISSUERS, &[co_issuer_index]);
        co_issuers.push(PreparedCoIssuer {
            co_issuer_index,
            year_of_inc: row.int::<i32>("year_of_inc", &co_issuer.year_of_inc),
            has_parse_issues: row.finish(),
        });
    }

    Ok(PreparedFormD {
        notice,
        co_issuers,
        _borrows: std::marker::PhantomData,
    })
}
