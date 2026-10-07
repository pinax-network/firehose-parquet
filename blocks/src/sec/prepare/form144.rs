//! Preflight of `form144_notices`, `form144_securities_information`, `form144_securities_to_be_sold`, `form144_sales_past_3_months` (§3.19, §3.20, §3.21, §3.22).
//! Owned by the `smallforms` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.
//!
//! Issue order (§4.6, `proto_map.py`): the notice (`notice_date`, each
//! `plan_adoption_dates` element, then the derived `overflow` of the two
//! totals), then the securities information entries, the lots to be sold and
//! the sales of the past 3 months, each in position order.

use anyhow::Result;

use super::idx;
use crate::sec::issues::{IssueSink, RowIssues};
use crate::sec::parse::{self, Family, IssueKind};
use crate::sec::prepare::FilingCtx;
use crate::sec::proto::sec;
use crate::sec::schema;

/// The parsed and derived values of `form144_notices`, `form144_securities_information`, `form144_securities_to_be_sold`, `form144_sales_past_3_months` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedForm144<'a> {
    /// Boxed: keeps `PreparedBody` variants small.
    pub notice: Box<PreparedNotice>,
    /// The source of the `form144_securities_information` rows: the
    /// `securities_information_entries`, or the singular
    /// `securities_information` as entry 0 when the list is empty (§3.20
    /// fallback, pre-0.13 blocks). Never both.
    pub entries: &'a [sec::SecuritiesInformation],
    /// One per element of [`Self::entries`], in order.
    pub information: Vec<PreparedInformation>,
    /// One per `securities_to_be_sold` element, in order.
    pub lots: Vec<PreparedLot>,
    /// One per `securities_sold_past_3_months` element, in order.
    pub sales: Vec<PreparedSale>,
}

/// The typed and derived values of the `form144_notices` row.
#[derive(Debug, Default)]
pub(crate) struct PreparedNotice {
    pub notice_date: Option<i32>,
    /// One item per `signature.plan_adoption_dates` element (NULL when it does
    /// not parse); `[]` without a signature.
    pub plan_adoption_dates: Vec<Option<i32>>,
    pub securities_information_count: u32,
    /// Σ `units_sold` (Q6 mantissa); NULL when there is no entry, when any is
    /// NULL, or on overflow.
    pub total_units_sold: Option<i128>,
    /// Σ `aggregate_market_value` (M2 mantissa), same rule.
    pub total_aggregate_market_value: Option<i128>,
    pub securities_to_be_sold_count: u32,
    pub sales_past_3_months_count: u32,
    pub has_parse_issues: bool,
}

/// The typed values of one `form144_securities_information` row.
#[derive(Debug, Default)]
pub(crate) struct PreparedInformation {
    pub entry_index: u32,
    pub units_sold: Option<i128>,
    pub aggregate_market_value: Option<i128>,
    pub units_outstanding: Option<i128>,
    pub approx_sale_date: Option<i32>,
    pub has_parse_issues: bool,
}

/// The typed values of one `form144_securities_to_be_sold` row.
#[derive(Debug, Default)]
pub(crate) struct PreparedLot {
    pub lot_index: u32,
    pub acquired_date: Option<i32>,
    pub donor_acquired_date: Option<i32>,
    pub amount_acquired: Option<i128>,
    pub payment_date: Option<i32>,
    pub has_parse_issues: bool,
}

/// The typed values of one `form144_sales_past_3_months` row.
#[derive(Debug, Default)]
pub(crate) struct PreparedSale {
    pub sale_index: u32,
    pub sale_date: Option<i32>,
    pub amount_sold: Option<i128>,
    pub gross_proceeds: Option<i128>,
    pub has_parse_issues: bool,
}

/// The source of the `form144_securities_information` rows (§3.20 fallback):
/// the entries, else the singular message as entry 0, else nothing.
pub(crate) fn securities_information_entries(
    body: &sec::Form144Notice,
) -> &[sec::SecuritiesInformation] {
    match &body.securities_information {
        Some(single) if body.securities_information_entries.is_empty() => {
            std::slice::from_ref(single)
        }
        _ => &body.securities_information_entries,
    }
}

/// A checked notice total (§4.3 derived arithmetic).
#[derive(Debug, PartialEq, Eq)]
enum Total {
    /// The sum, or NULL when there is no operand or an operand is NULL.
    Value(Option<i128>),
    /// The sum overflowed `i128` or 38 digits: NULL plus an `overflow` issue.
    Overflow,
}

/// Σ of the typed values of `raws`, each parsed exactly as its column is
/// (`""` is NULL, a rounded value counts rounded), checked in `i128` and then
/// against the 38-digit precision. A NULL operand makes the sum NULL without
/// an issue, whatever the other operands are.
fn checked_total<'r>(raws: impl IntoIterator<Item = &'r str>, family: Family) -> Total {
    let mut sum: Option<i128> = None;
    let mut overflow = false;
    for raw in raws {
        let value = if raw.is_empty() {
            None
        } else {
            parse::parse_decimal(raw, family.scale()).value
        };
        let Some(value) = value else {
            return Total::Value(None);
        };
        match sum.unwrap_or(0).checked_add(value) {
            Some(next) => sum = Some(next),
            None => overflow = true,
        }
    }
    match sum {
        _ if overflow => Total::Overflow,
        Some(sum) if !parse::fits_precision(sum) => Total::Overflow,
        sum => Total::Value(sum),
    }
}

/// A notice total, recording its `overflow` issue (`raw_value` = the operands'
/// source strings joined with ` + `, in source order).
fn notice_total<'a>(
    row: &mut RowIssues<'_, 'a>,
    column: &'static str,
    raws: &[&'a str],
    family: Family,
) -> Option<i128> {
    match checked_total(raws.iter().copied(), family) {
        Total::Value(value) => value,
        Total::Overflow => {
            row.record(column, None, raws.join(" + "), IssueKind::Overflow);
            None
        }
    }
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::Form144Notice,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedForm144<'a>> {
    let _ = fc;
    let entries = securities_information_entries(body);

    // form144_notices: parsed columns in schema order, then the derived totals.
    let mut row = issues.row(schema::FORM144_NOTICES, &[]);
    let signature = body.signature.as_ref();
    let notice_date = row.date(
        "notice_date",
        signature.map_or("", |s| s.notice_date.as_str()),
    );
    let raw_dates = signature.map_or(&[][..], |s| s.plan_adoption_dates.as_slice());
    let mut plan_adoption_dates = Vec::with_capacity(raw_dates.len());
    for (element, raw) in raw_dates.iter().enumerate() {
        plan_adoption_dates.push(row.date_item("plan_adoption_dates", idx(element)?, raw));
    }
    let units: Vec<&'a str> = entries.iter().map(|e| e.units_sold.as_str()).collect();
    let total_units_sold = notice_total(&mut row, "total_units_sold", &units, Family::Q6);
    let values: Vec<&'a str> = entries
        .iter()
        .map(|e| e.aggregate_market_value.as_str())
        .collect();
    let total_aggregate_market_value = notice_total(
        &mut row,
        "total_aggregate_market_value",
        &values,
        Family::M2,
    );
    let notice = Box::new(PreparedNotice {
        notice_date,
        plan_adoption_dates,
        securities_information_count: idx(entries.len())?,
        total_units_sold,
        total_aggregate_market_value,
        securities_to_be_sold_count: idx(body.securities_to_be_sold.len())?,
        sales_past_3_months_count: idx(body.securities_sold_past_3_months.len())?,
        has_parse_issues: row.finish(),
    });

    // form144_securities_information
    let mut information = Vec::with_capacity(entries.len());
    for (position, entry) in entries.iter().enumerate() {
        let entry_index = idx(position)?;
        let mut row = issues.row(schema::FORM144_SECURITIES_INFORMATION, &[entry_index]);
        information.push(PreparedInformation {
            entry_index,
            units_sold: row.decimal("units_sold", &entry.units_sold, Family::Q6),
            aggregate_market_value: row.decimal(
                "aggregate_market_value",
                &entry.aggregate_market_value,
                Family::M2,
            ),
            units_outstanding: row.decimal(
                "units_outstanding",
                &entry.units_outstanding,
                Family::Q6,
            ),
            approx_sale_date: row.date("approx_sale_date", &entry.approx_sale_date),
            has_parse_issues: row.finish(),
        });
    }

    // form144_securities_to_be_sold
    let mut lots = Vec::with_capacity(body.securities_to_be_sold.len());
    for (position, lot) in body.securities_to_be_sold.iter().enumerate() {
        let lot_index = idx(position)?;
        let mut row = issues.row(schema::FORM144_SECURITIES_TO_BE_SOLD, &[lot_index]);
        lots.push(PreparedLot {
            lot_index,
            acquired_date: row.date("acquired_date", &lot.acquired_date),
            donor_acquired_date: row.date("donor_acquired_date", &lot.donor_acquired_date),
            amount_acquired: row.decimal("amount_acquired", &lot.amount_acquired, Family::Q6),
            payment_date: row.date("payment_date", &lot.payment_date),
            has_parse_issues: row.finish(),
        });
    }

    // form144_sales_past_3_months
    let mut sales = Vec::with_capacity(body.securities_sold_past_3_months.len());
    for (position, sale) in body.securities_sold_past_3_months.iter().enumerate() {
        let sale_index = idx(position)?;
        let mut row = issues.row(schema::FORM144_SALES_PAST_3_MONTHS, &[sale_index]);
        sales.push(PreparedSale {
            sale_index,
            sale_date: row.date("sale_date", &sale.sale_date),
            amount_sold: row.decimal("amount_sold", &sale.amount_sold, Family::Q6),
            gross_proceeds: row.decimal("gross_proceeds", &sale.gross_proceeds, Family::M2),
            has_parse_issues: row.finish(),
        });
    }

    Ok(PreparedForm144 {
        notice,
        entries,
        information,
        lots,
        sales,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `10^38 − 1` at scale 2: the largest M2 value.
    const M2_MAX: &str = "999999999999999999999999999999999999.99";

    #[test]
    fn checked_total_is_null_without_operands_or_with_a_null_one() {
        assert_eq!(checked_total([], Family::Q6), Total::Value(None));
        assert_eq!(checked_total(["1", ""], Family::Q6), Total::Value(None));
        assert_eq!(checked_total(["N/A", "1"], Family::Q6), Total::Value(None));
        assert_eq!(
            checked_total(["1.5", "2.25"], Family::M2),
            Total::Value(Some(375))
        );
        // Rounded operands count as their typed (rounded) values.
        assert_eq!(
            checked_total(["0.125", "0.125"], Family::M2),
            Total::Value(Some(26))
        );
    }

    #[test]
    fn checked_total_overflows_beyond_38_digits_or_i128() {
        assert_eq!(
            checked_total([M2_MAX], Family::M2),
            Total::Value(Some(parse::DECIMAL_LIMIT - 1))
        );
        // Beyond 38 digits, within i128.
        assert_eq!(checked_total([M2_MAX, "0.01"], Family::M2), Total::Overflow);
        // An intermediate sum beyond i128 (2 × 10^38 > i128::MAX).
        assert_eq!(
            checked_total([M2_MAX, M2_MAX, "-1"], Family::M2),
            Total::Overflow
        );
        // A NULL operand wins over an overflow.
        assert_eq!(
            checked_total([M2_MAX, M2_MAX, ""], Family::M2),
            Total::Value(None)
        );
        // Only the final sum is range-checked.
        assert_eq!(
            checked_total([M2_MAX, "0.01", "-0.01"], Family::M2),
            Total::Value(Some(parse::DECIMAL_LIMIT - 1))
        );
    }
}
