//! Preflight of `form13f_reports`, `form13f_other_managers`, `form13f_holdings` (§3.14, §3.15, §3.16).
//! Owned by the `form13f-beneficial` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.
//!
//! Issue order (the reference's emit order): the report row, the other managers
//! (cover page, then summary page), then the holdings. The report's
//! `holdings_value_sum` needs the holdings' typed `value`s before their rows are
//! recorded, so each `value` is parsed once up front and its issue, if any, is
//! recorded later on its own holding row (`RowIssues::note`).

use anyhow::Result;

use super::{idx, FilingCtx};
use crate::sec::issues::IssueSink;
use crate::sec::parse::{self, IssueKind, Parsed};
use crate::sec::proto::sec;
use crate::sec::schema::{FORM13F_HOLDINGS, FORM13F_OTHER_MANAGERS, FORM13F_REPORTS};

/// `form13f_other_managers.list_kind` of a cover-page manager.
pub(crate) const LIST_KIND_COVER: &str = "cover";
/// `form13f_other_managers.list_kind` of a summary-page manager.
pub(crate) const LIST_KIND_SUMMARY: &str = "summary";

/// `value_multiplier_rule` before the SEC cutover (§4.4): values in thousands.
pub(crate) const MULTIPLIER_THOUSANDS: i32 = 1000;
/// `value_multiplier_rule` on and after the cutover: values in dollars.
pub(crate) const MULTIPLIER_DOLLARS: i32 = 1;

/// The parsed and derived values of `form13f_reports`, `form13f_other_managers`, `form13f_holdings` for one filing.
#[derive(Debug)]
pub(crate) struct PreparedForm13f<'a> {
    pub report: PreparedReport,
    /// Cover-page managers, then summary-page managers (§8.2).
    pub other_managers: Vec<PreparedOtherManager<'a>>,
    /// One per `holdings[]` element, in order.
    pub holdings: Vec<PreparedHolding>,
}

/// The typed and derived values of the `form13f_reports` row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparedReport {
    pub period_of_report: Option<i32>,
    pub amendment_number: Option<i32>,
    pub provide_info_for_instruction5: Option<bool>,
    pub table_value_total: Option<i64>,
    /// 1000 if `filing_date` (else the `date` partition) < 2023-01-03, else 1.
    pub value_multiplier_rule: i32,
    pub holdings_count: u32,
    /// Checked `i64` Σ of the typed `value`s; NULL without holdings, when any
    /// `value` is NULL, or on overflow.
    pub holdings_value_sum: Option<i64>,
    /// `holdings_count = table_entry_total`; NULL without a summary page.
    pub holdings_complete: Option<bool>,
    pub cover_other_manager_count: u32,
    pub summary_other_manager_count: u32,
    pub has_parse_issues: bool,
}

/// One `form13f_other_managers` row.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PreparedOtherManager<'a> {
    pub manager: &'a sec::OtherManager,
    /// [`LIST_KIND_COVER`] or [`LIST_KIND_SUMMARY`].
    pub list_kind: &'static str,
    pub other_manager_index: u32,
    pub sequence_number: Option<i32>,
    pub has_parse_issues: bool,
}

/// The typed values of one `form13f_holdings` row. The derived join keys
/// (`cusip_norm`, `put_call_norm`, `other_manager_sequence_numbers`) never log
/// an issue and are computed from the proto during the append.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparedHolding {
    pub holding_index: u32,
    pub value: Option<i64>,
    pub shares_or_principal_amount: Option<i64>,
    pub voting_authority_sole: Option<i64>,
    pub voting_authority_shared: Option<i64>,
    pub voting_authority_none: Option<i64>,
    pub has_parse_issues: bool,
}

/// The SEC 13F cutover (§4.4): every 13F filed on or after 2023-01-03 reports
/// dollars.
pub(crate) fn cutover_date() -> Option<i32> {
    parse::date32(2023, 1, 3)
}

/// `value_multiplier_rule` (§3.14): `filing_date`, else the `date` partition,
/// against 2023-01-03.
pub(crate) fn value_multiplier_rule(fc: &FilingCtx<'_>) -> i32 {
    let date = fc.filing_date.unwrap_or(fc.block_date);
    if cutover_date().is_some_and(|cutover| date < cutover) {
        MULTIPLIER_THOUSANDS
    } else {
        MULTIPLIER_DOLLARS
    }
}

/// The parsed `value` of every holding (`None` for `""`, which is NULL with no
/// issue).
fn parse_values(holdings: &[sec::InfoTableEntry]) -> Vec<Option<Parsed<i64>>> {
    holdings
        .iter()
        .map(|holding| {
            let raw = holding.value.as_str();
            (!raw.is_empty()).then(|| parse::parse_int::<i64>(raw))
        })
        .collect()
}

/// `holdings_value_sum` (§4.3): `None` when there is no holding or any value is
/// NULL; `Some(None)` when the checked `i64` sum overflows; else `Some(sum)`.
fn checked_sum(values: &[Option<Parsed<i64>>]) -> Option<Option<i64>> {
    if values.is_empty() {
        return None;
    }
    let mut sum = Some(0i64);
    for value in values {
        let value = (*value)?.value?;
        sum = sum.and_then(|total| total.checked_add(value));
    }
    Some(sum)
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::Form13fReport,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedForm13f<'a>> {
    let cover = body.cover_page.as_ref();
    let summary = body.summary_page.as_ref();
    let cover_managers = cover.map_or(&[][..], |c| c.other_managers.as_slice());
    let summary_managers = summary.map_or(&[][..], |s| s.other_managers.as_slice());
    let holdings_count = idx(body.holdings.len())?;
    let cover_other_manager_count = idx(cover_managers.len())?;
    let summary_other_manager_count = idx(summary_managers.len())?;
    let values = parse_values(&body.holdings);

    // The report row: parsed columns in schema order, then the derived overflow.
    let mut row = issues.row(FORM13F_REPORTS, &[]);
    let period_of_report = row.date(
        "period_of_report",
        cover.map_or("", |c| &c.period_of_report),
    );
    let amendment_number = row.int::<i32>(
        "amendment_number",
        cover.map_or("", |c| &c.amendment_number),
    );
    let provide_info_for_instruction5 = row.yn(
        "provide_info_for_instruction5",
        cover.map_or("", |c| &c.provide_info_for_instruction5),
    );
    let table_value_total = row.int::<i64>(
        "table_value_total",
        summary.map_or("", |s| &s.table_value_total),
    );
    let holdings_value_sum = match checked_sum(&values) {
        Some(None) => {
            let operands = body
                .holdings
                .iter()
                .map(|holding| holding.value.as_str())
                .collect::<Vec<_>>()
                .join(" + ");
            row.record("holdings_value_sum", None, operands, IssueKind::Overflow);
            None
        }
        sum => sum.flatten(),
    };
    let report = PreparedReport {
        period_of_report,
        amendment_number,
        provide_info_for_instruction5,
        table_value_total,
        value_multiplier_rule: value_multiplier_rule(fc),
        holdings_count,
        holdings_value_sum,
        holdings_complete: summary.map(|s| holdings_count == s.table_entry_total),
        cover_other_manager_count,
        summary_other_manager_count,
        has_parse_issues: row.finish(),
    };

    // Other managers: cover page, then summary page, one position sequence.
    let managers = cover_managers
        .iter()
        .map(|manager| (LIST_KIND_COVER, manager))
        .chain(
            summary_managers
                .iter()
                .map(|manager| (LIST_KIND_SUMMARY, manager)),
        );
    let mut other_managers = Vec::with_capacity(cover_managers.len() + summary_managers.len());
    for (position, (list_kind, manager)) in managers.enumerate() {
        let other_manager_index = idx(position)?;
        let mut row = issues.row(FORM13F_OTHER_MANAGERS, &[other_manager_index]);
        let sequence_number = row.int::<i32>("sequence_number", &manager.sequence_number);
        other_managers.push(PreparedOtherManager {
            manager,
            list_kind,
            other_manager_index,
            sequence_number,
            has_parse_issues: row.finish(),
        });
    }

    // Holdings, in XML order.
    let mut holdings = Vec::with_capacity(body.holdings.len());
    for (position, (holding, value)) in body.holdings.iter().zip(values).enumerate() {
        let holding_index = idx(position)?;
        let amount = holding.shares_or_principal.as_ref();
        let voting = holding.voting_authority.as_ref();
        let mut row = issues.row(FORM13F_HOLDINGS, &[holding_index]);
        let value = value.and_then(|parsed| row.note("value", None, &holding.value, parsed));
        let shares_or_principal_amount = row.int::<i64>(
            "shares_or_principal_amount",
            amount.map_or("", |a| &a.amount),
        );
        let voting_authority_sole =
            row.int::<i64>("voting_authority_sole", voting.map_or("", |v| &v.sole));
        let voting_authority_shared =
            row.int::<i64>("voting_authority_shared", voting.map_or("", |v| &v.shared));
        let voting_authority_none =
            row.int::<i64>("voting_authority_none", voting.map_or("", |v| &v.none));
        holdings.push(PreparedHolding {
            holding_index,
            value,
            shares_or_principal_amount,
            voting_authority_sole,
            voting_authority_shared,
            voting_authority_none,
            has_parse_issues: row.finish(),
        });
    }

    Ok(PreparedForm13f {
        report,
        other_managers,
        holdings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(values: &[&str]) -> Vec<Option<Parsed<i64>>> {
        values
            .iter()
            .map(|v| (!v.is_empty()).then(|| parse::parse_int::<i64>(v)))
            .collect()
    }

    #[test]
    fn cutover_is_2023_01_03() {
        assert_eq!(cutover_date(), Some(19_360));
        assert_eq!(parse::iso_date(19_360).as_deref(), Some("2023-01-03"));
    }

    #[test]
    fn checked_sum_rules() {
        assert_eq!(checked_sum(&parsed(&[])), None);
        assert_eq!(checked_sum(&parsed(&["1", "2"])), Some(Some(3)));
        assert_eq!(checked_sum(&parsed(&["0"])), Some(Some(0)));
        assert_eq!(checked_sum(&parsed(&["1", ""])), None);
        assert_eq!(checked_sum(&parsed(&["1", "N/A"])), None);
        assert_eq!(
            checked_sum(&parsed(&["9223372036854775807", "1"])),
            Some(None)
        );
        assert_eq!(
            checked_sum(&parsed(&["-9223372036854775808", "-1"])),
            Some(None)
        );
        // A NULL value makes the sum NULL even after an overflow: no issue.
        assert_eq!(
            checked_sum(&parsed(&["9223372036854775807", "1", "x"])),
            None
        );
    }
}
