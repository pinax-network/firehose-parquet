//! Preflight of `npx_reports`, `npx_votes`, `npx_vote_records`, `npx_other_managers` (§3.36, §3.37, §3.38, §3.39).
//! Owned by the `funds` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.
//!
//! N-PX is the volume hot spot of the SEC chain (about 5 M votes and 5 M vote
//! records per deadline day, 1.35 M rows in the peak block), so the per-vote
//! and per-record state is kept small: decimals are [`PackedDec`] (16 bytes
//! instead of 32 for `Option<i128>`), the records and the `vote_other_managers`
//! items are flat vectors in proto order, and every string, including the
//! `cusip_norm` join key (§4.4, never an issue), is read from the proto during
//! the append.
//!
//! Issue order (= `parse_issues` row order): the report, the other managers
//! (cover page, then summary page), then each vote followed by its records.

use anyhow::Result;

use super::{idx, FilingCtx};
use crate::sec::issues::IssueSink;
use crate::sec::parse::Family;
use crate::sec::proto::sec;
use crate::sec::schema::{NPX_OTHER_MANAGERS, NPX_REPORTS, NPX_VOTES, NPX_VOTE_RECORDS};

/// The parsed and derived values of `npx_reports`, `npx_votes`, `npx_vote_records`, `npx_other_managers` for one filing.
#[derive(Debug, Default)]
pub(crate) struct PreparedNpx<'a> {
    pub report: PreparedNpxReport,
    /// `npx_other_managers`: cover-page managers, then summary-page managers
    /// (§8.2 concatenated positions).
    pub managers: Vec<PreparedNpxManager<'a>>,
    /// `npx_votes`, one per `votes[]` element, in order.
    pub votes: Vec<PreparedVote>,
    /// Every `votes[].vote_other_managers[]` item, vote after vote, in order;
    /// vote `i` owns the next `votes[i].vote_other_managers.len()` items.
    pub vote_other_managers: Vec<Option<i32>>,
    /// `npx_vote_records.shares_voted` of every `votes[].records[]` element,
    /// vote after vote, in order; vote `i` owns the next
    /// `votes[i].records.len()` values.
    pub record_shares_voted: Vec<PackedDec>,
    /// `npx_vote_records.has_parse_issues`, aligned with
    /// [`Self::record_shares_voted`].
    pub record_has_parse_issues: Vec<bool>,
}

/// The typed and derived values of the `npx_reports` row. Every cover-page
/// value is NULL when `cover_page` is absent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct PreparedNpxReport {
    pub report_calendar_year: Option<i32>,
    pub confidential_treatment: Option<bool>,
    pub explanatory_choice: Option<bool>,
    pub amendment_number: Option<i32>,
    pub other_included_managers_count: Option<i32>,
    /// `cover_page.series_count`, renamed (§3.0, critic C7).
    pub declared_series_count: Option<i32>,
    pub vote_count: u32,
    pub vote_record_count: u32,
    pub other_manager_count: u32,
    pub has_parse_issues: bool,
}

/// One `npx_other_managers` row.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PreparedNpxManager<'a> {
    pub manager: &'a sec::NpxManager,
    pub other_manager_index: u32,
    /// `cover` (`other_managers`) or `summary` (`summary_managers`).
    pub list_kind: &'static str,
    pub serial_number: Option<i32>,
    pub has_parse_issues: bool,
}

/// The typed values of one `npx_votes` row (48 bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparedVote {
    pub shares_voted: PackedDec,
    pub shares_on_loan: PackedDec,
    pub meeting_date: Option<i32>,
    pub record_count: u32,
    pub has_parse_issues: bool,
}

/// An S16 mantissa, or NULL, in 16 bytes: NULL is `i128::MIN`, which no parsed
/// mantissa can be (every one is below `10^38` in magnitude, §4.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PackedDec(i128);

impl PackedDec {
    const NULL: i128 = i128::MIN;

    pub(crate) fn new(mantissa: Option<i128>) -> Self {
        Self(mantissa.unwrap_or(Self::NULL))
    }

    pub(crate) fn get(self) -> Option<i128> {
        (self.0 != Self::NULL).then_some(self.0)
    }
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::NpxReport,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedNpx<'a>> {
    let _ = fc;
    let cover = body.cover_page.as_ref();

    // Structural invariants (§4.7 item 6): the counts bound every position.
    let vote_count = idx(body.votes.len())?;
    let (record_total, item_total) = body.votes.iter().fold((0, 0), |(records, items), vote| {
        (
            records + vote.records.len(),
            items + vote.vote_other_managers.len(),
        )
    });
    let vote_record_count = idx(record_total)?;
    let manager_total = cover.map_or(0, |c| c.other_managers.len() + c.summary_managers.len());
    let other_manager_count = idx(manager_total)?;

    // npx_reports (§3.36): typed columns in schema order.
    let mut row = issues.row(NPX_REPORTS, &[]);
    let mut report = PreparedNpxReport {
        vote_count,
        vote_record_count,
        other_manager_count,
        ..PreparedNpxReport::default()
    };
    if let Some(c) = cover {
        report.report_calendar_year = row.int("report_calendar_year", &c.report_calendar_year);
        report.confidential_treatment = row.yn("confidential_treatment", &c.confidential_treatment);
        report.explanatory_choice = row.yn("explanatory_choice", &c.explanatory_choice);
        report.amendment_number = row.int("amendment_number", &c.amendment_number);
        report.other_included_managers_count = row.int(
            "other_included_managers_count",
            &c.other_included_managers_count,
        );
        report.declared_series_count = row.int("declared_series_count", &c.series_count);
    }
    report.has_parse_issues = row.finish();

    // npx_other_managers (§3.39): cover page, then summary page.
    let mut managers = Vec::with_capacity(manager_total);
    if let Some(c) = cover {
        let listed = c
            .other_managers
            .iter()
            .map(|manager| ("cover", manager))
            .chain(
                c.summary_managers
                    .iter()
                    .map(|manager| ("summary", manager)),
            );
        for (position, (list_kind, manager)) in listed.enumerate() {
            let other_manager_index = idx(position)?;
            let mut row = issues.row(NPX_OTHER_MANAGERS, &[other_manager_index]);
            let serial_number = row.int("serial_number", &manager.serial_number);
            managers.push(PreparedNpxManager {
                manager,
                other_manager_index,
                list_kind,
                serial_number,
                has_parse_issues: row.finish(),
            });
        }
    }

    // npx_votes (§3.37), each followed by its npx_vote_records (§3.38).
    let mut votes = Vec::with_capacity(body.votes.len());
    let mut vote_other_managers = Vec::with_capacity(item_total);
    let mut record_shares_voted = Vec::with_capacity(record_total);
    let mut record_has_parse_issues = Vec::with_capacity(record_total);
    for (position, vote) in body.votes.iter().enumerate() {
        let vote_index = idx(position)?;
        let record_count = idx(vote.records.len())?;
        let mut row = issues.row(NPX_VOTES, &[vote_index]);
        let meeting_date = row.date("meeting_date", &vote.meeting_date);
        let shares_voted = row.decimal("shares_voted", &vote.shares_voted, Family::S16);
        let shares_on_loan = row.decimal("shares_on_loan", &vote.shares_on_loan, Family::S16);
        for (element, item) in vote.vote_other_managers.iter().enumerate() {
            let element = idx(element)?;
            vote_other_managers.push(row.int_item::<i32>("vote_other_managers", element, item));
        }
        votes.push(PreparedVote {
            shares_voted: PackedDec::new(shares_voted),
            shares_on_loan: PackedDec::new(shares_on_loan),
            meeting_date,
            record_count,
            has_parse_issues: row.finish(),
        });

        for (position, record) in vote.records.iter().enumerate() {
            let record_index = idx(position)?;
            let mut row = issues.row(NPX_VOTE_RECORDS, &[vote_index, record_index]);
            let shares_voted = row.decimal("shares_voted", &record.shares_voted, Family::S16);
            record_shares_voted.push(PackedDec::new(shares_voted));
            record_has_parse_issues.push(row.finish());
        }
    }

    Ok(PreparedNpx {
        report,
        managers,
        votes,
        vote_other_managers,
        record_shares_voted,
        record_has_parse_issues,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_decimals_keep_null_and_every_mantissa() {
        let limit = crate::sec::parse::DECIMAL_LIMIT;
        for mantissa in [
            None,
            Some(0),
            Some(1),
            Some(-1),
            Some(limit - 1),
            Some(1 - limit),
        ] {
            assert_eq!(PackedDec::new(mantissa).get(), mantissa);
        }
    }

    #[test]
    fn per_vote_state_stays_small() {
        assert_eq!(std::mem::size_of::<PackedDec>(), 16);
        assert!(std::mem::size_of::<PreparedVote>() <= 48);
    }
}
