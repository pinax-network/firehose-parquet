//! Preflight of `beneficial_reports`, `beneficial_reporting_persons` (§3.17, §3.18).
//! Owned by the `form13f-beneficial` group: see `/tmp/sec-fireparq/impl/contracts.md`.
//!
//! Fallible only on the structural invariants of §4.7 (positions through
//! `super::idx`); every typed value goes through `IssueSink::row`.
//!
//! Issue order (the reference's emit order): the report row, then the
//! reporting persons. The report's `max_*` columns are derived from the
//! persons' typed values and log no issue of their own.

use std::borrow::Cow;

use anyhow::Result;

use super::{idx, FilingCtx};
use crate::sec::issues::IssueSink;
use crate::sec::parse::{self, Family};
use crate::sec::proto::sec;
use crate::sec::schema::{BENEFICIAL_REPORTING_PERSONS, BENEFICIAL_REPORTS};

/// `schedule_kind` of a Schedule 13D (active intent).
pub(crate) const SCHEDULE_KIND_13D: &str = "13D";
/// `schedule_kind` of a Schedule 13G (passive).
pub(crate) const SCHEDULE_KIND_13G: &str = "13G";

/// The parsed and derived values of `beneficial_reports`, `beneficial_reporting_persons` for one filing.
#[derive(Debug)]
pub(crate) struct PreparedBeneficial<'a> {
    pub report: PreparedReport<'a>,
    /// One per `reporting_persons[]` element, in order.
    pub persons: Vec<PreparedPerson>,
}

/// The typed and derived values of the `beneficial_reports` row; the persons
/// copy `cusip_norm`, `schedule_kind` and `event_date`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PreparedReport<'a> {
    /// §4.4 `cusip_norm(cusip)`.
    pub cusip_norm: Option<Cow<'a, str>>,
    /// §4.4: `13D` / `13G` from `schedule_type`, else `form_type`.
    pub schedule_kind: Option<&'static str>,
    pub event_date: Option<i32>,
    pub amendment_number: Option<i32>,
    pub reporting_person_count: u32,
    /// Max over the persons' non-NULL typed `aggregate_amount_owned` (Q6).
    pub max_aggregate_amount_owned: Option<i128>,
    /// Max over the persons' non-NULL typed `percent_of_class` (R12).
    pub max_percent_of_class: Option<i128>,
    pub signature_count: u32,
    pub has_parse_issues: bool,
}

/// The typed values of one `beneficial_reporting_persons` row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PreparedPerson {
    pub person_index: u32,
    pub sole_voting_power: Option<i128>,
    pub shared_voting_power: Option<i128>,
    pub sole_dispositive_power: Option<i128>,
    pub shared_dispositive_power: Option<i128>,
    pub aggregate_amount_owned: Option<i128>,
    pub percent_of_class: Option<i128>,
    pub has_parse_issues: bool,
}

/// `schedule_kind` (§4.4): `13D` if `schedule_type` (else `form_type`, when
/// `schedule_type` is empty) contains `13D`, `13G` if it contains `13G`, else
/// NULL.
pub(crate) fn schedule_kind(schedule_type: &str, form_type: &str) -> Option<&'static str> {
    let text = if schedule_type.is_empty() {
        form_type
    } else {
        schedule_type
    };
    if text.contains("13D") {
        Some(SCHEDULE_KIND_13D)
    } else if text.contains("13G") {
        Some(SCHEDULE_KIND_13G)
    } else {
        None
    }
}

pub(crate) fn prepare<'a>(
    fc: &FilingCtx<'a>,
    body: &'a sec::BeneficialOwnershipReport,
    issues: &mut IssueSink<'a>,
) -> Result<PreparedBeneficial<'a>> {
    let reporting_person_count = idx(body.reporting_persons.len())?;
    let signature_count = idx(body.signatures.len())?;

    // The report row: parsed columns in schema order.
    let mut row = issues.row(BENEFICIAL_REPORTS, &[]);
    let event_date = row.date("event_date", &body.event_date);
    let amendment_number = row.int::<i32>("amendment_number", &body.amendment_number);
    let has_parse_issues = row.finish();

    // The persons, in cover-page order.
    let mut persons = Vec::with_capacity(body.reporting_persons.len());
    for (position, person) in body.reporting_persons.iter().enumerate() {
        let person_index = idx(position)?;
        let mut row = issues.row(BENEFICIAL_REPORTING_PERSONS, &[person_index]);
        let sole_voting_power =
            row.decimal("sole_voting_power", &person.sole_voting_power, Family::Q6);
        let shared_voting_power = row.decimal(
            "shared_voting_power",
            &person.shared_voting_power,
            Family::Q6,
        );
        let sole_dispositive_power = row.decimal(
            "sole_dispositive_power",
            &person.sole_dispositive_power,
            Family::Q6,
        );
        let shared_dispositive_power = row.decimal(
            "shared_dispositive_power",
            &person.shared_dispositive_power,
            Family::Q6,
        );
        let aggregate_amount_owned = row.decimal(
            "aggregate_amount_owned",
            &person.aggregate_amount_owned,
            Family::Q6,
        );
        let percent_of_class =
            row.decimal("percent_of_class", &person.percent_of_class, Family::R12);
        persons.push(PreparedPerson {
            person_index,
            sole_voting_power,
            shared_voting_power,
            sole_dispositive_power,
            shared_dispositive_power,
            aggregate_amount_owned,
            percent_of_class,
            has_parse_issues: row.finish(),
        });
    }

    let report = PreparedReport {
        cusip_norm: parse::cusip_norm(&body.cusip),
        schedule_kind: schedule_kind(&body.schedule_type, fc.form_type),
        event_date,
        amendment_number,
        reporting_person_count,
        max_aggregate_amount_owned: persons
            .iter()
            .filter_map(|p| p.aggregate_amount_owned)
            .max(),
        max_percent_of_class: persons.iter().filter_map(|p| p.percent_of_class).max(),
        signature_count,
        has_parse_issues,
    };
    Ok(PreparedBeneficial { report, persons })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_kind_reads_schedule_type_then_form_type() {
        assert_eq!(schedule_kind("SCHEDULE 13D/A", "SC 13G"), Some("13D"));
        assert_eq!(schedule_kind("SC 13G/A", "SCHEDULE 13D"), Some("13G"));
        assert_eq!(schedule_kind("", "SC 13D"), Some("13D"));
        assert_eq!(schedule_kind("", "SCHEDULE 13G/A"), Some("13G"));
        // A non-empty schedule_type is not overridden by form_type.
        assert_eq!(schedule_kind("OTHER", "SC 13D"), None);
        assert_eq!(schedule_kind("", "4"), None);
    }
}
