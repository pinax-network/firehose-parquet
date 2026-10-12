//! Arguments of `fireparq hypercore-reference`, the HyperLiquid `/info`
//! reference tables job (docs/hypercore-reference.md).
//!
//! Only the command line lives here, because `Commands` does: the job is the
//! `fireparq-reference` crate (`reference/`), which converts these arguments
//! into its own settings. Options come before the job and job options after
//! it, as in `fireparq hypercore-reference --root s3://hypercore-ref outcomes
//! --max-probes 60`; none is global. Range checks the job reports as a
//! `config_error` line (the weight floor, the time budget, `--tables` names,
//! a missing root) are made by the job, so that `kubectl logs | tail -1`
//! shows them; a negative weight or budget is one of them. A negative
//! `--max-probes` or `--reprobe-after-minutes` is a usage error.
//!
//! `--info-url` and the AWS credentials are never shown: their environment
//! values are hidden from `--help`, `Debug` leaves them out, and the URL is a
//! plain `String`, so no clap error can echo it.

use clap::{Args, Subcommand};
use time::{Date, Month, PrimitiveDateTime, Time, UtcOffset};

use super::AwsArgs;

/// `fireparq hypercore-reference [OPTIONS] <JOB> [JOB OPTIONS]`.
#[derive(Args, Clone)]
#[command(
    subcommand_value_name = "JOB",
    subcommand_help_heading = "Jobs",
    disable_help_subcommand = true
)]
pub struct HypercoreReferenceArgs {
    /// s3://<bucket>[/<prefix>] or a local directory (required)
    #[arg(
        long,
        env = "HLREF_ROOT",
        hide_env_values = true,
        help_heading = "Reference tables"
    )]
    pub root: Option<String>,

    /// The /info endpoint (default https://api.hyperliquid.xyz/info). It may
    /// embed an API key: only its scheme and host are ever shown
    #[arg(
        long,
        env = "HL_INFO_URL",
        value_name = "URL",
        hide_env_values = true,
        help_heading = "Reference tables"
    )]
    pub info_url: Option<String>,

    /// This process's /info weight per minute (HyperLiquid allows 1200 per IP); at least 20
    #[arg(
        long,
        env = "HLREF_WEIGHT_PER_MINUTE",
        default_value_t = 400,
        allow_negative_numbers = true,
        hide_env_values = true,
        help_heading = "Reference tables"
    )]
    pub weight_per_minute: i64,

    /// Stop starting requests after this many seconds, commit, and exit 0
    #[arg(
        long,
        env = "HLREF_TIME_BUDGET_SECONDS",
        default_value_t = 240.0,
        allow_negative_numbers = true,
        hide_env_values = true,
        help_heading = "Reference tables"
    )]
    pub time_budget_seconds: f64,

    /// Skip the compaction and VACUUM that follow a run's commits
    #[arg(long, help_heading = "Reference tables")]
    pub no_maintenance: bool,

    #[command(flatten)]
    pub aws: AwsArgs,

    #[command(subcommand)]
    pub job: ReferenceJob,
}

impl std::fmt::Debug for HypercoreReferenceArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The /info URL may hold a key, and `aws` holds the credentials.
        f.debug_struct("HypercoreReferenceArgs")
            .field("root", &self.root)
            .field("weight_per_minute", &self.weight_per_minute)
            .field("time_budget_seconds", &self.time_budget_seconds)
            .field("no_maintenance", &self.no_maintenance)
            .field("job", &self.job)
            .finish_non_exhaustive()
    }
}

/// The job to run.
#[derive(Subcommand, Debug, Clone)]
pub enum ReferenceJob {
    /// outcomeMeta, then the settledOutcome sweep (hl_outcome_meta, hl_outcomes, hl_questions)
    Outcomes(OutcomesArgs),
    /// spotMeta, perpDexs, allPerpMetas and tokenDetails (hl_spot_*, hl_perp_*)
    Meta(MetaArgs),
    /// fundingHistory of every perp coin (hl_funding_history)
    Funding(FundingArgs),
    /// OPTIMIZE and VACUUM now (suspend the CronJobs first)
    Maintain(MaintainArgs),
    /// Check the tables' invariants (read-only)
    Verify,
}

/// `outcomes` options.
#[derive(Args, Debug, Clone)]
pub struct OutcomesArgs {
    /// settledOutcome probes of the sweep per run (named ids are all probed)
    #[arg(
        long,
        env = "HLREF_MAX_PROBES",
        default_value_t = 80,
        hide_env_values = true
    )]
    pub max_probes: u32,

    /// Cool-down before an id that was not settled is probed again
    #[arg(long, default_value_t = 60)]
    pub reprobe_after_minutes: u32,

    /// Lowest outcome id the sweep probes
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(i64).range(0..))]
    pub sweep_from: i64,

    /// Highest outcome id the sweep probes (a bounded slice)
    #[arg(long, value_parser = clap::value_parser!(i64).range(0..))]
    pub sweep_to: Option<i64>,

    /// Probe exactly these comma-separated ids, however many (a repair); no sweep
    #[arg(long, value_name = "IDS", value_parser = id_list)]
    pub outcome_ids: Option<IdList>,

    /// Record entities /info dropped even beyond the bound (the larger of 25
    /// and 20% of a table's listed entities); without it such a run writes no
    /// removal row and fails
    #[arg(long)]
    pub allow_mass_removal: bool,
}

/// `meta` options.
#[derive(Args, Debug, Clone)]
pub struct MetaArgs {
    /// Record entities /info dropped even beyond the bound (the larger of 25
    /// and 20% of a table's listed entities); without it such a run writes no
    /// removal row and fails
    #[arg(long)]
    pub allow_mass_removal: bool,
}

/// `funding` options.
#[derive(Args, Debug, Clone)]
pub struct FundingArgs {
    /// First funding time for a coin with none: YYYY-MM-DD, or a date and
    /// time (UTC unless it has an offset)
    #[arg(long, default_value = "2026-01-01", value_name = "TIME", value_parser = utc_date)]
    pub funding_start: UtcMicros,

    /// Repair: re-read [--funding-start, end) and append the hours missing at
    /// or before each coin's latest stored hour
    #[arg(long, value_name = "TIME", value_parser = utc_date)]
    pub funding_end: Option<UtcMicros>,

    /// Only these comma-separated coins, for example BTC,xyz:XYZ100
    #[arg(long, value_parser = name_list)]
    pub coins: Option<NameList>,

    /// Funding rows per commit
    #[arg(long, default_value_t = 50_000, value_parser = clap::value_parser!(u32).range(1..))]
    pub commit_rows: u32,
}

/// `maintain` options.
#[derive(Args, Debug, Clone)]
pub struct MaintainArgs {
    /// Only these comma-separated tables, for example hl_outcomes,_hlref/outcome_probes
    #[arg(long, value_parser = name_list)]
    pub tables: Option<NameList>,
}

/// Comma-separated ids (`--outcome-ids`); one value, not a repeated flag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdList(pub Vec<i64>);

/// Comma-separated names (`--coins`, `--tables`), trimmed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NameList(pub Vec<String>);

/// A UTC time in microseconds since the Unix epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UtcMicros(pub i64);

/// `"1, 2,3"`: the items that are not blank, each a signed integer. An empty
/// list is refused: it would silently mean the sweep.
pub fn id_list(text: &str) -> Result<IdList, String> {
    let ids = text
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(|item| {
            item.parse::<i64>()
                .map_err(|_| format!("`{item}` is not an outcome id"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if ids.is_empty() {
        return Err("expected at least one comma-separated id".into());
    }
    Ok(IdList(ids))
}

/// `"BTC, xyz:XYZ100"`: the trimmed items that are not blank. An empty list
/// is refused: it would silently mean all of them.
pub fn name_list(text: &str) -> Result<NameList, String> {
    let names: Vec<String> = text
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect();
    if names.is_empty() {
        return Err("expected at least one comma-separated name".into());
    }
    Ok(NameList(names))
}

const UTC_DATE_FORMS: &str =
    "expected YYYY-MM-DD, or YYYY-MM-DD then T or a space, HH[:MM[:SS[.fraction]]] and an \
     optional Z, ±HH, ±HHMM or ±HH:MM";

/// `YYYY-MM-DD` (00:00 UTC), or a date, `T` or a space, `HH[:MM[:SS[.f]]]`
/// and an optional `Z`, `±HH`, `±HHMM` or `±HH:MM` offset; without one the
/// time is UTC. Digits beyond microseconds are truncated. The ISO 8601 basic
/// and week-date forms are refused.
pub fn utc_date(text: &str) -> Result<UtcMicros, String> {
    parse_utc(text.trim()).ok_or_else(|| UTC_DATE_FORMS.to_owned())
}

fn parse_utc(text: &str) -> Option<UtcMicros> {
    let bytes = text.as_bytes();
    let date = date(bytes.get(..10)?)?;
    let rest = &bytes[10..];
    let (time, offset) = match rest.split_first() {
        None => (Time::MIDNIGHT, UtcOffset::UTC),
        Some((b'T' | b' ', rest)) => time_and_offset(rest)?,
        Some(_) => return None,
    };
    let micros = PrimitiveDateTime::new(date, time)
        .assume_offset(offset)
        .unix_timestamp_nanos()
        / 1000;
    i64::try_from(micros).ok().map(UtcMicros)
}

/// The value of `digits` ASCII digits, exactly.
fn number(digits: &[u8]) -> Option<u32> {
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(digits).ok()?.parse().ok()
}

/// `YYYY-MM-DD`, years 1 to 9999.
fn date(bytes: &[u8]) -> Option<Date> {
    if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    let year = i32::try_from(number(&bytes[..4])?).ok()?;
    let month = Month::try_from(u8::try_from(number(&bytes[5..7])?).ok()?).ok()?;
    let day = u8::try_from(number(&bytes[8..10])?).ok()?;
    if year == 0 {
        return None;
    }
    Date::from_calendar_date(year, month, day).ok()
}

/// `HH[:MM[:SS[.f]]]` then the offset, if any.
fn time_and_offset(bytes: &[u8]) -> Option<(Time, UtcOffset)> {
    let end = bytes
        .iter()
        .position(|byte| matches!(byte, b'Z' | b'+' | b'-'))
        .unwrap_or(bytes.len());
    let (clock, offset) = bytes.split_at(end);
    let (clock, fraction) = match clock.iter().position(|byte| *byte == b'.') {
        Some(dot) => (&clock[..dot], Some(&clock[dot + 1..])),
        None => (clock, None),
    };
    let fields: Vec<&[u8]> = clock.split(|byte| *byte == b':').collect();
    if fields.len() > 3 || fields.iter().any(|field| field.len() != 2) {
        return None;
    }
    if fraction.is_some() && fields.len() != 3 {
        return None;
    }
    let field = |index: usize| fields.get(index).map_or(Some(0), |digits| number(digits));
    let (hour, minute, second) = (field(0)?, field(1)?, field(2)?);
    let nanos = match fraction {
        None => 0,
        Some(digits) if (1..=9).contains(&digits.len()) => {
            // Nanoseconds, then truncated to microseconds.
            number(digits)? * 10u32.pow(9 - digits.len() as u32) / 1000 * 1000
        }
        Some(_) => return None,
    };
    let time = Time::from_hms_nano(
        u8::try_from(hour).ok()?,
        u8::try_from(minute).ok()?,
        u8::try_from(second).ok()?,
        nanos,
    )
    .ok()?;
    Some((time, offset_of(offset)?))
}

/// Nothing (UTC), `Z`, `±HH`, `±HHMM` or `±HH:MM`, below 24 hours.
fn offset_of(bytes: &[u8]) -> Option<UtcOffset> {
    let (sign, digits) = match bytes {
        [] | [b'Z'] => return Some(UtcOffset::UTC),
        [b'+', digits @ ..] => (1, digits),
        [b'-', digits @ ..] => (-1, digits),
        _ => return None,
    };
    let (hours, minutes) = match digits {
        [h1, h2] => (number(&[*h1, *h2])?, 0),
        [h1, h2, m1, m2] | [h1, h2, b':', m1, m2] => (number(&[*h1, *h2])?, number(&[*m1, *m2])?),
        _ => return None,
    };
    if hours > 23 || minutes > 59 {
        return None;
    }
    let hours = sign * i8::try_from(hours).ok()?;
    let minutes = sign * i8::try_from(minutes).ok()?;
    UtcOffset::from_hms(hours, minutes, 0).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-03-01T00:00:00Z.
    const MARCH_1: i64 = 1_772_323_200_000_000;
    const HOUR: i64 = 3_600_000_000;

    #[test]
    fn funding_dates_accept_the_documented_forms() {
        for (text, micros) in [
            ("2026-03-01", MARCH_1),
            (" 2026-03-01\n", MARCH_1),
            ("2026-01-01", 1_767_225_600_000_000),
            ("2026-03-01T05", MARCH_1 + 5 * HOUR),
            ("2026-03-01 05:00", MARCH_1 + 5 * HOUR),
            ("2026-03-01T05:00Z", MARCH_1 + 5 * HOUR),
            (
                "2026-03-01T05:00:00.123456789Z",
                MARCH_1 + 5 * HOUR + 123_456,
            ),
            ("2026-03-01T05:00:07.5", MARCH_1 + 5 * HOUR + 7_500_000),
            ("2026-03-01T05:30+05:30", MARCH_1),
            ("2026-03-01T05:30+0530", MARCH_1),
            ("2026-03-01T00:00-05", MARCH_1 + 5 * HOUR),
            ("2026-03-01T05:00:00+00:00", MARCH_1 + 5 * HOUR),
            ("2024-02-29", 1_709_164_800_000_000),
            ("0001-01-01", -62_135_596_800_000_000),
        ] {
            assert_eq!(utc_date(text), Ok(UtcMicros(micros)), "{text}");
        }
        for text in [
            "",
            "20260301T0500",
            "20260301",
            "2026-W09-7",
            "2026-03-01T05:00+05:30:00",
            "2026-03-01T5:00",
            "2026-03-01T05:0",
            "2026-03-01T05.5",
            "2026-03-01T05:00.5",
            "2026-03-01T05:00:00.",
            "2026-03-01T05:00:00.1234567890",
            "2026-03-01T24:00",
            "2026-03-01T05:00:60",
            "2026-03-01T05:00+24",
            "2026-03-01T05:00+05:60",
            "2026-03-01T05:00z",
            "2026-03-01t05:00",
            "2026-02-30",
            "2025-02-29",
            "0000-01-01",
            "2026-3-01",
            "2026-03-01X05",
            "2026-03-01T",
            "2026-03-01T05:00Z+01",
            "٢٠٢٦-03-01",
        ] {
            assert_eq!(utc_date(text), Err(UTC_DATE_FORMS.to_owned()), "{text:?}");
        }
    }

    #[test]
    fn lists_drop_blank_items_and_refuse_an_empty_one() {
        assert_eq!(id_list("1, 2,3"), Ok(IdList(vec![1, 2, 3])));
        assert_eq!(id_list(" 7768 ,, "), Ok(IdList(vec![7768])));
        assert_eq!(id_list("+5,-3"), Ok(IdList(vec![5, -3])));
        assert!(id_list("").is_err());
        assert!(id_list(" , ").is_err());
        assert_eq!(id_list("1,x"), Err("`x` is not an outcome id".into()));
        assert_eq!(
            name_list(" BTC, xyz:XYZ100 ,,"),
            Ok(NameList(vec!["BTC".into(), "xyz:XYZ100".into()]))
        );
        assert!(name_list("").is_err());
        assert!(name_list(" ,").is_err());
    }
}
