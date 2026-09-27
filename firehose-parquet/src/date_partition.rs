//! The `date=YYYY-MM-DD` partition key: the one place it is formatted and
//! parsed (#652).
//!
//! Every table is written as `<table>/date=YYYY-MM-DD/part-*.parquet`. The
//! key has the type and the value of every table's `date` column (`Date32`):
//! both come from the same checked whole-second block time, so a
//! Hive-partition-aware reader (DuckDB, Polars) sees one consistent `date`.
//! Parsing is strict, so a directory of an older layout (`year=YYYY/`,
//! `month=MM/`, `day=DD/`, or the v0.x day-of-month `date=DD/`) is never read
//! as a date.

use anyhow::{bail, Result};
use std::fmt;

/// Directory key of a date partition, and name of the `Date32` data column
/// it matches.
pub const DATE_KEY: &str = "date";

/// One UTC day: the partition of every row whose block time falls in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DatePartition(time::Date);

impl DatePartition {
    /// The partition that holds the whole-second unix time `seconds`, which is
    /// validated against the calendar range first
    /// ([`checked_timestamp`](crate::traits::checked_timestamp)).
    pub fn from_timestamp(seconds: i64) -> Result<Self> {
        let date = crate::traits::checked_timestamp(seconds)?.date();
        if date.year() < 0 {
            bail!("unix timestamp {seconds} is before year 0 and has no date partition");
        }
        Ok(Self(date))
    }

    /// The UTC day.
    pub fn date(&self) -> time::Date {
        self.0
    }

    /// The Arrow `Date32` value (days since 1970-01-01). It equals the `date`
    /// column of every row in the partition.
    pub fn date32(&self) -> i32 {
        const UNIX_EPOCH_JULIAN_DAY: i32 = 2_440_588;
        self.0.to_julian_day() - UNIX_EPOCH_JULIAN_DAY
    }

    /// The partition directory name, `date=YYYY-MM-DD`.
    pub fn path(&self) -> String {
        format!(
            "{DATE_KEY}={:04}-{:02}-{:02}",
            self.0.year(),
            u8::from(self.0.month()),
            self.0.day()
        )
    }

    /// Parses a directory name written by [`Self::path`]. Anything else is an
    /// error, including the directories of older layouts.
    pub fn parse(directory: &str) -> Result<Self> {
        match directory
            .strip_prefix(DATE_KEY)
            .and_then(|rest| rest.strip_prefix('='))
            .and_then(parse_date_value)
        {
            Some(date) => Ok(Self(date)),
            None => bail!("`{directory}` is not a `date=YYYY-MM-DD` partition directory"),
        }
    }
}

impl fmt::Display for DatePartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.path())
    }
}

/// Parses a strict `YYYY-MM-DD` value.
fn parse_date_value(value: &str) -> Option<time::Date> {
    let bytes = value.as_bytes();
    let shaped = bytes.len() == 10
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            4 | 7 => *byte == b'-',
            _ => byte.is_ascii_digit(),
        });
    if !shaped {
        return None;
    }
    let year: i32 = value[..4].parse().ok()?;
    let month: u8 = value[5..7].parse().ok()?;
    let day: u8 = value[8..].parse().ok()?;
    time::Date::from_calendar_date(year, time::Month::try_from(month).ok()?, day).ok()
}

/// Whether `pattern`, a `date=` filter value with at most one `*`, can match a
/// `YYYY-MM-DD` value: a date, or a glob whose literal parts fit that shape and
/// whose literal prefix, if any, spells out the whole year (`2026-01-*`,
/// `*-15`). A day of the month such as `15` is refused rather than silently
/// matching nothing.
pub fn is_date_value_pattern(pattern: &str) -> bool {
    const SHAPE: &[u8; 10] = b"DDDD-DD-DD";
    let fits = |text: &str, offset: usize| {
        text.bytes()
            .enumerate()
            .all(|(index, byte)| match SHAPE.get(offset + index) {
                Some(b'D') => byte.is_ascii_digit(),
                Some(shape) => byte == *shape,
                None => false,
            })
    };
    match pattern.split_once('*') {
        None => parse_date_value(pattern).is_some(),
        Some((prefix, suffix)) => {
            (prefix.is_empty() || prefix.len() >= 4)
                && prefix.len() + suffix.len() <= SHAPE.len()
                && fits(prefix, 0)
                && fits(suffix, SHAPE.len() - suffix.len())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_and_parses_the_utc_day() {
        for (seconds, expected) in [
            (1_705_329_045, "date=2024-01-15"), // 2024-01-15T14:30:45Z
            (1_705_363_199, "date=2024-01-15"), // 23:59:59
            (1_705_363_200, "date=2024-01-16"), // the next midnight
            (0, "date=1970-01-01"),
            (-1, "date=1969-12-31"),
            (-62_135_596_800, "date=0001-01-01"),
            (253_402_300_799, "date=9999-12-31"),
        ] {
            let partition = DatePartition::from_timestamp(seconds).unwrap();
            assert_eq!(partition.path(), expected, "{seconds}");
            assert_eq!(partition.to_string(), expected);
            assert_eq!(DatePartition::parse(expected).unwrap(), partition);
            // The key equals the canonical `date` column of the same time.
            assert_eq!(
                partition.date32(),
                crate::traits::date32_from_timestamp_seconds(seconds).unwrap(),
                "{seconds}"
            );
        }
        for seconds in [i64::MIN, i64::MAX, 253_402_300_800, -62_167_219_201] {
            assert!(DatePartition::from_timestamp(seconds).is_err(), "{seconds}");
        }
    }

    /// Only `date=YYYY-MM-DD` parses: never a v0.x day of the month (`date=25`)
    /// or a key of the pre-release `year=/month=/day=` layout.
    #[test]
    fn parsing_is_strict() {
        for invalid in [
            "date=25",
            "date=2024-1-15",
            "date=2024-02-30",
            "date=2024-13-01",
            "date=2024-01-15/hour=14",
            "date=",
            "date",
            "day=15",
            "year=2024",
            "block_range=0-100",
            "hour=14",
            "",
        ] {
            let error = DatePartition::parse(invalid).unwrap_err().to_string();
            assert!(error.contains("date=YYYY-MM-DD"), "{invalid}: {error}");
        }
    }

    #[test]
    fn date_filter_patterns_must_fit_the_date_shape() {
        for valid in [
            "2024-01-15",
            "2024-01-*",
            "2024-*",
            "2024*",
            "*-15",
            "*",
            "*01-15",
        ] {
            assert!(is_date_value_pattern(valid), "{valid}");
        }
        for invalid in [
            "15",
            "1",
            "2024-1-15",
            "2024-02-30",
            "*x",
            "2024-01-15-*",
            "15*",
            "2*",
        ] {
            assert!(!is_date_value_pattern(invalid), "{invalid}");
        }
    }
}
